"""Regression coverage for direct Telegram downloads and shared-file requests."""
from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import app as app_module


class FakeStreamClient:
    def __init__(self, content: bytes):
        self.content = content
        self.calls: list[tuple[object, int]] = []
        self.active = 0
        self.max_active = 0
        self.started: dict[object, asyncio.Event] = {}
        self.release: dict[object, asyncio.Event] = {}
        self.closed: dict[object, asyncio.Event] = {}

    def is_connected(self) -> bool:
        return True

    async def get_messages(self, *_args, **_kwargs):
        return SimpleNamespace(media="shared-media")

    def iter_download(self, media, *, offset, chunk_size, **_kwargs):
        self.calls.append((media, offset))

        async def generate():
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                started = self.started.get(media)
                if started is not None:
                    started.set()
                release = self.release.get(media)
                if release is not None:
                    await release.wait()
                else:
                    await asyncio.sleep(0.005)
                yield self.content[offset : offset + chunk_size]
            finally:
                self.active -= 1
                closed = self.closed.get(media)
                if closed is not None:
                    closed.set()

        class ManagedIterator:
            def __init__(self):
                self.generator = generate()

            def __aiter__(self):
                return self

            async def __anext__(self):
                return await self.generator.__anext__()

            async def close(self):
                await self.generator.aclose()

        return ManagedIterator()


def test_telethon_routine_flood_wait_info_is_suppressed():
    logger = logging.getLogger("telethon.client.users")
    assert logger.getEffectiveLevel() >= logging.WARNING


@pytest.mark.asyncio
async def test_two_uncached_full_downloads_share_global_request_limit(
    client, stored_file, monkeypatch
):
    chunk_size = app_module.STREAM_CHUNK_SIZE
    content = bytes(range(256)) * (chunk_size * 2 // 256)
    fake = FakeStreamClient(content)
    monkeypatch.setattr(app_module, "STREAM_CLIENT", fake)
    monkeypatch.setattr(
        app_module,
        "telegram_media_for_record",
        AsyncMock(return_value="shared-media"),
    )

    async def download():
        response = await client.get(f"/download/{stored_file}")
        assert response.status == 200
        assert await response.read() == content

    await asyncio.gather(download(), download())

    assert sorted(offset for _, offset in fake.calls) == [0, 0, chunk_size, chunk_size]
    assert 1 < fake.max_active <= app_module.STREAM_REQUEST_CONCURRENCY


@pytest.mark.asyncio
async def test_cancelling_one_telegram_request_does_not_cancel_another(monkeypatch):
    fake = FakeStreamClient(b"x" * app_module.STREAM_CHUNK_SIZE)
    fake.started["cancelled-client"] = asyncio.Event()
    fake.started["surviving-client"] = asyncio.Event()
    fake.release["cancelled-client"] = asyncio.Event()
    fake.release["surviving-client"] = asyncio.Event()
    fake.closed["cancelled-client"] = asyncio.Event()
    monkeypatch.setattr(app_module, "STREAM_CLIENT", fake)

    cancelled = asyncio.create_task(
        app_module.telegram_file_chunk(
            "cancelled-client", 0, app_module.STREAM_CHUNK_SIZE
        )
    )
    surviving = asyncio.create_task(
        app_module.telegram_file_chunk(
            "surviving-client", 0, app_module.STREAM_CHUNK_SIZE
        )
    )
    await asyncio.gather(
        fake.started["cancelled-client"].wait(),
        fake.started["surviving-client"].wait(),
    )

    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    await asyncio.wait_for(fake.closed["cancelled-client"].wait(), timeout=1)

    fake.release["surviving-client"].set()
    assert await surviving == b"x" * app_module.STREAM_CHUNK_SIZE
    assert fake.active == 0


@pytest.mark.asyncio
async def test_closing_one_http_download_leaves_second_download_intact(
    client, stored_file, db, monkeypatch
):
    chunk_size = app_module.STREAM_CHUNK_SIZE
    file_size = chunk_size * 8
    content = bytes(range(256)) * (file_size // 256)
    fake = FakeStreamClient(content)
    monkeypatch.setattr(app_module, "STREAM_CLIENT", fake)
    monkeypatch.setattr(app_module, "DOWNLOAD_CONCURRENCY", 1)
    monkeypatch.setattr(app_module, "FILE_DOWNLOAD_CONCURRENCY", 1)
    media_ids = iter(("http-client-a", "http-client-b"))
    abandoned_handler_finished = asyncio.Event()
    original_download = app_module._download

    async def track_abandoned_handler(request):
        try:
            return await original_download(request)
        finally:
            if request.match_info["token"] == stored_file:
                abandoned_handler_finished.set()

    monkeypatch.setattr(app_module, "_download", track_abandoned_handler)

    async def media_for_request(_record):
        return next(media_ids)

    monkeypatch.setattr(app_module, "telegram_media_for_record", media_for_request)
    await db.execute(
        "UPDATE files SET size_bytes = ? WHERE token = ?",
        (file_size, stored_file),
    )
    await db.commit()

    abandoned = await client.get(f"/download/{stored_file}")
    assert abandoned.status == 200
    abandoned.close()
    await asyncio.wait_for(abandoned_handler_finished.wait(), timeout=1)
    assert not app_module.DOWNLOAD_ACTIVE
    assert not app_module.FILE_DOWNLOAD_ACTIVE

    survivor = await client.get(f"/download/{stored_file}")
    assert survivor.status == 200
    assert await survivor.read() == content
    abandoned_offsets = [offset for media, offset in fake.calls if media == "http-client-a"]
    survivor_offsets = [offset for media, offset in fake.calls if media == "http-client-b"]
    assert len(survivor_offsets) == 8
    assert len(abandoned_offsets) < 8


@pytest.mark.asyncio
async def test_telegram_range_requests_can_resume_at_independent_offsets(
    client, stored_file, monkeypatch
):
    chunk_size = app_module.STREAM_CHUNK_SIZE
    content = bytes(range(256)) * (chunk_size * 2 // 256)
    fake = FakeStreamClient(content)
    monkeypatch.setattr(app_module, "STREAM_CLIENT", fake)
    monkeypatch.setattr(
        app_module,
        "telegram_media_for_record",
        AsyncMock(return_value="shared-media"),
    )

    for start, end in ((17, 113), (chunk_size + 71, chunk_size + 199)):
        response = await client.get(
            f"/download/{stored_file}",
            headers={"Range": f"bytes={start}-{end}"},
        )
        assert response.status == 206
        assert response.headers["Content-Range"] == (
            f"bytes {start}-{end}/{len(content)}"
        )
        assert await response.read() == content[start : end + 1]

    assert fake.calls == [("shared-media", 0), ("shared-media", chunk_size)]


@pytest.mark.asyncio
async def test_completed_cache_hits_avoid_telegram_requests(
    client, cached_download, monkeypatch
):
    telegram_media = AsyncMock(side_effect=AssertionError("cache miss"))
    monkeypatch.setattr(app_module, "telegram_media_for_record", telegram_media)

    async def cached_range(start: int, end: int):
        response = await client.get(
            f"/download/{cached_download['token']}",
            headers={"Range": f"bytes={start}-{end}"},
        )
        assert response.status == 206
        assert await response.read() == cached_download["content"][start : end + 1]

    await asyncio.gather(cached_range(0, 31), cached_range(32, 95))
    telegram_media.assert_not_awaited()


@pytest.mark.asyncio
async def test_flood_wait_longer_than_30_seconds_is_not_capped(monkeypatch):
    class FloodWaitError(Exception):
        seconds = 45

    attempts: dict[int, int] = {}
    sleeps: list[float] = []

    async def fake_chunk(_media, offset, _file_size):
        attempts[offset] = attempts.get(offset, 0) + 1
        if offset == 1 and attempts[offset] == 1:
            raise FloodWaitError("wait")
        return f"chunk-{offset}".encode()

    async def record_sleep(seconds: float):
        sleeps.append(seconds)

    monkeypatch.setattr(app_module, "telegram_file_chunk", fake_chunk)
    monkeypatch.setattr(app_module.asyncio, "sleep", record_sleep)
    monkeypatch.setattr(app_module, "MAX_RETRIES", 1)

    chunks = await app_module._fetch_batch_with_retry(
        ["media"], [0, 1], 2, {"file_id": "file"}
    )

    assert chunks == [b"chunk-0", b"chunk-1"]
    assert attempts == {0: 1, 1: 2}
    assert sleeps == [45]
