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


def test_concurrency_configuration_is_positive_and_bounded(monkeypatch):
    monkeypatch.setenv("TEST_CONCURRENCY", "100")
    assert app_module.bounded_int_env("TEST_CONCURRENCY", 3, 8) == 8
    monkeypatch.setenv("TEST_CONCURRENCY", "0")
    with pytest.raises(RuntimeError, match="positive integer"):
        app_module.bounded_int_env("TEST_CONCURRENCY", 3, 8)


def test_resource_semaphores_are_scoped_to_each_event_loop():
    async def get_resources():
        return (
            app_module.download_semaphore(),
            app_module.stream_semaphore(),
            app_module.stream_request_semaphore(),
            app_module.cache_semaphore(),
        )

    first = asyncio.run(get_resources())
    second = asyncio.run(get_resources())
    assert all(left is not right for left, right in zip(first, second))


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
    request_semaphore = asyncio.Semaphore(2)
    monkeypatch.setattr(app_module, "STREAM_CLIENT", fake)
    monkeypatch.setitem(
        app_module.STREAM_REQUEST_SEMAPHORES,
        asyncio.get_running_loop(),
        request_semaphore,
    )

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
    assert request_semaphore._value == 2


@pytest.mark.asyncio
async def test_chunk_cleanup_never_disconnects_shared_telegram_client(monkeypatch):
    class Client(FakeStreamClient):
        def __init__(self):
            super().__init__(b"x" * app_module.STREAM_CHUNK_SIZE)
            self.disconnect_calls = 0

        async def disconnect(self):
            self.disconnect_calls += 1

    fake = Client()
    monkeypatch.setattr(app_module, "STREAM_CLIENT", fake)

    result = await app_module.telegram_file_chunk(
        "media", 0, app_module.STREAM_CHUNK_SIZE
    )

    assert result == b"x" * app_module.STREAM_CHUNK_SIZE
    assert fake.disconnect_calls == 0


@pytest.mark.asyncio
async def test_cancelled_browser_does_not_cancel_shared_cache_materialization(
    monkeypatch, tmp_path
):
    started = asyncio.Event()
    release = asyncio.Event()
    cache_path = tmp_path / "shared.bin"

    async def materialize(_file_id):
        started.set()
        await release.wait()
        return cache_path

    monkeypatch.setattr(app_module, "materialize_file", materialize)
    app_module.FILE_TASKS.clear()
    record = {"file_id": "shared-file", "cached_path": None}
    abandoned = asyncio.create_task(app_module.cached_file(record))
    survivor = asyncio.create_task(app_module.cached_file(record))
    await started.wait()
    shared_task = app_module.FILE_TASKS["shared-file"]

    abandoned.cancel()
    with pytest.raises(asyncio.CancelledError):
        await abandoned
    assert not shared_task.cancelled()
    assert not survivor.done()

    release.set()
    assert await survivor == cache_path


@pytest.mark.asyncio
async def test_borrowed_sender_disconnect_waits_until_all_downloads_return_it(monkeypatch):
    class SharedSenderClient:
        def __init__(self):
            self.borrow_count = 0
            self.sender_disconnects = 0
            self.started = {"one": asyncio.Event(), "two": asyncio.Event()}
            self.release = {"one": asyncio.Event(), "two": asyncio.Event()}

        def is_connected(self):
            return True

        def iter_download(self, media, *, offset, chunk_size, **_kwargs):
            self.borrow_count += 1

            async def generate():
                self.started[media].set()
                await self.release[media].wait()
                yield b"x" * chunk_size

            client = self

            class Iterator:
                def __init__(self):
                    self.generator = generate()
                    self.returned = False

                def __aiter__(self):
                    return self

                async def __anext__(self):
                    return await self.generator.__anext__()

                async def close(self):
                    if not self.returned:
                        self.returned = True
                        client.borrow_count -= 1
                        if client.borrow_count == 0:
                            client.sender_disconnects += 1
                    await self.generator.aclose()

            return Iterator()

    fake = SharedSenderClient()
    monkeypatch.setattr(app_module, "STREAM_CLIENT", fake)
    first = asyncio.create_task(
        app_module.telegram_file_chunk("one", 0, app_module.STREAM_CHUNK_SIZE)
    )
    second = asyncio.create_task(
        app_module.telegram_file_chunk("two", 0, app_module.STREAM_CHUNK_SIZE)
    )
    await asyncio.gather(fake.started["one"].wait(), fake.started["two"].wait())

    fake.release["one"].set()
    await first
    assert fake.borrow_count == 1
    assert fake.sender_disconnects == 0
    assert not second.done()

    fake.release["two"].set()
    await second
    assert fake.borrow_count == 0
    assert fake.sender_disconnects == 1


@pytest.mark.asyncio
async def test_cancel_while_waiting_for_stream_semaphore_keeps_capacity(monkeypatch):
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(1)
    monkeypatch.setitem(app_module.STREAM_SEMAPHORES, loop, semaphore)
    holder_started = asyncio.Event()
    release_holder = asyncio.Event()

    async def holder():
        async with app_module.tracked_semaphore(
            app_module.stream_semaphore(), app_module.ACTIVE_STREAMS, "stream"
        ):
            holder_started.set()
            await release_holder.wait()

    owner = asyncio.create_task(holder())
    await holder_started.wait()
    waiter = asyncio.create_task(
        app_module.tracked_semaphore(
            app_module.stream_semaphore(), app_module.ACTIVE_STREAMS, "stream"
        ).__aenter__()
    )
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert semaphore._value == 0
    release_holder.set()
    await owner
    assert semaphore._value == 1
    assert app_module.active_count(app_module.ACTIVE_STREAMS) == 0


@pytest.mark.asyncio
async def test_closing_one_http_download_leaves_second_download_intact(
    client, stored_file, db, monkeypatch
):
    chunk_size = app_module.STREAM_CHUNK_SIZE
    file_size = chunk_size * 8
    content = bytes(range(256)) * (file_size // 256)
    fake = FakeStreamClient(content)
    monkeypatch.setattr(app_module, "STREAM_CLIENT", fake)
    media_ids = iter(("http-client-a", "http-client-b"))

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
    await asyncio.sleep(0.05)

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


@pytest.mark.asyncio
async def test_three_logical_2gb_streams_complete_without_buffering_file_bodies(monkeypatch):
    """Exercise full 2 GiB streams with a counting response sink, not RAM buffers."""
    file_size = 2 * 1024**3
    chunk = b"x" * app_module.STREAM_CHUNK_SIZE
    written: list[int] = []

    class FakeResponse:
        def __init__(self, *, status, headers):
            self.status = status
            self.headers = headers
            self.prepared = False
            self.bytes_written = 0

        async def prepare(self, _request):
            self.prepared = True

        async def write(self, data):
            self.bytes_written += len(data)
            await asyncio.sleep(0)

        async def write_eof(self):
            written.append(self.bytes_written)

        def force_close(self):
            pass

    class ConnectedClient:
        def is_connected(self):
            return True

    class Request:
        headers = {}
        method = "GET"

    async def fake_chunk(_media, _offset, _size):
        return chunk

    monkeypatch.setattr(app_module, "STREAM_CLIENT", ConnectedClient())
    monkeypatch.setattr(app_module, "STREAM_PIPELINE_CHUNKS", 2)
    monkeypatch.setattr(app_module, "telegram_media_for_record", AsyncMock(side_effect=lambda r: r["file_id"]))
    monkeypatch.setattr(app_module, "telegram_file_chunk", fake_chunk)
    monkeypatch.setattr(app_module, "touch_file_cache", AsyncMock())
    monkeypatch.setattr(app_module.web, "StreamResponse", FakeResponse)

    records = [
        {
            "file_id": f"file-{index}",
            "name": f"large-{index}.bin",
            "size_bytes": file_size,
            "token": f"token-{index}",
        }
        for index in range(3)
    ]
    responses = await asyncio.gather(
        *(app_module.stream_telegram_file(Request(), record) for record in records)
    )

    assert len(responses) == 3
    assert written == [file_size, file_size, file_size]


@pytest.mark.asyncio
async def test_failed_download_does_not_stop_an_independent_stream(monkeypatch):
    class PermanentTelegramError(Exception):
        pass

    calls: dict[str, int] = {}

    async def fetch(media, offset, _size):
        calls[media] = calls.get(media, 0) + 1
        if media == "bad" and offset == 0:
            raise PermanentTelegramError("bad media")
        return b"ok"

    monkeypatch.setattr(app_module, "telegram_file_chunk", fetch)
    monkeypatch.setattr(app_module, "MAX_RETRIES", 2)

    async def run(media):
        return await app_module._fetch_batch_with_retry(
            [media], [0], 2, {"file_id": media}
        )

    failed, survived = await asyncio.gather(run("bad"), run("good"), return_exceptions=True)
    assert isinstance(failed, PermanentTelegramError)
    assert survived == [b"ok"]
    assert calls == {"bad": 1, "good": 1}


@pytest.mark.asyncio
async def test_stream_keeps_http_range_offsets_with_two_gib_file_size(monkeypatch):
    size = 2 * 1024**3
    chunk_size = app_module.STREAM_CHUNK_SIZE
    chunk = b"z" * chunk_size

    class Response:
        def __init__(self, *, status, headers):
            self.status = status
            self.headers = headers
            self.prepared = False
            self.body = bytearray()

        async def prepare(self, _request):
            self.prepared = True

        async def write(self, data):
            self.body.extend(data)

        async def write_eof(self):
            pass

        def force_close(self):
            pass

    class ConnectedClient:
        def is_connected(self):
            return True

    class Request:
        headers = {"Range": f"bytes={size - 37}-{size - 1}"}
        method = "GET"

    calls = []

    async def fetch(_media, offset, _size):
        calls.append(offset)
        return chunk

    record = {"file_id": "large", "name": "large.bin", "size_bytes": size, "token": "large-token"}
    monkeypatch.setattr(app_module, "STREAM_CLIENT", ConnectedClient())
    monkeypatch.setattr(app_module, "telegram_media_for_record", AsyncMock(return_value="large-media"))
    monkeypatch.setattr(app_module, "telegram_file_chunk", fetch)
    monkeypatch.setattr(app_module, "touch_file_cache", AsyncMock())
    monkeypatch.setattr(app_module.web, "StreamResponse", Response)

    response = await app_module.stream_telegram_file(Request(), record)
    assert response.status == 206
    assert response.headers["Content-Length"] == "37"
    assert response.headers["Content-Range"] == f"bytes {size - 37}-{size - 1}/{size}"
    assert len(response.body) == 37
    assert calls == [size - chunk_size]


@pytest.mark.asyncio
async def test_error_after_headers_closes_partial_response_for_range_resume(monkeypatch, caplog):
    class FakeResponse:
        def __init__(self, *, status, headers):
            self.status = status
            self.headers = headers
            self.prepared = False
            self.body = bytearray()
            self.closed = False
            self.completed = False

        async def prepare(self, _request):
            self.prepared = True

        async def write(self, data):
            self.body.extend(data)

        async def write_eof(self):
            self.completed = True

        def force_close(self):
            self.closed = True

    class ConnectedClient:
        def is_connected(self):
            return True

    class Request:
        headers = {}
        method = "GET"

    async def broken_chunk(_media, offset, _size):
        if offset:
            raise OSError("simulated Telegram connection loss")
        return b"a" * app_module.STREAM_CHUNK_SIZE

    record = {
        "file_id": "partial",
        "name": "partial.bin",
        "size_bytes": app_module.STREAM_CHUNK_SIZE * 2,
        "token": "partial-token",
    }
    monkeypatch.setattr(app_module, "STREAM_CLIENT", ConnectedClient())
    monkeypatch.setattr(app_module, "STREAM_PIPELINE_CHUNKS", 1)
    monkeypatch.setattr(app_module, "telegram_media_for_record", AsyncMock(return_value="partial-media"))
    monkeypatch.setattr(app_module, "telegram_file_chunk", broken_chunk)
    monkeypatch.setattr(app_module.web, "StreamResponse", FakeResponse)

    response = await app_module.stream_telegram_file(Request(), record)
    assert response.prepared
    assert response.closed
    assert not response.completed
    assert len(response.body) == app_module.STREAM_CHUNK_SIZE
    assert "Telegram stream ended after the browser response started" in caplog.text


@pytest.mark.asyncio
async def test_public_transfer_semaphore_is_released_after_fallback_failure(
    client, stored_file, monkeypatch
):
    monkeypatch.setattr(app_module, "STREAM_CLIENT", None)
    semaphore = app_module.download_semaphore()
    initial_capacity = semaphore._value

    response = await client.get(f"/download/{stored_file}")
    assert response.status == 502
    assert semaphore._value == initial_capacity

    second_response = await client.get(f"/download/{stored_file}")
    assert second_response.status == 502
    assert semaphore._value == initial_capacity


@pytest.mark.asyncio
async def test_two_files_with_four_parallel_ranges_each_are_admitted(
    client, db, stored_file, sample_file_values, monkeypatch
):
    chunk_size = app_module.STREAM_CHUNK_SIZE
    second = list(sample_file_values)
    second[0] = "test_token_second_file"
    second[2] = "AgACAgIAAxkBAAIBN2X_other_file_id"
    second[5] = "second.bin"
    second[6] = chunk_size * 2
    await app_module.add_file(tuple(second))
    await db.execute(
        "UPDATE files SET size_bytes = ? WHERE token IN (?, ?)",
        (chunk_size * 2, stored_file, second[0]),
    )
    await db.commit()

    fake = FakeStreamClient(b"r" * (chunk_size * 2))
    monkeypatch.setattr(app_module, "STREAM_CLIENT", fake)
    release_media_lookup = asyncio.Event()
    all_media_lookups_started = asyncio.Event()
    media_lookups = 0

    async def media_for_record(record):
        nonlocal media_lookups
        media_lookups += 1
        if media_lookups == 8:
            all_media_lookups_started.set()
        await release_media_lookup.wait()
        return record["file_id"]

    monkeypatch.setattr(app_module, "telegram_media_for_record", media_for_record)

    async def range_get(token, start):
        response = await client.get(
            f"/download/{token}",
            headers={"Range": f"bytes={start}-{start + 31}"},
        )
        return response, await response.read()

    requests = [
        asyncio.create_task(range_get(token, start))
        for token in (stored_file, second[0])
        for start in (0, 64, chunk_size, chunk_size + 64)
    ]
    await asyncio.wait_for(all_media_lookups_started.wait(), timeout=1)
    assert app_module.active_count(app_module.ACTIVE_DOWNLOADS) == 8
    release_media_lookup.set()
    results = await asyncio.gather(*requests)

    assert all(response.status == 206 and len(body) == 32 for response, body in results)
    async def wait_for_release():
        while app_module.download_semaphore()._value < app_module.DOWNLOAD_CONCURRENCY:
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait_for_release(), timeout=1)
    assert app_module.download_semaphore()._value == app_module.DOWNLOAD_CONCURRENCY
    assert app_module.active_count(app_module.ACTIVE_DOWNLOADS) == 0


@pytest.mark.asyncio
async def test_two_files_can_each_issue_four_parallel_telegram_chunks(monkeypatch):
    chunk_size = app_module.STREAM_CHUNK_SIZE
    all_started = asyncio.Event()
    release = asyncio.Event()

    class ParallelClient:
        def __init__(self):
            self.started = 0
            self.active = 0
            self.max_active = 0

        def iter_download(self, _media, *, offset, **_kwargs):
            async def generate():
                self.started += 1
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                if self.started == 8:
                    all_started.set()
                try:
                    await release.wait()
                    yield bytes([offset // chunk_size % 255]) * chunk_size
                finally:
                    self.active -= 1

            class Iterator:
                def __init__(self):
                    self.generator = generate()

                def __aiter__(self):
                    return self

                async def __anext__(self):
                    return await self.generator.__anext__()

                async def close(self):
                    await self.generator.aclose()

            return Iterator()

    fake = ParallelClient()
    monkeypatch.setattr(app_module, "STREAM_CLIENT", fake)
    offsets = [0, chunk_size, chunk_size * 2, chunk_size * 3]

    first = asyncio.create_task(
        app_module._fetch_batch_with_retry(
            ["file-a"], offsets, chunk_size * 4, {"file_id": "a"}
        )
    )
    second = asyncio.create_task(
        app_module._fetch_batch_with_retry(
            ["file-b"], offsets, chunk_size * 4, {"file_id": "b"}
        )
    )
    await asyncio.wait_for(all_started.wait(), timeout=1)
    assert fake.max_active == 8
    assert app_module.active_count(app_module.ACTIVE_TELEGRAM_REQUESTS) == 8
    release.set()
    chunks_a, chunks_b = await asyncio.gather(first, second)

    assert len(chunks_a) == len(chunks_b) == 4
    assert fake.active == 0
    assert app_module.active_count(app_module.ACTIVE_TELEGRAM_REQUESTS) == 0


@pytest.mark.asyncio
async def test_global_capacity_rejection_recovers_after_active_transfer_finishes(
    client, cached_download, monkeypatch
):
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(1)
    monkeypatch.setitem(app_module.DOWNLOAD_SEMAPHORES, loop, semaphore)
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def controlled_cached_response(*_args):
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
        return app_module.web.Response(text="ok")

    monkeypatch.setattr(app_module, "_serve_cached_file", controlled_cached_response)
    first = asyncio.create_task(
        client.get(f"/download/{cached_download['token']}")
    )
    await started.wait()

    overloaded = await client.get(f"/download/{cached_download['token']}")
    assert overloaded.status == 503
    assert overloaded.headers["Retry-After"] == "1"

    release.set()
    first_response = await first
    assert first_response.status == 200
    recovered = await client.get(f"/download/{cached_download['token']}")
    assert recovered.status == 200
    assert semaphore._value == 1
    assert app_module.active_count(app_module.ACTIVE_DOWNLOADS) == 0


@pytest.mark.asyncio
async def test_per_file_limit_preserves_global_capacity_for_other_links(
    client, cached_download, sample_file_values, db, monkeypatch
):
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(app_module.DOWNLOAD_CONCURRENCY)
    monkeypatch.setitem(app_module.DOWNLOAD_SEMAPHORES, loop, semaphore)
    monkeypatch.setattr(app_module, "PER_FILE_DOWNLOAD_CONCURRENCY", 1)
    other_values = list(sample_file_values)
    other_values[0] = "unrelated_file_token"
    other_values[2] = "AgACAgIAAxkBAAIBN2X_unrelated_file_id"
    await app_module.add_file(tuple(other_values))
    started = asyncio.Event()
    release = asyncio.Event()

    async def controlled_cached_response(*_args):
        started.set()
        await release.wait()
        return app_module.web.Response(text="ok")

    monkeypatch.setattr(app_module, "_serve_cached_file", controlled_cached_response)
    first = asyncio.create_task(
        client.get(f"/download/{cached_download['token']}")
    )
    await started.wait()
    same_file = await client.get(f"/download/{cached_download['token']}")
    assert same_file.status == 429
    assert same_file.headers["Retry-After"] == "1"
    assert semaphore._value == app_module.DOWNLOAD_CONCURRENCY - 1
    other_link = await client.get(f"/download/{other_values[0]}")
    assert other_link.status == 502  # admitted; direct streaming is intentionally unavailable
    assert semaphore._value == app_module.DOWNLOAD_CONCURRENCY - 1

    release.set()
    assert (await first).status == 200
    assert semaphore._value == app_module.DOWNLOAD_CONCURRENCY


@pytest.mark.asyncio
async def test_cached_and_direct_streams_can_run_at_the_same_time(
    client, cached_download, sample_file_values, db, monkeypatch
):
    chunk = app_module.STREAM_CHUNK_SIZE
    direct_values = list(sample_file_values)
    direct_values[0] = "direct_concurrent_token"
    direct_values[2] = "AgACAgIAAxkBAAIBN2X_direct_concurrent_id"
    direct_values[6] = chunk * 2
    await app_module.add_file(tuple(direct_values))
    direct_content = b"d" * (chunk * 2)
    fake = FakeStreamClient(direct_content)
    monkeypatch.setattr(app_module, "STREAM_CLIENT", fake)
    monkeypatch.setattr(
        app_module,
        "telegram_media_for_record",
        AsyncMock(return_value="direct-file"),
    )
    monkeypatch.setattr(app_module, "touch_file_cache", AsyncMock())

    cached_task = asyncio.create_task(
        client.get(f"/download/{cached_download['token']}")
    )
    direct_task = asyncio.create_task(client.get(f"/download/{direct_values[0]}"))
    cached_response, direct_response = await asyncio.gather(cached_task, direct_task)

    assert cached_response.status == direct_response.status == 200
    assert await cached_response.read() == cached_download["content"]
    assert await direct_response.read() == direct_content
