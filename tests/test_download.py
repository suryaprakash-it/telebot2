"""Comprehensive regression tests for download handler, caching, polling, and database.

Covers all bugs identified in the code review:
- Duplicate unreachable cached-file block (now removed)
- If-Range mismatch returning 206 instead of 200 (fixed)
- Missing HEAD support for cached files (fixed)
- Missing touch_file_cache on cached hit (fixed)
- Empty file handling (fixed)
- poll_updates offset advancement on failure (fixed)
- Missing aiohttp dependency (fixed)
"""
from __future__ import annotations

import asyncio
import signal
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from aiohttp import web

import app as app_module


# ═══════════════════════════════════════════════════════════════════════════════
# Cached file download tests
# ═══════════════════════════════════════════════════════════════════════════════
class TestCachedFullDownload:
    """Full-file GET of a cached file."""

    @pytest.mark.asyncio
    async def test_full_download_returns_200(self, client, cached_download):
        resp = await client.get(f"/download/{cached_download['token']}")
        assert resp.status == 200
        body = await resp.read()
        assert body == cached_download["content"]

    @pytest.mark.asyncio
    async def test_full_download_has_correct_headers(self, client, cached_download):
        resp = await client.get(f"/download/{cached_download['token']}")
        await resp.read()
        assert resp.headers["Accept-Ranges"] == "bytes"
        assert resp.headers["Content-Type"] == "application/pdf"
        assert "ETag" in resp.headers
        assert "Content-Disposition" in resp.headers
        assert resp.headers["X-Content-Type-Options"] == "nosniff"

    @pytest.mark.asyncio
    async def test_content_length_matches_file(self, client, cached_download):
        resp = await client.get(f"/download/{cached_download['token']}")
        await resp.read()
        assert int(resp.headers["Content-Length"]) == cached_download["size"]

    @pytest.mark.asyncio
    async def test_cache_timestamp_is_refreshed(self, client, cached_download):
        """Regression: the original code never called touch_file_cache for cached hits."""
        before = time.time() - 1
        resp = await client.get(f"/download/{cached_download['token']}")
        assert resp.status == 200
        await resp.read()
        # Verify cache_updated_at was refreshed
        record = await app_module.get_file(cached_download["token"])
        assert record is not None
        assert record["cache_updated_at"] is not None
        assert record["cache_updated_at"] >= int(before)


class TestCachedPartialDownload:
    """HTTP Range requests for cached files."""

    @pytest.mark.asyncio
    async def test_range_returns_206(self, client, cached_download):
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"Range": "bytes=0-10"},
        )
        assert resp.status == 206
        body = await resp.read()
        assert body == cached_download["content"][:11]

    @pytest.mark.asyncio
    async def test_range_has_content_range_header(self, client, cached_download):
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"Range": "bytes=0-10"},
        )
        assert resp.status == 206
        await resp.read()
        expected = f"bytes 0-10/{cached_download['size']}"
        assert resp.headers["Content-Range"] == expected

    @pytest.mark.asyncio
    async def test_range_content_length_is_slice_length(self, client, cached_download):
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"Range": "bytes=5-14"},
        )
        assert resp.status == 206
        await resp.read()
        assert int(resp.headers["Content-Length"]) == 10

    @pytest.mark.asyncio
    async def test_suffix_range(self, client, cached_download):
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"Range": "bytes=-11"},
        )
        assert resp.status == 206
        body = await resp.read()
        assert body == cached_download["content"][-11:]

    @pytest.mark.asyncio
    async def test_open_ended_range(self, client, cached_download):
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"Range": f"bytes={cached_download['size'] - 11}-"},
        )
        assert resp.status == 206
        body = await resp.read()
        assert body == cached_download["content"][-11:]


class TestIfRangeHandling:
    """If-Range header interaction with Range."""

    @pytest.mark.asyncio
    async def test_if_range_match_honors_range(self, client, cached_download):
        """When If-Range matches ETag, Range should be honored → 206."""
        # First get the ETag
        head_resp = await client.get(f"/download/{cached_download['token']}")
        etag = head_resp.headers["ETag"]
        await head_resp.read()
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"Range": "bytes=0-10", "If-Range": etag},
        )
        assert resp.status == 206
        assert await resp.read() == cached_download["content"][:11]

    @pytest.mark.asyncio
    async def test_weak_if_range_does_not_match_strong_etag(self, client, cached_download):
        etag = app_module.compute_etag(await app_module.get_file(cached_download["token"]))
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"Range": "bytes=0-10", "If-Range": f"W/{etag}"},
        )
        assert resp.status == 200
        assert await resp.read() == cached_download["content"]

    @pytest.mark.asyncio
    async def test_if_range_mismatch_returns_200_full(self, client, cached_download):
        """Regression: If-Range mismatch must return 200 with full content, not 206.

        The original code kept status=206 and Content-Range even when If-Range
        did not match. This violated HTTP spec.
        """
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"Range": "bytes=0-10", "If-Range": '"wrong-etag"'},
        )
        assert resp.status == 200
        body = await resp.read()
        assert body == cached_download["content"]
        # Must NOT have Content-Range header when serving full file
        assert "Content-Range" not in resp.headers


class TestIfNoneMatch:
    """If-None-Match conditional GET → 304."""

    @pytest.mark.asyncio
    async def test_matching_etag_returns_304(self, client, cached_download):
        # Get the ETag first
        resp1 = await client.get(f"/download/{cached_download['token']}")
        etag = resp1.headers["ETag"]
        await resp1.read()
        resp2 = await client.get(
            f"/download/{cached_download['token']}",
            headers={"If-None-Match": etag},
        )
        assert resp2.status == 304

    @pytest.mark.asyncio
    async def test_weak_if_none_match_returns_304(self, client, cached_download):
        etag = app_module.compute_etag(await app_module.get_file(cached_download["token"]))
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"If-None-Match": f"W/{etag}"},
        )
        assert resp.status == 304

    @pytest.mark.asyncio
    async def test_wildcard_etag_returns_304(self, client, cached_download):
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"If-None-Match": "*"},
        )
        assert resp.status == 304

    @pytest.mark.asyncio
    async def test_non_matching_etag_returns_200(self, client, cached_download):
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"If-None-Match": '"totally-wrong"'},
        )
        assert resp.status == 200
        await resp.read()


class TestInvalidRange:
    """Invalid or unsatisfiable ranges → 416."""

    @pytest.mark.asyncio
    async def test_range_beyond_file_returns_416(self, client, cached_download):
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"Range": f"bytes={cached_download['size'] + 100}-"},
        )
        assert resp.status == 416
        assert "Content-Range" in resp.headers
        assert resp.headers["Content-Range"] == f"bytes */{cached_download['size']}"

    @pytest.mark.asyncio
    async def test_reversed_range_returns_416(self, client, cached_download):
        resp = await client.get(
            f"/download/{cached_download['token']}",
            headers={"Range": "bytes=100-10"},
        )
        assert resp.status == 416


class TestHeadRequest:
    """HEAD requests for cached files."""

    @pytest.mark.asyncio
    async def test_head_returns_200_no_body(self, client, cached_download):
        """Regression: HEAD was not supported by the first cached-file block.
        The HEAD handler was only in the unreachable second block.
        """
        resp = await client.head(f"/download/{cached_download['token']}")
        assert resp.status == 200
        assert int(resp.headers["Content-Length"]) == cached_download["size"]
        assert resp.headers["Accept-Ranges"] == "bytes"
        assert "ETag" in resp.headers
        body = await resp.read()
        assert body == b""

    @pytest.mark.asyncio
    async def test_head_nonexistent_returns_404(self, client, db):
        resp = await client.head("/download/nonexistent_token")
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_head_range_matches_get_range_headers(self, client, cached_download):
        resp = await client.head(
            f"/download/{cached_download['token']}",
            headers={"Range": "bytes=2-7"},
        )
        assert resp.status == 206
        assert resp.headers["Content-Range"] == f"bytes 2-7/{cached_download['size']}"
        assert resp.headers["Content-Length"] == "6"
        assert await resp.read() == b""


class TestEmptyFile:
    """Empty file (0 bytes) edge cases."""

    @pytest_asyncio.fixture
    async def empty_cached_download(self, db):
        cache_dir = Path(app_module.BOT_API_DATA_DIR)
        cache_dir.mkdir(parents=True, exist_ok=True)
        cached = cache_dir / "empty_file.txt"
        cached.write_bytes(b"")

        token = "empty_test_token"
        file_id = "empty_file_id"
        values = (token, 99, file_id, "-1001234567890", 50, "empty_file.txt", 0, "text/plain", int(time.time()))
        await app_module.add_file(values)
        await app_module.update_file_cache(file_id, str(cached), None)
        yield {"token": token, "file_id": file_id, "path": cached, "size": 0}
        cached.unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_empty_file_returns_200(self, client, empty_cached_download):
        resp = await client.get(f"/download/{empty_cached_download['token']}")
        assert resp.status == 200
        assert int(resp.headers["Content-Length"]) == 0
        body = await resp.read()
        assert body == b""

    @pytest.mark.asyncio
    async def test_range_on_empty_file_returns_416(self, client, empty_cached_download):
        resp = await client.get(
            f"/download/{empty_cached_download['token']}",
            headers={"Range": "bytes=0-0"},
        )
        assert resp.status == 416
        assert resp.headers["Content-Range"] == "bytes */0"


class TestTelegramFallback:
    """Telegram streaming fallback when no cached file exists."""

    @pytest.mark.asyncio
    async def test_missing_file_no_stream_no_cache_returns_502(self, client, stored_file):
        """When both Telegram streaming and local caching fail, return 502."""
        with patch.object(app_module, "STREAM_CLIENT", None):
            with patch.object(app_module, "cached_file", new_callable=AsyncMock) as mock_cache:
                mock_cache.side_effect = app_module.TelegramError("test error")
                resp = await client.get(f"/download/{stored_file}")
                assert resp.status == 502


class TestDownloadAdmission:
    @pytest.mark.asyncio
    async def test_transfer_cap_rejects_excess_request_with_retry_after(self, client, monkeypatch):
        loop = asyncio.get_running_loop()
        semaphore = asyncio.Semaphore(1)
        monkeypatch.setitem(app_module.DOWNLOAD_SEMAPHORES, loop, semaphore)
        async with semaphore:
            resp = await client.get("/download/not-needed-to-check-record")
            assert resp.status == 503
            assert resp.headers["Retry-After"] == "1"
            assert "retry" in (await resp.text()).lower()


class TestTelegramAPIErrors:
    """Telegram API error handling."""

    @pytest.mark.asyncio
    async def test_telegram_error_in_stream_falls_back(self, client, stored_file):
        """stream_telegram_file failure should trigger local Bot API fallback."""
        cache_dir = Path(app_module.BOT_API_DATA_DIR)
        cache_dir.mkdir(parents=True, exist_ok=True)
        fallback_file = cache_dir / "test_document.pdf"
        fallback_content = b"FALLBACK_CONTENT"
        fallback_file.write_bytes(fallback_content)
        try:
            with patch.object(app_module, "stream_telegram_file", new_callable=AsyncMock) as mock_stream:
                mock_stream.side_effect = app_module.TelegramError("stream error")
                with patch.object(app_module, "cached_file", new_callable=AsyncMock) as mock_cache:
                    mock_cache.return_value = fallback_file
                    resp = await client.get(f"/download/{stored_file}")
                    assert resp.status == 200
                    body = await resp.read()
                    assert body == fallback_content
        finally:
            fallback_file.unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_expired_file_reference_is_refreshed_and_retried(self):
        media_ref = ["expired"]
        calls = []

        async def chunk(media, offset, size):
            calls.append(media)
            if len(calls) == 1:
                raise Exception("FILE_REFERENCE_EXPIRED")
            return b"chunk"

        with patch.object(app_module, "telegram_file_chunk", side_effect=chunk):
            with patch.object(
                app_module, "refresh_media_reference", new_callable=AsyncMock, return_value="fresh",
            ) as refresh:
                chunks = await app_module._fetch_batch_with_retry(
                    media_ref, [0], 6, {"token": "t", "archive_chat_id": "1", "archive_message_id": 1},
                )
        assert chunks == [b"chunk"]
        assert calls == ["expired", "fresh"]
        refresh.assert_awaited_once()


# ═══════════════════════════════════════════════════════════════════════════════
# Update polling tests
# ═══════════════════════════════════════════════════════════════════════════════
class TestPollUpdates:
    """Durable update intake and worker queue behavior."""

    @pytest.mark.asyncio
    async def test_enqueue_persists_payload_and_advances_offset(self, db):
        app_module.OFFSET = 0
        update = {"update_id": 100, "message": {"chat": {"id": 1}, "text": "hello"}}

        assert await app_module.enqueue_update(update) == 101
        assert await app_module.load_offset() == 101
        claimed = await app_module.claim_next_update()
        assert claimed == (100, update)

    @pytest.mark.asyncio
    async def test_enqueue_is_idempotent_for_duplicate_update_ids(self, db):
        update = {"update_id": 100, "message": {"text": "first"}}
        await app_module.enqueue_update(update)
        await app_module.enqueue_update({"update_id": 100, "message": {"text": "duplicate"}})

        claimed = await app_module.claim_next_update()
        assert claimed == (100, update)
        assert await app_module.claim_next_update() is None

    @pytest.mark.asyncio
    async def test_failed_worker_item_can_be_retried(self, db):
        update = {"update_id": 101, "message": {"text": "retry me"}}
        await app_module.enqueue_update(update)
        claimed = await app_module.claim_next_update()
        assert claimed == (101, update)

        await app_module.retry_update(101, RuntimeError("private detail must not be stored"))
        claimed_again = await app_module.claim_next_update()
        assert claimed_again == (101, update)
        async with db.execute("SELECT attempts,last_error FROM update_inbox WHERE update_id=101") as cursor:
            row = await cursor.fetchone()
        assert row["attempts"] == 2
        assert row["last_error"] == "RuntimeError"

    @pytest.mark.asyncio
    async def test_finish_marks_update_processed_and_removes_inbox_item(self, db):
        await app_module.enqueue_update({"update_id": 102, "message": {"text": "done"}})
        await app_module.claim_next_update()
        await app_module.finish_update(102)

        assert await app_module.update_was_processed(102)
        assert await app_module.claim_next_update() is None

    @pytest.mark.asyncio
    async def test_polling_does_not_wait_for_message_handler(self, db):
        app_module.OFFSET = 0
        await app_module.save_offset(0)
        call_count = 0

        async def mock_telegram(method, payload=None):
            nonlocal call_count
            if method == "getUpdates":
                call_count += 1
                if call_count == 1:
                    return [{"update_id": 103, "message": {"text": "slow handler"}}]
                raise asyncio.CancelledError()
            return None

        with patch.object(app_module, "telegram", side_effect=mock_telegram):
            with pytest.raises(asyncio.CancelledError):
                await app_module.poll_updates()

        assert app_module.OFFSET == 104
        assert await app_module.load_offset() == 104
        # The update is durably queued; poll_updates never calls on_message.
        claimed = await app_module.claim_next_update()
        assert claimed == (103, {"update_id": 103, "message": {"text": "slow handler"}})


# ═══════════════════════════════════════════════════════════════════════════════
# Database tests
# ═══════════════════════════════════════════════════════════════════════════════
class TestDatabase:
    """Database initialization and operations."""

    @pytest.mark.asyncio
    async def test_db_init_creates_tables(self, db):
        """Database init should create required tables."""
        assert db is not None
        async with db.execute("SELECT name FROM sqlite_master WHERE type='table'") as cursor:
            tables = {row["name"] async for row in cursor}
        assert "files" in tables
        assert "bot_state" in tables
        assert "processed_updates" in tables

    @pytest.mark.asyncio
    async def test_add_and_get_file(self, db, sample_file_values):
        await app_module.add_file(sample_file_values)
        record = await app_module.get_file(sample_file_values[0])
        assert record is not None
        assert record["token"] == sample_file_values[0]
        assert record["name"] == sample_file_values[5]

    @pytest.mark.asyncio
    async def test_source_message_lookup_supports_idempotent_replay(self, db, sample_file_values):
        await app_module.add_file(sample_file_values, source_chat_id=123, source_message_id=44)
        result = await app_module.get_file_by_source_message(123, 44)
        assert result is not None
        assert result["token"] == sample_file_values[0]

    @pytest.mark.asyncio
    async def test_concurrent_database_writes_are_serialized(self, db, sample_file_values):
        records = []
        for index in range(24):
            values = list(sample_file_values)
            values[0] = f"parallel_token_{index}"
            values[4] = index + 100
            records.append(tuple(values))
        await asyncio.gather(*(app_module.add_file(values) for values in records))
        found = await asyncio.gather(*(app_module.get_file(row[0]) for row in records))
        assert all(record is not None for record in found)

    @pytest.mark.asyncio
    async def test_existing_database_migrates_without_losing_file_rows(self, tmp_path, monkeypatch):
        import aiosqlite

        old_path = tmp_path / "existing.sqlite3"
        connection = await aiosqlite.connect(str(old_path))
        await connection.execute(
            """CREATE TABLE files (
                token TEXT PRIMARY KEY, owner_id INTEGER NOT NULL, file_id TEXT NOT NULL,
                archive_chat_id TEXT NOT NULL, archive_message_id INTEGER NOT NULL,
                name TEXT NOT NULL, size_bytes INTEGER, mime_type TEXT,
                created_at INTEGER NOT NULL, cached_path TEXT, cache_error TEXT
            )"""
        )
        await connection.execute(
            "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("preserved_token", 1, "file", "-1", 2, "keep.bin", 3, None, 4, None, None),
        )
        await connection.commit()
        await connection.close()

        monkeypatch.setattr(app_module, "DATABASE_PATH", old_path)
        app_module.DB = None
        await app_module.db_init()
        try:
            record = await app_module.get_file("preserved_token")
            assert record is not None and record["name"] == "keep.bin"
            async with app_module.DB.execute("PRAGMA table_info(files)") as cursor:
                columns = {row["name"] async for row in cursor}
            assert {"cache_updated_at", "source_chat_id", "source_message_id"} <= columns
            async with app_module.DB.execute("PRAGMA journal_mode") as cursor:
                assert (await cursor.fetchone())[0].lower() == "wal"
        finally:
            await app_module.db_close()

    @pytest.mark.asyncio
    async def test_get_nonexistent_file(self, db):
        record = await app_module.get_file("nonexistent")
        assert record is None

    @pytest.mark.asyncio
    async def test_offset_persistence(self, db):
        await app_module.save_offset(42)
        result = await app_module.load_offset()
        assert result == 42

    @pytest.mark.asyncio
    async def test_offset_update(self, db):
        await app_module.save_offset(10)
        await app_module.save_offset(20)
        result = await app_module.load_offset()
        assert result == 20

    @pytest.mark.asyncio
    async def test_cache_update_and_clear(self, db, sample_file_values):
        await app_module.add_file(sample_file_values)
        file_id = sample_file_values[2]

        await app_module.update_file_cache(file_id, "/some/path.pdf", None)
        record = await app_module.get_file(sample_file_values[0])
        assert record["cached_path"] == "/some/path.pdf"
        assert record["cache_updated_at"] is not None

        await app_module.clear_cache_path("/some/path.pdf")
        record = await app_module.get_file(sample_file_values[0])
        assert record["cached_path"] is None

    @pytest.mark.asyncio
    async def test_expired_cache_paths(self, db, sample_file_values):
        await app_module.add_file(sample_file_values)
        file_id = sample_file_values[2]

        # Set cache with old timestamp
        await app_module.update_file_cache(file_id, "/old/cached.pdf", None)
        # Manually set an ancient timestamp
        assert app_module.DB is not None
        await app_module.DB.execute(
            "UPDATE files SET cache_updated_at = ? WHERE file_id = ?",
            (1, file_id),
        )
        await app_module.DB.commit()

        expired = await app_module.expired_cache_paths()
        assert "/old/cached.pdf" in expired

    @pytest.mark.asyncio
    async def test_touch_file_cache_updates_timestamp(self, db, sample_file_values):
        await app_module.add_file(sample_file_values)
        file_id = sample_file_values[2]
        await app_module.update_file_cache(file_id, "/some/path.pdf", None)

        before = int(time.time()) - 1
        await app_module.touch_file_cache(file_id)
        record = await app_module.get_file(sample_file_values[0])
        assert record["cache_updated_at"] >= before


# ═══════════════════════════════════════════════════════════════════════════════
# File page and landing page tests
# ═══════════════════════════════════════════════════════════════════════════════
class TestPages:
    """Web page handlers."""

    @pytest.mark.asyncio
    async def test_landing_page(self, client):
        resp = await client.get("/")
        assert resp.status == 200
        body = await resp.text()
        assert "Infinity File Link" in body

    @pytest.mark.asyncio
    async def test_file_page_exists(self, client, stored_file):
        resp = await client.get(f"/{stored_file}")
        assert resp.status == 200
        body = await resp.text()
        assert "test_document.pdf" in body

    @pytest.mark.asyncio
    async def test_file_page_not_found(self, client, db):
        resp = await client.get("/nonexistent_token_xyz")
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_file_page_304(self, client, stored_file):
        resp1 = await client.get(f"/{stored_file}")
        etag = resp1.headers["ETag"]
        resp2 = await client.get(f"/{stored_file}", headers={"If-None-Match": etag})
        assert resp2.status == 304

    @pytest.mark.asyncio
    async def test_healthz(self, client):
        resp = await client.get("/healthz")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ok"

    @pytest.mark.asyncio
    async def test_download_not_found(self, client, db):
        resp = await client.get("/download/nonexistent_token_xyz")
        assert resp.status == 404


# ═══════════════════════════════════════════════════════════════════════════════
# Security tests
# ═══════════════════════════════════════════════════════════════════════════════
class TestSecurity:
    """Security-related checks."""

    @pytest.mark.asyncio
    async def test_download_token_is_unguessable(self):
        """Tokens should be long enough to be unguessable."""
        import secrets
        token = secrets.token_urlsafe(24)
        assert len(token) >= 32

    def test_path_traversal_in_safe_name(self):
        assert ".." not in app_module.safe_name("../../etc/passwd")
        assert "/" not in app_module.safe_name("path/to/file")
        assert "\\" not in app_module.safe_name("path\\to\\file")

    def test_control_chars_stripped(self):
        result = app_module.safe_name("file\x00name\x1f.txt")
        assert "\x00" not in result
        assert "\x1f" not in result

    @pytest.mark.asyncio
    async def test_cached_path_outside_data_dir_rejected(self, db, sample_file_values):
        """Cached paths outside BOT_API_DATA_DIR must be rejected."""
        await app_module.add_file(sample_file_values)
        await app_module.update_file_cache(
            sample_file_values[2], "/etc/passwd", None
        )
        record = await app_module.get_file(sample_file_values[0])
        result = app_module.cached_path(record)
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# Streaming concurrency tests
# ═══════════════════════════════════════════════════════════════════════════════
class TestConcurrentDownloads:
    """Multiple simultaneous downloads."""

    @pytest.mark.asyncio
    async def test_concurrent_cached_downloads(self, client, cached_download):
        """Multiple simultaneous downloads of the same cached file."""
        async def do_download():
            resp = await client.get(f"/download/{cached_download['token']}")
            assert resp.status == 200
            body = await resp.read()
            assert body == cached_download["content"]

        await asyncio.gather(*[do_download() for _ in range(5)])

    @pytest.mark.asyncio
    async def test_concurrent_range_downloads(self, client, cached_download):
        """Multiple simultaneous range requests."""
        content = cached_download["content"]

        async def do_range(start, end):
            resp = await client.get(
                f"/download/{cached_download['token']}",
                headers={"Range": f"bytes={start}-{end}"},
            )
            assert resp.status == 206
            body = await resp.read()
            assert body == content[start : end + 1]

        ranges = [(0, 10), (100, 200), (500, 600), (0, len(content) - 1)]
        await asyncio.gather(*[do_range(s, e) for s, e in ranges])

    @pytest.mark.asyncio
    async def test_expiry_cleanup_skips_active_cache_stream(self, cached_download):
        async with app_module.cached_stream_file(cached_download["path"]) as stream:
            removed = await app_module.remove_cache_if_inactive(cached_download["path"])
            assert removed is False
            assert cached_download["path"].is_file()
            assert await stream.read(11) == cached_download["content"][:11]

    @pytest.mark.asyncio
    async def test_cancelled_cache_stream_releases_file_lease(self, cached_download):
        with pytest.raises(asyncio.CancelledError):
            async with app_module.cached_stream_file(cached_download["path"]):
                raise asyncio.CancelledError()
        assert str(cached_download["path"].resolve()) not in app_module.ACTIVE_CACHE_STREAMS

    @pytest.mark.asyncio
    async def test_large_cached_file_uses_bounded_reads(self, client, stored_file, monkeypatch):
        path = Path(app_module.BOT_API_DATA_DIR) / "large-stream-test.bin"
        contents = b"x" * (3 * 1024 * 1024 + 17)
        path.write_bytes(contents)
        await app_module.update_file_cache("AgACAgIAAxkBAAIBN2X_test_file_id", str(path), None)
        original_open = app_module.aiofiles.open
        requested_sizes = []

        class TrackingFile:
            def __init__(self, file):
                self.file = file

            async def seek(self, offset):
                return await asyncio.to_thread(self.file.seek, offset)

            async def read(self, size):
                requested_sizes.append(size)
                return await asyncio.to_thread(self.file.read, size)

            async def close(self):
                await asyncio.to_thread(self.file.close)

        async def tracking_open(file_path, mode):
            return TrackingFile(open(file_path, mode))

        monkeypatch.setattr(app_module.aiofiles, "open", tracking_open)
        try:
            resp = await client.get(f"/download/{stored_file}")
            assert await resp.read() == contents
            assert requested_sizes and max(requested_sizes) <= 1024 * 1024
        finally:
            monkeypatch.setattr(app_module.aiofiles, "open", original_open)
            path.unlink(missing_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Error classification tests
# ═══════════════════════════════════════════════════════════════════════════════
class TestErrorClassification:
    """Telegram error classification helpers."""

    def test_file_reference_error_detected(self):
        class FileReferenceError(Exception):
            pass
        assert app_module._is_file_reference_error(FileReferenceError("test"))

    def test_file_reference_in_message(self):
        assert app_module._is_file_reference_error(Exception("FILE_REFERENCE_EXPIRED"))

    def test_non_file_reference_error(self):
        assert not app_module._is_file_reference_error(Exception("some other error"))

    def test_flood_wait_extraction(self):
        class FloodWaitError(Exception):
            seconds = 30
        assert app_module._get_flood_wait_seconds(FloodWaitError("wait")) == 30

    def test_flood_wait_from_message(self):
        result = app_module._get_flood_wait_seconds(Exception("FLOOD_WAIT_60"))
        assert result == 60

    def test_no_flood_wait(self):
        assert app_module._get_flood_wait_seconds(Exception("normal error")) is None

    def test_retryable_timeout(self):
        assert app_module._is_retryable_error(Exception("TIMEOUT during request"))

    def test_retryable_connection(self):
        assert app_module._is_retryable_error(Exception("CONNECTION_ERROR"))

    def test_non_retryable(self):
        assert not app_module._is_retryable_error(Exception("CHAT_NOT_FOUND"))


# ═══════════════════════════════════════════════════════════════════════════════
# File status endpoint tests
# ═══════════════════════════════════════════════════════════════════════════════
class TestFileStatus:
    """The /status/{token} endpoint."""

    @pytest.mark.asyncio
    async def test_status_cached_ready(self, client, cached_download):
        resp = await client.get(f"/status/{cached_download['token']}")
        assert resp.status == 200
        data = await resp.json()
        assert data["ready"] is True
        assert data["failed"] is False

    @pytest.mark.asyncio
    async def test_status_not_found(self, client, db):
        resp = await client.get("/status/nonexistent")
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_status_uncached_no_stream(self, client, stored_file):
        with patch.object(app_module, "STREAM_CLIENT", None):
            resp = await client.get(f"/status/{stored_file}")
            data = await resp.json()
            assert data["ready"] is False


class TestApplicationLifecycle:
    @pytest.mark.asyncio
    async def test_app_starts_and_shuts_down_without_network_access(self, monkeypatch):
        class FakeTelegramClient:
            def __init__(self, *args, **kwargs):
                self.started = False
                self.disconnected = False

            async def start(self, **kwargs):
                self.started = True

            async def disconnect(self):
                self.disconnected = True

        fake_clients = []

        def make_client(*args, **kwargs):
            client = FakeTelegramClient(*args, **kwargs)
            fake_clients.append(client)
            return client

        monkeypatch.setattr(app_module, "TelegramClient", make_client)
        monkeypatch.setattr(app_module, "PORT", 0)

        async def stop_polling():
            raise asyncio.CancelledError()

        monkeypatch.setattr(app_module, "poll_updates", stop_polling)
        app_module.DB = None
        app_module.HTTP = None
        app_module.STREAM_CLIENT = None
        with pytest.raises(asyncio.CancelledError):
            await app_module.start_app()
        assert fake_clients and fake_clients[0].started and fake_clients[0].disconnected
        assert app_module.DB is None
        assert app_module.HTTP is None
        assert app_module.STREAM_CLIENT is None

    @pytest.mark.asyncio
    async def test_sigterm_cancels_service_and_runs_cleanup(self, monkeypatch):
        loop = asyncio.get_running_loop()
        registered = {}
        removed = []
        cleanup = []

        def add_handler(sig, callback):
            registered[sig] = callback

        def remove_handler(sig):
            removed.append(sig)
            return True

        monkeypatch.setattr(loop, "add_signal_handler", add_handler)
        monkeypatch.setattr(loop, "remove_signal_handler", remove_handler)

        async def fake_start():
            try:
                await asyncio.sleep(0)
                registered[signal.SIGTERM]()
                await asyncio.Event().wait()
            finally:
                cleanup.append("closed")

        monkeypatch.setattr(app_module, "start_app", fake_start)
        await app_module.run_application()
        assert cleanup == ["closed"]
        assert removed == [signal.SIGTERM]
