from __future__ import annotations

import asyncio
import hashlib
import aiofiles
import html
import logging
import mimetypes
import os
import re
import secrets
import signal
import time
import weakref
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiosqlite
from aiohttp import ClientError, ClientSession, ClientTimeout, web
from telethon import TelegramClient, utils as telethon_utils


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("telegram-link-bot")

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
API_ID = os.environ.get("TELEGRAM_API_ID", "").strip()
API_HASH = os.environ.get("TELEGRAM_API_HASH", "").strip()
_archive_raw = os.environ["ARCHIVE_CHAT_ID"].strip()
try:
    ARCHIVE_CHAT_ID: int = int(_archive_raw)
except ValueError:
    raise RuntimeError(f"ARCHIVE_CHAT_ID must be an integer, got {_archive_raw!r}")
PUBLIC_BASE_URL = os.environ["PUBLIC_BASE_URL"].rstrip("/")
BOT_API_BASE_URL = os.getenv("TELEGRAM_API_BASE_URL", "http://telegram-bot-api:8081").rstrip("/")
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "/app/data/files.sqlite3"))
BOT_API_DATA_DIR = Path(os.getenv("BOT_API_DATA_DIR", "/var/lib/telegram-bot-api")).resolve()
MAX_FILE_BYTES = int(os.getenv("MAX_FILE_BYTES", str(4 * 1024**3)))
CACHE_TTL_SECONDS = max(1, int(os.getenv("CACHE_TTL_HOURS", "24"))) * 60 * 60
CACHE_CONCURRENCY = max(1, int(os.getenv("CACHE_CONCURRENCY", "1")))
STREAM_CONCURRENCY = max(1, int(os.getenv("STREAM_CONCURRENCY", "8")))
DOWNLOAD_CONCURRENCY = max(1, int(os.getenv("DOWNLOAD_CONCURRENCY", "16")))
STREAM_PIPELINE_CHUNKS = max(1, int(os.getenv("STREAM_PIPELINE_CHUNKS", "4")))
STREAM_REQUEST_CONCURRENCY = max(1, int(os.getenv("STREAM_REQUEST_CONCURRENCY", "16")))
STREAM_CHUNK_SIZE = 512 * 1024
MAX_RETRIES = max(0, int(os.getenv("MAX_RETRIES", "3")))
RETRY_BASE_DELAY = max(0.1, float(os.getenv("RETRY_BASE_DELAY", "0.5")))
TELETHON_SESSION_PATH = Path(
    os.getenv("TELETHON_SESSION_PATH", str(DATABASE_PATH.parent / "telegram-stream"))
)
PORT = int(os.getenv("PORT", "8080"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip().lstrip("@")
ALLOWED_USERS = {
    int(value.strip())
    for value in os.getenv("ALLOWED_TELEGRAM_USER_IDS", "").split(",")
    if value.strip()
}
ROOT = Path(__file__).parent

if not BOT_TOKEN or not PUBLIC_BASE_URL:
    raise RuntimeError("BOT_TOKEN and PUBLIC_BASE_URL must be configured")
if not API_ID or not API_HASH:
    raise RuntimeError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required for the local Bot API server")

# ---------------------------------------------------------------------------
# Global mutable state
# ---------------------------------------------------------------------------
HTTP: ClientSession | None = None
STREAM_CLIENT: TelegramClient | None = None
DB: aiosqlite.Connection | None = None
OFFSET = 0
FILE_TASKS: dict[str, asyncio.Task[Path]] = {}
ACTIVE_CACHE_STREAMS: dict[str, int] = {}
CACHE_STREAM_LOCKS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = weakref.WeakKeyDictionary()
DATABASE_LOCKS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = weakref.WeakKeyDictionary()
DOWNLOAD_SEMAPHORES: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = weakref.WeakKeyDictionary()
CACHE_SEMAPHORE = asyncio.Semaphore(CACHE_CONCURRENCY)
STREAM_SEMAPHORE = asyncio.Semaphore(STREAM_CONCURRENCY)
STREAM_REQUEST_SEMAPHORE = asyncio.Semaphore(STREAM_REQUEST_CONCURRENCY)
TEMPLATE_LANDING: str = ""
TEMPLATE_HOME: str = ""


# ---------------------------------------------------------------------------
# Database layer (async via aiosqlite)
# ---------------------------------------------------------------------------
def cache_stream_lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock = CACHE_STREAM_LOCKS.get(loop)
    if lock is None:
        lock = asyncio.Lock()
        CACHE_STREAM_LOCKS[loop] = lock
    return lock


def database_lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock = DATABASE_LOCKS.get(loop)
    if lock is None:
        lock = asyncio.Lock()
        DATABASE_LOCKS[loop] = lock
    return lock


def download_semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    semaphore = DOWNLOAD_SEMAPHORES.get(loop)
    if semaphore is None:
        semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
        DOWNLOAD_SEMAPHORES[loop] = semaphore
    return semaphore


async def db_init() -> None:
    """Open the SQLite database and create tables if they do not exist."""
    global DB
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    DB = await aiosqlite.connect(str(DATABASE_PATH), timeout=30)
    DB.row_factory = aiosqlite.Row
    await DB.execute("PRAGMA journal_mode=WAL")
    await DB.execute(
        """CREATE TABLE IF NOT EXISTS files (
            token TEXT PRIMARY KEY,
            owner_id INTEGER NOT NULL,
            file_id TEXT NOT NULL,
            archive_chat_id TEXT NOT NULL,
            archive_message_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            size_bytes INTEGER,
            mime_type TEXT,
            created_at INTEGER NOT NULL,
            cached_path TEXT,
            cache_error TEXT,
            cache_updated_at INTEGER,
            source_chat_id INTEGER,
            source_message_id INTEGER
        )"""
    )
    columns: set[str] = set()
    async with DB.execute("PRAGMA table_info(files)") as cursor:
        async for row in cursor:
            columns.add(row["name"])
    for column, declaration in (
        ("cache_updated_at", "INTEGER"),
        ("source_chat_id", "INTEGER"),
        ("source_message_id", "INTEGER"),
    ):
        if column not in columns:
            await DB.execute(f"ALTER TABLE files ADD COLUMN {column} {declaration}")
    await DB.execute("CREATE INDEX IF NOT EXISTS files_owner_created ON files(owner_id, created_at DESC)")
    await DB.execute("CREATE INDEX IF NOT EXISTS files_file_id ON files(file_id)")
    await DB.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS files_source_message "
        "ON files(source_chat_id, source_message_id) "
        "WHERE source_chat_id IS NOT NULL AND source_message_id IS NOT NULL"
    )
    await DB.execute("CREATE TABLE IF NOT EXISTS bot_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    await DB.execute(
        "CREATE TABLE IF NOT EXISTS processed_updates (update_id INTEGER PRIMARY KEY, processed_at INTEGER NOT NULL)"
    )
    await DB.commit()


async def db_close() -> None:
    """Close the database connection cleanly."""
    global DB
    if DB is not None:
        await DB.close()
        DB = None


async def load_offset() -> int:
    assert DB is not None
    async with database_lock():
        async with DB.execute("SELECT value FROM bot_state WHERE key = 'update_offset'") as cursor:
            row = await cursor.fetchone()
            return int(row["value"]) if row else 0


async def save_offset(offset: int) -> None:
    assert DB is not None
    async with database_lock():
        await DB.execute(
            "INSERT INTO bot_state(key, value) VALUES('update_offset', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(offset),),
        )
        await DB.commit()


async def commit_update(update_id: int) -> int:
    """Persist update receipt and next polling offset atomically."""
    assert DB is not None
    next_offset = update_id + 1
    async with database_lock():
        await DB.execute("BEGIN IMMEDIATE")
        try:
            await DB.execute(
                "INSERT OR IGNORE INTO processed_updates(update_id, processed_at) VALUES (?, ?)",
                (update_id, int(time.time())),
            )
            await DB.execute(
                "INSERT INTO bot_state(key, value) VALUES('update_offset', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(next_offset),),
            )
            await DB.commit()
        except BaseException:
            await DB.rollback()
            raise
    return next_offset


async def update_was_processed(update_id: int) -> bool:
    assert DB is not None
    async with database_lock():
        async with DB.execute("SELECT 1 FROM processed_updates WHERE update_id = ?", (update_id,)) as cursor:
            return await cursor.fetchone() is not None


async def get_file(token: str) -> dict[str, Any] | None:
    assert DB is not None
    async with database_lock():
        async with DB.execute("SELECT * FROM files WHERE token = ?", (token,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def add_file(
    values: tuple[Any, ...], source_chat_id: int | None = None, source_message_id: int | None = None,
) -> None:
    assert DB is not None
    async with database_lock():
        await DB.execute(
            """INSERT INTO files
               (token, owner_id, file_id, archive_chat_id, archive_message_id, name,
                size_bytes, mime_type, created_at, source_chat_id, source_message_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (*values, source_chat_id, source_message_id),
        )
        await DB.commit()


async def get_file_by_source_message(chat_id: int, message_id: int) -> dict[str, Any] | None:
    assert DB is not None
    async with database_lock():
        async with DB.execute(
            "SELECT * FROM files WHERE source_chat_id = ? AND source_message_id = ?",
            (chat_id, message_id),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def update_file_cache(file_id: str, path: str | None, error: str | None) -> None:
    assert DB is not None
    async with database_lock():
        await DB.execute(
            "UPDATE files SET cached_path = ?, cache_error = ?, cache_updated_at = ? WHERE file_id = ?",
            (path, error, int(time.time()) if path else None, file_id),
        )
        await DB.commit()


async def touch_file_cache(file_id: str) -> None:
    assert DB is not None
    async with database_lock():
        await DB.execute("UPDATE files SET cache_updated_at = ? WHERE file_id = ?", (int(time.time()), file_id))
        await DB.commit()


async def expired_cache_paths() -> list[str]:
    assert DB is not None
    cutoff = int(time.time()) - CACHE_TTL_SECONDS
    async with database_lock():
        async with DB.execute(
            "SELECT DISTINCT cached_path FROM files WHERE cached_path IS NOT NULL AND cache_updated_at < ?",
            (cutoff,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [row["cached_path"] for row in rows]


async def clear_cache_path(path: str) -> None:
    assert DB is not None
    async with database_lock():
        await DB.execute(
            "UPDATE files SET cached_path = NULL, cache_error = NULL, cache_updated_at = NULL WHERE cached_path = ?",
            (path,),
        )
        await DB.commit()


async def clear_cache_error(file_id: str) -> None:
    assert DB is not None
    async with database_lock():
        await DB.execute("UPDATE files SET cache_error = NULL WHERE file_id = ?", (file_id,))
        await DB.commit()


async def remove_cache_if_inactive(path: Path) -> bool:
    """Delete an expired cache path only when no response currently reads it."""
    resolved = path.resolve(strict=False)
    key = str(resolved)
    async with cache_stream_lock():
        if ACTIVE_CACHE_STREAMS.get(key, 0):
            return False
        resolved.unlink(missing_ok=True)
    return True


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------
def escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


def human_size(size: int | None) -> str:
    if size is None:
        return "Size unavailable"
    units = ("B", "KB", "MB", "GB", "TB")
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"  # unreachable; keeps the type checker happy


def safe_name(value: str | None) -> str:
    name = Path((value or "telegram-file").replace("\\", "/")).name
    name = re.sub(r"[\x00-\x1f\x7f]", "_", name).strip()
    return name or "telegram-file"


def content_disposition(name: str) -> str:
    name = safe_name(name)
    fallback = name.encode("ascii", "ignore").decode("ascii") or "telegram-file"
    fallback = re.sub(r'["\\/;]', "_", fallback)
    encoded_name = quote(name, safe="")
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded_name}"


def compute_etag(record: dict[str, Any]) -> str:
    """Stable ETag derived from the immutable token and file size.

    Using the token (not file_id) guarantees stability across Telegram
    file-reference refreshes — the public link never changes.
    """
    raw = f"{record['token']}:{record.get('size_bytes', 0)}".encode()
    return f'"{hashlib.sha256(raw).hexdigest()[:16]}"'


def if_none_match_matches(header: str | None, etag: str) -> bool:
    """Evaluate If-None-Match using weak comparison as required for GET/HEAD."""
    if not header:
        return False
    target = etag[2:] if etag.startswith("W/") else etag
    for candidate in header.split(","):
        candidate = candidate.strip()
        if candidate == "*":
            return True
        if candidate.startswith("W/"):
            candidate = candidate[2:]
        if candidate == target:
            return True
    return False


@asynccontextmanager
async def cached_stream_file(path: Path):
    """Open a cache file while protecting it from the expiry cleanup loop."""
    key = str(path.resolve(strict=False))
    async with cache_stream_lock():
        if not path.is_file():
            raise FileNotFoundError(path)
        handle = await aiofiles.open(path, "rb")
        ACTIVE_CACHE_STREAMS[key] = ACTIVE_CACHE_STREAMS.get(key, 0) + 1
    try:
        yield handle
    finally:
        try:
            await handle.close()
        finally:
            async with cache_stream_lock():
                remaining = ACTIVE_CACHE_STREAMS.get(key, 1) - 1
                if remaining:
                    ACTIVE_CACHE_STREAMS[key] = remaining
                else:
                    ACTIVE_CACHE_STREAMS.pop(key, None)


# ---------------------------------------------------------------------------
# Telegram error classification (string-based for Telethon-version safety)
# ---------------------------------------------------------------------------
def _is_file_reference_error(exc: BaseException) -> bool:
    """Detect expired or invalid Telegram file references."""
    cls = type(exc).__name__
    if "FileReference" in cls:
        return True
    return "FILE_REFERENCE" in str(exc).upper()


def _get_flood_wait_seconds(exc: BaseException) -> int | None:
    """Extract the required wait from a Telegram flood-wait error, or *None*."""
    cls = type(exc).__name__
    if "FloodWait" in cls and hasattr(exc, "seconds"):
        try:
            return int(exc.seconds)  # type: ignore[union-attr]
        except (TypeError, ValueError):
            pass
    m = re.search(r"FLOOD_WAIT[_\s]*(\d+)", str(exc).upper())
    return int(m.group(1)) if m else None


def _is_retryable_error(exc: BaseException) -> bool:
    """Check whether a Telegram operation error is worth retrying."""
    if _is_file_reference_error(exc):
        return True
    if _get_flood_wait_seconds(exc) is not None:
        return True
    msg = str(exc).upper()
    return any(k in msg for k in ("TIMEOUT", "NETWORK_ERROR", "CONNECTION", "TEMPORARY"))


# ---------------------------------------------------------------------------
# Telegram Bot API helpers
# ---------------------------------------------------------------------------
class TelegramError(RuntimeError):
    pass


async def telegram(method: str, payload: dict[str, Any] | None = None) -> Any:
    if HTTP is None:
        raise RuntimeError("HTTP session is not ready")
    url = f"{BOT_API_BASE_URL}/bot{BOT_TOKEN}/{method}"
    try:
        async with HTTP.post(url, json=payload or {}) as response:
            data = await response.json(content_type=None)
            if response.status >= 400 or not data.get("ok"):
                description = data.get("description", f"HTTP {response.status}")
                raise TelegramError(f"{method}: {description}")
            return data.get("result")
    except (ClientError, asyncio.TimeoutError) as exc:
        # The Bot API token is part of the request URL. Do not retain aiohttp's
        # URL-bearing exception or traceback where it could reach logs.
        raise TelegramError(
            f"{method}: Telegram API request failed ({type(exc).__name__})"
        ) from None


async def send_message(chat_id: int | str, text: str, reply_markup: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    await telegram("sendMessage", payload)


def media_info(message: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    for key in ("document", "video", "audio", "animation", "voice", "video_note", "sticker"):
        item = message.get(key)
        if item:
            default = f"telegram-{key}"
            filename = item.get("file_name") or default
            if key == "video" and not Path(filename).suffix:
                filename += ".mp4"
            if key == "audio" and not Path(filename).suffix:
                filename += ".mp3"
            if key == "animation" and not Path(filename).suffix:
                filename += ".mp4"
            if key == "voice" and not Path(filename).suffix:
                filename += ".ogg"
            if key == "sticker" and not Path(filename).suffix:
                filename += ".webp"
            return item, safe_name(filename)
    photos = message.get("photo")
    if photos:
        largest = max(photos, key=lambda photo: photo.get("file_size", 0))
        return largest, "telegram-photo.jpg"
    return None, ""


# ---------------------------------------------------------------------------
# File caching (local Bot API fallback path)
# ---------------------------------------------------------------------------
async def materialize_file(file_id: str) -> Path:
    try:
        async with CACHE_SEMAPHORE:
            result = await telegram("getFile", {"file_id": file_id})
        file_path = result.get("file_path") if result else None
        if not file_path:
            raise TelegramError("The local Bot API did not return a file path")
        path = Path(file_path)
        if not path.is_absolute():
            path = BOT_API_DATA_DIR / path
        path = path.resolve(strict=True)
        if not path.is_relative_to(BOT_API_DATA_DIR):
            raise TelegramError("Telegram returned a file path outside the configured cache")
        if not path.is_file():
            raise TelegramError("The cached Telegram file is not a regular file")
        await update_file_cache(file_id, str(path), None)
        log.info("Cached Telegram file %s (%s bytes)", path.name, path.stat().st_size)
        return path
    except Exception as exc:
        await update_file_cache(file_id, None, str(exc)[:500])
        raise


def start_materialize(file_id: str) -> asyncio.Task[Path]:
    task = FILE_TASKS.get(file_id)
    if task is None or task.done():
        task = asyncio.create_task(materialize_file(file_id), name=f"cache-{file_id[:8]}")
        FILE_TASKS[file_id] = task

        def forget(done: asyncio.Task[Path]) -> None:
            if FILE_TASKS.get(file_id) is done:
                FILE_TASKS.pop(file_id, None)
            if not done.cancelled() and done.exception() is not None:
                log.warning("Could not cache Telegram file: %s", done.exception())

        task.add_done_callback(forget)
    return task


async def cached_file(record: dict[str, Any]) -> Path:
    cached = record.get("cached_path")
    if cached:
        path = Path(cached)
        try:
            resolved = path.resolve(strict=True)
            if resolved.is_relative_to(BOT_API_DATA_DIR) and resolved.is_file():
                return resolved
        except OSError:
            pass
    return await start_materialize(record["file_id"])


def user_allowed(user_id: int) -> bool:
    return not ALLOWED_USERS or user_id in ALLOWED_USERS


# ---------------------------------------------------------------------------
# Telegram media reference management
# ---------------------------------------------------------------------------
async def telegram_media_for_record(record: dict[str, Any]) -> Any:
    """Get a Telegram media object for streaming, preferring the archive message."""
    if STREAM_CLIENT is None:
        raise TelegramError("The direct Telegram streaming connection is unavailable")
    try:
        message = await STREAM_CLIENT.get_messages(
            int(record["archive_chat_id"]),
            ids=int(record["archive_message_id"]),
        )
        if message is not None and message.media is not None:
            return message
    except Exception as exc:
        log.debug("Could not refresh media from the archive message: %s", exc)
    media = telethon_utils.resolve_bot_file_id(record["file_id"])
    if media is None:
        raise TelegramError("Telethon could not read the Telegram file identifier")
    return media


async def refresh_media_reference(record: dict[str, Any]) -> Any:
    """Re-fetch the archive message to obtain a fresh Telegram file reference.

    Called transparently when mid-download file-reference expiry is detected.
    Falls back to :func:`resolve_bot_file_id` if the archive message cannot
    be retrieved.
    """
    if STREAM_CLIENT is None:
        raise TelegramError("Stream client unavailable for reference refresh")
    try:
        message = await STREAM_CLIENT.get_messages(
            int(record["archive_chat_id"]),
            ids=int(record["archive_message_id"]),
        )
        if message is not None and message.media is not None:
            log.info(
                "Refreshed file reference for archive message %d",
                record["archive_message_id"],
            )
            return message
    except Exception as exc:
        log.warning("Could not refresh file reference from archive: %s", exc)
    media = telethon_utils.resolve_bot_file_id(record["file_id"])
    if media is None:
        raise TelegramError("Could not refresh file reference by any method")
    return media


# ---------------------------------------------------------------------------
# Telegram chunk downloading with retry
# ---------------------------------------------------------------------------
async def telegram_file_chunk(media: Any, offset: int, file_size: int) -> bytes:
    """Fetch one Telegram-aligned block while sharing a bounded request pool."""
    if STREAM_CLIENT is None:
        raise TelegramError("The direct Telegram streaming connection is unavailable")
    async with STREAM_REQUEST_SEMAPHORE:
        stream = STREAM_CLIENT.iter_download(
            media,
            offset=offset,
            limit=1,
            chunk_size=STREAM_CHUNK_SIZE,
            request_size=STREAM_CHUNK_SIZE,
            file_size=file_size,
        )
        try:
            return await stream.__anext__()
        finally:
            close = getattr(stream, "close", None)
            if close is not None:
                result = close()
                if asyncio.iscoroutine(result):
                    await result


async def _fetch_batch_with_retry(
    media_ref: list[Any],
    offsets: list[int],
    file_size: int,
    record: dict[str, Any],
) -> list[bytes]:
    """Fetch a batch of chunks with automatic retry on transient errors.

    *media_ref* is a single-element list so that a refreshed reference
    propagates to subsequent batches in the caller's loop.
    """
    last_exc: BaseException | None = None
    for attempt in range(MAX_RETRIES + 1):
        chunks = await asyncio.gather(
            *(telegram_file_chunk(media_ref[0], offset, file_size) for offset in offsets),
            return_exceptions=True,
        )
        errors = [(i, c) for i, c in enumerate(chunks) if isinstance(c, BaseException)]
        if not errors:
            return chunks  # type: ignore[return-value]

        last_exc = errors[0][1]
        if attempt >= MAX_RETRIES:
            break

        # --- file reference expired → refresh and retry the whole batch ---
        ref_errors = [e for _, e in errors if _is_file_reference_error(e)]
        if ref_errors:
            log.warning(
                "File reference expired during batch (attempt %d/%d), refreshing",
                attempt + 1,
                MAX_RETRIES,
            )
            try:
                media_ref[0] = await refresh_media_reference(record)
            except Exception as refresh_exc:
                log.error("Reference refresh failed: %s", refresh_exc)
                raise ref_errors[0] from refresh_exc
            continue

        # --- flood wait → sleep the requested duration, then retry ---
        flood_waits = [
            _get_flood_wait_seconds(e) for _, e in errors if _get_flood_wait_seconds(e) is not None
        ]
        if flood_waits:
            wait = min(max(flood_waits), 30)
            log.warning(
                "Telegram flood wait %ds (attempt %d/%d)",
                wait,
                attempt + 1,
                MAX_RETRIES,
            )
            await asyncio.sleep(wait)
            continue

        # --- other retryable errors → exponential backoff ---
        retryable = [e for _, e in errors if _is_retryable_error(e)]
        if retryable:
            delay = min(RETRY_BASE_DELAY * (2**attempt), 10)
            log.warning(
                "Retryable chunk error (attempt %d/%d), backing off %.1fs: %s",
                attempt + 1,
                MAX_RETRIES,
                delay,
                retryable[0],
            )
            await asyncio.sleep(delay)
            continue

        # non-retryable error — stop immediately
        break

    assert last_exc is not None
    raise last_exc


# ---------------------------------------------------------------------------
# Byte-range parsing
# ---------------------------------------------------------------------------
def parse_byte_range(value: str | None, size: int) -> tuple[int, int, int]:
    if not value:
        return 0, max(0, size - 1), 200
    if not value.startswith("bytes=") or "," in value:
        raise ValueError("Unsupported byte range")
    match = re.fullmatch(r"bytes=(\d*)-(\d*)", value.strip())
    if not match or (not match.group(1) and not match.group(2)):
        raise ValueError("Invalid byte range")
    first, last = match.groups()
    if not first:
        suffix = int(last)
        if suffix <= 0 or size <= 0:
            raise ValueError("Invalid suffix range")
        start = max(0, size - suffix)
        end = size - 1
    else:
        start = int(first)
        end = int(last) if last else size - 1
        if start >= size or end < start:
            raise ValueError("Range is outside the file")
        end = min(end, size - 1)
    return start, end, 206


def cached_path(record: dict[str, Any]) -> Path | None:
    cached = record.get("cached_path")
    if not cached:
        return None
    try:
        path = Path(cached).resolve(strict=True)
        if path.is_relative_to(BOT_API_DATA_DIR) and path.is_file():
            return path
    except OSError:
        pass
    return None


# ---------------------------------------------------------------------------
# Streaming engine
# ---------------------------------------------------------------------------
async def stream_telegram_file(request: web.Request, record: dict[str, Any]) -> web.StreamResponse:
    if STREAM_CLIENT is None or not STREAM_CLIENT.is_connected():
        raise TelegramError("The direct Telegram streaming connection is unavailable")
    size = record.get("size_bytes")
    if size is None:
        raise TelegramError("The file size is unavailable, so it cannot be streamed")
    size = int(size)

    etag = compute_etag(record)

    # --- Conditional request: If-None-Match → 304 ---
    if if_none_match_matches(request.headers.get("If-None-Match"), etag):
        return web.Response(status=304, headers={"ETag": etag})

    # --- Range negotiation with If-Range ---
    range_header = request.headers.get("Range")
    if_range = request.headers.get("If-Range")
    if if_range and range_header:
        # If-Range requires a strong ETag match; otherwise serve full content.
        if if_range != etag:
            range_header = None

    try:
        start, end, status = parse_byte_range(range_header, size)
    except ValueError:
        return web.Response(
            status=416,
            headers={"Content-Range": f"bytes */{size}", "Accept-Ranges": "bytes"},
        )

    length = max(0, end - start + 1)
    headers = {
        "Content-Type": mimetypes.guess_type(record["name"])[0] or "application/octet-stream",
        "Content-Disposition": content_disposition(record["name"]),
        "Cache-Control": "private, no-store",
        "X-Content-Type-Options": "nosniff",
        "Accept-Ranges": "bytes",
        "Content-Length": str(length),
        "ETag": etag,
    }
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    if request.method == "HEAD" or length == 0:
        return web.Response(status=status, headers=headers)

    media = await telegram_media_for_record(record)
    media_ref: list[Any] = [media]  # mutable so refresh propagates to later batches
    chunk_size = STREAM_CHUNK_SIZE
    first_offset = start - (start % chunk_size)
    last_offset = end - (end % chunk_size)
    offsets = list(range(first_offset, last_offset + 1, chunk_size))
    response = web.StreamResponse(status=status, headers=headers)
    bytes_sent = 0
    started_at = time.monotonic()
    async with STREAM_SEMAPHORE:
        try:
            for batch_start in range(0, len(offsets), STREAM_PIPELINE_CHUNKS):
                batch_offsets = offsets[batch_start : batch_start + STREAM_PIPELINE_CHUNKS]
                chunks = await _fetch_batch_with_retry(
                    media_ref, batch_offsets, size, record,
                )
                if not response.prepared:
                    await response.prepare(request)
                for offset, chunk in zip(batch_offsets, chunks):
                    left = max(start - offset, 0)
                    right = min(end + 1 - offset, len(chunk))
                    data = chunk[left:right]
                    if data:
                        await response.write(data)
                        bytes_sent += len(data)
            if bytes_sent != length:
                log.warning(
                    "Telegram stream ended %s bytes early for file %s",
                    max(0, length - bytes_sent),
                    record["name"],
                )
                raise OSError("Telegram stream did not produce the advertised byte count")
            await response.write_eof()
            await touch_file_cache(record["file_id"])
            elapsed = max(time.monotonic() - started_at, 0.001)
            log.info(
                "Streamed %s/%s bytes for %s in %.2fs (%.2f MB/s)",
                bytes_sent,
                length,
                record["name"],
                elapsed,
                bytes_sent / elapsed / 1_000_000,
            )
            return response
        except (ConnectionResetError, ConnectionError):
            if response.prepared:
                log.info(
                    "Client disconnected during download of %s after %s bytes",
                    record["name"],
                    bytes_sent,
                )
                response.force_close()
                return response
            raise
        except Exception:
            if response.prepared:
                log.exception("Telegram stream ended after the browser response started")
                response.force_close()
                return response
            raise


# ---------------------------------------------------------------------------
# Web handlers
# ---------------------------------------------------------------------------
async def landing(_: web.Request) -> web.Response:
    template = TEMPLATE_LANDING
    if re.fullmatch(r"[A-Za-z0-9_]{5,32}", BOT_USERNAME):
        bot_link = (
            f'<a class="download-button bot-button" href="https://t.me/{escape(BOT_USERNAME)}">'
            '<span>Open Telegram bot</span><svg viewBox="0 0 24 24" aria-hidden="true">'
            '<path d="M7 17 17 7M7 7h10v10"/></svg></a>'
        )
    else:
        bot_link = '<div class="setup-hint">Set <code>BOT_USERNAME</code> to show the bot link here.</div>'
    template = template.replace("@@BOT_LINK@@", bot_link)
    return web.Response(text=template, content_type="text/html", headers={"Cache-Control": "no-store"})


async def file_page(request: web.Request) -> web.Response:
    token = request.match_info["token"]
    record = await get_file(token)
    if record is None:
        raise web.HTTPNotFound(text="This download link was not found.")

    etag = compute_etag(record)

    # Conditional: If-None-Match → 304
    if_none_match = request.headers.get("If-None-Match")
    if if_none_match_matches(if_none_match, etag):
        return web.Response(status=304, headers={"ETag": etag})

    template = TEMPLATE_HOME
    suffix = Path(record["name"]).suffix.lstrip(".").upper() or "FILE"
    replacements = {
        "{{ kind }}": escape(suffix[:12]),
        "{{ name }}": escape(record["name"]),
        "{{ size }}": escape(human_size(record["size_bytes"])),
        "{{ download_url }}": f"/download/{escape(token)}",
        "@@TOKEN@@": escape(token),
    }
    template = re.sub(
        r"\{\{ (?:kind|name|size|download_url) \}\}|@@TOKEN@@",
        lambda match: replacements[match.group(0)],
        template,
    )
    return web.Response(
        text=template,
        content_type="text/html",
        headers={"Cache-Control": "no-store", "ETag": etag},
    )


async def file_status(request: web.Request) -> web.Response:
    record = await get_file(request.match_info["token"])
    if record is None:
        raise web.HTTPNotFound()
    cached = record.get("cached_path")
    ready = False
    if cached:
        try:
            path = Path(cached).resolve(strict=True)
            ready = path.is_relative_to(BOT_API_DATA_DIR) and path.is_file()
        except OSError:
            ready = False
    if not ready and STREAM_CLIENT is not None:
        ready = STREAM_CLIENT.is_connected()
    return web.json_response(
        {
            "ready": ready,
            "failed": bool(record.get("cache_error")) and not ready,
        },
        headers={"Cache-Control": "no-store"},
    )


async def _serve_cached_file(
    request: web.Request, record: dict[str, Any], path: Path, etag: str,
) -> web.StreamResponse:
    """Serve a cache hit with bounded reads and consistent validators/ranges."""
    file_size = path.stat().st_size
    range_header = request.headers.get("Range")
    if_range = request.headers.get("If-Range")
    if range_header and if_range and if_range != etag:
        # If-Range requires a strong entity-tag match. Dates do not match
        # because this endpoint does not publish a Last-Modified validator.
        range_header = None

    status = 200
    start, end = 0, file_size - 1
    if range_header:
        try:
            start, end, status = parse_byte_range(range_header, file_size)
        except ValueError:
            return web.Response(
                status=416,
                headers={
                    "Content-Range": f"bytes */{file_size}",
                    "Accept-Ranges": "bytes",
                    "ETag": etag,
                },
            )

    length = max(0, end - start + 1)
    headers = {
        "Content-Type": mimetypes.guess_type(record["name"])[0] or "application/octet-stream",
        "Content-Disposition": content_disposition(record["name"]),
        "Cache-Control": "private, no-store",
        "X-Content-Type-Options": "nosniff",
        "Accept-Ranges": "bytes",
        "Content-Length": str(length),
        "ETag": etag,
    }
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
    if request.method == "HEAD":
        await touch_file_cache(record["file_id"])
        return web.Response(status=status, headers=headers)

    response = web.StreamResponse(status=status, headers=headers)
    sent = 0
    try:
        async with cached_stream_file(path) as stream:
            await stream.seek(start)
            await response.prepare(request)
            while sent < length:
                chunk = await stream.read(min(1024 * 1024, length - sent))
                if not chunk:
                    raise OSError("Cached file ended before its advertised size")
                await response.write(chunk)
                sent += len(chunk)
            await response.write_eof()
        await touch_file_cache(record["file_id"])
    except (ConnectionResetError, ConnectionError):
        log.info("Client disconnected during cached download of %s", record["name"])
        response.force_close()
    except OSError:
        if response.prepared:
            log.exception("Cached file stream failed after response headers were sent")
            response.force_close()
            return response
        raise
    return response


async def download(request: web.Request) -> web.StreamResponse:
    """Bound concurrent public transfers and reject excess load promptly."""
    semaphore = download_semaphore()
    if semaphore.locked():
        raise web.HTTPServiceUnavailable(
            text="The download service is busy. Please retry shortly.",
            headers={"Retry-After": "1", "Cache-Control": "no-store"},
        )
    async with semaphore:
        return await _download(request)


async def _download(request: web.Request) -> web.StreamResponse:
    record = await get_file(request.match_info["token"])
    if record is None:
        raise web.HTTPNotFound(text="This download link was not found.")

    etag = compute_etag(record)

    # --- Conditional: If-None-Match → 304 ---
    if_none_match = request.headers.get("If-None-Match")
    if if_none_match_matches(if_none_match, etag):
        return web.Response(status=304, headers={"ETag": etag})

    # --- Try locally cached file first ---
    path = cached_path(record)
    if path is not None:
        try:
            return await _serve_cached_file(request, record, path, etag)
        except FileNotFoundError:
            await clear_cache_path(str(path))

    # --- Try direct Telegram streaming ---
    try:
        return await stream_telegram_file(request, record)
    except Exception as stream_error:
        log.warning(
            "Direct Telegram streaming unavailable; falling back to local Bot API staging: %s",
            stream_error,
        )

    # --- Fallback: stage via local Bot API, then serve cached ---
    try:
        path = await cached_file(record)
    except Exception as exc:
        log.warning("Download file could not be prepared: %s", exc)
        raise web.HTTPBadGateway(
            text="Telegram could not prepare this file. Please retry in a few minutes.",
        ) from exc
    return await _serve_cached_file(request, record, path, etag)


async def health(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


# ---------------------------------------------------------------------------
# Bot message handler
# ---------------------------------------------------------------------------
async def on_message(message: dict[str, Any]) -> None:
    chat = message.get("chat", {})
    chat_id = chat.get("id")
    if chat_id is None:
        return
    user = message.get("from", {})
    user_id = user.get("id")
    if user_id is not None and not user_allowed(user_id):
        return

    text = (message.get("text") or "").strip()
    if text.startswith("/start") or text.startswith("/help"):
        await send_message(
            chat_id,
            "<b>Send me a file or forward one here.</b> I'll copy it to the private archive and reply with a download page.\n\n"
            "The link does not expire. Anyone with it can download the file. Telegram allows up to 4 GB per upload for Premium accounts.",
        )
        return
    if text.startswith("/myfiles"):
        assert DB is not None
        async with database_lock():
            async with DB.execute(
                "SELECT token, name, created_at FROM files WHERE owner_id = ? ORDER BY created_at DESC LIMIT 8",
                (user_id,),
            ) as cursor:
                rows = await cursor.fetchall()
        if not rows:
            await send_message(chat_id, "You don't have any files yet. Send me a file to start.")
            return
        lines = ["<b>Your recent download links</b>"]
        for row in rows:
            url = f"{PUBLIC_BASE_URL}/{row['token']}"
            lines.append(f"• <a href=\"{escape(url)}\">{escape(row['name'])}</a>")
        await send_message(chat_id, "\n".join(lines))
        return

    media, name = media_info(message)
    if not media:
        await send_message(chat_id, "Send or forward a file, photo, video, audio, or voice message and I'll make a download link.")
        return

    source_message_id = message.get("message_id")
    existing = (
        await get_file_by_source_message(int(chat_id), int(source_message_id))
        if source_message_id is not None else None
    )
    if existing is not None:
        link = f"{PUBLIC_BASE_URL}/{existing['token']}"
        await send_message(
            chat_id,
            f"<b>File archived.</b> Your download link is ready:\n\n<a href=\"{escape(link)}\">{escape(link)}</a>",
            {"inline_keyboard": [[{"text": "Open download page", "url": link}]]},
        )
        return

    size = media.get("file_size")
    if size is not None and size > MAX_FILE_BYTES:
        await send_message(chat_id, f"This file is larger than my configured {human_size(MAX_FILE_BYTES)} limit.")
        return

    try:
        archived = await telegram(
            "copyMessage",
            {"chat_id": ARCHIVE_CHAT_ID, "from_chat_id": chat_id, "message_id": source_message_id},
        )
    except TelegramError:
        log.exception("Could not copy message into archive")
        await send_message(chat_id, "I couldn't copy that message into the archive. Check that I'm an admin in the private archive channel and try again.")
        return

    token = secrets.token_urlsafe(24)
    link = f"{PUBLIC_BASE_URL}/{token}"
    await add_file(
        (
            token,
            int(user_id or 0),
            media["file_id"],
            str(ARCHIVE_CHAT_ID),
            int(archived["message_id"]),
            name,
            int(size) if size is not None else None,
            media.get("mime_type"),
            int(time.time()),
        ),
        int(chat_id),
        int(source_message_id) if source_message_id is not None else None,
    )
    await send_message(
        chat_id,
        f"<b>File archived.</b> Your download link is ready:\n\n<a href=\"{escape(link)}\">{escape(link)}</a>\n\n"
        "The download page is ready. The server streams file data to your browser from Telegram as it arrives. This link has no expiry.",
        {"inline_keyboard": [[{"text": "Open download page", "url": link}]]},
    )


# ---------------------------------------------------------------------------
# Update polling
# ---------------------------------------------------------------------------
async def poll_updates() -> None:
    global OFFSET
    while True:
        try:
            updates = await telegram(
                "getUpdates",
                {"offset": OFFSET, "timeout": 30, "allowed_updates": ["message"]},
            )
            for update in sorted(updates or [], key=lambda item: int(item["update_id"])):
                update_id = int(update["update_id"])
                if update_id < OFFSET:
                    continue
                message = update.get("message")
                try:
                    if not await update_was_processed(update_id) and message:
                        await on_message(message)
                    # Processing receipts and offsets share one transaction.
                    # Keep the in-memory offset unchanged if SQLite fails so
                    # Telegram is polled again from the last durable position.
                    new_offset = await commit_update(update_id)
                    OFFSET = max(OFFSET, new_offset)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception(
                        "Failed to process or persist Telegram update %s; "
                        "leaving offset at %s for retry",
                        update_id,
                        OFFSET,
                    )
                    await asyncio.sleep(1)
                    break
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Telegram polling failed; retrying shortly")
            await asyncio.sleep(3)


# ---------------------------------------------------------------------------
# Cache cleanup
# ---------------------------------------------------------------------------
async def cache_cleanup_loop() -> None:
    while True:
        await asyncio.sleep(3600)
        if any(not task.done() for task in FILE_TASKS.values()):
            continue
        for cached in await expired_cache_paths():
            try:
                path = Path(cached).resolve(strict=False)
                if not path.is_relative_to(BOT_API_DATA_DIR):
                    log.error("Refusing to clean cache path outside configured directory: %s", path)
                    continue
                if not await remove_cache_if_inactive(path):
                    continue
                await clear_cache_path(cached)
                log.info("Removed inactive local download cache %s", path.name)
            except OSError:
                log.exception("Could not clean inactive file cache %s", cached)


# ---------------------------------------------------------------------------
# Application startup and shutdown
# ---------------------------------------------------------------------------
async def start_app() -> None:
    global HTTP, OFFSET, STREAM_CLIENT, TEMPLATE_LANDING, TEMPLATE_HOME
    await db_init()
    OFFSET = await load_offset()
    TEMPLATE_LANDING = (ROOT / "templates" / "landing.html").read_text(encoding="utf-8")
    TEMPLATE_HOME = (ROOT / "templates" / "home.html").read_text(encoding="utf-8")
    timeout = ClientTimeout(total=None, connect=30, sock_read=None)
    app = web.Application(client_max_size=1024 * 1024)
    app.router.add_static("/static/", ROOT / "static", show_index=False)
    app.router.add_get("/", landing)
    app.router.add_get("/healthz", health)
    app.router.add_get("/status/{token}", file_status)
    app.router.add_get("/download/{token}", download, allow_head=False)
    app.router.add_route("HEAD", "/download/{token}", download)
    app.router.add_get("/{token}", file_page)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    cleanup_task: asyncio.Task[None] | None = None
    try:
        HTTP = ClientSession(timeout=timeout)
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        log.info("Download website listening on port %s", PORT)
        try:
            TELETHON_SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
            STREAM_CLIENT = TelegramClient(
                str(TELETHON_SESSION_PATH),
                int(API_ID),
                API_HASH,
                receive_updates=False,
                request_retries=5,
                connection_retries=5,
            )
            await STREAM_CLIENT.start(bot_token=BOT_TOKEN)
            log.info("Telegram direct streaming connected with update delivery disabled")
        except Exception:
            log.exception("Could not start Telegram direct streaming; downloads will use the local Bot API staging fallback")
            if STREAM_CLIENT is not None:
                await STREAM_CLIENT.disconnect()
                STREAM_CLIENT = None
        cleanup_task = asyncio.create_task(cache_cleanup_loop(), name="file-cache-cleanup")
        await poll_updates()
    finally:
        if cleanup_task is not None:
            cleanup_task.cancel()
            await asyncio.gather(cleanup_task, return_exceptions=True)
        await runner.cleanup()
        if HTTP is not None:
            await HTTP.close()
            HTTP = None
        if STREAM_CLIENT is not None:
            await STREAM_CLIENT.disconnect()
            STREAM_CLIENT = None
        await db_close()


async def run_application() -> None:
    """Run the service with graceful SIGTERM handling for container stops."""
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    stopping = False

    def request_shutdown() -> None:
        nonlocal stopping
        stopping = True
        if task is not None:
            task.cancel()

    signal_handler_installed = False
    previous_handler = None
    try:
        loop.add_signal_handler(signal.SIGTERM, request_shutdown)
        signal_handler_installed = True
    except (NotImplementedError, RuntimeError):
        try:
            previous_handler = signal.signal(
                signal.SIGTERM,
                lambda *_: loop.call_soon_threadsafe(request_shutdown),
            )
        except (ValueError, OSError):
            pass

    try:
        await start_app()
    except asyncio.CancelledError:
        if not stopping:
            raise
    finally:
        if signal_handler_installed:
            loop.remove_signal_handler(signal.SIGTERM)
        elif previous_handler is not None:
            signal.signal(signal.SIGTERM, previous_handler)


if __name__ == "__main__":

    try:
        asyncio.run(run_application())
    except KeyboardInterrupt:
        pass
