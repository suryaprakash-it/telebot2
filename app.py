from __future__ import annotations

import asyncio
import html
import logging
import mimetypes
import os
import re
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

from aiohttp import ClientError, ClientSession, ClientTimeout, web


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("telegram-link-bot")

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
API_ID = os.environ.get("TELEGRAM_API_ID", "").strip()
API_HASH = os.environ.get("TELEGRAM_API_HASH", "").strip()
ARCHIVE_CHAT_ID = os.environ["ARCHIVE_CHAT_ID"].strip()
PUBLIC_BASE_URL = os.environ["PUBLIC_BASE_URL"].rstrip("/")
BOT_API_BASE_URL = os.getenv("TELEGRAM_API_BASE_URL", "http://telegram-bot-api:8081").rstrip("/")
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "/app/data/files.sqlite3"))
BOT_API_DATA_DIR = Path(os.getenv("BOT_API_DATA_DIR", "/var/lib/telegram-bot-api")).resolve()
MAX_FILE_BYTES = int(os.getenv("MAX_FILE_BYTES", str(4 * 1024**3)))
CACHE_TTL_SECONDS = max(1, int(os.getenv("CACHE_TTL_HOURS", "24"))) * 60 * 60
CACHE_CONCURRENCY = max(1, int(os.getenv("CACHE_CONCURRENCY", "1")))
PORT = int(os.getenv("PORT", "8080"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip().lstrip("@")
ALLOWED_USERS = {
    int(value.strip())
    for value in os.getenv("ALLOWED_TELEGRAM_USER_IDS", "").split(",")
    if value.strip()
}
ROOT = Path(__file__).parent

if not BOT_TOKEN or not ARCHIVE_CHAT_ID or not PUBLIC_BASE_URL:
    raise RuntimeError("BOT_TOKEN, ARCHIVE_CHAT_ID, and PUBLIC_BASE_URL must be configured")
if not API_ID or not API_HASH:
    raise RuntimeError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required for the local Bot API server")

HTTP: ClientSession | None = None
OFFSET = 0
FILE_TASKS: dict[str, asyncio.Task[Path]] = {}
CACHE_SEMAPHORE = asyncio.Semaphore(CACHE_CONCURRENCY)


def db_connect() -> sqlite3.Connection:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATABASE_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    return connection


def initialize_db() -> None:
    with db_connect() as db:
        db.execute(
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
                cache_updated_at INTEGER
            )"""
        )
        columns = {row["name"] for row in db.execute("PRAGMA table_info(files)")}
        if "cache_updated_at" not in columns:
            db.execute("ALTER TABLE files ADD COLUMN cache_updated_at INTEGER")
        db.execute("CREATE INDEX IF NOT EXISTS files_owner_created ON files(owner_id, created_at DESC)")
        db.execute("CREATE INDEX IF NOT EXISTS files_file_id ON files(file_id)")
        db.execute("CREATE TABLE IF NOT EXISTS bot_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")


def load_offset() -> int:
    with db_connect() as db:
        row = db.execute("SELECT value FROM bot_state WHERE key = 'update_offset'").fetchone()
        return int(row["value"]) if row else 0


def save_offset(offset: int) -> None:
    with db_connect() as db:
        db.execute(
            "INSERT INTO bot_state(key, value) VALUES('update_offset', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(offset),),
        )


def get_file(token: str) -> dict[str, Any] | None:
    with db_connect() as db:
        row = db.execute("SELECT * FROM files WHERE token = ?", (token,)).fetchone()
        return dict(row) if row else None


def add_file(values: tuple[Any, ...]) -> None:
    with db_connect() as db:
        db.execute(
            """INSERT INTO files
               (token, owner_id, file_id, archive_chat_id, archive_message_id, name,
                size_bytes, mime_type, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            values,
        )


def update_file_cache(file_id: str, path: str | None, error: str | None) -> None:
    with db_connect() as db:
        db.execute(
            "UPDATE files SET cached_path = ?, cache_error = ?, cache_updated_at = ? WHERE file_id = ?",
            (path, error, int(time.time()) if path else None, file_id),
        )


def touch_file_cache(file_id: str) -> None:
    with db_connect() as db:
        db.execute("UPDATE files SET cache_updated_at = ? WHERE file_id = ?", (int(time.time()), file_id))


def expired_cache_paths() -> list[str]:
    cutoff = int(time.time()) - CACHE_TTL_SECONDS
    with db_connect() as db:
        rows = db.execute(
            "SELECT DISTINCT cached_path FROM files WHERE cached_path IS NOT NULL AND cache_updated_at < ?",
            (cutoff,),
        ).fetchall()
        return [row["cached_path"] for row in rows]


def clear_cache_path(path: str) -> None:
    with db_connect() as db:
        db.execute(
            "UPDATE files SET cached_path = NULL, cache_error = NULL, cache_updated_at = NULL WHERE cached_path = ?",
            (path,),
        )


def clear_cache_error(file_id: str) -> None:
    with db_connect() as db:
        db.execute("UPDATE files SET cache_error = NULL WHERE file_id = ?", (file_id,))


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
    return f"{size} B"


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
        raise TelegramError(f"{method}: Telegram API request failed: {exc}") from exc


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
        update_file_cache(file_id, str(path), None)
        log.info("Cached Telegram file %s (%s bytes)", path.name, path.stat().st_size)
        return path
    except Exception as exc:
        update_file_cache(file_id, None, str(exc)[:500])
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
            "<b>Send me a file or forward one here.</b> I’ll copy it to the private archive and reply with a download page.\n\n"
            "The link does not expire. Anyone with it can download the file. Telegram allows up to 4 GB per upload for Premium accounts.",
        )
        return
    if text.startswith("/myfiles"):
        with db_connect() as db:
            rows = db.execute(
                "SELECT token, name, created_at FROM files WHERE owner_id = ? ORDER BY created_at DESC LIMIT 8",
                (user_id,),
            ).fetchall()
        if not rows:
            await send_message(chat_id, "You don’t have any files yet. Send me a file to start.")
            return
        lines = ["<b>Your recent download links</b>"]
        for row in rows:
            url = f"{PUBLIC_BASE_URL}/{row['token']}"
            lines.append(f"• <a href=\"{escape(url)}\">{escape(row['name'])}</a>")
        await send_message(chat_id, "\n".join(lines))
        return

    media, name = media_info(message)
    if not media:
        await send_message(chat_id, "Send or forward a file, photo, video, audio, or voice message and I’ll make a download link.")
        return

    size = media.get("file_size")
    if size is not None and size > MAX_FILE_BYTES:
        await send_message(chat_id, f"This file is larger than my configured {human_size(MAX_FILE_BYTES)} limit.")
        return

    try:
        archived = await telegram(
            "copyMessage",
            {"chat_id": ARCHIVE_CHAT_ID, "from_chat_id": chat_id, "message_id": message["message_id"]},
        )
    except TelegramError as exc:
        log.exception("Could not copy message into archive")
        await send_message(chat_id, "I couldn’t copy that message into the archive. Check that I’m an admin in the private archive channel and try again.")
        return

    token = secrets.token_urlsafe(24)
    link = f"{PUBLIC_BASE_URL}/{token}"
    add_file(
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
        )
    )
    await send_message(
        chat_id,
        f"<b>File archived.</b> Your download link is ready:\n\n<a href=\"{escape(link)}\">{escape(link)}</a>\n\n"
        "The server is preparing the download in the background. The page will enable the download button when it is ready. This link has no expiry.",
        {"inline_keyboard": [[{"text": "Open download page", "url": link}]]},
    )
    start_materialize(media["file_id"])


async def poll_updates() -> None:
    global OFFSET
    while True:
        try:
            updates = await telegram(
                "getUpdates",
                {"offset": OFFSET, "timeout": 30, "allowed_updates": ["message"]},
            )
            for update in updates or []:
                message = update.get("message")
                if message:
                    try:
                        await on_message(message)
                    except Exception:
                        log.exception("Failed to process Telegram update %s", update.get("update_id"))
                OFFSET = max(OFFSET, int(update["update_id"]) + 1)
                save_offset(OFFSET)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Telegram polling failed; retrying shortly")
            await asyncio.sleep(3)


async def landing(_: web.Request) -> web.Response:
    template = (ROOT / "templates" / "landing.html").read_text(encoding="utf-8")
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
    record = get_file(token)
    if record is None:
        raise web.HTTPNotFound(text="This download link was not found.")
    if record.get("cache_error"):
        clear_cache_error(record["file_id"])
        start_materialize(record["file_id"])
    template = (ROOT / "templates" / "home.html").read_text(encoding="utf-8")
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
    return web.Response(text=template, content_type="text/html", headers={"Cache-Control": "no-store"})


async def file_status(request: web.Request) -> web.Response:
    record = get_file(request.match_info["token"])
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
    if not ready and not record.get("cache_error"):
        start_materialize(record["file_id"])
    return web.json_response(
        {"ready": ready, "failed": bool(record.get("cache_error")), "error": record.get("cache_error")},
        headers={"Cache-Control": "no-store"},
    )


async def download(request: web.Request) -> web.StreamResponse:
    record = get_file(request.match_info["token"])
    if record is None:
        raise web.HTTPNotFound(text="This download link was not found.")
    try:
        path = await cached_file(record)
    except Exception as exc:
        log.warning("Download file could not be prepared: %s", exc)
        raise web.HTTPBadGateway(text="Telegram could not prepare this file. Please retry in a few minutes.") from exc
    touch_file_cache(record["file_id"])
    content_type = mimetypes.guess_type(record["name"])[0] or "application/octet-stream"
    headers = {
        "Content-Type": content_type,
        "Content-Disposition": content_disposition(record["name"]),
        "Cache-Control": "private, no-store",
        "X-Content-Type-Options": "nosniff",
        "Accept-Ranges": "bytes",
    }
    return web.FileResponse(path, headers=headers, chunk_size=1024 * 1024)


async def health(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def cache_cleanup_loop() -> None:
    while True:
        await asyncio.sleep(3600)
        if any(not task.done() for task in FILE_TASKS.values()):
            continue
        for cached in expired_cache_paths():
            try:
                path = Path(cached).resolve(strict=False)
                if not path.is_relative_to(BOT_API_DATA_DIR):
                    log.error("Refusing to clean cache path outside configured directory: %s", path)
                    continue
                path.unlink(missing_ok=True)
                clear_cache_path(cached)
                log.info("Removed inactive local download cache %s", path.name)
            except OSError:
                log.exception("Could not clean inactive file cache %s", cached)


async def start_app() -> None:
    global HTTP, OFFSET
    initialize_db()
    OFFSET = load_offset()
    timeout = ClientTimeout(total=None, connect=30, sock_read=None)
    HTTP = ClientSession(timeout=timeout)
    app = web.Application(client_max_size=1024 * 1024)
    app.router.add_static("/static/", ROOT / "static", show_index=False, cache_max_age=86400)
    app.router.add_get("/", landing)
    app.router.add_get("/healthz", health)
    app.router.add_get("/status/{token}", file_status)
    app.router.add_get("/download/{token}", download)
    app.router.add_get("/{token}", file_page)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Download website listening on port %s", PORT)
    cleanup_task = asyncio.create_task(cache_cleanup_loop(), name="file-cache-cleanup")
    try:
        await poll_updates()
    finally:
        cleanup_task.cancel()
        await asyncio.gather(cleanup_task, return_exceptions=True)
        await runner.cleanup()
        await HTTP.close()
        HTTP = None


if __name__ == "__main__":
    try:
        asyncio.run(start_app())
    except KeyboardInterrupt:
        pass
