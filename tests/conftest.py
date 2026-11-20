"""Shared fixtures for the Infinity File Link test suite.

Environment variables are set here **before** ``app`` is imported so that the
module-level configuration reads pick up test-safe values.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

# ── Environment for test isolation (must precede app import) ──────────────────
_test_data = tempfile.mkdtemp(prefix="ifl_test_data_")
_test_cache = tempfile.mkdtemp(prefix="ifl_test_cache_")

os.environ.update(
    {
        "BOT_TOKEN": "TEST_BOT_TOKEN_NOT_A_REAL_CREDENTIAL",
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_API_HASH": "TEST_API_HASH_NOT_A_REAL_CREDENTIAL",
        "ARCHIVE_CHAT_ID": "-1001234567890",
        "PUBLIC_BASE_URL": "https://test.example.com",
        "DATABASE_PATH": str(Path(_test_data) / "test.sqlite3"),
        "BOT_API_DATA_DIR": _test_cache,
        "BOT_USERNAME": "TestBot",
        "MAX_RETRIES": "3",
        "RETRY_BASE_DELAY": "0.01",  # fast retries in tests
    }
)

# Ensure the project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from aiohttp import web  # noqa: E402
from aiohttp.test_utils import TestClient, TestServer  # noqa: E402

import app as app_module  # noqa: E402


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def db():
    """Initialise a fresh database, yield it, then tear it down."""
    await app_module.db_init()
    try:
        yield app_module.DB
    finally:
        try:
            if app_module.DB is not None:
                await app_module.DB.execute("DELETE FROM files")
                await app_module.DB.execute("DELETE FROM bot_state")
                await app_module.DB.commit()
        except Exception:
            pass
        await app_module.db_close()


# ---------------------------------------------------------------------------
# Template fixtures
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def loaded_templates():
    """Load HTML templates into module-level globals."""
    app_module.TEMPLATE_LANDING = (
        (app_module.ROOT / "templates" / "landing.html").read_text(encoding="utf-8")
    )
    app_module.TEMPLATE_HOME = (
        (app_module.ROOT / "templates" / "home.html").read_text(encoding="utf-8")
    )


# ---------------------------------------------------------------------------
# aiohttp application & client
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def test_app(db, loaded_templates):
    """Create a test aiohttp Application with all routes."""
    application = web.Application()
    application.router.add_get("/", app_module.landing)
    application.router.add_get("/healthz", app_module.health)
    application.router.add_get("/status/{token}", app_module.file_status)
    application.router.add_get("/download/{token}", app_module.download)
    application.router.add_get("/{token}", app_module.file_page)
    return application


@pytest_asyncio.fixture
async def client(test_app):
    """aiohttp test client wired to the test application."""
    async with TestClient(TestServer(test_app)) as c:
        yield c


# ---------------------------------------------------------------------------
# Sample data helpers
# ---------------------------------------------------------------------------
@pytest.fixture
def sample_file_values():
    """Tuple of values matching the ``add_file()`` INSERT column order."""
    return (
        "test_token_abc123",  # token
        12345,  # owner_id
        "AgACAgIAAxkBAAIBN2X_test_file_id",  # file_id
        "-1001234567890",  # archive_chat_id
        42,  # archive_message_id
        "test_document.pdf",  # name
        1048576,  # size_bytes  (1 MB)
        "application/pdf",  # mime_type
        1700000000,  # created_at
    )


@pytest_asyncio.fixture
async def stored_file(db, sample_file_values):
    """Insert a sample file record and return its token."""
    await app_module.add_file(sample_file_values)
    return sample_file_values[0]


@pytest_asyncio.fixture
async def cached_download(db, sample_file_values):
    """Insert a file record with a real cached file on disk.

    Yields a dict with *token*, *file_id*, *name*, *path*, *size*, *content*.
    """
    cache_dir = Path(app_module.BOT_API_DATA_DIR)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cached = cache_dir / "test_document.pdf"
    content = b"TESTCONTENT" * 100  # 1 100 bytes
    cached.write_bytes(content)

    token = sample_file_values[0]
    file_id = sample_file_values[2]
    name = sample_file_values[5]

    values = list(sample_file_values)
    values[6] = len(content)  # real size
    await app_module.add_file(tuple(values))
    await app_module.update_file_cache(file_id, str(cached), None)

    yield {
        "token": token,
        "file_id": file_id,
        "name": name,
        "path": cached,
        "size": len(content),
        "content": content,
    }

    cached.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Session-level cleanup
# ---------------------------------------------------------------------------
def pytest_sessionfinish(session, exitstatus):
    for d in (_test_data, _test_cache):
        shutil.rmtree(d, ignore_errors=True)
