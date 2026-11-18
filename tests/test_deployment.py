"""Regression checks for deployment image settings and startup contracts."""
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def test_production_and_render_images_define_health_checks():
    production = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    render = (ROOT / "render.Dockerfile").read_text(encoding="utf-8")
    assert "HEALTHCHECK" in production
    assert "/healthz" in production
    assert "HEALTHCHECK" in render
    assert "os.getenv('PORT', '10000')" in render


def test_render_bot_api_build_uses_memory_bounded_parallelism():
    render = (ROOT / "render.Dockerfile").read_text(encoding="utf-8")
    assert "cmake --build /src/build --target install -j1" in render
    standalone = (ROOT / "telegram-bot-api.Dockerfile").read_text(encoding="utf-8")
    assert "cmake --build /src/build --target install -j1" in standalone


def test_render_startup_waits_for_bot_api_and_runs_app():
    startup = (ROOT / "run-render.sh").read_text(encoding="utf-8")
    bot_api = (ROOT / "run-telegram-bot-api.sh").read_text(encoding="utf-8")
    assert '"$api_port"' in startup
    assert "wait -n \"$api_pid\" \"$app_pid\"" in startup
    assert '--api-id="${TELEGRAM_API_ID:?TELEGRAM_API_ID is required}"' in bot_api
    assert '--api-hash="${TELEGRAM_API_HASH:?TELEGRAM_API_HASH is required}"' in bot_api
    assert "--dir=\"${BOT_API_DATA_DIR:-/var/lib/telegram-bot-api}\"" in bot_api


def test_compose_app_can_expire_shared_bot_api_cache():
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    app = compose.split("\n  app:\n", 1)[1].split("\n  caddy:\n", 1)[0]
    assert "telegram-files:/var/lib/telegram-bot-api\n" in app
    assert "telegram-files:/var/lib/telegram-bot-api:ro" not in app
    assert "healthcheck:" in app and "/healthz" in app
