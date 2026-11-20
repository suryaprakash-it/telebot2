# Phase 2 Verification Report

**Project:** Infinity File Link Telegram bot
**Verification date:** 2026-11-20
**Final readiness: NOT READY**

Local regression and Docker checks are verified. No production service or credentials were used. Live Telegram and Render staging could not be performed because staging credentials and Render access are missing. The actual Telegram download speed remains unmeasured. See [STAGING_DEPLOYMENT.md](STAGING_DEPLOYMENT.md) for the secure setup procedure.

## Verification results

| Area | Status | Evidence and limits |
| --- | --- | --- |
| Docker Desktop Linux engine | **VERIFIED** | Docker Desktop 29.9.3 was installed but not running. WSL listed `docker-desktop` as stopped and the `desktop-linux` pipe was missing. Starting the per-user app restored the engine. |
| Production Docker image | **VERIFIED** | `docker build -t infinity-file-link:phase2 -f Dockerfile .` succeeded. Image ID: `sha256:0e12dc29b03ddd4349d2d81dbfa099535804827f103928b5f86d83f55d0936a5`. Build context: 53.77 kB. |
| Render Docker image | **VERIFIED** | `docker build -t infinity-file-link-render:phase2 -f render.Dockerfile .` succeeded. Image ID: `sha256:b77163a5968ed1a35fefa72ac2105eba69775f1314dc21d48b709e382380fc9e`. Build context: 2.11 kB; the Bot API CMake layer was cached. |
| Image runtime | **VERIFIED** | Image uses `/app`, starts `python app.py`, imports runtime dependencies, and `pip check` reports no broken requirements. No env files, databases, Python caches, ZIPs, logs, or temp files were present under `/app`. |
| Isolated container startup/health | **VERIFIED** | Placeholder-only environment with `--network none`; container stayed running and Docker health was `healthy`. In-container `/healthz` returned HTTP 200 with `{"status":"ok"}`. Placeholder token/hash were absent from logs. Bot API DNS failures were expected with networking disabled. |
| Render image startup/health | **VERIFIED** | Isolated Render image started the local Bot API on port 8081, started the app on Render's `PORT=10000`, returned HTTP 200 from `/healthz`, and reached Docker health `healthy`. Placeholder token/hash were absent from logs. |
| Graceful shutdown | **VERIFIED** | The app image exited with code 0 on `docker stop --timeout 15`. The Render wrapper handled SIGTERM, stopped child processes, and exited with code 143 as configured. |
| Container streaming and integrity | **VERIFIED** | Cached-file handler streamed 64 MiB in bounded chunks. `Range: bytes=123-456` returned exact `206` bytes. Full response SHA-256 matched `3b6a07d0d404fab4e23b6d34bc6696a6a312dd92821332385e5af7c01c421351`. |
| Container concurrency/cancellation | **VERIFIED** | Eight open same-link responses caused the ninth to receive `429`; a different link completed during saturation. Closing the clients released capacity. Sixteen active different links caused the seventeenth to receive `503`; closing clients released capacity. Follow-up downloads recovered. |
| Local cached-stream performance | **VERIFIED (cache path only)** | Warm 64 MiB local-file streams over Docker loopback; measurements below. These do not measure Telegram or a public Render route. |
| Actual Telegram download speed | **NOT RUN** | No Telegram credentials or staging test archive were available. The local cache benchmark is not used as a proxy. |
| Live Telegram integration | **NOT RUN** | No `BOT_TOKEN`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, or archive chat was available in environment variables or a root `.env`. No real Telegram authentication, downloads, retries, or 1DM resume was attempted. No Telegram messages were sent. |
| Render staging | **BLOCKED** | No Render connector/service access or staging Telegram credentials were available. No service was created or deployed. Exact isolated setup and acceptance checks are in `STAGING_DEPLOYMENT.md`. |
| Linux regression environment | **NOT RUN** | Regression ran on Windows Python 3.12. The Linux image was built and smoke-tested; pytest was not run inside it. |
| Lint/type checks | **NOT RUN** | No lint or type-check tool/configuration exists in the project. |
| Git/secret audit | **BLOCKED** | No `.env`, session, or database file was found in the tree; `.env.example` contains placeholders. This folder has no `.git` metadata, so tracked/staged secret checks could not be performed. |

## Cached-stream performance

Each measurement transferred 64 MiB per stream from a warm local cache file to an HTTP client inside the isolated container. Throughput is aggregate MiB/s; time to first byte is measured from request start to first response body byte.

| Concurrent streams | Payload | Wall time | Aggregate throughput | Mean first byte |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 64 MiB | 0.046 s | 1,383.38 MiB/s | 1.66 ms |
| 2 | 128 MiB | 0.132 s | 969.14 MiB/s | 3.41 ms |
| 4 | 256 MiB | 0.267 s | 958.87 MiB/s | 7.34 ms |
| 8 | 512 MiB | 0.504 s | 1,015.79 MiB/s | 15.12 ms |

During the benchmark the container cgroup reported 1.47 CPU-seconds over 0.949 seconds (155.08% of one CPU), 255.14 MiB current memory, and 307.81 MiB peak memory. These figures include the app and test client; the container had no CPU quota. The benchmark generated the test file on disk and streamed in chunks without loading the whole file into application memory.

This cache-only result cannot explain a real Telegram stream averaging about 5 MB/s. Direct Telegram speed also depends on Telegram response time, network path, host resources, and recipient throughput. There was no direct-Telegram baseline, so no performance defaults were changed.

## Docker Desktop recovery

Docker Desktop was installed at `%LOCALAPPDATA%\Programs\DockerDesktop\Docker Desktop.exe`, but it was not running; WSL listed `docker-desktop` as stopped and the `desktop-linux` pipe did not exist. Starting the app restored the engine. If it stops again, run in PowerShell:

```powershell
wsl --update
wsl --shutdown
$desktop = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\Docker Desktop.exe'
Start-Process -FilePath $desktop -WindowStyle Hidden
docker context use desktop-linux
docker info --format '{{.ServerVersion}}'
```

See [Docker Desktop on Windows](https://docs.docker.com/desktop/setup/install/windows-install/) and [the WSL 2 backend](https://docs.docker.com/desktop/features/wsl/) for the current official setup steps.

## Regression checks

| Check | Status | Result |
| --- | --- | --- |
| `python -m compileall -q .` | **VERIFIED** | Exit code 0. |
| `python -m pytest -q` | **VERIFIED** | 143 passed, 0 failed, 0 skipped (2.08 s). |
| `python -m pip check` | **VERIFIED** | No broken requirements found. |
| Docker container regression | **VERIFIED** | Production image: isolated startup, health, cached stream, range, integrity, admission, cancellation, and graceful shutdown. Render image: isolated Bot API/app startup, health, local API port, log-secret check, and SIGTERM wrapper shutdown. Both used placeholder config and no network. |

## Staging handoff

Create a separate service named `infinity-file-link-staging`, use `render.Dockerfile`, set `/healthz` as the health check, leave the image start command unset, and attach a new disk at `/var/lib/telegram-bot-api`. Use only staging bot/API credentials, a private staging archive, staging URL, and staging storage. Keep automatic deploys disabled until the target branch, disk, and environment are reviewed. Required variables are `BOT_TOKEN`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `ARCHIVE_CHAT_ID`, `PUBLIC_BASE_URL`, and `BOT_USERNAME`; storage variables are `DATABASE_PATH`, `BOT_API_DATA_DIR`, `TELEGRAM_API_BASE_URL`, and `TELETHON_SESSION_PATH`. Store secrets in Render's environment settings, never in chat or source files. See [STAGING_DEPLOYMENT.md](STAGING_DEPLOYMENT.md) and Render's official docs for [Web Services](https://render.com/docs/web-services), [Docker](https://render.com/docs/docker), [Health Checks](https://render.com/docs/health-checks), and [Persistent Disks](https://render.com/docs/disks).

## Exact commands executed

`wsl --status`; `wsl --list --verbose`; `docker version`; `docker context ls`; `$desktop = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\Docker Desktop.exe'; Start-Process -FilePath $desktop -WindowStyle Hidden`; `docker info --format '{{.ServerVersion}}'`; `docker build -t infinity-file-link:phase2 -f Dockerfile .`; `docker build -t infinity-file-link-render:phase2 -f render.Dockerfile .`; `docker run --detach --name infinity-file-link-phase2 --network none` with required placeholder-only variables; `docker exec infinity-file-link-phase2 python -c "import urllib.request; r=urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=3); print(r.status, r.read().decode())"`; `docker cp .\phase2_probe.py infinity-file-link-phase2:/tmp/phase2_probe.py`; `docker exec infinity-file-link-phase2 python /tmp/phase2_probe.py`; `docker stop --timeout 15 infinity-file-link-phase2`; `docker run --detach --name infinity-file-link-render-phase2 --network none` with placeholder-only variables; `docker exec infinity-file-link-render-phase2 python -c` for `/healthz` and local port 8081; `docker stop --timeout 15 infinity-file-link-render-phase2`; `python -m compileall -q .`; `python -m pytest -q`; `python -m pip check`.

The temporary probe was removed after use. Its first attempt used a global limit of one and hit the expected `503` before the per-link check; the check was rerun with global limit 16 and per-link limit eight. The results above are from the corrected run.
