# Isolated Render Staging Deployment

**Status:** Staging is not deployed or verified. This workspace has no Render
service connection and no non-production Telegram credentials. Use these steps
to create a separate staging service without touching production.

## Create the service

1. In Render, create a **new Web Service** connected to a staging-only branch
   or repository. Name it `infinity-file-link-staging`; do not select or edit a
   production service.
2. Set the Dockerfile path to `render.Dockerfile`, the build context to the
   repository root, and the health check path to `/healthz`. Leave the start
   command unset so the image runs `/app/run-render.sh`.
3. Disable automatic deploys while configuring the service. Attach a new
   staging-only persistent disk at `/var/lib/telegram-bot-api`. Never mount a
   production disk or database. Size the disk for the test files you intend to
   keep; this deployment requires persistent storage for SQLite, the local Bot
   API archive, and the Telethon session.
4. Set these environment variables in the **staging service only**:

   | Variable | Staging value |
   | --- | --- |
   | `BOT_TOKEN` | Token for a newly created staging bot. |
   | `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` | Credentials for a staging Telegram application. |
   | `ARCHIVE_CHAT_ID` | ID of a new private staging archive channel where the staging bot is an administrator. |
   | `PUBLIC_BASE_URL` | The HTTPS URL assigned to the staging Render service. |
   | `BOT_USERNAME` | Username of the staging bot, without `@`. |
   | `DATABASE_PATH` | `/var/lib/telegram-bot-api/files.sqlite3` |
   | `BOT_API_DATA_DIR` | `/var/lib/telegram-bot-api` |
   | `TELEGRAM_API_BASE_URL` | `http://127.0.0.1:8081` |
   | `TELETHON_SESSION_PATH` | `/var/lib/telegram-bot-api/telegram-stream` |
   | `ALLOWED_TELEGRAM_USER_IDS` | Your own test Telegram user ID(s), comma-separated. |
   | `LOG_LEVEL` | `INFO` |

   Store secrets in Render's environment settings. Do not put them in the
   repository, deployment logs, build arguments, or a ZIP file. Do not reuse
   production bot, archive, database, disk, or session values.
5. Review the service name, branch, health path, disk mount, and staging-only
   variables. Then trigger a deploy for this staging service only. Render's
   Docker service setup supports repository-backed Dockerfiles, environment
   variables, health checks, and persistent disks in the service settings:
   [Web Services](https://render.com/docs/web-services),
   [Docker](https://render.com/docs/docker),
   [Health Checks](https://render.com/docs/health-checks), and
   [Persistent Disks](https://render.com/docs/disks).

## Securely provide staging credentials

The app does not automatically load a dotenv file. For a local Docker staging
run, create `.env.staging` from `.env.example`, replace the placeholders only
with staging values, and pass it with Docker's `--env-file .env.staging`
option. `.env*` files are excluded by `.gitignore` and `.dockerignore`; keep
the file in the project root, do not commit it, and do not paste its contents
into chat or logs. Mount a separate local staging data directory for that
container. For Render, enter the same staging-only values in the staging
service's environment settings; Render does not read the local file.

The six required secrets/config values are `BOT_TOKEN`, `TELEGRAM_API_ID`,
`TELEGRAM_API_HASH`, `ARCHIVE_CHAT_ID`, `PUBLIC_BASE_URL`, and `BOT_USERNAME`.
The storage/service values are `DATABASE_PATH`, `BOT_API_DATA_DIR`,
`TELEGRAM_API_BASE_URL`, and `TELETHON_SESSION_PATH`. Render supplies `PORT`.
Restrict uploads to your own test Telegram user with
`ALLOWED_TELEGRAM_USER_IDS`. Generate a new staging bot, private archive, and
Telegram app credentials; never reuse production bot or archive credentials.

## Staging acceptance checks

After Render reports the service healthy:

1. Confirm the deploy log reports `Telegram Bot API is listening` and the
   download service starts. Check that no tokens or hashes appear in logs.
2. Request `https://<staging-service>.onrender.com/healthz`; expect HTTP 200
   and `{"status":"ok"}`.
3. Use only the staging bot and your allowlisted test account. Send a small
   test file, download it, and compare its SHA-256 with the original.
4. Repeat with a larger test file. Exercise an HTTP byte range, resume the
   same staging link in 1DM, and compare the final SHA-256.
5. Start parallel downloads of the staging test link and of separate staging
   links. Confirm the configured `429` and `503` caps, and cancel one client
   while another download continues.
6. Restart the **staging service only**. Confirm the staging link and archive
   remain available from the staging disk, then repeat the checksum check.

Do not call Telegram's cloud `logOut` endpoint as part of staging. That is a
bot migration action and is not needed to verify this isolated service. Do not
send messages to other users or touch existing archive messages.

## Current limitations

The local Docker container was verified with placeholder credentials, no
network, and temporary test files. That confirms app startup, cache streaming,
HTTP ranges, admission responses, cancellation cleanup, and graceful shutdown;
it does not authenticate with Telegram or prove persistent-disk behavior on
Render. Real Telegram integration and Render staging remain blocked until
separate staging credentials, a staging archive, and Render service access are
available.
