# Telegram File Link Bot

A small self-hosted Telegram bot that copies incoming files into a private Telegram channel and creates a permanent, shareable download page. The page is served by your server and streams Telegram file chunks directly to the browser, with HTTP byte ranges for download managers and resume. It avoids waiting for a full server-side copy before the download can start.

## What it does

- Accepts files sent to the bot or forwarded to it.
- Copies each file into a private channel you control, so the archive message is retained in Telegram until you delete it.
- Creates an unguessable bearer link with no expiration timer. The browser page and download button appear immediately, and the app starts streaming Telegram chunks without first staging a complete copy.
- Serves files from your domain with `Content-Disposition: attachment`, `HEAD`, and byte-range support for browser download managers and resume. Telegram data streams to the browser as chunks arrive.
- Keeps link metadata in SQLite. The original file remains in your Telegram archive; downloaded local cache copies are automatically cleared after 24 hours without a download.
- Includes an optional allowlist for who can upload through the bot.

## Limits and speed

Telegram permits uploads up to 2 GB for regular accounts and 4 GB for Premium accounts. The official cloud Bot API only downloads files up to 20 MB. Its local Bot API server can download files without a size limit, but its documented upload limit is 2,000 MB. This project uses `copyMessage` to copy an already received Telegram message into the archive instead of uploading the media again. The web app uses Telethon's MTProto chunked download client to stream data as the browser requests it; the client disables update delivery so the local Bot API continues polling for messages. Premium is needed for a user to send a 4 GB file to Telegram in the first place. Check 4 GB forwarding with your deployed Bot API version before relying on it for important files.

The download endpoint does not impose a speed cap. It cannot guarantee 10 MB/s: download speed depends on Telegram's delivery, the host's network, the public reverse proxy, and the recipient's connection. Sustaining 10 MB/s requires at least 80 Mbps of usable throughput at every part of that path. Browser range support allows compatible download tools to resume and request parts of the file; it does not create bandwidth.

## Setup

1. Create a bot with [@BotFather](https://t.me/BotFather) and create a **private channel** for the archive. Add the bot as an administrator with permission to post messages. Copy the channel's numeric ID (usually starts with `-100`).
2. Get a Telegram API ID and hash from [my.telegram.org](https://my.telegram.org/apps). These are used by Telegram's local Bot API server and the direct chunk streaming client.
3. Copy `.env.example` to `.env` and fill in the bot token, API credentials, channel ID, bot username, domain name, and public HTTPS URL. Point the domain's DNS to your server and allow inbound ports 80 and 443. Caddy in the Compose file obtains and renews HTTPS certificates automatically.
4. Before moving this bot from Telegram's cloud Bot API to a local Bot API server for the first time, log it out of the cloud endpoint once:

   ```sh
   curl "https://api.telegram.org/bot${BOT_TOKEN}/logOut"
   ```

   Set `BOT_TOKEN` in your shell first, or substitute the token directly. This is a one-time migration step. Telegram documents a 10-minute wait before logging back into the cloud Bot API after this call.

5. Build and start the app:

   ```sh
   docker compose up --build -d
   ```

6. The Compose file routes public traffic through Caddy to the app. Keep `PUBLIC_BASE_URL` set to `https://` plus `PUBLIC_HOST`. The local Bot API service must not be exposed publicly. If you already run a reverse proxy, route it to the app on port 8080 and remove or disable the Caddy service.

Open the bot in Telegram and send or forward a file. The bot replies with its download page. Anyone who has the link can download that file, so treat links as private bearer credentials.

## Deploying on Render

The Compose hostname `telegram-bot-api` only exists inside Docker Compose. If the app alone is deployed to Render, DNS fails with `Cannot connect to host telegram-bot-api:8081`.

For Render, run the local Bot API and download app in one Web Service. The local API handles bot updates and archive copies; the app also starts a Telethon stream connection for browser downloads.

1. Set the service's Dockerfile path to `render.Dockerfile` and keep it as a **Web Service**. Clear any Render Start Command or Docker Command override so the Dockerfile command runs both processes.
2. Add `BOT_TOKEN`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `ARCHIVE_CHAT_ID`, `PUBLIC_BASE_URL`, and `BOT_USERNAME` in the Render environment settings.
3. Attach a persistent disk at `/var/lib/telegram-bot-api`. The Render Dockerfile defaults `DATABASE_PATH`, `BOT_API_DATA_DIR`, and `TELEGRAM_API_BASE_URL` to paths and the localhost API endpoint on that disk. The Telethon session is saved beside the SQLite database so it survives restarts. If you already set these variables in Render, change them to `/var/lib/telegram-bot-api/files.sqlite3`, `/var/lib/telegram-bot-api`, and `http://127.0.0.1:8081` respectively.
4. Redeploy. `run-render.sh` starts the local Telegram Bot API, waits until port 8081 is accepting connections, then starts the download app. Render supplies `PORT` for the public web server. In the deploy logs, confirm `Telegram Bot API is listening` appears. If the API process exits first or the port never opens, inspect the earlier Telegram Bot API startup lines and verify `TELEGRAM_API_ID` and `TELEGRAM_API_HASH`; the app will now stop with a direct startup error instead of retrying localhost forever.

This approach avoids the unresolved `telegram-bot-api` hostname. The Telethon session file is sensitive authentication data; keep the persistent disk private and do not add session files to source control. Persistent disks may require a paid Render plan; see Render's [disk guide](https://render.com/docs/disks). Do not use the public `https://api.telegram.org` endpoint for this large-file setup: its Bot API download limit is 20 MB.

## Configuration

| Variable | Purpose |
| --- | --- |
| `BOT_TOKEN` | Token from @BotFather. |
| `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` | Credentials for the local Bot API server and the direct streaming connection. |
| `ARCHIVE_CHAT_ID` | Private channel or supergroup where copied messages are retained. |
| `PUBLIC_BASE_URL` | Public HTTPS origin used in generated links. |
| `BOT_USERNAME` | Username shown on the landing page. |
| `ALLOWED_TELEGRAM_USER_IDS` | Optional comma-separated IDs. Empty allows any Telegram user to upload. |
| `MAX_FILE_BYTES` | Maximum accepted size; defaults to 4 GiB. |
| `CACHE_TTL_HOURS` | Hours without a download before a local cache copy is removed; defaults to 24. The Telegram archive and link remain. |
| `CACHE_CONCURRENCY` | Number of fallback local Bot API cache jobs to run simultaneously; defaults to 1. |
| `STREAM_CONCURRENCY` | Number of direct Telegram streams to serve at once; defaults to 2. |
| `TELETHON_SESSION_PATH` | Path prefix for the persistent Telethon bot session; defaults to a file beside `DATABASE_PATH`. |
| `DATABASE_PATH` | SQLite metadata database. |
| `BOT_API_DATA_DIR` | Shared local Bot API directory where downloaded files are cached. |
| `PUBLIC_HOST` | DNS name used by Caddy for the HTTPS download site. |
| `PORT` | Optional loopback-only host port for the app; Caddy uses the private Compose network. |

## Storage and operations

The channel is the durable archive. Back up the SQLite database to preserve generated links across host failures. Local cache copies use disk space and are automatically removed after the configured idle period; the bot can fetch them from Telegram again when a link is opened. Provision enough free disk for files currently being cached. Deleting an archive message or its database row breaks the corresponding link. There is no automatic link expiry.

The app's `/healthz` endpoint reports whether the HTTP service is alive. Keep the bot token, API hash, database, and download links private. Anyone with a bearer link can download its file.

## API notes

- `GET /<token>`: file information and download page.
- `GET /download/<token>`: attachment; supports `Range` requests.
- `HEAD /download/<token>`: attachment metadata.
- `GET /healthz`: liveness check.

## References

- [Telegram Bot API: local server and file limits](https://core.telegram.org/bots/api#using-a-local-bot-api-server)
- [Telegram FAQ: file sizes and cloud storage](https://www.telegram.org/faq)
- [Telegram Premium: 4 GB uploads](https://telegram.org/blog/700-million-and-premium)
