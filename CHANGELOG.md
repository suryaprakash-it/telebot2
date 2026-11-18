# Changelog

## Unreleased — reliability and verification fixes

- Kept direct Telegram range transfers bounded by the existing per-stream and
  shared request limits. Batch retries now retain successful chunks and retry
  only failed offsets; FloodWait retries wait the full reported duration.
- Filtered Telethon's routine FloodWait INFO messages below WARNING and moved
  normal browser disconnects to DEBUG; useful stream throughput remains at
  INFO, and warnings/errors stay visible.
- Added direct-stream tests for concurrent full downloads, HTTP client
  cancellation isolation, independent resumable ranges, cache-hit bypass, and
  exact FloodWait retry timing. Confirmed cached fallback hits avoid Telegram
  requests; successful direct streams themselves are not written to cache.
- Consolidated cached downloads into one asynchronous streaming path with
  bounded 1 MiB reads, strong `If-Range` matching, weak `If-None-Match`
  comparison, correct `HEAD` range metadata, empty-file handling, and accurate
  `200`, `206`, `304`, and `416` responses.
- Kept cached files protected while a response is reading them. Cache expiry
  skips active streams, interrupted streams release their file handles, and
  successful cached reads refresh the cache activity time.
- Added a configurable cap for concurrent public transfers across cached and
  Telegram-backed downloads. Excess transfers receive a retryable `503` with
  `Retry-After` instead of entering an unbounded wait queue.
- Reduced both local Bot API image builds to one CMake job after the two-job
  Render build lost its BuildKit connection on this constrained Docker host.
- Added Docker health checks against `/healthz` for both app images and the
  Compose app service. Container SIGTERM now cancels the service task so its
  shutdown path closes HTTP, Telethon, and SQLite resources.
- Made the Compose app's shared Bot API cache volume writable so the existing
  expiry worker can remove idle cached files.
- Prevented a failed SQLite offset commit from advancing the in-memory
  Telegram polling offset. Updates are processed in order, update receipt and
  offset persistence are atomic, and SQLite access is serialized.
- Added source chat/message identifiers for newly processed files. Replayed
  Telegram messages reuse an already-persisted link instead of copying the
  same message into the archive again. Existing databases receive nullable
  columns and a partial unique index without deleting existing rows.
- Added startup/shutdown regression coverage and fixed the duplicate GET/HEAD
  route registration that prevented `start_app()` from starting.
- Removed internal cache error text from the public status response and
  suppressed URL-bearing aiohttp exception context so a Bot API token cannot
  escape through request error logs.
- Documented Python 3.12 setup, test commands, and a non-destructive SQLite
  integrity-check and restore procedure.
- Added regression coverage for weak and matching validators, empty and HEAD
  ranges, interrupted cache stream cleanup, large bounded downloads, expired
  Telegram file references, database concurrency, and failed offset saves.

## Existing behavior retained

- Telegram archive copies and permanent bearer download links.
- Direct Telethon chunk streaming with retries and file-reference refresh.
- Local Bot API staging fallback, configured file-size limit, and idle cache
  expiry.
- Existing Docker Compose and Render deployment architecture and environment
  variable names.
