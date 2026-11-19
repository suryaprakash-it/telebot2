# Infinity File Link — Verification Report

**Verification date:** 2026-11-19  
**Source:** `Infinity-File-Link-FINAL.zip` extracted to a clean temporary directory  
**Application source revision in the repository:** `85c924e` plus the uncommitted source and regression-test changes included in the archive  
**Status:** Local verification passed; production acceptance is not verified.

## Test-count discrepancy

The ZIP's previous copy of this report recorded **139 passed**. That result describes an earlier test-suite state and was not refreshed after the concurrency regression tests were expanded. The current `tests/test_stream_concurrency.py` has 23 tests; the earlier suite had 7, adding 16 tests. The complete current ZIP suite reports **155 passed** (139 + 16). The source files in the ZIP match the working project; the discrepancy was stale report content, not a different application/test source.

## Exact ZIP verification

The final archive `Infinity-File-Link-FINAL.zip` was extracted into a clean temporary directory and commands were run from the extracted project root.

```text
python -m pytest -q
........................................................................ [ 46%]
........................................................................ [ 92%]
...........                                                              [100%]
155 passed in 3.29s
PYTEST_EXIT_CODE 0

python -m compileall -q app.py tests
COMPILEALL_EXIT_CODE 0

python -m pip check
No broken requirements found.
PIP_CHECK_EXIT_CODE 0
```

No actual Telegram downloads, Render service inspection, or deployment were performed. The 2 GiB concurrency tests use mocked Telegram clients and a counting sink; they do not allocate real 2 GiB files or establish real-world throughput.

## Concurrency behavior

Every HTTP GET or Range request takes one global download permit. A request from the same file link also counts toward that file's limit. With the documented defaults, global capacity is 10 and per-file capacity is 5. The per-file value is automatically capped to at most half of the global capacity (minimum 1). A per-file rejection returns HTTP 429 with `Retry-After: 1`; global exhaustion returns HTTP 503 with `Retry-After: 1`. Admission is immediate and does not create a waiting queue.

The streaming defaults are 3 active direct streams, 4 pipelined 512 KiB Telegram chunks per stream, and 8 shared Telegram requests in flight. At those limits the maximum stream-batch payload is about 6 MiB, excluding Python objects and socket buffers. Each semaphore is created per event loop. Cache materialization is limited to one job by default; requests for the same file share a shielded task, so cancellation of one browser does not cancel work another request is awaiting.

The request diagnostics include a request ID, a truncated SHA-256 file-token identifier, active counts, limits and availability, resource wait time, cache hit/miss, completion, cancellation reason, and exception type. Full download tokens, bot tokens, and API hashes are not included in these diagnostics. Render's `RENDER_GIT_COMMIT` and effective limits are logged at app startup when available.

## Regression coverage

The suite includes mocked checks for concurrent logical 2 GiB streams, two files with four concurrent Range requests each, eight simultaneous Telegram chunk requests, same-file parallel requests, failure and browser-cancellation isolation, borrowed-sender reference counting, FloodWait, expired file-reference refresh, cancellation while waiting for a semaphore, permit restoration, cache/direct overlap, and capacity recovery. Existing HEAD, 206, 416, Range resume, cache fallback, bot, database, UI, and deployment-configuration tests remain in the full run.

## Recommended Render settings

Use these as conservative starting values for a reference 1 CPU / 2 GB service:

```text
DOWNLOAD_CONCURRENCY=10
PER_FILE_DOWNLOAD_CONCURRENCY=5
STREAM_CONCURRENCY=3
STREAM_PIPELINE_CHUNKS=4
STREAM_REQUEST_CONCURRENCY=8
CACHE_CONCURRENCY=1
```

The stream-batch payload bound is a code-derived estimate, not measured Render memory use. Actual Render CPU, memory, and outbound-bandwidth metrics were unavailable; review them during a real transfer before changing limits. This repository has no Render service export or dashboard configuration, so production environment overrides and deployed commit remain unknown. The Render Dockerfile builds Telegram Bot API from an unpinned upstream checkout, which can change between image rebuilds.

## Deployment and limitations

No deployment was attempted. Deploy the updated source using `render.Dockerfile`, preserve the persistent disk at `/var/lib/telegram-bot-api`, and verify startup logs show the expected commit and limits. Then test two separate large files and interrupted Range resume with real Telegram and the live Render service. Until those checks are performed, real Telegram and Render behavior remain unverified.


