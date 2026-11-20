# Telegram download concurrency review

**Review date:** 2026-11-20
**Result:** Direct-download concurrency is bounded. The observed short FloodWait
messages are not, by themselves, evidence of an uncontrolled retry loop. Two
retry/logging fixes were justified and made; deployment concurrency values were
not raised.

## Findings

### HTTP download admission

- Public downloads reserve capacity synchronously in the event-loop thread
  before the handler's first await. The global limit defaults to 16 active
  transfers; requests over it receive `503 Service Unavailable`.
- A separate per-link limit defaults to eight active transfers for one token;
  requests over it receive `429 Too Many Requests`. Both limits reject excess
  requests immediately instead of adding them to a semaphore wait queue.
- The reservations cover the complete aiohttp handler operation. The handler
  streams the response before returning, so the `finally` cleanup runs only
  after the response finishes or streaming exits on disconnect/error. The
  cleanup decrements each reservation once, including cancellation paths.
- HTTP tests exercise simultaneous burst admission, both rejection statuses,
  another file link proceeding while one link is saturated, exception and
  cancellation recovery, and client disconnect cleanup.

### Telegram request volume

- Telegram reads use aligned 512 KiB `iter_download` requests. Each HTTP range
  stream fetches up to four chunks per batch (`STREAM_PIPELINE_CHUNKS=4`).
- The existing stream semaphore allows up to eight direct HTTP streams, and the
  shared chunk semaphore allows at most 16 in-flight Telegram chunk requests
  across all streams (`STREAM_REQUEST_CONCURRENCY=16`). The public transfer
  cap is 16. Two simultaneous full downloads therefore cause independent
  Telegram reads for each browser, but their in-flight reads remain under the
  shared cap. Overlapping ranges may read the same chunk more than once.
- The code does not coalesce identical in-flight ranges between browsers. This
  is bounded duplicate work, not an unbounded request fan-out. Nothing in the
  supplied log description establishes that the existing limits are too high,
  so concurrency settings were left unchanged.

### Cancellation isolation

Each HTTP handler owns its batch tasks. Cancelling one chunk operation closes
its Telethon iterator and releases its semaphore slot; it does not cancel
another handler's task group. Regression tests cancel one chunk operation and
close one actual HTTP response while verifying another full download finishes.

### HTTP ranges and resume

The direct Telegram route parses each browser range, rounds Telegram reads down
to the 512 KiB boundary, then slices the returned chunk to the exact requested
bytes. Each HTTP request has its own offsets, so a later range can resume at a
different position while another browser is downloading. Direct-stream tests
verify exact `206` bodies and `Content-Range` headers at unaligned offsets.
Existing cached-route tests cover ranges, validators, HEAD, and concurrent
range reads.

### Completed-file cache behavior

A valid local Bot API cache path is checked before direct streaming. Full and
range cache hits are served locally and do not call Telethon; the new test
asserts that behavior with concurrent cache-hit range requests.

There is an important scope limit: successful direct Telethon streams are not
saved as completed local cache files. The handler only refreshes cache activity
metadata after streaming. Thus a later browser request for a file that has
never been materialized through the local Bot API fallback will read its bytes
from Telegram again. This matches the current direct-to-browser streaming path,
but it is not a write-through cache. Adding one would be a separate storage and
product behavior change, so this review does not introduce it.

### FloodWait and logging

Telethon 1.45 automatically sleeps for FloodWait durations up to its default
60-second threshold. The observed one-to-two-second `InvokeWithoutUpdatesRequest`
messages come from Telethon's routine INFO log in `telethon.client.users`; the
library is already sleeping for those waits. The app now filters that logger
below WARNING, leaving Telethon warnings and errors visible.

For FloodWaits that reach the app retry path, the code previously capped the
wait at 30 seconds. That could retry before Telegram's requested duration. It
now waits for the full reported duration. Batch retries also retain successful
chunks and re-request only failed offsets, avoiding duplicate reads after a
partial batch failure. Ordinary browser disconnect messages moved from INFO to
DEBUG; completed-stream throughput remains available at INFO.

## Changes made

1. Removed the 30-second FloodWait cap.
2. Kept successful chunk results and retried only failed offsets.
3. Filtered routine Telethon FloodWait INFO messages below WARNING.
4. Moved expected HTTP client disconnect messages to DEBUG.
5. Added regression tests for two simultaneous full downloads, cancellation
   isolation, independent HTTP ranges, cache-hit bypass, and FloodWait retry
   timing/partial-batch behavior.
6. Documented that direct successful streams do not populate a complete-file
   cache.

No production configuration, concurrency limits, or production data were
changed. No live Telegram credentials were used; all Telegram traffic in these
tests is simulated. This report verifies the local control flow, not live
Telegram service behavior.

## Verification

Focused direct-stream regression tests: **7 passed** in the original review.
Current complete project suite (2026-11-20): **143 passed**.
The tests are automated with a fake Telethon client and aiohttp test server;
they are not live Telegram integration tests.
