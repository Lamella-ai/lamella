# ADR-0027: External HTTP Calls Use Tenacity + 30s Timeout

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0006](0006-long-running-ops-as-jobs.md), [ADR-0020](0020-adapter-pattern-for-external-data-sources.md), `src/lamella/simplefin/`, `src/lamella/paperless/`, `src/lamella/ai/`

## Context

Lamella makes outbound HTTP calls to four external services: SimpleFIN
Bridge, Paperless-ngx, OpenRouter, and notification endpoints (ntfy,
Pushover). Each adapter was written independently and applies
inconsistent retry and timeout behavior. SimpleFIN already uses
tenacity. Paperless and OpenRouter have partial retry. The ntfy and
Pushover adapters have no retry at all.

The Phase 7 violation scan confirms 3 unprotected external HTTP
callers (`paperless/client.py`, `notify/pushover.py`, `notify/ntfy.py`).

A call that hangs indefinitely blocks a job worker thread. In the
single-container deployment, the job thread pool is small. One hung
call during a nightly SimpleFIN fetch can stall the entire job queue
until the container restarts.

Transient network errors, connection resets, 503s from a briefly
overloaded Paperless host, gateway timeouts from OpenRouter, are
recoverable. A blanket "no retry" policy turns recoverable errors into
user-visible failures.

## Decision

Every external HTTP call made from `src/lamella/` MUST be wrapped in a
tenacity retry policy and MUST carry an explicit timeout. The canonical
policy:

```python
tenacity.retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException,
                                   httpx.HTTPStatusError)),
    reraise=True,
)
```

Timeout on every `httpx.AsyncClient` instantiation or per-request
`timeout` kwarg: `timeout=30.0`. No call may rely on httpx's default
(which is 5 s for connect and unlimited for read).

Specific obligations:

- Retry on: 5xx responses, `ConnectError`, `TimeoutException`. The
  retry decorator MUST check `response.status_code >= 500` before
  re-raising for status errors.
- Do NOT retry on: 4xx responses (client error means the request is
  wrong; retrying will not fix it), authentication failures (401/403
  indicate a bad credential that will not change between retries).
- Retry exhaustion MUST log a structured `WARNING` with the service
  name, endpoint, attempt count, and final exception before re-raising.
  The caller (job handler) surfaces the failure in the job event log.
- A shared helper in `src/lamella/http.py` (or equivalent module) MAY
  encapsulate the decorator and client factory so adapters don't
  duplicate the policy inline.

## Consequences

### Positive
- Transient Paperless or OpenRouter hiccups no longer produce immediate
  job failures. The job log shows retry attempts, giving the user
  information about intermittent connectivity.
- Hung calls are capped at 30 s per attempt times 3 attempts, at most 90 s
  before a job worker is released. That is a known bound.
- The compliance check can enforce the policy mechanically across all
  adapters.

### Negative / Costs
- Three attempts at 30 s each means a fully hung endpoint holds a job
  thread for up to 90 s before the job surfaces a failure. For a
  single-user app this is tolerable but not invisible.
- Every new adapter author must know to use the shared helper. Without
  tooling, new adapters will drift back to bare httpx calls.

### Mitigations
- The `http.py` helper module is the single place to update if the
  policy changes (e.g., adjusting attempt count or timeout for a
  specific service).
- The AST compliance check flags bare `httpx` calls immediately on PR.

## Compliance

How `/adr-check` detects violations:

- **Missing tenacity:** AST scan `src/lamella/` for `httpx.AsyncClient(`,
  `httpx.get(`, `httpx.post(`, `httpx.request(` where the enclosing
  function or class method does not have a `@retry` decorator AND does
  not call a helper that has one. Flag every hit.
- **Missing timeout:** same scan checks that every `httpx.AsyncClient(...)`
  includes `timeout=` in its constructor args, and every per-request
  call includes `timeout=` kwarg.
- **Retry on 4xx:** grep the retry policy instantiations for
  `retry_if_exception_type`. Confirm 4xx status codes are excluded.

## References

- [ADR-0006](0006-long-running-ops-as-jobs.md): long-running operations
  run as jobs (failed retries surface in job event log)
- [ADR-0020](0020-adapter-pattern-for-external-data-sources.md): adapter
  interface contract
- `src/lamella/simplefin/client.py`: existing tenacity usage (reference
  implementation)
- `src/lamella/paperless/client.py`: needs retrofit
- `src/lamella/notify/pushover.py`, `notify/ntfy.py`: need retrofit
