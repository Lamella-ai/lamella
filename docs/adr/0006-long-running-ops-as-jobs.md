# ADR-0006: Long-Running Operations Run as Background Jobs

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** [ADR-0005](0005-htmx-endpoints-return-partials.md), `CLAUDE.md` ("Long-running operations run as jobs with a progress modal"), `src/lamella/jobs/`

## Context

A single-user FastAPI app still has one request thread per active
request. A route handler that calls OpenRouter, iterates 200
SimpleFIN transactions, or walks the entire Paperless index blocks
that thread for tens of seconds. From the browser's perspective
the form submission is pending; the user has no feedback and cannot
navigate away. A killed browser tab or a network hiccup drops the
result silently.

Beyond UX, a handler holding the SQLite write lock for >5 s during
classification blocks every other concurrent read from the same
connection, including the status-check XHR that would have shown
progress.

## Decision

Any handler that (a) calls AI/OpenRouter, (b) hits an external API
(Paperless, SimpleFIN), or (c) iterates N items where N could be
50+ MUST run as a background job via
`app.state.job_runner.submit(kind, title, fn, ...)`. The route
MUST return the `partials/_job_modal.html` partial immediately,
pointed at the new job id. It MUST NOT block waiting for the
worker to finish.

Specific obligations:

1. The worker function signature is `fn(ctx: JobContext) -> dict | None`.
2. Workers MUST call `ctx.raise_if_cancelled()` at each item boundary.
3. Workers MUST call `ctx.emit(message, outcome=...)` for each
   meaningful event (success, error, not_found, info).
4. Workers MUST call `ctx.set_total(n)` before the loop when N is
   known.
5. The triggering form MUST use `hx-post` with
   `hx-target="body" hx-swap="beforeend"` so the modal overlays the
   current page.
6. The browser polls `/jobs/{id}/partial` every 1 s. This endpoint
   MUST return the partial per [ADR-0005](0005-htmx-endpoints-return-partials.md).

Already-ported handlers (as of migration 040): `/search/receipt-hunt`,
`/search/bulk-apply`, `/audit/run`, `/simplefin/fetch`,
`/settings/data-integrity/scan`, `/settings/rewrite`,
`/import/{id}/ingest`, `/status/paperless/full-sync`,
`/status/vector-index/rebuild`, `/recurring/scan`.

## Consequences

### Positive
- The browser gets a progress modal with live event log, ETA,
  counters, and Cancel within 1 s of form submission.
- The main app connection is never held across a long job. Workers
  use short-lived SQLite connections per `emit` call.
- Jobs survive browser-tab refreshes; the docked active-jobs strip
  re-attaches on next navigation.

### Negative / Costs
- Worker functions cannot return values to the route handler
  synchronously; results must be surfaced via `ctx.emit` or written
  to the DB for the next GET to read.
- Thread-pool workers share address space; an uncaught exception in
  a worker terminates the job and surfaces the traceback in the
  event log, not in the HTTP response.

### Mitigations
- `JobRunner` stamps terminal status (`done`, `error`, `cancelled`,
  `interrupted`) automatically, so workers do not need try/finally.
- On process startup, `mark_interrupted_on_startup` flips leftover
  `running`/`queued` rows to `interrupted` so stale jobs do not
  block the UI.

## Compliance

How `/adr-check` detects violations:

- **Synchronous handler calling OpenRouter:** grep for
  `await client.complete(` or `openrouter` imports in route files
  that do NOT also call `job_runner.submit`.
- **Synchronous handler calling external APIs:** grep for
  `httpx.get(`, `httpx.post(`, `requests.` in route files not
  inside a `JobContext` worker function.
- **Blocking loop in handler:** AST-flag `for` loops over
  collections in route handler bodies (not worker bodies) where
  the loop body contains an `await` or a subprocess call.
- **Missing cancel check:** grep worker functions for the absence
  of `ctx.raise_if_cancelled()` inside loops.

## References

- CLAUDE.md §"Long-running operations run as jobs with a progress modal"
- `src/lamella/jobs/runner.py`: `JobRunner`, `submit`
- `src/lamella/jobs/context.py`: `JobContext`, `JobCancelled`
- `src/lamella/jobs/models.py`: `Job`, `JobEvent`
- [ADR-0005](0005-htmx-endpoints-return-partials.md): job modal partial is an HTMX partial
