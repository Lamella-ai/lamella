---
audience: agents
read-cost-target: 110 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0006-long-running-ops-as-jobs.md, docs/adr/0005-htmx-endpoints-return-partials.md
last-derived-from-code: 2026-04-26
---
# Jobs

## Summary

Generic background job runner: progress modals, deep-link pages, retry / cancel surface.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/jobs/active/dock` | `jobs_active_dock` | `src/lamella/web/routes/jobs.py:102` |
| GET | `/jobs/{job_id}` | `job_detail` | `src/lamella/web/routes/jobs.py:70` |
| POST | `/jobs/{job_id}/cancel` | `job_cancel` | `src/lamella/web/routes/jobs.py:85` |
| GET | `/jobs/{job_id}/partial` | `job_partial` | `src/lamella/web/routes/jobs.py:46` |
| GET | `/jobs/{job_id}/stream` | `job_stream` | `src/lamella/web/routes/jobs.py:113` |

## Owned templates

- `src/lamella/web/templates/job_detail.html`

## Owned source files

- `src/lamella/core/jobs/context.py`
- `src/lamella/core/jobs/models.py`
- `src/lamella/core/jobs/runner.py`

## Owned tests

- `tests/test_jobs_runner.py`

## ADR compliance


- **ADR-0006**: this module IS the implementation; any handler matching the
  criteria (AI call, external API, N>50 iteration) MUST use it.
- **ADR-0005**: job submit returns a partial modal, not a full-page redirect.

## Current state


### Core components

**`JobRunner`** (`jobs/runner.py`), one instance on `app.state.job_runner`.
Backed by `ThreadPoolExecutor(max_workers=4, thread_name_prefix="jobrunner")`.
`submit()` inserts a `jobs` row, creates a `threading.Event` for cancel, and
calls `_pool.submit(self._run, ctx, fn)`. The caller gets the `job_id` back
immediately. On process restart, `mark_interrupted_on_startup()` flips any
surviving `queued`/`running` rows to `interrupted`.

**`JobContext`** (`jobs/context.py`), the handle passed to the worker callable.
Thread-owned, not shared. Key methods:
- `emit(message, outcome?, detail?)`: appends to `job_events` AND increments
  the per-outcome counter (`success_count`, `failure_count`, `not_found_count`,
  `error_count`, `info_count`) atomically in one short-lived connection.
- `advance(n=1)`: increments `completed` for progress-bar updates.
- `set_total(n)`: updates `total` (can be called from inside the worker when
  count is not known at submit time).
- `raise_if_cancelled()`: raises `JobCancelled` if the cancel event is set.
- `set_return_url(url)`: patches `return_url` from inside the worker for paths
  where the final URL depends on the worker's own output.

Connection discipline: every `emit`/`advance`/`set_total` call opens a
SHORT-LIVED SQLite connection (WAL mode). This avoids holding the app's
request-thread lock across a multi-minute job.

**`Job`** and **`JobEvent`** (`jobs/models.py`), frozen dataclasses with
computed properties: `percent` (0 to 100 or None), `elapsed_seconds`,
`eta_seconds` (remaining time from rate), `humanize_eta()`, `humanize_elapsed()`.
`eta_seconds` uses completed/elapsed rate; returns None until at least one item
is done.

Terminal statuses: `done`, `cancelled`, `error`, `interrupted`. Any non-terminal
row in the DB on startup is flipped to `interrupted`.

### UI layer

Routes return `partials/_job_modal.html` as a `hx-swap="beforeend"` on `<body>`
(ADR-0005), so the modal overlays the current page without replacing it. The
modal polls `/jobs/{id}/partial` every 1s and renders:
- Progress bar (percent)
- Live event log with outcome icons
- Counters: Success / Failure / Not Found / Error
- ETA and elapsed time
- Cancel button (`POST /jobs/{id}/cancel`)

`base.html` renders a docked active-jobs strip so active jobs are visible across
page navigation. The strip queries `job_runner.active()` on every full-page render.

### Already-ported handlers (as of migration 040)

Per CLAUDE.md: `/search/receipt-hunt`, `/search/bulk-apply`, `/audit/run`,
`/simplefin/fetch`, `/settings/data-integrity/scan`, `/settings/rewrite`,
`/import/{id}/ingest`, `/status/paperless/full-sync`,
`/status/vector-index/rebuild`, `/recurring/scan`. Recovery bulk-apply also
runs as a job.

### Compliant ADRs

- **ADR-0006**: this module IS the implementation; any handler matching the
  criteria (AI call, external API, N>50 iteration) MUST use it.
- **ADR-0005**: job submit returns a partial modal, not a full-page redirect.

### Known violations

None observed in the runner itself. Any route that calls OpenRouter or
Paperless synchronously (blocking >5s) is a violation of ADR-0006. Check
new route additions against the criteria.

## Known gaps


None observed in the runner itself. Any route that calls OpenRouter or
Paperless synchronously (blocking >5s) is a violation of ADR-0006. Check
new route additions against the criteria.

## Remaining tasks


- Audit all routes added after migration 040 to confirm they use job runner
  for qualifying operations.
- Consider a job-history page at `/jobs/` listing recent completed jobs with
  their event logs (currently only active jobs are surfaced in the strip).
- ETA computation is linear-rate only; bursty workloads (AI cascade with
  variable per-item latency) produce misleading ETAs. No fix scheduled.
