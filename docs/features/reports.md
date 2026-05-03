---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0006-long-running-ops-as-jobs.md
last-derived-from-code: 2026-04-29
---
# Reports

## Summary

Schedule C / Schedule F PDF + CSV report generators, balance audit, intercompany audit.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/audit` | `audit_page` | `src/lamella/web/routes/audit.py:55` |
| POST | `/audit/items/{item_id}/accept` | `audit_accept` | `src/lamella/web/routes/audit.py:212` |
| POST | `/audit/items/{item_id}/dismiss` | `audit_dismiss` | `src/lamella/web/routes/audit.py:337` |
| POST | `/audit/run` | `audit_run` | `src/lamella/web/routes/audit.py:118` |
| GET | `/reports` | `reports_index` | `src/lamella/web/routes/reports.py:93` |
| GET | `/reports/{entity_slug}` | `reports_index_entity_redirect` | `src/lamella/web/routes/reports.py` |
| GET | `/reports/audit-portfolio.pdf` | `audit_portfolio` | `src/lamella/web/routes/reports.py:297` |
| GET | `/reports/balance-audit` | `balance_audit_report` | `src/lamella/web/routes/balances.py:171` |
| GET | `/reports/estimated-tax.csv` | `estimated_tax_csv` | `src/lamella/web/routes/reports.py:356` |
| GET | `/reports/estimated-tax.pdf` | `estimated_tax_pdf` | `src/lamella/web/routes/reports.py:336` |
| GET | `/reports/intercompany` | `intercompany_report` | `src/lamella/web/routes/intercompany.py:36` |
| GET | `/reports/schedule-c-detail.csv` | `schedule_c_detail` | `src/lamella/web/routes/reports.py:153` |
| GET | `/reports/schedule-c.csv` | `schedule_c_summary` | `src/lamella/web/routes/reports.py:133` |
| GET | `/reports/schedule-c.pdf` | `schedule_c_pdf` | `src/lamella/web/routes/reports.py:173` |
| GET | `/reports/schedule-c.preview.html` | `schedule_c_preview` | `src/lamella/web/routes/reports.py:198` |
| GET | `/reports/schedule-f-detail.csv` | `schedule_f_detail` | `src/lamella/web/routes/reports.py:238` |
| GET | `/reports/schedule-f.csv` | `schedule_f_summary` | `src/lamella/web/routes/reports.py:218` |
| GET | `/reports/schedule-f.pdf` | `schedule_f_pdf` | `src/lamella/web/routes/reports.py:258` |
| GET | `/reports/schedule-f.preview.html` | `schedule_f_preview` | `src/lamella/web/routes/reports.py:280` |
| GET | `/reports/vehicles/form-4562-worksheet.pdf` | `vehicle_form_4562_worksheet_pdf` | `src/lamella/web/routes/reports.py:574` |
| GET | `/reports/vehicles/mileage-log.pdf` | `vehicle_mileage_log_pdf` | `src/lamella/web/routes/reports.py:494` |
| GET | `/reports/vehicles/schedule-c-part-iv.pdf` | `vehicle_schedule_c_part_iv_pdf` | `src/lamella/web/routes/reports.py:537` |

## Owned templates

- `src/lamella/web/templates/audit.html`
- `src/lamella/web/templates/balance_audit_report.html`
- `src/lamella/web/templates/reports.html`

## Owned source files

- `src/lamella/features/reports/_pdf.py`
- `src/lamella/features/reports/audit_portfolio.py`
- `src/lamella/features/reports/estimated_tax.py`
- `src/lamella/features/reports/intercompany.py`
- `src/lamella/features/reports/line_map.py`
- `src/lamella/features/reports/receipt_fetcher.py`
- `src/lamella/features/reports/schedule_c.py`
- `src/lamella/features/reports/schedule_c_pdf.py`
- `src/lamella/features/reports/schedule_f.py`
- `src/lamella/features/reports/schedule_f_pdf.py`
- `src/lamella/features/reports/vehicles_pdf.py`

## Owned tests

- `tests/test_audit.py`
- `tests/test_audit_portfolio.py`
- `tests/test_estimated_tax.py`
- `tests/test_schedule_c_csv.py`
- `tests/test_schedule_c_pdf.py`
- `tests/test_schedule_f_csv.py`
- `tests/test_schedule_f_pdf.py`

## Behavior

- `/reports?entity=<slug>` filters the matrix (scheduled + unscheduled report
  lists and the estimated-tax form) to a single entity, scoped via the entity
  registry. Without the param the page lists every entity.
- `/reports/{entity_slug}` (no year) is a convenience alias that 303-redirects
  to `/reports?entity={slug}` after the slug is validated against the registry.
  The route is declared LAST in the router so explicit endpoints like
  `/reports/schedule-c.csv` and `/reports/audit-portfolio.pdf` are not
  shadowed; values containing `.` or `/` are rejected so file-style suffixes
  cannot be misrouted as slugs.
- `entity_type` renders through the `| humanize` Jinja filter, preserving
  acronym case (LLC stays uppercase, S-Corp keeps the hyphen,
  `sole_proprietorship` becomes "Sole Proprietorship").
- Money figures in report tables go through the global `|money` filter, which
  wraps output in `<span class="money money--{pos|neg|zero} num">` so refunds
  and charges are visually distinguished by sign without per-template churn.

## ADR compliance

- ADR-0001: Every report reads directly from `LedgerReader.load().entries`;
  no SQLite state feeds the computed figures (except mileage and vehicle tables
  which are SQLite caches of user-logged data).
- ADR-0007: Schedule C and F aggregation respects entity-first hierarchy, 
  `build_schedule_c(entity=entity, ...)` filters by `Expenses:<entity>:*`.
- PDFRenderingUnavailable is surfaced as an HTTP 503 with a plain message; the
  WeasyPrint import is lazy so the container runs without it.

## Current state


### Compliant ADRs
- ADR-0001: Every report reads directly from `LedgerReader.load().entries`;
  no SQLite state feeds the computed figures (except mileage and vehicle tables
  which are SQLite caches of user-logged data).
- ADR-0007: Schedule C and F aggregation respects entity-first hierarchy, 
  `build_schedule_c(entity=entity, ...)` filters by `Expenses:<entity>:*`.
- PDFRenderingUnavailable is surfaced as an HTTP 503 with a plain message; the
  WeasyPrint import is lazy so the container runs without it.

### Known violations
- ADR-0006: All report endpoints are synchronous GET handlers that render inline.
  `audit_portfolio` fetches Paperless receipts sequentially inside an `async`
  handler but does not use the job runner. For large ledgers (many receipt
  downloads), this blocks the event loop. The handler is `async def` but
  does not yield control during the Paperless calls.
- ADR-0005: Report endpoints return `HTMLResponse` or `StreamingResponse`
  directly, never checked against `HX-Request`. No HTMX swap scenarios exist
  today (all downloads are triggered by `<a>` links), so this is low risk but
  not formally compliant.
- No bean-query integration. Schedule C/F use a hand-written ledger walk
  (`build_schedule_c`, `build_schedule_f`), not `beancount.query.query`.
- P&L per entity is not a standalone report; it appears only as a dashboard widget
  (`registry/dashboard_service.py::money_groups`).

## Known gaps

- ADR-0006: All report endpoints are synchronous GET handlers that render inline.
  `audit_portfolio` fetches Paperless receipts sequentially inside an `async`
  handler but does not use the job runner. For large ledgers (many receipt
  downloads), this blocks the event loop. The handler is `async def` but
  does not yield control during the Paperless calls.
- ADR-0005: Report endpoints return `HTMLResponse` or `StreamingResponse`
  directly, never checked against `HX-Request`. No HTMX swap scenarios exist
  today (all downloads are triggered by `<a>` links), so this is low risk but
  not formally compliant.
- No bean-query integration. Schedule C/F use a hand-written ledger walk
  (`build_schedule_c`, `build_schedule_f`), not `beancount.query.query`.
- P&L per entity is not a standalone report; it appears only as a dashboard widget
  (`registry/dashboard_service.py::money_groups`).

## Remaining tasks

- Port `audit_portfolio` to the job runner (ADR-0006): receipt fetching is
  multi-network and proportional to ledger size; it should emit progress events.
- Add P&L report endpoint per entity (currently dashboard-widget-only).
- Verify mileage rate displayed on Schedule C Part IV PDF matches
  `settings.mileage_rate` (currently passed through `build_c_context`).
- Intercompany report (`reports/intercompany.py`) has no route; only surfaced via
  `routes/intercompany.py`. Decide if a standalone PDF belongs here.
