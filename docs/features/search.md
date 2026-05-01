---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0005-htmx-endpoints-return-partials.md, docs/adr/0006-long-running-ops-as-jobs.md, docs/adr/0002-in-place-rewrites-default.md
last-derived-from-code: 2026-04-26
---
# Search

## Summary

Cross-ledger search with filter chips, override-aware results, bulk-apply rule mining.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/search` | `search_page` | `src/lamella/web/routes/search.py:154` |
| POST | `/search/bulk-apply` | `search_bulk_apply` | `src/lamella/web/routes/search.py:1762` |
| POST | `/search/mark-transfer-pair` | `search_mark_transfer_pair` | `src/lamella/web/routes/search.py:1394` |
| GET | `/search/palette.json` | `palette_json` | `src/lamella/web/routes/search.py:2030` |
| POST | `/search/receipt-hunt` | `search_receipt_hunt` | `src/lamella/web/routes/search.py:1643` |
| GET | `/search/receipt-hunt/result` | `search_receipt_hunt_result` | `src/lamella/web/routes/search.py:1709` |
| GET | `/txn/{target_hash}` | `txn_detail` | `src/lamella/web/routes/search.py:423` |
| POST | `/txn/{target_hash}/apply` | `txn_apply` | `src/lamella/web/routes/search.py:1171` |
| POST | `/txn/{target_hash}/ask-ai` | `txn_ask_ai` | `src/lamella/web/routes/search.py:710` |
| POST | `/txn/{target_hash}/categorize-inplace` | `txn_categorize_inplace` | `src/lamella/web/routes/search.py:971` |
| GET | `/txn/{target_hash}/notes-partial` | `txn_notes_partial` | `src/lamella/web/routes/search.py:833` |
| GET | `/txn/{target_hash}/pair-candidates` | `txn_pair_candidates` | `src/lamella/web/routes/search.py:1313` |
| GET | `/txn/{target_hash}/panel` | `txn_panel` | `src/lamella/web/routes/search.py:864` |
| POST | `/txn/{target_hash}/revert-override` | `txn_revert_override` | `src/lamella/web/routes/search.py:1123` |

## Owned templates

- `src/lamella/web/templates/search.html`

## Owned source files

_No source files own this feature._

## Owned tests

- `tests/test_search_filters_overrides.py`
- `tests/test_search_receipt_hunt.py`

## ADR compliance

- ADR-0006: Both `receipt-hunt` and `bulk-apply` submit to `job_runner` and
  return `_job_modal.html` as the HTMX response. Per-item progress events
  stream via `JobContext.emit`.
- ADR-0002: `bulk-apply` tries `rewrite_fixme_to_account` (in-place) first;
  falls back to `OverrideWriter.append` only when in-place is infeasible
  (no filename/lineno, path safety check fails).
- ADR-0008: Override dedup is enforced, before writing a new override,
  `rewrite_without_hash` strips any existing override block for the same hash
  to prevent double-counting.
- ADR-0001: Search walks the ledger directly; no search index shadows the
  ledger. Staged rows are SQLite but are clearly a pre-promotion cache.

## Current state


### Compliant ADRs
- ADR-0006: Both `receipt-hunt` and `bulk-apply` submit to `job_runner` and
  return `_job_modal.html` as the HTMX response. Per-item progress events
  stream via `JobContext.emit`.
- ADR-0002: `bulk-apply` tries `rewrite_fixme_to_account` (in-place) first;
  falls back to `OverrideWriter.append` only when in-place is infeasible
  (no filename/lineno, path safety check fails).
- ADR-0008: Override dedup is enforced, before writing a new override,
  `rewrite_without_hash` strips any existing override block for the same hash
  to prevent double-counting.
- ADR-0001: Search walks the ledger directly; no search index shadows the
  ledger. Staged rows are SQLite but are clearly a pre-promotion cache.

### Known violations
- ADR-0005: `GET /search` and `GET /txn/<hash>` return full-page templates
  without checking `HX-Request`. No partial variant exists for either endpoint.
  In-page HTMX actions (undo, apply) return HTML fragments directly, those
  are compliant, but the top-level search page is not.
- No vector/FTS backend: search is a linear scan over all parsed entries.
  For ledgers with thousands of entries and a `lookback_days=365` window, this
  runs in the HTTP request path with no timeout guard.
- `POST /txn/<hash>/apply` does not run as a job (ADR-0006). A single-txn
  apply is fast in practice but could block if bean-check is slow.
- Staged hit search uses SQLite LIKE which is case-insensitive via LOWER().
  Ledger hit search uses Python `.lower()` on narration/payee strings.
  The two surfaces use different backends but the user sees a unified result.

## Known gaps

- ADR-0005: `GET /search` and `GET /txn/<hash>` return full-page templates
  without checking `HX-Request`. No partial variant exists for either endpoint.
  In-page HTMX actions (undo, apply) return HTML fragments directly, those
  are compliant, but the top-level search page is not.
- No vector/FTS backend: search is a linear scan over all parsed entries.
  For ledgers with thousands of entries and a `lookback_days=365` window, this
  runs in the HTTP request path with no timeout guard.
- `POST /txn/<hash>/apply` does not run as a job (ADR-0006). A single-txn
  apply is fast in practice but could block if bean-check is slow.
- Staged hit search uses SQLite LIKE which is case-insensitive via LOWER().
  Ledger hit search uses Python `.lower()` on narration/payee strings.
  The two surfaces use different backends but the user sees a unified result.

## Remaining tasks

- Add a SQLite FTS5 virtual table over `staged_transactions` for the staged
  path so LIKE stops doing full table scans on large inboxes.
- Add `GET /search` partial variant for HTMX refresh (fixes ADR-0005).
- Consider delegating `POST /txn/<hash>/apply` to the job runner when the
  bean-check step is projected to be slow (e.g., large ledger).
- `vector_index.py` (sentence-transformers) exists for classify context but
  is not wired into search. Semantic search over classified transactions is a
  future capability worth noting in the roadmap.
