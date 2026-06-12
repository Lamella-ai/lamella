---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0006-long-running-ops-as-jobs.md, docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0004-bean-check-after-every-write.md
last-derived-from-code: 2026-04-26
---
# Recurring

## Summary

Detect recurring transactions; user confirms cadence + counterparty; emits anchored expectations.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/recurring` | `recurring_page` | `src/lamella/web/routes/recurring.py:71` |
| POST | `/recurring/scan` | `recurring_scan_now` | `src/lamella/web/routes/recurring.py:79` |
| POST | `/recurring/{recurring_id}/confirm` | `recurring_confirm` | `src/lamella/web/routes/recurring.py:128` |
| POST | `/recurring/{recurring_id}/edit` | `recurring_edit` | `src/lamella/web/routes/recurring.py:268` |
| POST | `/recurring/{recurring_id}/ignore` | `recurring_ignore` | `src/lamella/web/routes/recurring.py:236` |
| POST | `/recurring/{recurring_id}/stop` | `recurring_stop` | `src/lamella/web/routes/recurring.py:214` |

## Owned templates

- `src/lamella/web/templates/partials/recurring_confirmation_card.html`
- `src/lamella/web/templates/partials/recurring_row.html`
- `src/lamella/web/templates/recurring.html`

## Owned source files

- `src/lamella/features/recurring/confirmations.py`
- `src/lamella/features/recurring/detector.py`
- `src/lamella/features/recurring/service.py`
- `src/lamella/features/recurring/writer.py`

## Owned tests

- `tests/test_recurring_confirmations.py`
- `tests/test_recurring_detector.py`
- `tests/test_step5_recurring.py`

## ADR compliance

- ADR-0006: `/recurring/scan` submits to `job_runner`; returns `_job_modal.html` partial; progress reported via `ctx.emit`
- ADR-0001: confirmation state goes to `connector_rules.bean`; `recurring_expenses` rows are cache; `read_recurring_from_entries` is the reconstruct reader
- ADR-0004: `append_recurring_confirmed` calls `append_custom_directive` with `run_check=True`; reverts on new bean-check errors

## Current state


**Detection** (`src/lamella/recurring/detector.py`): `run_detection` groups classified ledger transactions by `(canonical_merchant, source_account)`. A canonical merchant is the lowercased word-tokenized payee (or first 40 chars of narration), trailing pure-digit tokens stripped. Groups are filtered to `min_occurrences` (default configurable). Inter-arrival intervals are computed; `_classify_cadence` maps median ± stddev to `monthly` (27 to 33d, σ≤5), `quarterly` (85 to 95d, σ≤8), or `annual` (355 to 375d, σ≤15). No match → not a recurring candidate.

**Service** (`src/lamella/recurring/service.py`): `RecurringService.upsert` inserts as `proposed` or updates `last_seen` / `next_expected`. Confirmed rows use an EMA (0.8 × old + 0.2 × new) on `expected_amount` to track drift. `in_quarantine` blocks re-proposal for 90 days after a user ignore. Status transitions: `proposed → confirmed | ignored`, `confirmed | ignored → stopped` (subscription ended).

**Writer** (`src/lamella/recurring/writer.py`): `append_recurring_confirmed` and `append_recurring_ignored` append `custom` directives to `connector_rules.bean`. Each carries `lamella-*` meta: entity, source_account, merchant_pattern, cadence, amount-hint, confirmed_at. `read_recurring_from_entries` rebuilds the active state from those directives for reconstruct. Revoke directives null out the key `(source_account, merchant_pattern)`.

**Confirmations** (`src/lamella/recurring/confirmations.py`): holds helpers used during reconstruct to rehydrate `recurring_expenses` rows from ledger directives.

**Route** (`src/lamella/routes/recurring.py`): `POST /recurring/scan` runs `run_detection` as a background job via `app.state.job_runner.submit` (ADR-0006 compliant). `POST /recurring/{id}/confirm` calls `RecurringService.confirm` then `append_recurring_confirmed`; `POST /recurring/{id}/ignore` calls `RecurringService.ignore` then `append_recurring_ignored`. The confirmation form accepts `label`, `expected_day`, `source_account` (validated against open accounts), and optional `save_rule` to also create a `classification_rules` entry.

The `/recurring` page renders four status tabs (proposed, confirmed, ignored, stopped) and the last detection run metadata from `recurring_detections`.

### Compliant ADRs
- ADR-0006: `/recurring/scan` submits to `job_runner`; returns `_job_modal.html` partial; progress reported via `ctx.emit`
- ADR-0001: confirmation state goes to `connector_rules.bean`; `recurring_expenses` rows are cache; `read_recurring_from_entries` is the reconstruct reader
- ADR-0004: `append_recurring_confirmed` calls `append_custom_directive` with `run_check=True`; reverts on new bean-check errors

### Known violations
- ADR-0011: `source_account` field on the confirm form does not use `<datalist>` backed by opened accounts; it is a plain `<input type="text">` (minor, account validation happens server-side via `open_accounts` check, but the UX is degraded for long account lists)
- ADR-0001: `expected_amount` EMA mutations on confirmed rows are not written back to `connector_rules.bean`; reconstruct would rebuild with the original confirmed amount, not the drifted EMA (low, amount-hint is informational, not load-bearing for correct classification)

## Known gaps

- ADR-0011: `source_account` field on the confirm form does not use `<datalist>` backed by opened accounts; it is a plain `<input type="text">` (minor, account validation happens server-side via `open_accounts` check, but the UX is degraded for long account lists)
- ADR-0001: `expected_amount` EMA mutations on confirmed rows are not written back to `connector_rules.bean`; reconstruct would rebuild with the original confirmed amount, not the drifted EMA (low, amount-hint is informational, not load-bearing for correct classification)

## Remaining tasks

1. Add `<datalist>` to the `source_account` input on the confirm form (ADR-0011)
2. Write EMA-updated `expected_amount` back to `connector_rules.bean` on each `upsert` of a confirmed row, or document explicitly that amount drift is intentionally transient
3. Expose `next_expected` dates on the confirmed tab so the user can see when a payment is overdue
4. Add an "overdue" band: confirmed rows where `next_expected < today` and no matching transaction in the last N days
5. Wire `recurring_confirmed` patterns as a classify signal (when a FIXME matches a confirmed recurring merchant+account, prefer the confirmed target_account as a rule suggestion without a separate AI call)
