---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0004-bean-check-after-every-write.md, docs/adr/0011-autocomplete-everywhere.md, docs/adr/0015-reconstruct-capability-invariant.md
last-derived-from-code: 2026-04-26
---
# Budgets

## Summary

Per-account / per-period budget definitions with progress tracking and alert dispatch.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/budgets` | `budgets_page` | `src/lamella/web/routes/budgets.py:67` |
| POST | `/budgets` | `create_budget` | `src/lamella/web/routes/budgets.py:76` |
| POST | `/budgets/{budget_id}` | `update_budget` | `src/lamella/web/routes/budgets.py:120` |
| POST | `/budgets/{budget_id}/delete` | `delete_budget` | `src/lamella/web/routes/budgets.py:149` |

## Owned templates

- `src/lamella/web/templates/budgets.html`

## Owned source files

- `src/lamella/features/budgets/alerts.py`
- `src/lamella/features/budgets/models.py`
- `src/lamella/features/budgets/progress.py`
- `src/lamella/features/budgets/service.py`
- `src/lamella/features/budgets/writer.py`

## Owned tests

- `tests/test_budgets_alerts.py`
- `tests/test_budgets_crud.py`
- `tests/test_budgets_progress.py`
- `tests/test_step3_budgets.py`

## ADR compliance

- ADR-0001: budgets persist as `custom "budget"` directives in `connector_budgets.bean`. Deletes write `custom "budget-revoked"` tombstones, not SQL deletes alone. Reconstruct step3 is registered and produces the `budgets` state table.
- ADR-0004: `append_budget` and `append_budget_revoke` call `append_custom_directive` with `run_check=True`; `BeanCheckError` surfaces to the UI.
- ADR-0015: step3 declares `state_tables=["budgets"]` and is registered via `@register`. `TablePolicy` for `budgets` is registered in `step3_budgets.py`.

## Current state


Create and delete are fully implemented end-to-end: the route validates, writes
the ledger directive, then writes the SQLite row. The `/budgets` page renders
progress bars from `progress_for_budget` against parsed ledger entries.
Reconstruct step3 re-materializes budgets from `custom "budget"` and
`custom "budget-revoked"` directives using `(label, entity, account_pattern, period)` as the identity key.

### Compliant ADRs
- ADR-0001: budgets persist as `custom "budget"` directives in `connector_budgets.bean`. Deletes write `custom "budget-revoked"` tombstones, not SQL deletes alone. Reconstruct step3 is registered and produces the `budgets` state table.
- ADR-0004: `append_budget` and `append_budget_revoke` call `append_custom_directive` with `run_check=True`; `BeanCheckError` surfaces to the UI.
- ADR-0015: step3 declares `state_tables=["budgets"]` and is registered via `@register`. `TablePolicy` for `budgets` is registered in `step3_budgets.py`.

### Known violations
- ADR-0001 (update path): `POST /budgets/{id}` (`update_budget` handler) calls `BudgetService.update` which mutates the SQLite row but does NOT write a new `custom "budget"` directive. After a reconstruct, the update is lost, the budget reverts to its creation-time values. The fix is to write a superseding `custom "budget"` directive on every update (same slug identity pattern used by loans).
- ADR-0011: the `account_pattern` input on the create form is a plain `<input type="text">` with a static placeholder string. It has no `<datalist>` backed by open ledger accounts. Users who mistype an account name discover the error only after validation fails at the service layer, not interactively.

## Known gaps

- ADR-0001 (update path): `POST /budgets/{id}` (`update_budget` handler) calls `BudgetService.update` which mutates the SQLite row but does NOT write a new `custom "budget"` directive. After a reconstruct, the update is lost, the budget reverts to its creation-time values. The fix is to write a superseding `custom "budget"` directive on every update (same slug identity pattern used by loans).
- ADR-0011: the `account_pattern` input on the create form is a plain `<input type="text">` with a static placeholder string. It has no `<datalist>` backed by open ledger accounts. Users who mistype an account name discover the error only after validation fails at the service layer, not interactively.

## Remaining tasks


1. Add a superseding-directive write to the update path (`POST /budgets/{id}`): append a new `custom "budget"` with the updated values before committing the SQLite mutation. This closes the ADR-0001 reconstruct gap.
2. Add a `<datalist>` backed by open ledger accounts to the `account_pattern` field on `budgets.html`. The existing `/api/accounts` or similar endpoint (used elsewhere) supplies the list.
3. Consider exposing a `/budgets/{id}` edit page (currently update is an inline form POST from the list page with no dedicated page) so users can see all fields before editing.
