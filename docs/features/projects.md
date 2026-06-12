---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0011-autocomplete-everywhere.md, docs/adr/0015-reconstruct-capability-invariant.md, docs/adr/0018-classification-intentionally-slow.md
last-derived-from-code: 2026-04-26
---
# Projects

## Summary

Project / job tagging across transactions; active-project context surfaced into classify.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/projects` | `projects_index` | `src/lamella/web/routes/projects.py:111` |
| POST | `/projects` | `create_project` | `src/lamella/web/routes/projects.py:140` |
| GET | `/projects/{slug}` | `project_detail` | `src/lamella/web/routes/projects.py:184` |
| POST | `/projects/{slug}` | `update_project` | `src/lamella/web/routes/projects.py:234` |
| POST | `/projects/{slug}/close` | `close_project` | `src/lamella/web/routes/projects.py:280` |
| POST | `/projects/{slug}/delete` | `delete_project` | `src/lamella/web/routes/projects.py:308` |

## Owned templates

- `src/lamella/web/templates/projects.html`

## Owned source files

- `src/lamella/features/projects/reader.py`
- `src/lamella/features/projects/service.py`
- `src/lamella/features/projects/writer.py`

## Owned tests

- `tests/test_projects.py`

## ADR compliance

- **ADR-0001**: `projects` table is state, `custom "project"` directives in
  `connector_config.bean` back it. Reconstruct step 11 (`step11_projects.py`)
  rebuilds `projects` from the ledger. `project_txns` is explicitly marked
  cache in step 11's docstring.
- **ADR-0003**: All metadata keys use `lamella-project-*` prefix (e.g.,
  `lamella-project-display-name`, `lamella-project-start-date`).
- **ADR-0004**: `append_project` delegates to `append_custom_directive` which
  runs bean-check and rolls back on new errors.
- **ADR-0011**: Project picker on classify UI and note form uses `<datalist>`
  backed by the `/api/accounts` pattern.
- **ADR-0015**: step11 reconstruct is wired; `projects` table survives DB wipe.
- **ADR-0018**: Projects narrow the classifier's merchant-matching scope; they
  do not bypass the AI or auto-apply a classification.

## Current state


### Compliant ADRs
- **ADR-0001**: `projects` table is state, `custom "project"` directives in
  `connector_config.bean` back it. Reconstruct step 11 (`step11_projects.py`)
  rebuilds `projects` from the ledger. `project_txns` is explicitly marked
  cache in step 11's docstring.
- **ADR-0003**: All metadata keys use `lamella-project-*` prefix (e.g.,
  `lamella-project-display-name`, `lamella-project-start-date`).
- **ADR-0004**: `append_project` delegates to `append_custom_directive` which
  runs bean-check and rolls back on new errors.
- **ADR-0011**: Project picker on classify UI and note form uses `<datalist>`
  backed by the `/api/accounts` pattern.
- **ADR-0015**: step11 reconstruct is wired; `projects` table survives DB wipe.
- **ADR-0018**: Projects narrow the classifier's merchant-matching scope; they
  do not bypass the AI or auto-apply a classification.

### Known violations
- `project_txns`: transaction attribution rows, are cache per the step11
  docstring but they record `decided_by` (ai vs manual) and `decided_at`
  timestamps that are not reconstructable from the ledger alone. On DB wipe,
  the attribution history is lost even though project membership can be
  re-derived. This is a partial ADR-0015 gap.
- `closeout_json` is stored in SQLite only. If a project is closed with a
  summary (budget overrun analysis, notes), that closeout data is not
  persisted to the ledger. A DB wipe loses it.

## Known gaps

- `project_txns`: transaction attribution rows, are cache per the step11
  docstring but they record `decided_by` (ai vs manual) and `decided_at`
  timestamps that are not reconstructable from the ledger alone. On DB wipe,
  the attribution history is lost even though project membership can be
  re-derived. This is a partial ADR-0015 gap.
- `closeout_json` is stored in SQLite only. If a project is closed with a
  summary (budget overrun analysis, notes), that closeout data is not
  persisted to the ledger. A DB wipe loses it.

## Remaining tasks

- Persist `closeout_json` as a `custom "project-closed"` directive so
  closeout state survives a DB wipe (ADR-0015 gap).
- Decide whether `project_txns.decided_by` / `decided_at` are worth
  stamping as `custom "project-txn"` directives (low priority, the
  classification itself is preserved in the ledger; only the project
  membership record is cache).
- Add a `POST /projects/{slug}/close` route that writes a
  `custom "project-closed"` directive alongside the SQLite close.
- Budget tracking: wire `project_txns.txn_amount` totals against
  `budget_amount` and surface overrun warnings on the detail page.
