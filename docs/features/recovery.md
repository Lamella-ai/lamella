---
audience: agents
read-cost-target: 110 lines
authority: informative
status: Active Development
cross-refs: docs/specs/RECOVERY_SYSTEM.md, docs/specs/LEDGER_LAYOUT.md, docs/specs/NORMALIZE_TXN_IDENTITY.md, docs/adr/0004-bean-check-after-every-write.md, docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0015-reconstruct-capability-invariant.md
last-derived-from-code: 2026-04-26
---
# Recovery

## Summary

Boot-time ledger state detection + targeted heal UI under /setup/recovery; atomic groups with bean-check rollback.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/setup/check` | `setup_check_page` | `src/lamella/web/routes/setup_check.py:605` |
| GET | `/setup/legacy-paths` | `legacy_paths_page` | `src/lamella/web/routes/setup_legacy_paths.py:91` |
| POST | `/setup/legacy-paths/heal` | `legacy_paths_heal` | `src/lamella/web/routes/setup_legacy_paths.py:120` |
| GET | `/setup/recovery` | `recovery_page` | `src/lamella/web/routes/setup_recovery.py:219` |
| POST | `/setup/recovery/apply` | `recovery_apply` | `src/lamella/web/routes/setup_recovery.py:715` |
| POST | `/setup/recovery/draft/{finding_id}/dismiss` | `draft_dismiss` | `src/lamella/web/routes/setup_recovery.py:347` |
| POST | `/setup/recovery/draft/{finding_id}/edit` | `draft_edit` | `src/lamella/web/routes/setup_recovery.py:410` |
| GET | `/setup/recovery/finalizing` | `recovery_finalizing` | `src/lamella/web/routes/setup_recovery.py:873` |
| GET | `/setup/recovery/schema` | `schema_drift_page` | `src/lamella/web/routes/setup_schema.py:98` |
| GET | `/setup/recovery/schema/confirm` | `schema_drift_confirm` | `src/lamella/web/routes/setup_schema.py:125` |
| POST | `/setup/recovery/schema/heal` | `schema_drift_heal` | `src/lamella/web/routes/setup_schema.py:186` |

## Owned templates

- `src/lamella/web/templates/setup_check.html`

## Owned source files

- `src/lamella/core/bootstrap/classifier.py`
- `src/lamella/core/bootstrap/detection.py`
- `src/lamella/core/bootstrap/import_apply.py`
- `src/lamella/core/bootstrap/markers.py`
- `src/lamella/core/bootstrap/scaffold.py`
- `src/lamella/core/bootstrap/templates.py`
- `src/lamella/core/bootstrap/transforms.py`
- `src/lamella/features/recovery/bulk_apply.py`
- `src/lamella/features/recovery/findings/legacy_paths.py`
- `src/lamella/features/recovery/findings/schema_drift.py`
- `src/lamella/features/recovery/heal/legacy_paths.py`
- `src/lamella/features/recovery/heal/schema_drift.py`
- `src/lamella/features/recovery/lock.py`
- `src/lamella/features/recovery/migrations/base.py`
- `src/lamella/features/recovery/migrations/catch_up_sqlite.py`
- `src/lamella/features/recovery/migrations/migrate_ledger_v0_to_v1.py`
- `src/lamella/features/recovery/models.py`
- `src/lamella/features/recovery/repair_state.py`
- `src/lamella/features/recovery/snapshot.py`

## Owned tests

- `tests/test_legacy_paths_detector.py`
- `tests/test_legacy_paths_heal.py`
- `tests/test_recovery_migration_registry.py`
- `tests/test_registry_discovery_guard.py`
- `tests/test_repair_state.py`
- `tests/test_schema_drift_detector.py`
- `tests/test_schema_drift_heal.py`
- `tests/test_setup_check.py`
- `tests/test_setup_recovery_apply.py`
- `tests/test_setup_recovery_draft.py`
- `tests/test_setup_recovery_route.py`
- `tests/test_setup_repair_state_migration.py`
- `tests/test_setup_resurrection.py`
- `tests/test_setup_schema_route.py`
- `tests/test_setup_schema_route_integration.py`

## ADR compliance


- **ADR-0004**: every heal wraps in snapshot + bean-check + restore; no heal
  commits to disk without a clean bean-check.
- **ADR-0001**: heals only touch connector-owned files; never user-authored files.
- **ADR-0015**: the recovery migration path (Pattern C) is the mechanism by
  which the reconstruct contract is upgraded between schema versions.

## Current state


### State machine

`bootstrap/detection.py::detect_ledger_state` classifies in this order:

1. `MISSING`: `main.bean` absent → first-run wizard
2. `UNPARSEABLE`: fatal parse error → error page, manual fix required
3. `NEEDS_NEWER_SOFTWARE`: `version > LATEST_LEDGER_VERSION` → refusal page;
   resolved 2026-04-26; prevents a v(N) app from silently corrupting v(N+1) data
4. `READY`: has `lamella-ledger-version "1"` and version == LATEST → dashboard
5. `NEEDS_MIGRATION`: has stamp but `version < LATEST` → migration prompt
6. `NEEDS_VERSION_STAMP`: parses cleanly, has content, no stamp → confirm page
7. `STRUCTURALLY_EMPTY`: parses, no content, no stamp → first-run wizard

### Detection and heals

Detectors are pure `(conn, entries) -> tuple[Finding, ...]` functions under
`bootstrap/recovery/findings/`. Two exist today:
- `legacy_paths.py::detect_legacy_paths`: non-canonical account paths
- `schema_drift.py::detect_schema_drift`: version mismatches, missing SQLite columns

Detection runs per request, not cached. Cost is bounded; the spec notes a
per-detector cache keyed on `(mtime, user_version)` as the future fix if any
detector becomes expensive.

Heals are under `bootstrap/recovery/heal/`. Each category has one heal
module. Dispatch in `bulk_apply._heal_one()` is a hand-coded `if category ==`
chain; new categories must be wired manually.

### Migration registry and `@register_migration`

`@register_migration` decorator (landed commit `82087be`, gap §11.5) marks a
`Migration` subclass for auto-discovery. The decorator stamps
`__lamella_migration_keys__` on the class without side effects on import
order. The scanner in `migrations/__init__.py` walks every `Migration`
subclass loaded under the package and populates the axis-keyed registry.

Two migrations exist today:
- `CatchUpSqliteMigrations` (axis=`sqlite`): catches SQLite schema to head
- `MigrateLedgerV0ToV1` (axis=`ledger`, keys `("none",1)` and `("0",1)`)

Four schema-evolution patterns are in use (see spec §5):
- **A**: at-load compat via `_legacy_meta.normalize_entries` (permanent, no disk write)
- **B**: on-touch normalize via `rewrite/txn_inplace._opportunistic_normalize`
- **C**: versioned `Migration` subclass (requires `LATEST_LEDGER_VERSION` bump)
- **D**: optional bulk transform CLI (`transform/*.py`)

### Bulk-apply orchestrator

`bootstrap/recovery/bulk_apply.py` runs as a `JobRunner` worker (ADR-0006).
Groups run in order: Group 1 (schema, best-effort) → Group 2 (labels, atomic)
→ Group 3 (cleanup, atomic). Atomic groups use a single outer
`with_bean_snapshot` envelope; any per-finding failure triggers
`_GroupRollbackTrigger` and all declared files restore byte-identically.
`BulkContext` kwarg lets a heal skip its own snapshot wrap and participate
in the outer envelope.

Pre-flight validation (`_preflight_edit_payloads`, Phase 8.8) validates all
selected edit-action payloads before any group opens. Every stale payload
appears as `FindingFailed` in one pass.

### Compliant ADRs

- **ADR-0004**: every heal wraps in snapshot + bean-check + restore; no heal
  commits to disk without a clean bean-check.
- **ADR-0001**: heals only touch connector-owned files; never user-authored files.
- **ADR-0015**: the recovery migration path (Pattern C) is the mechanism by
  which the reconstruct contract is upgraded between schema versions.

### Known violations / open gaps (from spec §11)

- Gap #3: honest dry-run for recompute migrations deferred (no scratch-ledger infra)
- Gap #6: repair-state staleness, pre-flight validation covers only `legacy_path`
  edit; needs extension to all categories
- Gap #9: `SUPPORTS_DRY_RUN=False` confirm route deferred until a second case appears
- Gap #10: version-bump checklist enforcement deferred until next actual bump

## Known gaps


- Gap #3: honest dry-run for recompute migrations deferred (no scratch-ledger infra)
- Gap #6: repair-state staleness, pre-flight validation covers only `legacy_path`
  edit; needs extension to all categories
- Gap #9: `SUPPORTS_DRY_RUN=False` confirm route deferred until a second case appears
- Gap #10: version-bump checklist enforcement deferred until next actual bump

## Remaining tasks


- Implement `NEEDS_NEWER_SOFTWARE` refusal page + setup-progress gate (gap #1, resolved
  by spec but implementation must land)
- `tests/test_lamella_keys_registered.py` introspection test (gap #2, resolved)
- In-flight lock for `/setup/recovery` (gap #7, in flight)
- Flip `_BULK_APPLICABLE` default to False, audit existing categories (gap #8, in flight)
- Extend pre-flight validation to all finding categories (gap #6)
