---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0004-bean-check-after-every-write.md, docs/adr/0006-long-running-ops-as-jobs.md, docs/adr/0015-reconstruct-capability-invariant.md
last-derived-from-code: 2026-04-26
---
# Mileage

## Summary

IRS-compliant mileage logging: trip CRUD, CSV import, beancount writebacks, vehicle log density.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/mileage` | `mileage_page` | `src/lamella/web/routes/mileage.py:129` |
| POST | `/mileage` | `create_mileage_entry` | `src/lamella/web/routes/mileage.py:618` |
| GET | `/mileage/all` | `mileage_all` | `src/lamella/web/routes/mileage.py:152` |
| GET | `/mileage/import` | `mileage_import_page` | `src/lamella/web/routes/mileage.py:934` |
| POST | `/mileage/import/batches/{batch_id}/delete` | `mileage_import_undo` | `src/lamella/web/routes/mileage.py:1134` |
| POST | `/mileage/import/commit` | `mileage_import_commit` | `src/lamella/web/routes/mileage.py:1059` |
| POST | `/mileage/import/preview` | `mileage_import_preview` | `src/lamella/web/routes/mileage.py:964` |
| GET | `/mileage/last-odometer/{vehicle:path}` | `last_odometer` | `src/lamella/web/routes/mileage.py:580` |
| GET | `/mileage/quick` | `mileage_quick_page` | `src/lamella/web/routes/mileage.py:429` |
| POST | `/mileage/quick` | `mileage_quick_submit` | `src/lamella/web/routes/mileage.py:451` |
| GET | `/mileage/summary` | `mileage_summary` | `src/lamella/web/routes/mileage.py:788` |
| POST | `/mileage/summary/generate` | `generate_mileage_summary` | `src/lamella/web/routes/mileage.py:806` |
| POST | `/mileage/{entry_id:int}` | `mileage_edit_submit` | `src/lamella/web/routes/mileage.py:240` |
| POST | `/mileage/{entry_id:int}/delete` | `mileage_delete` | `src/lamella/web/routes/mileage.py:354` |
| GET | `/mileage/{entry_id:int}/edit` | `mileage_edit_form` | `src/lamella/web/routes/mileage.py:216` |
| GET | `/settings/mileage-rates` | `mileage_rates_page` | `src/lamella/web/routes/mileage.py:523` |
| POST | `/settings/mileage-rates` | `mileage_rate_add` | `src/lamella/web/routes/mileage.py:542` |
| POST | `/settings/mileage-rates/{rate_id}/delete` | `mileage_rate_delete` | `src/lamella/web/routes/mileage.py:568` |

## Owned templates

- `src/lamella/web/templates/mileage.html`
- `src/lamella/web/templates/mileage_all.html`
- `src/lamella/web/templates/mileage_edit.html`
- `src/lamella/web/templates/mileage_import.html`
- `src/lamella/web/templates/mileage_quick.html`
- `src/lamella/web/templates/partials/mileage_entry.html`
- `src/lamella/web/templates/settings_mileage_rates.html`

## Owned source files

- `src/lamella/features/mileage/backfill_audit.py`
- `src/lamella/features/mileage/beancount_writer.py`
- `src/lamella/features/mileage/csv_store.py`
- `src/lamella/features/mileage/import_parser.py`
- `src/lamella/features/mileage/service.py`
- `src/lamella/features/mileage/trip_meta_writer.py`

## Owned tests

- `tests/test_mileage_all_and_edit.py`
- `tests/test_mileage_backfill_audit.py`
- `tests/test_mileage_beancount_writer.py`
- `tests/test_mileage_commuting_bucket.py`
- `tests/test_mileage_csv_store.py`
- `tests/test_mileage_import_parser.py`
- `tests/test_mileage_import_route.py`
- `tests/test_mileage_link_unlinked_entity_scoped.py`
- `tests/test_mileage_purpose_substantiation.py`
- `tests/test_mileage_quick.py`
- `tests/test_mileage_service.py`

## ADR compliance

- **ADR-0001**: `mileage_summary.bean` is a connector-owned file; the year-end
  deduction block carries full provenance metadata for reconstruct.
  `mileage_trip_meta` (state) is reconstructed by step 13 from
  `custom "mileage-trip-meta"` directives. `mileage_entries` (the raw trip log)
  is NOT ledger-persisted, it bootstraps from the CSV backup on empty DB.
- **ADR-0004**: `MileageBeancountWriter.write_year()` runs bean-check after
  every write and reverts both files on `BeanCheckError`.
- **ADR-0015**: step 13 reconstruct (`step13_mileage_trip_meta.py`) rebuilds
  `mileage_trip_meta` from `custom "mileage-trip-meta"` directives.

## Current state


### Compliant ADRs
- **ADR-0001**: `mileage_summary.bean` is a connector-owned file; the year-end
  deduction block carries full provenance metadata for reconstruct.
  `mileage_trip_meta` (state) is reconstructed by step 13 from
  `custom "mileage-trip-meta"` directives. `mileage_entries` (the raw trip log)
  is NOT ledger-persisted, it bootstraps from the CSV backup on empty DB.
- **ADR-0004**: `MileageBeancountWriter.write_year()` runs bean-check after
  every write and reverts both files on `BeanCheckError`.
- **ADR-0015**: step 13 reconstruct (`step13_mileage_trip_meta.py`) rebuilds
  `mileage_trip_meta` from `custom "mileage-trip-meta"` directives.

### Known violations
- **ADR-0015 (partial)**: `mileage_entries` (the raw trip log, dates,
  vehicles, odometers, purposes, from/to) is NOT reconstructed from the
  ledger. On DB wipe with no CSV backup, trip-level history is gone.
  The CSV backup mitigates this in practice but it is not the ledger.
  Trip-level entries are not `custom` directives, so a full DB wipe with
  no CSV requires re-import from any surviving backup source.
- Import batch history (`mileage_imports`) is cache-only, not ledger-backed.

## Known gaps

- **ADR-0015 (partial)**: `mileage_entries` (the raw trip log, dates,
  vehicles, odometers, purposes, from/to) is NOT reconstructed from the
  ledger. On DB wipe with no CSV backup, trip-level history is gone.
  The CSV backup mitigates this in practice but it is not the ledger.
  Trip-level entries are not `custom` directives, so a full DB wipe with
  no CSV requires re-import from any surviving backup source.
- Import batch history (`mileage_imports`) is cache-only, not ledger-backed.

## Remaining tasks

- Decision: accept that `mileage_entries` is CSV-backed (not ledger-backed)
  and document that explicitly, OR stamp per-trip `custom "mileage-entry"`
  directives to make `mileage_entries` fully reconstruct-capable from the
  ledger alone. The CSV backup path is probably sufficient; stamp only if
  users report data loss.
- AI trip categorization: `MileageContextEntry.category` is read by classify
  but the AI does not yet write inferred categories back to `mileage_trip_meta`
  (`auto_from_ai` column exists; the write path exists in `upsert_trip_meta`;
  the AI caller is not yet wired).
- Printable IRS mileage log and Schedule C Part IV worksheet routes (planned
  in archived FEATURE_VEHICLES_PLAN.md Phase 6).
