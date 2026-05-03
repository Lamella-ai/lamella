---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0002-in-place-rewrites-default.md, docs/adr/0004-bean-check-after-every-write.md, docs/adr/0007-entity-first-account-hierarchy.md, docs/adr/0011-autocomplete-everywhere.md, docs/adr/0015-reconstruct-capability-invariant.md
last-derived-from-code: 2026-04-26
---
# Properties

## Summary

Real-property registry: address, ownership entity, linked loans, depreciation basis.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/settings/properties` | `properties_page` | `src/lamella/web/routes/properties.py:99` |
| POST | `/settings/properties` | `save_property` | `src/lamella/web/routes/properties.py:121` |
| GET | `/settings/properties/{slug}` | `property_detail` | `src/lamella/web/routes/properties.py:306` |
| GET | `/settings/properties/{slug}/change-ownership` | `property_change_ownership_page` | `src/lamella/web/routes/properties.py:1083` |
| POST | `/settings/properties/{slug}/change-ownership/rename` | `property_change_ownership_rename` | `src/lamella/web/routes/properties.py:814` |
| POST | `/settings/properties/{slug}/change-ownership/transfer` | `property_change_ownership_transfer` | `src/lamella/web/routes/properties.py:1123` |
| GET | `/settings/properties/{slug}/dispose` | `property_dispose_form` | `src/lamella/web/routes/properties.py:569` |
| POST | `/settings/properties/{slug}/dispose` | `property_dispose_commit` | `src/lamella/web/routes/properties.py:624` |
| POST | `/settings/properties/{slug}/valuations` | `add_property_valuation` | `src/lamella/web/routes/properties.py:478` |
| POST | `/settings/properties/{slug}/valuations/{valuation_id}/delete` | `delete_property_valuation` | `src/lamella/web/routes/properties.py:523` |

## Owned templates

- `src/lamella/web/templates/settings_properties.html`

## Owned source files

- `src/lamella/features/properties/disposal_writer.py`
- `src/lamella/features/properties/loans_summary.py`
- `src/lamella/features/properties/property_companion.py`
- `src/lamella/features/properties/reader.py`
- `src/lamella/features/properties/transfer_writer.py`
- `src/lamella/features/properties/writer.py`

## Owned tests

- `tests/test_properties_loans_summary.py`

## ADR compliance

- ADR-0001: property state (`custom "property"`) writes to `connector_config.bean`; reconstruct step10 rebuilds `properties` + `property_valuations` from entries.
- ADR-0002: disposal and transfer write override blocks; in-place rewrite is used for entity renaming (case-A override + case-B textual rewrite).
- ADR-0003: all metadata keys use `lamella-property-*` and `lamella-valuation-*` prefixes.
- ADR-0004: every write path calls `append_custom_directive` with `run_check=True`; disposal/transfer use `recovery_write_envelope` for atomic rollback.
- ADR-0007: entity-first account paths are enforced in `_property_paths`; entity-less paths raise `ValueError`.
- ADR-0015: `step10_properties.py` is registered; both `properties` and `property_valuations` are state tables.

## Current state


The module is substantially complete. Reader, writer, routes, disposal writer,
transfer writer, and reconstruct step10 all exist and are wired. The detail
page computes book value, cost-basis gap, expense rollup, equity, and surfaces
acquisition FIXME candidates from the parsed ledger.

### Compliant ADRs
- ADR-0001: property state (`custom "property"`) writes to `connector_config.bean`; reconstruct step10 rebuilds `properties` + `property_valuations` from entries.
- ADR-0002: disposal and transfer write override blocks; in-place rewrite is used for entity renaming (case-A override + case-B textual rewrite).
- ADR-0003: all metadata keys use `lamella-property-*` and `lamella-valuation-*` prefixes.
- ADR-0004: every write path calls `append_custom_directive` with `run_check=True`; disposal/transfer use `recovery_write_envelope` for atomic rollback.
- ADR-0007: entity-first account paths are enforced in `_property_paths`; entity-less paths raise `ValueError`.
- ADR-0015: `step10_properties.py` is registered; both `properties` and `property_valuations` are state tables.

### Known violations
- ADR-0001 (partial): valuation deletion (`POST /settings/properties/{slug}/valuations/{id}/delete`) deletes from SQLite only. No tombstone directive (`custom "property-valuation-deleted"` or equivalent) is appended to `connector_config.bean`. A reconstruct after a valuation delete will re-materialize the deleted row.
- ADR-0011: the `/settings/properties` create form does not use an account `<datalist>` for `asset_account_path`. The field is a plain text input with no ledger-derived suggestions.

## Known gaps

- ADR-0001 (partial): valuation deletion (`POST /settings/properties/{slug}/valuations/{id}/delete`) deletes from SQLite only. No tombstone directive (`custom "property-valuation-deleted"` or equivalent) is appended to `connector_config.bean`. A reconstruct after a valuation delete will re-materialize the deleted row.
- ADR-0011: the `/settings/properties` create form does not use an account `<datalist>` for `asset_account_path`. The field is a plain text input with no ledger-derived suggestions.

## Remaining tasks


1. Write a `custom "property-valuation-deleted"` directive on valuation delete (mirrors `append_property_deleted`). Update `read_property_valuations` and step10 to filter tombstoned valuations.
2. Add account `<datalist>` backing for the `asset_account_path` input on the create/edit form (ADR-0011).
3. Verify `property_valuations` is declared in the `verify.py` TablePolicy as `state` (not cache) so drift checks catch the tombstone gap.
