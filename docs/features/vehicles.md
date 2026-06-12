---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0004-bean-check-after-every-write.md, docs/adr/0007-entity-first-account-hierarchy.md, docs/adr/0015-reconstruct-capability-invariant.md
last-derived-from-code: 2026-04-26
---
# Vehicles

## Summary

Vehicle registry: identity fields, allocation method (mileage/actual), trip templates, disposal, depreciation.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/settings/vehicles` | `_legacy_index` | `src/lamella/web/routes/vehicles.py:3016` |
| POST | `/settings/vehicles` | `_legacy_save` | `src/lamella/web/routes/vehicles.py:3021` |
| GET | `/settings/vehicles/{slug}` | `_legacy_detail` | `src/lamella/web/routes/vehicles.py:3026` |
| POST | `/settings/vehicles/{slug}/mileage` | `_legacy_save_mileage` | `src/lamella/web/routes/vehicles.py:3031` |
| POST | `/settings/vehicles/{slug}/valuations` | `_legacy_add_valuation` | `src/lamella/web/routes/vehicles.py:3036` |
| POST | `/settings/vehicles/{slug}/valuations/{valuation_id}/delete` | `_legacy_delete_valuation` | `src/lamella/web/routes/vehicles.py:3043` |
| GET | `/vehicle-templates` | `vehicle_templates_index` | `src/lamella/web/routes/vehicles.py:2793` |
| POST | `/vehicle-templates` | `save_vehicle_template` | `src/lamella/web/routes/vehicles.py:2810` |
| POST | `/vehicle-templates/{slug}/delete` | `delete_vehicle_template` | `src/lamella/web/routes/vehicles.py:2867` |
| GET | `/vehicles` | `vehicles_index` | `src/lamella/web/routes/vehicles.py:555` |
| POST | `/vehicles` | `save_vehicle` | `src/lamella/web/routes/vehicles.py:1790` |
| GET | `/vehicles/backfill-audit` | `backfill_audit_index` | `src/lamella/web/routes/vehicles.py:846` |
| GET | `/vehicles/new` | `vehicle_new` | `src/lamella/web/routes/vehicles.py:667` |
| GET | `/vehicles/{slug}` | `vehicle_detail` | `src/lamella/web/routes/vehicles.py:683` |
| POST | `/vehicles/{slug}/attribution` | `set_vehicle_trip_attribution` | `src/lamella/web/routes/vehicles.py:2875` |
| POST | `/vehicles/{slug}/banner/{change_key}/dismiss` | `dismiss_breaking_change_banner` | `src/lamella/web/routes/vehicles.py:1024` |
| GET | `/vehicles/{slug}/change-ownership` | `vehicle_change_ownership_page` | `src/lamella/web/routes/vehicles.py:1047` |
| POST | `/vehicles/{slug}/change-ownership/rename` | `vehicle_change_ownership_rename` | `src/lamella/web/routes/vehicles.py:1103` |
| POST | `/vehicles/{slug}/change-ownership/transfer` | `vehicle_change_ownership_transfer` | `src/lamella/web/routes/vehicles.py:1436` |
| POST | `/vehicles/{slug}/credits` | `add_vehicle_credit` | `src/lamella/web/routes/vehicles.py:2690` |
| POST | `/vehicles/{slug}/credits/{credit_id:int}/delete` | `delete_vehicle_credit` | `src/lamella/web/routes/vehicles.py:2723` |
| GET | `/vehicles/{slug}/dispose` | `dispose_form` | `src/lamella/web/routes/vehicles.py:2171` |
| POST | `/vehicles/{slug}/dispose` | `dispose_preview` | `src/lamella/web/routes/vehicles.py:2267` |
| POST | `/vehicles/{slug}/dispose/commit` | `dispose_commit` | `src/lamella/web/routes/vehicles.py:2307` |
| POST | `/vehicles/{slug}/dispose/{disposal_id}/revoke` | `dispose_revoke` | `src/lamella/web/routes/vehicles.py:2412` |
| GET | `/vehicles/{slug}/edit` | `vehicle_edit` | `src/lamella/web/routes/vehicles.py:1766` |
| POST | `/vehicles/{slug}/elections` | `save_vehicle_election` | `src/lamella/web/routes/vehicles.py:2024` |
| POST | `/vehicles/{slug}/elections/{tax_year}/delete` | `delete_vehicle_election` | `src/lamella/web/routes/vehicles.py:2111` |
| POST | `/vehicles/{slug}/fuel` | `add_vehicle_fuel_event` | `src/lamella/web/routes/vehicles.py:2608` |
| POST | `/vehicles/{slug}/fuel/{event_id:int}/delete` | `delete_vehicle_fuel_event` | `src/lamella/web/routes/vehicles.py:2675` |
| POST | `/vehicles/{slug}/mileage` | `save_mileage` | `src/lamella/web/routes/vehicles.py:1948` |
| POST | `/vehicles/{slug}/promote-trips` | `promote_trips_to_yearly` | `src/lamella/web/routes/vehicles.py:2528` |
| POST | `/vehicles/{slug}/renewals` | `add_vehicle_renewal` | `src/lamella/web/routes/vehicles.py:2733` |
| POST | `/vehicles/{slug}/renewals/{renewal_id:int}/complete` | `complete_vehicle_renewal` | `src/lamella/web/routes/vehicles.py:2766` |
| POST | `/vehicles/{slug}/renewals/{renewal_id:int}/delete` | `delete_vehicle_renewal` | `src/lamella/web/routes/vehicles.py:2777` |
| GET | `/vehicles/{slug}/trips` | `vehicle_trips` | `src/lamella/web/routes/vehicles.py:894` |
| POST | `/vehicles/{slug}/valuations` | `add_vehicle_valuation` | `src/lamella/web/routes/vehicles.py:2940` |
| POST | `/vehicles/{slug}/valuations/{valuation_id}/delete` | `delete_vehicle_valuation` | `src/lamella/web/routes/vehicles.py:2996` |

## Owned templates

- `src/lamella/web/templates/vehicle_backfill_audit.html`
- `src/lamella/web/templates/vehicle_change_ownership.html`
- `src/lamella/web/templates/vehicle_detail.html`
- `src/lamella/web/templates/vehicle_disposal_form.html`
- `src/lamella/web/templates/vehicle_disposal_preview.html`
- `src/lamella/web/templates/vehicle_edit.html`
- `src/lamella/web/templates/vehicle_new.html`
- `src/lamella/web/templates/vehicle_trip_templates.html`
- `src/lamella/web/templates/vehicle_trips.html`
- `src/lamella/web/templates/vehicles_index.html`

## Owned source files

- `src/lamella/features/vehicles/allocation.py`
- `src/lamella/features/vehicles/credits.py`
- `src/lamella/features/vehicles/disposal_writer.py`
- `src/lamella/features/vehicles/forecasting.py`
- `src/lamella/features/vehicles/fuel.py`
- `src/lamella/features/vehicles/fuel_writer.py`
- `src/lamella/features/vehicles/health.py`
- `src/lamella/features/vehicles/method_lock.py`
- `src/lamella/features/vehicles/reader.py`
- `src/lamella/features/vehicles/renewals.py`
- `src/lamella/features/vehicles/templates.py`
- `src/lamella/features/vehicles/transfer_writer.py`
- `src/lamella/features/vehicles/vehicle_companion.py`
- `src/lamella/features/vehicles/writer.py`

## Owned tests

- `tests/test_reconstruct_vehicles.py`
- `tests/test_vehicle_allocation_and_method_lock.py`
- `tests/test_vehicle_credits_and_renewals.py`
- `tests/test_vehicle_data_health.py`
- `tests/test_vehicle_disposal_route.py`
- `tests/test_vehicle_disposal_writer.py`
- `tests/test_vehicle_elections.py`
- `tests/test_vehicle_forecasting.py`
- `tests/test_vehicle_fuel_log.py`
- `tests/test_vehicle_identity_fields.py`
- `tests/test_vehicle_promote_trips.py`
- `tests/test_vehicle_reports_pdf.py`
- `tests/test_vehicle_schedule_c_supplementary.py`
- `tests/test_vehicle_trip_templates.py`
- `tests/test_vehicle_writer_roundtrip.py`
- `tests/test_vehicles_dashboard.py`

## ADR compliance

- **ADR-0001**: All vehicle state tables (`vehicles`, `vehicle_yearly_mileage`,
  `vehicle_valuations`, `vehicle_elections`, `vehicle_credits`, `vehicle_renewals`,
  `vehicle_trip_templates`, `vehicle_disposals`) are registered as `state` in
  `step8_vehicles.py` and rebuild from `connector_config.bean` /
  `connector_overrides.bean` via `vehicles/reader.py`.
- **ADR-0003**: All metadata keys use `lamella-vehicle-*`, `lamella-disposal-*`,
  `lamella-election-*`, `lamella-valuation-*`, etc. prefixes.
- **ADR-0004**: All `append_*` writers delegate to `append_custom_directive`;
  disposal and transfer writers use `run_bean_check_vs_baseline` and roll back
  on new errors.
- **ADR-0007**: Auto-scaffolded expense subtree follows entity-first hierarchy:
  `Expenses:<Entity>:Vehicles:<Slug>:{Fuel,Insurance,Maintenance,...}`.
- **ADR-0015**: step 8 reconstruct covers all state tables. `vehicle_fuel_log`
  and `vehicle_data_health_cache` are explicitly registered as cache.

## Current state


### Compliant ADRs
- **ADR-0001**: All vehicle state tables (`vehicles`, `vehicle_yearly_mileage`,
  `vehicle_valuations`, `vehicle_elections`, `vehicle_credits`, `vehicle_renewals`,
  `vehicle_trip_templates`, `vehicle_disposals`) are registered as `state` in
  `step8_vehicles.py` and rebuild from `connector_config.bean` /
  `connector_overrides.bean` via `vehicles/reader.py`.
- **ADR-0003**: All metadata keys use `lamella-vehicle-*`, `lamella-disposal-*`,
  `lamella-election-*`, `lamella-valuation-*`, etc. prefixes.
- **ADR-0004**: All `append_*` writers delegate to `append_custom_directive`;
  disposal and transfer writers use `run_bean_check_vs_baseline` and roll back
  on new errors.
- **ADR-0007**: Auto-scaffolded expense subtree follows entity-first hierarchy:
  `Expenses:<Entity>:Vehicles:<Slug>:{Fuel,Insurance,Maintenance,...}`.
- **ADR-0015**: step 8 reconstruct covers all state tables. `vehicle_fuel_log`
  and `vehicle_data_health_cache` are explicitly registered as cache.

### Known violations
- None identified. The Phase 7 reconstruct work (closing the original gap noted
  in archived FEATURE_VEHICLES_PLAN.md) is complete. `vehicle_disposals` reconstructs
  from `#lamella-vehicle-disposal`-tagged transactions on `connector_overrides.bean`
, not a custom directive, but the reader handles that path explicitly.
- `mileage_entries` (trip log) is NOT a vehicle-feature state table; it is
  mileage-feature cache. See the mileage blueprint for that gap.

## Known gaps

- None identified. The Phase 7 reconstruct work (closing the original gap noted
  in archived FEATURE_VEHICLES_PLAN.md) is complete. `vehicle_disposals` reconstructs
  from `#lamella-vehicle-disposal`-tagged transactions on `connector_overrides.bean`
, not a custom directive, but the reader handles that path explicitly.
- `mileage_entries` (trip log) is NOT a vehicle-feature state table; it is
  mileage-feature cache. See the mileage blueprint for that gap.

## Remaining tasks

- Printable IRS mileage log (Form 4562 / Schedule C Part IV worksheet) PDF
  export, planned in archived FEATURE_VEHICLES_PLAN.md Phase 6, not yet built.
- Fuel-log MPG derivation page: `vehicle_fuel_log` table exists; the derivation
  UI and `fuel_writer.py` are present but the full data-health panel (Phase 2
  plan item) is partially built.
- AI trip categorization backfill: the `mileage_trip_meta.auto_from_ai` column
  exists; the AI caller path exists in `upsert_trip_meta`; a batch-categorize
  job over historical uncategorized trips is not yet wired.
- `vehicle_breaking_change_seen` (UI state) is cache-only: acceptable, but
  worth documenting if a breaking change banner is ever shown conditionally
  per vehicle.
