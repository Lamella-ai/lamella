---
audience: agents
read-cost-target: 110 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0011-autocomplete-everywhere.md, docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0004-bean-check-after-every-write.md
last-derived-from-code: 2026-04-26
---
# Setup

## Summary

First-run wizard: welcome → entities → bank → accounts → property/vehicle → done; gates dashboard until complete.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/setup` | `setup_page` | `src/lamella/web/routes/setup.py:5993` |
| GET | `/setup/accounts` | `setup_accounts_page` | `src/lamella/web/routes/setup.py:1918` |
| POST | `/setup/accounts/add` | `setup_account_add` | `src/lamella/web/routes/setup.py:2175` |
| POST | `/setup/accounts/close` | `setup_account_close` | `src/lamella/web/routes/setup.py:2458` |
| POST | `/setup/accounts/save` | `setup_account_save` | `src/lamella/web/routes/setup.py:2360` |
| GET | `/setup/charts` | `setup_charts_page` | `src/lamella/web/routes/setup.py:2597` |
| POST | `/setup/charts/{slug}/scaffold` | `setup_chart_scaffold` | `src/lamella/web/routes/setup.py:2660` |
| GET | `/setup/entities` | `setup_entities_page` | `src/lamella/web/routes/setup.py:86` |
| POST | `/setup/entities/add-business` | `setup_entity_add_business` | `src/lamella/web/routes/setup.py:379` |
| POST | `/setup/entities/add-person` | `setup_entity_add_person` | `src/lamella/web/routes/setup.py:306` |
| POST | `/setup/entities/{slug}/cleanup-stale-meta` | `setup_entity_cleanup_stale_meta` | `src/lamella/web/routes/setup.py:1243` |
| POST | `/setup/entities/{slug}/close-unused-opens` | `setup_entity_close_unused_opens` | `src/lamella/web/routes/setup.py:1274` |
| POST | `/setup/entities/{slug}/deactivate` | `setup_entity_deactivate` | `src/lamella/web/routes/setup.py:672` |
| POST | `/setup/entities/{slug}/delete` | `setup_entity_delete` | `src/lamella/web/routes/setup.py:768` |
| GET | `/setup/entities/{slug}/manage` | `setup_entity_manage_page` | `src/lamella/web/routes/setup.py:1061` |
| POST | `/setup/entities/{slug}/migrate-account` | `setup_entity_migrate_account` | `src/lamella/web/routes/setup.py:1423` |
| POST | `/setup/entities/{slug}/reactivate` | `setup_entity_reactivate` | `src/lamella/web/routes/setup.py:721` |
| POST | `/setup/entities/{slug}/save` | `setup_entity_save` | `src/lamella/web/routes/setup.py:468` |
| POST | `/setup/entities/{slug}/skip` | `setup_entity_skip` | `src/lamella/web/routes/setup.py:618` |
| POST | `/setup/fix-duplicate-closes` | `setup_fix_duplicate_closes` | `src/lamella/web/routes/setup.py:1778` |
| POST | `/setup/fix-orphan-overrides` | `setup_fix_orphan_overrides` | `src/lamella/web/routes/setup.py:1655` |
| GET | `/setup/import` | `setup_import_page` | `src/lamella/web/routes/setup.py:6275` |
| GET | `/setup/import-rewrite` | `setup_import_rewrite_page` | `src/lamella/web/routes/setup.py:4399` |
| POST | `/setup/import/apply` | `setup_import_apply` | `src/lamella/web/routes/setup.py:6311` |
| GET | `/setup/loans` | `setup_loans_page` | `src/lamella/web/routes/setup.py:3453` |
| POST | `/setup/loans/add` | `setup_loan_add` | `src/lamella/web/routes/setup.py:3681` |
| POST | `/setup/loans/{slug}/edit` | `setup_loan_edit` | `src/lamella/web/routes/setup.py:3867` |
| POST | `/setup/normalize-txn-identity` | `setup_normalize_txn_identity` | `src/lamella/web/routes/setup.py:1855` |
| GET | `/setup/progress` | `setup_progress_page` | `src/lamella/web/routes/setup.py:5977` |
| GET | `/setup/properties` | `setup_properties_page` | `src/lamella/web/routes/setup.py:2761` |
| POST | `/setup/properties/add` | `setup_property_add` | `src/lamella/web/routes/setup.py:2981` |
| POST | `/setup/properties/{slug}/edit` | `setup_property_edit` | `src/lamella/web/routes/setup.py:3172` |
| POST | `/setup/properties/{slug}/scaffold` | `setup_property_scaffold` | `src/lamella/web/routes/setup.py:3375` |
| GET | `/setup/reconstruct` | `setup_reconstruct_page` | `src/lamella/web/routes/setup.py:6719` |
| POST | `/setup/reconstruct` | `setup_reconstruct_run` | `src/lamella/web/routes/setup.py:6748` |
| POST | `/setup/refresh-progress` | `setup_refresh_progress` | `src/lamella/web/routes/setup.py:5955` |
| POST | `/setup/scaffold` | `setup_scaffold` | `src/lamella/web/routes/setup.py:6190` |
| GET | `/setup/simplefin` | `setup_simplefin_page` | `src/lamella/web/routes/setup.py:4089` |
| POST | `/setup/simplefin/bind` | `setup_simplefin_bind` | `src/lamella/web/routes/setup.py:4298` |
| POST | `/setup/simplefin/connect` | `setup_simplefin_connect` | `src/lamella/web/routes/setup.py:4120` |
| POST | `/setup/simplefin/disconnect` | `setup_simplefin_disconnect` | `src/lamella/web/routes/setup.py:4267` |
| POST | `/setup/simplefin/skip` | `setup_simplefin_skip` | `src/lamella/web/routes/setup.py:4240` |
| POST | `/setup/stamp-version` | `setup_stamp_version` | `src/lamella/web/routes/setup.py:1585` |
| GET | `/setup/vector-progress-partial` | `setup_vector_progress_partial` | `src/lamella/web/routes/setup.py:5930` |
| GET | `/setup/vehicles` | `setup_vehicles_page` | `src/lamella/web/routes/setup.py:4504` |
| POST | `/setup/vehicles/add` | `setup_vehicle_add` | `src/lamella/web/routes/setup.py:4807` |
| POST | `/setup/vehicles/close-unused-orphans` | `setup_vehicles_close_unused_orphans` | `src/lamella/web/routes/setup.py:5157` |
| POST | `/setup/vehicles/{slug}/edit` | `setup_vehicle_edit` | `src/lamella/web/routes/setup.py:4993` |
| GET | `/setup/vehicles/{slug}/migrate` | `setup_vehicle_migrate_page` | `src/lamella/web/routes/setup.py:5289` |
| POST | `/setup/vehicles/{slug}/migrate` | `setup_vehicle_migrate_apply` | `src/lamella/web/routes/setup.py:5391` |
| POST | `/setup/vehicles/{slug}/scaffold` | `setup_vehicle_scaffold` | `src/lamella/web/routes/setup.py:5861` |
| GET | `/setup/welcome` | `setup_welcome_page` | `src/lamella/web/routes/setup.py:6932` |
| POST | `/setup/welcome/continue` | `setup_welcome_continue` | `src/lamella/web/routes/setup.py:6964` |
| GET | `/setup/wizard` | `wizard_entry` | `src/lamella/web/routes/setup_wizard.py:439` |
| GET | `/setup/wizard/accounts` | `wizard_accounts` | `src/lamella/web/routes/setup_wizard.py:1824` |
| POST | `/setup/wizard/accounts` | `wizard_accounts_continue` | `src/lamella/web/routes/setup_wizard.py:2077` |
| POST | `/setup/wizard/accounts/remove` | `wizard_accounts_remove` | `src/lamella/web/routes/setup_wizard.py:2033` |
| POST | `/setup/wizard/accounts/save` | `wizard_accounts_save` | `src/lamella/web/routes/setup_wizard.py:1911` |
| GET | `/setup/wizard/bank` | `wizard_bank` | `src/lamella/web/routes/setup_wizard.py:1217` |
| POST | `/setup/wizard/bank/connect` | `wizard_bank_connect` | `src/lamella/web/routes/setup_wizard.py:1319` |
| POST | `/setup/wizard/bank/connected` | `wizard_bank_connected` | `src/lamella/web/routes/setup_wizard.py:1262` |
| POST | `/setup/wizard/bank/skip` | `wizard_bank_skip` | `src/lamella/web/routes/setup_wizard.py:1247` |
| GET | `/setup/wizard/done` | `wizard_done` | `src/lamella/web/routes/setup_wizard.py:3392` |
| POST | `/setup/wizard/done` | `wizard_finalize` | `src/lamella/web/routes/setup_wizard.py:3425` |
| GET | `/setup/wizard/entities` | `wizard_entities` | `src/lamella/web/routes/setup_wizard.py:847` |
| POST | `/setup/wizard/entities` | `wizard_entities_continue` | `src/lamella/web/routes/setup_wizard.py:1106` |
| POST | `/setup/wizard/entities/remove` | `wizard_entities_remove` | `src/lamella/web/routes/setup_wizard.py:1083` |
| POST | `/setup/wizard/entities/save-business` | `wizard_entities_save_business` | `src/lamella/web/routes/setup_wizard.py:1009` |
| POST | `/setup/wizard/entities/save-person` | `wizard_entities_save_person` | `src/lamella/web/routes/setup_wizard.py:940` |
| GET | `/setup/wizard/finalizing` | `wizard_finalizing` | `src/lamella/web/routes/setup_wizard.py:3507` |
| GET | `/setup/wizard/property-vehicle` | `wizard_property_vehicle` | `src/lamella/web/routes/setup_wizard.py:2776` |
| POST | `/setup/wizard/property-vehicle/continue` | `wizard_propvehicle_continue` | `src/lamella/web/routes/setup_wizard.py:3196` |
| POST | `/setup/wizard/property-vehicle/remove` | `wizard_propvehicle_remove` | `src/lamella/web/routes/setup_wizard.py:2982` |
| POST | `/setup/wizard/property-vehicle/save-property` | `wizard_save_property` | `src/lamella/web/routes/setup_wizard.py:2831` |
| POST | `/setup/wizard/property-vehicle/save-vehicle` | `wizard_save_vehicle` | `src/lamella/web/routes/setup_wizard.py:2900` |
| POST | `/setup/wizard/reset` | `wizard_reset` | `src/lamella/web/routes/setup_wizard.py:3541` |
| GET | `/setup/wizard/welcome` | `wizard_welcome` | `src/lamella/web/routes/setup_wizard.py:525` |
| POST | `/setup/wizard/welcome` | `wizard_welcome_submit` | `src/lamella/web/routes/setup_wizard.py:544` |

## Owned templates

- `src/lamella/web/templates/setup.html`
- `src/lamella/web/templates/setup_accounts.html`
- `src/lamella/web/templates/setup_charts.html`
- `src/lamella/web/templates/setup_check.html`
- `src/lamella/web/templates/setup_entities.html`
- `src/lamella/web/templates/setup_entity_manage.html`
- `src/lamella/web/templates/setup_import.html`
- `src/lamella/web/templates/setup_import_rewrite.html`
- `src/lamella/web/templates/setup_loans.html`
- `src/lamella/web/templates/setup_properties.html`
- `src/lamella/web/templates/setup_reconstruct.html`
- `src/lamella/web/templates/setup_simplefin.html`
- `src/lamella/web/templates/setup_vehicle_migrate.html`
- `src/lamella/web/templates/setup_vehicles.html`
- `src/lamella/web/templates/setup_welcome.html`

## Owned source files

- `src/lamella/core/bootstrap/classifier.py`
- `src/lamella/core/bootstrap/detection.py`
- `src/lamella/core/bootstrap/import_apply.py`
- `src/lamella/core/bootstrap/markers.py`
- `src/lamella/core/bootstrap/scaffold.py`
- `src/lamella/core/bootstrap/templates.py`
- `src/lamella/core/bootstrap/transforms.py`
- `src/lamella/features/setup/posting_counts.py`
- `src/lamella/features/setup/recovery.py`
- `src/lamella/features/setup/setup_progress.py`
- `src/lamella/features/setup/wizard_state.py`

## Owned tests

- `tests/test_scaffold.py`
- `tests/test_setup_accounts_modal.py`
- `tests/test_setup_check.py`
- `tests/test_setup_e2e.py`
- `tests/test_setup_e2e_guardrails.py`
- `tests/test_setup_entities_modal.py`
- `tests/test_setup_filter_parity.py`
- `tests/test_setup_import_route.py`
- `tests/test_setup_loans_modal.py`
- `tests/test_setup_normalize_txn_identity_route.py`
- `tests/test_setup_posting_counts.py`
- `tests/test_setup_properties_modal.py`
- `tests/test_setup_recovery_apply.py`
- `tests/test_setup_recovery_draft.py`
- `tests/test_setup_recovery_route.py`
- `tests/test_setup_repair_state_migration.py`
- `tests/test_setup_resurrection.py`
- `tests/test_setup_route.py`
- `tests/test_setup_schema_route.py`
- `tests/test_setup_schema_route_integration.py`
- `tests/test_setup_simplefin_modal.py`
- `tests/test_setup_smoke.py`
- `tests/test_setup_vehicles_modal.py`
- `tests/test_setup_wizard.py`

## ADR compliance


- **ADR-0011**: every account/entity/vehicle/property field in the wizard is
  a text input backed by `<datalist>`; never a `<select>`.
- **ADR-0001**: wizard state blob is a transient draft; the only durable
  state after Done is ledger directives + canonical table rows.
- **ADR-0004**: every connector-owned file write wraps in bean-check + rollback.

## Current state


### Wizard steps

Five steps in order (`STEP_ORDER` in `setup/wizard_state.py`):
1. **welcome**, display name + intent radio (`personal`, `business`, `both`,
   `household`, `everything`, `manual`)
2. **entities**, add individuals + businesses; entity_type + tax_schedule per entity
3. **bank**, connect SimpleFIN Bridge; seed known accounts from the bridge response
4. **accounts**, label bank/card/loan accounts (kind, entity_slug, institution,
   last_four); per-account loan scaffold
5. **propvehicle**, register properties and vehicles
6. **done**, commits all drafts atomically; stamps the wizard's `completed_at`

Draft mode: the wizard writes to `draft_entities`, `draft_accounts`,
`draft_properties`, `draft_vehicles` fields in the JSON blob. Nothing is
committed to canonical tables until the Done step. This means a browser
crash or back-navigation loses nothing.

Commit discipline at Done: every entity/account/property/vehicle is written
once. Each write follows snapshot → edit → bean-check → restore-on-error
(ADR-0004). If the bean-check fails the done step rolls back all writes and
surfaces the error.

### Compliant ADRs

- **ADR-0011**: every account/entity/vehicle/property field in the wizard is
  a text input backed by `<datalist>`; never a `<select>`.
- **ADR-0001**: wizard state blob is a transient draft; the only durable
  state after Done is ledger directives + canonical table rows.
- **ADR-0004**: every connector-owned file write wraps in bean-check + rollback.

### Known violations

None confirmed by code inspection. The `scaffolded_paths_by_slug` + rollback
path in `wizard_state.py` (tracking wizard-created account Open directives
for Close on rollback) is partially documented but no rollback code was found
in the wizard route, rollback logic may be incomplete. Needs a route-level
audit of `routes/setup_wizard.py`.

## Known gaps


None confirmed by code inspection. The `scaffolded_paths_by_slug` + rollback
path in `wizard_state.py` (tracking wizard-created account Open directives
for Close on rollback) is partially documented but no rollback code was found
in the wizard route, rollback logic may be incomplete. Needs a route-level
audit of `routes/setup_wizard.py`.

## Remaining tasks


- Confirm rollback of `connector_accounts.bean` Open directives when the
  user resets mid-wizard (the `scaffolded_paths_by_slug` field tracks created
  paths, but the actual Close-directive emission at reset was not found in
  this read).
- Verify the in-flight lock (commit `2ab834a`, gap §11.7) extends to the
  wizard's Done step, two-tab concurrent Done submissions could double-write.
- Add per-step edit modals so returning users can update individual entities
  or accounts from the wizard without restarting.
