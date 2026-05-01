---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0001-ledger-as-source-of-truth.md, docs/adr/0002-in-place-rewrites-default.md, docs/adr/0003-lamella-metadata-namespace.md, docs/adr/0004-bean-check-after-every-write.md, docs/adr/0006-long-running-ops-as-jobs.md, docs/adr/0015-reconstruct-capability-invariant.md
last-derived-from-code: 2026-04-26
---
# Loans

## Summary

Mortgage / auto / student / HELOC / revolving loan modeling: amortization, escrow, anomalies, wizards.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/settings/loans` | `loans_page` | `src/lamella/web/routes/loans.py:144` |
| POST | `/settings/loans` | `save_loan` | `src/lamella/web/routes/loans.py:200` |
| GET | `/settings/loans/wizard/{flow_name}` | `wizard_entry` | `src/lamella/web/routes/loans_wizard.py:103` |
| POST | `/settings/loans/wizard/{flow_name}/commit` | `wizard_commit` | `src/lamella/web/routes/loans_wizard.py:254` |
| POST | `/settings/loans/wizard/{flow_name}/preview` | `wizard_preview` | `src/lamella/web/routes/loans_wizard.py:194` |
| POST | `/settings/loans/wizard/{flow_name}/step` | `wizard_step` | `src/lamella/web/routes/loans_wizard.py:129` |
| GET | `/settings/loans/{slug}` | `loan_detail` | `src/lamella/web/routes/loans.py:1845` |
| POST | `/settings/loans/{slug}/anchors` | `add_balance_anchor` | `src/lamella/web/routes/loans.py:1674` |
| POST | `/settings/loans/{slug}/anchors/{anchor_id}/delete` | `delete_balance_anchor` | `src/lamella/web/routes/loans.py:1721` |
| POST | `/settings/loans/{slug}/autofix` | `loan_autofix` | `src/lamella/web/routes/loans.py:666` |
| GET | `/settings/loans/{slug}/backfill` | `backfill_entry` | `src/lamella/web/routes/loans_backfill.py:89` |
| POST | `/settings/loans/{slug}/backfill/preview` | `backfill_preview` | `src/lamella/web/routes/loans_backfill.py:111` |
| POST | `/settings/loans/{slug}/backfill/run` | `backfill_run` | `src/lamella/web/routes/loans_backfill.py:151` |
| GET | `/settings/loans/{slug}/backfill/sample.csv` | `backfill_sample_csv` | `src/lamella/web/routes/loans_backfill.py:228` |
| POST | `/settings/loans/{slug}/categorize-draw` | `loan_categorize_draw` | `src/lamella/web/routes/loans.py:719` |
| GET | `/settings/loans/{slug}/edit` | `loan_edit_page` | `src/lamella/web/routes/loans.py:484` |
| POST | `/settings/loans/{slug}/escrow/reconcile` | `loan_escrow_reconcile` | `src/lamella/web/routes/loans.py:835` |
| POST | `/settings/loans/{slug}/fund-initial` | `fund_initial_balance` | `src/lamella/web/routes/loans.py:517` |
| POST | `/settings/loans/{slug}/groups/{group_id}/confirm` | `loan_group_confirm` | `src/lamella/web/routes/loans.py:949` |
| POST | `/settings/loans/{slug}/open-accounts` | `open_loan_accounts` | `src/lamella/web/routes/loans.py:607` |
| POST | `/settings/loans/{slug}/pauses` | `add_pause` | `src/lamella/web/routes/loans.py:1737` |
| POST | `/settings/loans/{slug}/pauses/{pause_id}/delete` | `delete_pause_route` | `src/lamella/web/routes/loans.py:1824` |
| POST | `/settings/loans/{slug}/pauses/{pause_id}/end` | `end_pause_route` | `src/lamella/web/routes/loans.py:1794` |
| GET | `/settings/loans/{slug}/projection.json` | `loan_projection_json` | `src/lamella/web/routes/loans.py:1180` |
| POST | `/settings/loans/{slug}/record-missing-payment` | `record_missing_payment` | `src/lamella/web/routes/loans.py:1274` |
| POST | `/settings/loans/{slug}/record-payment` | `record_mortgage_payment` | `src/lamella/web/routes/loans.py:1409` |

## Owned templates

- `src/lamella/web/templates/loans_wizard_import_step_accounts.html`
- `src/lamella/web/templates/loans_wizard_import_step_anchor.html`
- `src/lamella/web/templates/loans_wizard_import_step_backfill_choice.html`
- `src/lamella/web/templates/loans_wizard_import_step_terms_full.html`
- `src/lamella/web/templates/loans_wizard_import_step_terms_source.html`
- `src/lamella/web/templates/loans_wizard_import_step_terms_statement.html`
- `src/lamella/web/templates/loans_wizard_payoff_step_details.html`
- `src/lamella/web/templates/loans_wizard_payoff_step_select_loan.html`
- `src/lamella/web/templates/loans_wizard_preview.html`
- `src/lamella/web/templates/loans_wizard_purchase_step_accounts.html`
- `src/lamella/web/templates/loans_wizard_purchase_step_choose_property.html`
- `src/lamella/web/templates/loans_wizard_purchase_step_funding.html`
- `src/lamella/web/templates/loans_wizard_purchase_step_loan_terms.html`
- `src/lamella/web/templates/loans_wizard_purchase_step_new_property.html`
- `src/lamella/web/templates/loans_wizard_refi_step_accounts.html`
- `src/lamella/web/templates/loans_wizard_refi_step_funding.html`
- `src/lamella/web/templates/loans_wizard_refi_step_new_loan_terms.html`
- `src/lamella/web/templates/loans_wizard_refi_step_payoff_terms.html`
- `src/lamella/web/templates/loans_wizard_refi_step_select_old.html`
- `src/lamella/web/templates/settings_loan_backfill.html`
- `src/lamella/web/templates/settings_loan_detail_adaptive.html`
- `src/lamella/web/templates/settings_loan_edit.html`

## Owned source files

- `src/lamella/features/loans/amortization.py`
- `src/lamella/features/loans/anomalies.py`
- `src/lamella/features/loans/auto_classify.py`
- `src/lamella/features/loans/backfill.py`
- `src/lamella/features/loans/claim.py`
- `src/lamella/features/loans/coverage.py`
- `src/lamella/features/loans/escrow.py`
- `src/lamella/features/loans/groups.py`
- `src/lamella/features/loans/health.py`
- `src/lamella/features/loans/layout.py`
- `src/lamella/features/loans/next_action_priorities.py`
- `src/lamella/features/loans/pauses.py`
- `src/lamella/features/loans/projection.py`
- `src/lamella/features/loans/reader.py`
- `src/lamella/features/loans/revolving.py`
- `src/lamella/features/loans/scaffolding.py`
- `src/lamella/features/loans/wizard/_base.py`
- `src/lamella/features/loans/wizard/import_existing.py`
- `src/lamella/features/loans/wizard/payoff.py`
- `src/lamella/features/loans/wizard/purchase.py`
- `src/lamella/features/loans/wizard/refi.py`
- `src/lamella/features/loans/writer.py`

## Owned tests

- `tests/test_loans_anomalies.py`
- `tests/test_loans_auto_classify.py`
- `tests/test_loans_auto_classify_inplace.py`
- `tests/test_loans_backfill.py`
- `tests/test_loans_claim.py`
- `tests/test_loans_coverage.py`
- `tests/test_loans_detail_renders.py`
- `tests/test_loans_escrow.py`
- `tests/test_loans_groups.py`
- `tests/test_loans_health.py`
- `tests/test_loans_layout.py`
- `tests/test_loans_pauses.py`
- `tests/test_loans_projection.py`
- `tests/test_loans_revolving.py`
- `tests/test_loans_revolving_draws.py`
- `tests/test_loans_scaffolding.py`
- `tests/test_loans_site1_3_4_5_preemption.py`
- `tests/test_loans_site2_ingest_preemption.py`
- `tests/test_loans_site2_tier2_autoclassify.py`
- `tests/test_loans_wizard_base.py`
- `tests/test_loans_wizard_import_existing.py`
- `tests/test_loans_wizard_payoff.py`
- `tests/test_loans_wizard_purchase.py`
- `tests/test_loans_wizard_refi.py`

## ADR compliance

- ADR-0001: loan state writes to `connector_config.bean`; steps 9, 22, 23 reconstruct `loans`, `loan_payment_groups`, `loan_pauses` from entries.
- ADR-0002: loan-funding and historical backfill writes go to `connector_overrides.bean` (override block pattern), not in-place rewrite. This is the documented exception for multi-leg entries with no FIXME source.
- ADR-0003: metadata keys use `lamella-loan-*` and `lamella-anchor-*` / `lamella-pause-*` prefixes throughout.
- ADR-0004: `write_loan_funding` and `write_synthesized_payment` both run bean-check vs. baseline and restore on failure.
- ADR-0006: the backfill job runs as a background job via `app.state.job_runner.submit`; the route returns the job modal partial.
- ADR-0015: reconstruct steps 9/22/23 are registered with correct `state_tables`.

## Current state


The loans feature is the most complete in the assets/money domain. Wizard,
coverage engine, health scorer, anomaly detector, payment group classifier,
projection endpoint, and adaptive panel layout all exist. The four wizard modes
(purchase, refi, payoff, import) are implemented under `loans/wizard/`.

### Compliant ADRs
- ADR-0001: loan state writes to `connector_config.bean`; steps 9, 22, 23 reconstruct `loans`, `loan_payment_groups`, `loan_pauses` from entries.
- ADR-0002: loan-funding and historical backfill writes go to `connector_overrides.bean` (override block pattern), not in-place rewrite. This is the documented exception for multi-leg entries with no FIXME source.
- ADR-0003: metadata keys use `lamella-loan-*` and `lamella-anchor-*` / `lamella-pause-*` prefixes throughout.
- ADR-0004: `write_loan_funding` and `write_synthesized_payment` both run bean-check vs. baseline and restore on failure.
- ADR-0006: the backfill job runs as a background job via `app.state.job_runner.submit`; the route returns the job modal partial.
- ADR-0015: reconstruct steps 9/22/23 are registered with correct `state_tables`.

### Known violations
- ADR-0001 (deferred): credit-limit history is not reconstruct-capable. When `credit_limit` changes on a HELOC, the new `custom "loan"` directive overwrites the prior value with no history. A `custom "loan-credit-limit-change"` directive + `loan_credit_limit_history` table is tracked as `DEFERRED-WP13-PHASE2` in `loans/writer.py`. The available-headroom calculation is correct for today but historical headroom queries are not answerable.

## Known gaps

- ADR-0001 (deferred): credit-limit history is not reconstruct-capable. When `credit_limit` changes on a HELOC, the new `custom "loan"` directive overwrites the prior value with no history. A `custom "loan-credit-limit-change"` directive + `loan_credit_limit_history` table is tracked as `DEFERRED-WP13-PHASE2` in `loans/writer.py`. The available-headroom calculation is correct for today but historical headroom queries are not answerable.

## Remaining tasks


1. Implement `custom "loan-credit-limit-change"` directive and `loan_credit_limit_history` table (DEFERRED-WP13-PHASE2).
2. Add reconstruct step for `loan_credit_limit_history` once the directive exists.
3. Review wizard `import_existing.py` for coverage of edge cases (interest-only periods, balloon payments).
4. Consider a `custom "loan-rate-change"` directive for ARM adjustments (noted in `coverage.py` comment at line 220).
