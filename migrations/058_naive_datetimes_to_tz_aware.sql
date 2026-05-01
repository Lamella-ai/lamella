-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 058 — convert naive datetime columns to TZ-aware UTC strings (+00:00 suffix).
--
-- ADR-0023 ("Datetimes are TZ-aware UTC at rest, user-local at display")
-- followup. ADR-0023 commit 1f36779f flipped every datetime.now() to
-- datetime.now(UTC) in Python; this migration brings the SQLite values
-- in line with the at-rest invariant.
--
-- Background. Every TIMESTAMP column populated by SQLite's
-- ``CURRENT_TIMESTAMP`` (the column DEFAULT pattern used since mig 001)
-- yields a naive UTC string ``YYYY-MM-DD HH:MM:SS`` — no timezone
-- offset, no ``Z`` suffix. ADR-0023 requires TZ-aware ``+00:00`` UTC at
-- rest so the bytes carry their own meaning rather than depending on a
-- "stored UTC by convention" comment in the writer code.
--
-- The values are already UTC — SQLite's ``CURRENT_TIMESTAMP`` is
-- documented to be UTC. The fix is purely lexical: append ``+00:00`` to
-- the naive string so ``datetime.fromisoformat`` returns an aware
-- datetime on read. Nothing else moves; no time math runs here.
--
-- Idempotent — the WHERE guard on each UPDATE skips rows that already
-- carry an offset (``+HH:MM`` form, including ``+00:00``) or the
-- ``Z`` suffix. Re-running the migration on a partially-migrated DB is
-- a no-op for the rows that already match the new shape, and a
-- one-shot fix for the rows still naive (e.g. inserted by an older
-- container revving up briefly between this migration and a Python
-- code rev that started writing aware ISO strings directly).
--
-- ─── Scope ───────────────────────────────────────────────────────────
--
-- Every TIMESTAMP column shipped by migrations 001..057 that gets
-- populated via SQLite's CURRENT_TIMESTAMP (either as DEFAULT or in
-- explicit UPDATE/INSERT statements). The list below was derived by
-- parsing each prior migration's CREATE TABLE / ALTER TABLE clauses
-- for TIMESTAMP-typed columns.
--
-- Columns whose only writer is Python code that already emits aware
-- ISO ("...+00:00") are unaffected by the WHERE guard and remain
-- untouched; including them costs nothing and protects against any
-- forgotten CURRENT_TIMESTAMP write site that might surface later.
--
-- Out of scope for THIS migration:
--   * ``schema_migrations.applied_at`` — written exclusively by the
--     migration runner itself (lamella.core.db.migrate); the runner's
--     INSERT does not supply a value, so the column DEFAULT
--     CURRENT_TIMESTAMP fires. Touching this column inside a
--     migration is fine (the runner reads version not applied_at) but
--     it is more readable to leave it as the raw naive value the
--     runner produced — there is no reader that does time math.
--     Excluded for clarity.
--   * Date-typed columns (DATE, last_seen, next_expected, etc.) —
--     dates are TZ-agnostic by definition.
--   * Money / decimal / counter columns — orthogonal to ADR-0023.
--
-- The migration runner (lamella.core.db.migrate) wraps every file in
-- a single BEGIN/COMMIT envelope already; this file is a pure list of
-- statements without its own BEGIN/COMMIT (mirrors mig 057).

-- ─── account_balance_anchors ─────────────────────────────────────────
UPDATE account_balance_anchors
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── account_classify_context ────────────────────────────────────────
UPDATE account_classify_context
   SET updated_at = updated_at || '+00:00'
 WHERE updated_at IS NOT NULL
   AND updated_at NOT LIKE '%+__:__'
   AND updated_at NOT LIKE '%Z';

-- ─── accounts_meta ───────────────────────────────────────────────────
UPDATE accounts_meta
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

UPDATE accounts_meta
   SET updated_at = updated_at || '+00:00'
 WHERE updated_at IS NOT NULL
   AND updated_at NOT LIKE '%+__:__'
   AND updated_at NOT LIKE '%Z';

-- ─── ai_decisions ────────────────────────────────────────────────────
UPDATE ai_decisions
   SET decided_at = decided_at || '+00:00'
 WHERE decided_at IS NOT NULL
   AND decided_at NOT LIKE '%+__:__'
   AND decided_at NOT LIKE '%Z';

-- ─── app_settings ────────────────────────────────────────────────────
UPDATE app_settings
   SET updated_at = updated_at || '+00:00'
 WHERE updated_at IS NOT NULL
   AND updated_at NOT LIKE '%+__:__'
   AND updated_at NOT LIKE '%Z';

-- ─── audit_dismissals ────────────────────────────────────────────────
UPDATE audit_dismissals
   SET dismissed_at = dismissed_at || '+00:00'
 WHERE dismissed_at IS NOT NULL
   AND dismissed_at NOT LIKE '%+__:__'
   AND dismissed_at NOT LIKE '%Z';

-- ─── audit_items ─────────────────────────────────────────────────────
UPDATE audit_items
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

UPDATE audit_items
   SET decided_at = decided_at || '+00:00'
 WHERE decided_at IS NOT NULL
   AND decided_at NOT LIKE '%+__:__'
   AND decided_at NOT LIKE '%Z';

-- ─── audit_runs ──────────────────────────────────────────────────────
UPDATE audit_runs
   SET started_at = started_at || '+00:00'
 WHERE started_at IS NOT NULL
   AND started_at NOT LIKE '%+__:__'
   AND started_at NOT LIKE '%Z';

UPDATE audit_runs
   SET finished_at = finished_at || '+00:00'
 WHERE finished_at IS NOT NULL
   AND finished_at NOT LIKE '%+__:__'
   AND finished_at NOT LIKE '%Z';

-- ─── budgets ─────────────────────────────────────────────────────────
UPDATE budgets
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

UPDATE budgets
   SET updated_at = updated_at || '+00:00'
 WHERE updated_at IS NOT NULL
   AND updated_at NOT LIKE '%+__:__'
   AND updated_at NOT LIKE '%Z';

-- ─── business_cache ──────────────────────────────────────────────────
UPDATE business_cache
   SET computed_at = computed_at || '+00:00'
 WHERE computed_at IS NOT NULL
   AND computed_at NOT LIKE '%+__:__'
   AND computed_at NOT LIKE '%Z';

-- ─── classification_rules ────────────────────────────────────────────
UPDATE classification_rules
   SET added_at = added_at || '+00:00'
 WHERE added_at IS NOT NULL
   AND added_at NOT LIKE '%+__:__'
   AND added_at NOT LIKE '%Z';

UPDATE classification_rules
   SET last_used = last_used || '+00:00'
 WHERE last_used IS NOT NULL
   AND last_used NOT LIKE '%+__:__'
   AND last_used NOT LIKE '%Z';

-- ─── day_reviews ─────────────────────────────────────────────────────
UPDATE day_reviews
   SET last_reviewed_at = last_reviewed_at || '+00:00'
 WHERE last_reviewed_at IS NOT NULL
   AND last_reviewed_at NOT LIKE '%+__:__'
   AND last_reviewed_at NOT LIKE '%Z';

UPDATE day_reviews
   SET ai_summary_at = ai_summary_at || '+00:00'
 WHERE ai_summary_at IS NOT NULL
   AND ai_summary_at NOT LIKE '%+__:__'
   AND ai_summary_at NOT LIKE '%Z';

UPDATE day_reviews
   SET ai_audit_result_at = ai_audit_result_at || '+00:00'
 WHERE ai_audit_result_at IS NOT NULL
   AND ai_audit_result_at NOT LIKE '%+__:__'
   AND ai_audit_result_at NOT LIKE '%Z';

UPDATE day_reviews
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

UPDATE day_reviews
   SET updated_at = updated_at || '+00:00'
 WHERE updated_at IS NOT NULL
   AND updated_at NOT LIKE '%+__:__'
   AND updated_at NOT LIKE '%Z';

-- ─── entities ────────────────────────────────────────────────────────
UPDATE entities
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── imports ─────────────────────────────────────────────────────────
UPDATE imports
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

UPDATE imports
   SET committed_at = committed_at || '+00:00'
 WHERE committed_at IS NOT NULL
   AND committed_at NOT LIKE '%+__:__'
   AND committed_at NOT LIKE '%Z';

-- ─── job_events ──────────────────────────────────────────────────────
UPDATE job_events
   SET ts = ts || '+00:00'
 WHERE ts IS NOT NULL
   AND ts NOT LIKE '%+__:__'
   AND ts NOT LIKE '%Z';

-- ─── jobs ────────────────────────────────────────────────────────────
UPDATE jobs
   SET started_at = started_at || '+00:00'
 WHERE started_at IS NOT NULL
   AND started_at NOT LIKE '%+__:__'
   AND started_at NOT LIKE '%Z';

UPDATE jobs
   SET finished_at = finished_at || '+00:00'
 WHERE finished_at IS NOT NULL
   AND finished_at NOT LIKE '%+__:__'
   AND finished_at NOT LIKE '%Z';

UPDATE jobs
   SET last_progress_at = last_progress_at || '+00:00'
 WHERE last_progress_at IS NOT NULL
   AND last_progress_at NOT LIKE '%+__:__'
   AND last_progress_at NOT LIKE '%Z';

-- ─── loan_autoclass_log ──────────────────────────────────────────────
UPDATE loan_autoclass_log
   SET decided_at = decided_at || '+00:00'
 WHERE decided_at IS NOT NULL
   AND decided_at NOT LIKE '%+__:__'
   AND decided_at NOT LIKE '%Z';

-- ─── loan_balance_anchors ────────────────────────────────────────────
UPDATE loan_balance_anchors
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── loan_pauses ─────────────────────────────────────────────────────
UPDATE loan_pauses
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── loan_payment_groups ─────────────────────────────────────────────
UPDATE loan_payment_groups
   SET confirmed_at = confirmed_at || '+00:00'
 WHERE confirmed_at IS NOT NULL
   AND confirmed_at NOT LIKE '%+__:__'
   AND confirmed_at NOT LIKE '%Z';

UPDATE loan_payment_groups
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── loans ───────────────────────────────────────────────────────────
UPDATE loans
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── merchant_memory ─────────────────────────────────────────────────
UPDATE merchant_memory
   SET last_used_at = last_used_at || '+00:00'
 WHERE last_used_at IS NOT NULL
   AND last_used_at NOT LIKE '%+__:__'
   AND last_used_at NOT LIKE '%Z';

-- ─── mileage_entries ─────────────────────────────────────────────────
UPDATE mileage_entries
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- mileage_entries.csv_mtime represents the source-file mtime, written by
-- Python with the file's stat mtime; not CURRENT_TIMESTAMP. The guard
-- below still normalizes any stray naive value but is unlikely to fire.
UPDATE mileage_entries
   SET csv_mtime = csv_mtime || '+00:00'
 WHERE csv_mtime IS NOT NULL
   AND csv_mtime NOT LIKE '%+__:__'
   AND csv_mtime NOT LIKE '%Z';

-- ─── mileage_imports ─────────────────────────────────────────────────
UPDATE mileage_imports
   SET imported_at = imported_at || '+00:00'
 WHERE imported_at IS NOT NULL
   AND imported_at NOT LIKE '%+__:__'
   AND imported_at NOT LIKE '%Z';

-- ─── mileage_rates ───────────────────────────────────────────────────
UPDATE mileage_rates
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── mileage_trip_meta ───────────────────────────────────────────────
UPDATE mileage_trip_meta
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── notes ───────────────────────────────────────────────────────────
UPDATE notes
   SET captured_at = captured_at || '+00:00'
 WHERE captured_at IS NOT NULL
   AND captured_at NOT LIKE '%+__:__'
   AND captured_at NOT LIKE '%Z';

-- ─── notifications ───────────────────────────────────────────────────
UPDATE notifications
   SET sent_at = sent_at || '+00:00'
 WHERE sent_at IS NOT NULL
   AND sent_at NOT LIKE '%+__:__'
   AND sent_at NOT LIKE '%Z';

-- ─── paperless_doc_index ─────────────────────────────────────────────
UPDATE paperless_doc_index
   SET modified_at = modified_at || '+00:00'
 WHERE modified_at IS NOT NULL
   AND modified_at NOT LIKE '%+__:__'
   AND modified_at NOT LIKE '%Z';

UPDATE paperless_doc_index
   SET last_synced_at = last_synced_at || '+00:00'
 WHERE last_synced_at IS NOT NULL
   AND last_synced_at NOT LIKE '%+__:__'
   AND last_synced_at NOT LIKE '%Z';

-- ─── paperless_field_map ─────────────────────────────────────────────
UPDATE paperless_field_map
   SET updated_at = updated_at || '+00:00'
 WHERE updated_at IS NOT NULL
   AND updated_at NOT LIKE '%+__:__'
   AND updated_at NOT LIKE '%Z';

-- ─── paperless_sync_state ────────────────────────────────────────────
UPDATE paperless_sync_state
   SET last_full_sync_at = last_full_sync_at || '+00:00'
 WHERE last_full_sync_at IS NOT NULL
   AND last_full_sync_at NOT LIKE '%+__:__'
   AND last_full_sync_at NOT LIKE '%Z';

UPDATE paperless_sync_state
   SET last_incremental_sync_at = last_incremental_sync_at || '+00:00'
 WHERE last_incremental_sync_at IS NOT NULL
   AND last_incremental_sync_at NOT LIKE '%+__:__'
   AND last_incremental_sync_at NOT LIKE '%Z';

UPDATE paperless_sync_state
   SET last_modified_cursor = last_modified_cursor || '+00:00'
 WHERE last_modified_cursor IS NOT NULL
   AND last_modified_cursor NOT LIKE '%+__:__'
   AND last_modified_cursor NOT LIKE '%Z';

-- ─── paperless_writeback_log ─────────────────────────────────────────
UPDATE paperless_writeback_log
   SET applied_at = applied_at || '+00:00'
 WHERE applied_at IS NOT NULL
   AND applied_at NOT LIKE '%+__:__'
   AND applied_at NOT LIKE '%Z';

-- ─── project_txns ────────────────────────────────────────────────────
UPDATE project_txns
   SET decided_at = decided_at || '+00:00'
 WHERE decided_at IS NOT NULL
   AND decided_at NOT LIKE '%+__:__'
   AND decided_at NOT LIKE '%Z';

-- ─── projects ────────────────────────────────────────────────────────
UPDATE projects
   SET closed_at = closed_at || '+00:00'
 WHERE closed_at IS NOT NULL
   AND closed_at NOT LIKE '%+__:__'
   AND closed_at NOT LIKE '%Z';

UPDATE projects
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

UPDATE projects
   SET updated_at = updated_at || '+00:00'
 WHERE updated_at IS NOT NULL
   AND updated_at NOT LIKE '%+__:__'
   AND updated_at NOT LIKE '%Z';

-- ─── properties ──────────────────────────────────────────────────────
UPDATE properties
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── property_valuations ─────────────────────────────────────────────
UPDATE property_valuations
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── receipt_dismissals ──────────────────────────────────────────────
UPDATE receipt_dismissals
   SET dismissed_at = dismissed_at || '+00:00'
 WHERE dismissed_at IS NOT NULL
   AND dismissed_at NOT LIKE '%+__:__'
   AND dismissed_at NOT LIKE '%Z';

-- ─── receipt_links ───────────────────────────────────────────────────
UPDATE receipt_links
   SET linked_at = linked_at || '+00:00'
 WHERE linked_at IS NOT NULL
   AND linked_at NOT LIKE '%+__:__'
   AND linked_at NOT LIKE '%Z';

-- ─── recurring_detections ────────────────────────────────────────────
UPDATE recurring_detections
   SET run_at = run_at || '+00:00'
 WHERE run_at IS NOT NULL
   AND run_at NOT LIKE '%+__:__'
   AND run_at NOT LIKE '%Z';

-- ─── recurring_expenses (the canonical motivator: ignored_at) ────────
UPDATE recurring_expenses
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

UPDATE recurring_expenses
   SET confirmed_at = confirmed_at || '+00:00'
 WHERE confirmed_at IS NOT NULL
   AND confirmed_at NOT LIKE '%+__:__'
   AND confirmed_at NOT LIKE '%Z';

UPDATE recurring_expenses
   SET ignored_at = ignored_at || '+00:00'
 WHERE ignored_at IS NOT NULL
   AND ignored_at NOT LIKE '%+__:__'
   AND ignored_at NOT LIKE '%Z';

-- ─── review_actions ──────────────────────────────────────────────────
UPDATE review_actions
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

UPDATE review_actions
   SET undone_at = undone_at || '+00:00'
 WHERE undone_at IS NOT NULL
   AND undone_at NOT LIKE '%+__:__'
   AND undone_at NOT LIKE '%Z';

-- ─── review_queue ────────────────────────────────────────────────────
UPDATE review_queue
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

UPDATE review_queue
   SET resolved_at = resolved_at || '+00:00'
 WHERE resolved_at IS NOT NULL
   AND resolved_at NOT LIKE '%+__:__'
   AND resolved_at NOT LIKE '%Z';

UPDATE review_queue
   SET deferred_until = deferred_until || '+00:00'
 WHERE deferred_until IS NOT NULL
   AND deferred_until NOT LIKE '%+__:__'
   AND deferred_until NOT LIKE '%Z';

-- ─── setup_recovery_lock ─────────────────────────────────────────────
UPDATE setup_recovery_lock
   SET acquired_at = acquired_at || '+00:00'
 WHERE acquired_at IS NOT NULL
   AND acquired_at NOT LIKE '%+__:__'
   AND acquired_at NOT LIKE '%Z';

-- ─── setup_repair_state ──────────────────────────────────────────────
UPDATE setup_repair_state
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

UPDATE setup_repair_state
   SET updated_at = updated_at || '+00:00'
 WHERE updated_at IS NOT NULL
   AND updated_at NOT LIKE '%+__:__'
   AND updated_at NOT LIKE '%Z';

-- ─── setup_wizard_state ──────────────────────────────────────────────
UPDATE setup_wizard_state
   SET started_at = started_at || '+00:00'
 WHERE started_at IS NOT NULL
   AND started_at NOT LIKE '%+__:__'
   AND started_at NOT LIKE '%Z';

UPDATE setup_wizard_state
   SET updated_at = updated_at || '+00:00'
 WHERE updated_at IS NOT NULL
   AND updated_at NOT LIKE '%+__:__'
   AND updated_at NOT LIKE '%Z';

UPDATE setup_wizard_state
   SET completed_at = completed_at || '+00:00'
 WHERE completed_at IS NOT NULL
   AND completed_at NOT LIKE '%+__:__'
   AND completed_at NOT LIKE '%Z';

-- ─── simplefin_discovered_accounts ───────────────────────────────────
UPDATE simplefin_discovered_accounts
   SET discovered_at = discovered_at || '+00:00'
 WHERE discovered_at IS NOT NULL
   AND discovered_at NOT LIKE '%+__:__'
   AND discovered_at NOT LIKE '%Z';

-- ─── simplefin_ingests ───────────────────────────────────────────────
UPDATE simplefin_ingests
   SET started_at = started_at || '+00:00'
 WHERE started_at IS NOT NULL
   AND started_at NOT LIKE '%+__:__'
   AND started_at NOT LIKE '%Z';

UPDATE simplefin_ingests
   SET finished_at = finished_at || '+00:00'
 WHERE finished_at IS NOT NULL
   AND finished_at NOT LIKE '%+__:__'
   AND finished_at NOT LIKE '%Z';

-- ─── txn_classification_modified ─────────────────────────────────────
UPDATE txn_classification_modified
   SET modified_at = modified_at || '+00:00'
 WHERE modified_at IS NOT NULL
   AND modified_at NOT LIKE '%+__:__'
   AND modified_at NOT LIKE '%Z';

-- ─── user_ui_state ───────────────────────────────────────────────────
UPDATE user_ui_state
   SET updated_at = updated_at || '+00:00'
 WHERE updated_at IS NOT NULL
   AND updated_at NOT LIKE '%+__:__'
   AND updated_at NOT LIKE '%Z';

-- ─── vector_index_runs ───────────────────────────────────────────────
UPDATE vector_index_runs
   SET started_at = started_at || '+00:00'
 WHERE started_at IS NOT NULL
   AND started_at NOT LIKE '%+__:__'
   AND started_at NOT LIKE '%Z';

UPDATE vector_index_runs
   SET finished_at = finished_at || '+00:00'
 WHERE finished_at IS NOT NULL
   AND finished_at NOT LIKE '%+__:__'
   AND finished_at NOT LIKE '%Z';

-- ─── vehicle_breaking_change_seen ────────────────────────────────────
UPDATE vehicle_breaking_change_seen
   SET seen_at = seen_at || '+00:00'
 WHERE seen_at IS NOT NULL
   AND seen_at NOT LIKE '%+__:__'
   AND seen_at NOT LIKE '%Z';

UPDATE vehicle_breaking_change_seen
   SET dismissed_at = dismissed_at || '+00:00'
 WHERE dismissed_at IS NOT NULL
   AND dismissed_at NOT LIKE '%+__:__'
   AND dismissed_at NOT LIKE '%Z';

-- ─── vehicle_credits ─────────────────────────────────────────────────
UPDATE vehicle_credits
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── vehicle_data_health_cache ───────────────────────────────────────
UPDATE vehicle_data_health_cache
   SET computed_at = computed_at || '+00:00'
 WHERE computed_at IS NOT NULL
   AND computed_at NOT LIKE '%+__:__'
   AND computed_at NOT LIKE '%Z';

-- ─── vehicle_disposals ───────────────────────────────────────────────
UPDATE vehicle_disposals
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── vehicle_elections ───────────────────────────────────────────────
UPDATE vehicle_elections
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── vehicle_fuel_log ────────────────────────────────────────────────
UPDATE vehicle_fuel_log
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── vehicle_renewals ────────────────────────────────────────────────
UPDATE vehicle_renewals
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── vehicle_trip_templates ──────────────────────────────────────────
UPDATE vehicle_trip_templates
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── vehicle_valuations ──────────────────────────────────────────────
UPDATE vehicle_valuations
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';

-- ─── vehicles ────────────────────────────────────────────────────────
UPDATE vehicles
   SET created_at = created_at || '+00:00'
 WHERE created_at IS NOT NULL
   AND created_at NOT LIKE '%+__:__'
   AND created_at NOT LIKE '%Z';
