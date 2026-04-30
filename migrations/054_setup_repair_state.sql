-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 054 — recovery / repair-state for Phase 6 bulk-apply.
--
-- Stores the user's draft decisions during a /setup/recovery
-- bulk-review pass: which findings to apply, edits to proposed
-- fixes, which to dismiss for the session, and a per-group
-- applied_history list that enables Resume-from-failed-group.
--
-- This is NOT a state-of-truth table. The user's decisions land
-- on the ledger via the heal-action chain (Phase 5's
-- heal_schema_drift, Phase 3's heal_legacy_path, etc.); this row
-- only persists the in-progress draft + the partial-success
-- audit trail. Per the Phase 6 spec freeze:
--
--   * In-progress drafts are inherently transient — there's no
--     canonical ledger source for "what the user was about to
--     apply but didn't yet click Apply on." Reconstruct does NOT
--     rebuild this table from the ledger; instead, reconstruct
--     leaves an existing repair_state row alone (the user's
--     work-in-progress is theirs).
--
--   * The applied_history list is durable forensic data — it
--     records which findings landed in which group at what
--     timestamp. A reconstruct or migration re-run mid-batch
--     must NOT drop this list, because the Resume-from-failed-
--     group button relies on it to know where to pick up.
--
-- IF NOT EXISTS guards both clauses: a re-run of db.migrate after
-- an upgrade re-applies the file, but the IF NOT EXISTS prevents
-- the re-create from blowing away an existing draft. SQLite's
-- migration ordering — apply per-file inside BEGIN/COMMIT —
-- already gives us per-file atomicity; the IF NOT EXISTS is
-- belt-and-suspenders for the reconstruct path that bypasses the
-- migration runner.
--
-- state_json blob shape (lock per the Phase 6 spec freeze):
--
--   {
--     "findings": {
--       "<finding_id>": {
--         "action":       "apply" | "edit" | "dismiss",
--         "edit_payload": null | { <per-category editable fields> }
--       }
--     },
--     "applied_history": [
--       {
--         "group":               "schema" | "labels" | "cleanup",
--         "committed_at":        "<ISO-8601 UTC>",
--         "applied_finding_ids": ["<id>", ...],
--         "failed_finding_ids":  ["<id>", ...]
--       }
--     ]
--   }
--
-- session_id is the literal string "current" for v1 — recovery
-- is single-session, unlike the wizard which can span days. A
-- future expansion to multiple parallel repair sessions can
-- introduce real session IDs without a schema change (column
-- type is TEXT and accepts any value).

CREATE TABLE IF NOT EXISTS setup_repair_state (
    session_id  TEXT NOT NULL PRIMARY KEY,
    state_json  TEXT NOT NULL DEFAULT '{"findings":{},"applied_history":[]}',
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
