-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 055 — bridge importer categorizations to the post-Phase-7
-- lineage UUID schema.
--
-- Pre-Phase-7 the importer's AI calls logged
-- ai_decisions.input_ref = "import:<imports.id>:row:<raw_rows.id>" — both
-- sides SQLite PKs, neither reconstruct-stable, neither matchable to the
-- eventual ledger entry without joining live staging tables. After
-- Phase 7 dropped lamella-import-source from the writer, even that
-- shaky bridge collapsed: AI decisions logged at categorize time were
-- orphaned from /txn AI history for every newly-imported entry.
--
-- The fix is structural: mint the entry's lamella-txn-id at categorize
-- time, persist it on the categorizations row, use it as the AI
-- input_ref, and have emit.render_transaction read it back so the
-- on-disk lamella-txn-id matches what the ai_decisions row already
-- claims it to be. Single shared identifier, end to end.
--
-- This column is nullable to keep the migration backwards-compatible
-- — pre-migration categorization rows have no lineage, and that's OK
-- (the read-side already falls back to txn_hash for entries lacking
-- lineage, and the on-touch normalize / /setup/recovery action
-- backfills lineage on disk over time).

ALTER TABLE categorizations
    ADD COLUMN lamella_txn_id TEXT;

CREATE INDEX IF NOT EXISTS categorizations_lamella_txn_id_idx
    ON categorizations(lamella_txn_id);
