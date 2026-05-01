-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 059 — staged_transactions.lamella_txn_id (immutable identity).
--
-- The staging side has used (id, source_ref_hash) as its addressing pair
-- since migration 021. Neither is a stable public surface:
--
--   * ``id`` is a SQLite PK — reconstruct-violation. A wiped DB
--     re-mints fresh ids on re-stage.
--   * ``source_ref_hash`` changes whenever the source schema changes
--     and is by definition source-bound (a SimpleFIN id ≠ a CSV
--     natural-key hash). It can also collide post-promotion: the
--     ledger's ``txn_hash`` is a different content-hash entirely and
--     does not equal the staged row's ``source_ref_hash``.
--
-- ``lamella-txn-id`` (UUIDv7) per ADR-0019 is the canonical immutable
-- identity for an entry once it lives in the ledger. Migration 055
-- bridged the importer's ``categorizations`` row to that same id so
-- the AI ``input_ref`` and the on-disk lineage agree end-to-end.
--
-- This migration extends the same identity to the staging surface so
-- a /txn/{token} URL can resolve to:
--
--   1. The staged row (status != 'promoted') — render the staged
--      detail shape (no postings yet, source_ref blob, AI proposals).
--   2. The ledger entry (post-promotion) — render the existing
--      txn_detail shape.
--
-- Both with the SAME URL. A bookmark on a staged row stays valid
-- after the row is classified, paired, promoted, or edited.
--
-- The column is nullable to keep the migration backwards-compatible.
-- Backfill mints a UUIDv7 for every existing row (Python-side, since
-- SQLite has no UUIDv7 generator). The application layer enforces
-- non-null on insert via the StagingService.
--
-- Index but NOT UNIQUE here — see migration runner notes. UNIQUE is
-- enforced at the application layer (mint_txn_id collision is
-- astronomically unlikely under 48-bit-millisecond + 74-bit-random;
-- adding a UNIQUE index in the same migration as the backfill is
-- racy under SQLite's lack of PRAGMA defer). A future migration can
-- add UNIQUE once we're confident every install is post-backfill.

ALTER TABLE staged_transactions
    ADD COLUMN lamella_txn_id TEXT;

CREATE INDEX IF NOT EXISTS staged_tx_lamella_txn_id_idx
    ON staged_transactions(lamella_txn_id);
