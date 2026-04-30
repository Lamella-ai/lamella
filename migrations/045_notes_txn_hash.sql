-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 045_notes_txn_hash — per-transaction memos.
--
-- Adds a nullable `txn_hash` scope to notes so a user can attach a
-- memo directly to a specific transaction (not just to its date).
-- Txn-scoped notes feed into the existing classify_txn active-notes
-- pipeline alongside date-proximity and active-window matches.
--
-- Mirrored to the ledger via a new `bcg-note-txn-hash` meta key on
-- the `custom "note"` directive. Step 16 reconstruct picks it up.

ALTER TABLE notes ADD COLUMN txn_hash TEXT;

CREATE INDEX IF NOT EXISTS notes_txn_hash_idx ON notes (txn_hash);
