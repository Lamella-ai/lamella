-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Immediate Fix — wire active notes into the classification context.
--
-- Notes get a date range (active_from / active_to), optional entity
-- and card scopes, a card_override flag (stubbed now, consumed by
-- Phase G7), and a keywords column. The classifier can then query
-- "give me every note whose range covers this transaction's date"
-- and include the results in the prompt.
--
-- All new columns are nullable or have safe defaults so existing
-- rows (captured before this migration) continue to work. An
-- existing note without active_from/active_to is treated as a
-- single-day note on its captured_at date at query time.

ALTER TABLE notes ADD COLUMN active_from TEXT;      -- ISO date
ALTER TABLE notes ADD COLUMN active_to   TEXT;      -- ISO date
ALTER TABLE notes ADD COLUMN entity_scope TEXT;     -- null = global
ALTER TABLE notes ADD COLUMN card_scope   TEXT;     -- null = any card
ALTER TABLE notes ADD COLUMN card_override INTEGER NOT NULL DEFAULT 0;
ALTER TABLE notes ADD COLUMN keywords_json TEXT;    -- JSON array

CREATE INDEX IF NOT EXISTS notes_active_range_idx
    ON notes (active_from, active_to);
CREATE INDEX IF NOT EXISTS notes_entity_scope_idx
    ON notes (entity_scope);
CREATE INDEX IF NOT EXISTS notes_card_scope_idx
    ON notes (card_scope);
