-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Add bcg-added-at cache column on classification_rules so a future
-- engine change can switch tie-breaking to most-specific-wins +
-- most-recently-added without needing a schema migration at that time.
-- The column is populated by the step-2 reconstruct pass today, and
-- by rule writes going forward (see rule_writer.py).

ALTER TABLE classification_rules ADD COLUMN added_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS classification_rules_added_at_idx
    ON classification_rules (added_at DESC);
