-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 2: rules engine + editable settings KV.
-- Still deferred: budgets, recurring_expenses, ai_decisions (Phase 3+).

CREATE TABLE IF NOT EXISTS classification_rules (
    id             INTEGER PRIMARY KEY,
    pattern_type   TEXT NOT NULL,
    pattern_value  TEXT NOT NULL,
    card_account   TEXT,
    target_account TEXT NOT NULL,
    confidence     REAL NOT NULL DEFAULT 1.0,
    hit_count      INTEGER NOT NULL DEFAULT 0,
    last_used      TIMESTAMP,
    created_by     TEXT NOT NULL DEFAULT 'user'
);

CREATE INDEX IF NOT EXISTS classification_rules_lookup_idx
    ON classification_rules (pattern_type, card_account);

-- Dedup key for learn_from_decision: avoid inserting the same pattern twice.
CREATE UNIQUE INDEX IF NOT EXISTS classification_rules_dedup_idx
    ON classification_rules (pattern_type, pattern_value, IFNULL(card_account, ''), target_account);

CREATE TABLE IF NOT EXISTS app_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
