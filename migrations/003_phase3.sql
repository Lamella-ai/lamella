-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 3: AI integration.
-- ai_decisions logs every AI call (classify_txn, match_receipt, parse_note,
-- rule_promotion). prompt_hash enables dedup caching across identical
-- renders. Still deferred: budgets, recurring_expenses (Phase 6).

CREATE TABLE IF NOT EXISTS ai_decisions (
    id                INTEGER PRIMARY KEY,
    decided_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    decision_type     TEXT NOT NULL,
    input_ref         TEXT NOT NULL,
    model             TEXT NOT NULL,
    prompt_tokens     INTEGER,
    completion_tokens INTEGER,
    prompt_hash       TEXT,
    result            TEXT NOT NULL,
    user_corrected    BOOLEAN NOT NULL DEFAULT 0,
    user_correction   TEXT
);

CREATE INDEX IF NOT EXISTS ai_decisions_recent_idx
    ON ai_decisions (decided_at DESC);
CREATE INDEX IF NOT EXISTS ai_decisions_lookup_idx
    ON ai_decisions (decision_type, input_ref);
CREATE INDEX IF NOT EXISTS ai_decisions_prompt_hash_idx
    ON ai_decisions (prompt_hash);
