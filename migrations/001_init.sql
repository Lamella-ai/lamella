-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 1 schema for beancounter-glue.
-- Later phases introduce classification_rules, budgets, recurring_expenses,
-- ai_decisions. Do not add them here.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS review_queue (
    id            INTEGER PRIMARY KEY,
    kind          TEXT NOT NULL,
    source_ref    TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at   TIMESTAMP,
    priority      INTEGER NOT NULL DEFAULT 0,
    ai_suggestion TEXT,
    ai_model      TEXT,
    user_decision TEXT
);

CREATE INDEX IF NOT EXISTS review_queue_open_idx
    ON review_queue (resolved_at, priority DESC, created_at);

CREATE TABLE IF NOT EXISTS notes (
    id               INTEGER PRIMARY KEY,
    captured_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    body             TEXT NOT NULL,
    entity_hint      TEXT,
    merchant_hint    TEXT,
    resolved_txn     TEXT,
    resolved_receipt INTEGER,
    status           TEXT NOT NULL DEFAULT 'open'
);

CREATE INDEX IF NOT EXISTS notes_status_idx ON notes (status, captured_at DESC);

CREATE TABLE IF NOT EXISTS receipt_links (
    id               INTEGER PRIMARY KEY,
    paperless_id     INTEGER NOT NULL,
    txn_hash         TEXT NOT NULL,
    txn_date         DATE,
    txn_amount       REAL,
    match_method     TEXT,
    match_confidence REAL,
    linked_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (paperless_id, txn_hash)
);

CREATE INDEX IF NOT EXISTS receipt_links_paperless_idx ON receipt_links (paperless_id);
