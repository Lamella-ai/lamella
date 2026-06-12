-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 6: durable budgets + recurring expenses + detection-run audit.
-- The Phase 5 in-memory predictor is deleted; the dashboard now reads
-- from recurring_expenses where status='confirmed'.

CREATE TABLE IF NOT EXISTS budgets (
    id              INTEGER PRIMARY KEY,
    label           TEXT NOT NULL,
    entity          TEXT NOT NULL,
    account_pattern TEXT NOT NULL,
    period          TEXT NOT NULL,
    amount          REAL NOT NULL,
    alert_threshold REAL NOT NULL DEFAULT 0.8,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS budgets_entity_period_idx
    ON budgets (entity, period);

CREATE TABLE IF NOT EXISTS recurring_expenses (
    id               INTEGER PRIMARY KEY,
    label            TEXT NOT NULL,
    entity           TEXT NOT NULL,
    expected_amount  REAL NOT NULL,
    expected_day     INTEGER,
    source_account   TEXT NOT NULL,
    merchant_pattern TEXT NOT NULL,
    cadence          TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'proposed',
    last_seen        DATE,
    next_expected    DATE,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    confirmed_at     TIMESTAMP,
    ignored_at       TIMESTAMP
);

CREATE INDEX IF NOT EXISTS recurring_expenses_status_idx
    ON recurring_expenses (status);
CREATE INDEX IF NOT EXISTS recurring_expenses_due_idx
    ON recurring_expenses (next_expected);

-- Dedup key for upsert on detection — one proposal per (pattern, source).
CREATE UNIQUE INDEX IF NOT EXISTS recurring_expenses_unique_idx
    ON recurring_expenses (merchant_pattern, source_account);

CREATE TABLE IF NOT EXISTS recurring_detections (
    id                INTEGER PRIMARY KEY,
    run_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    scan_window_days  INTEGER NOT NULL,
    candidates_found  INTEGER NOT NULL DEFAULT 0,
    new_proposals     INTEGER NOT NULL DEFAULT 0,
    updates           INTEGER NOT NULL DEFAULT 0,
    error             TEXT
);
