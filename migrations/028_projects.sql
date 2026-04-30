-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Projects: named, dated, budgeted classification contexts.
--
-- A project spans a date range and carries an expected-merchants
-- list. During its active window, any transaction at an expected
-- merchant gets the project's full paragraph (description +
-- budget + business purpose) injected into the classify prompt
-- as another directional context source — same as an active
-- note, but structured and persistent.
--
-- Reconstructable via a future `custom "project"` directive in
-- connector_config.bean; initially persisted in SQLite only.

CREATE TABLE IF NOT EXISTS projects (
    slug                TEXT PRIMARY KEY,
    display_name        TEXT NOT NULL,
    description         TEXT,
    entity_slug         TEXT,                       -- owning entity (Acme, Personal, etc.)
    property_slug       TEXT,                       -- optional property association
    project_type        TEXT,                       -- 'home_improvement' | 'business' | 'medical' | 'home_office' | 'other'
    start_date          DATE NOT NULL,
    end_date            DATE,                       -- nullable = still active
    budget_amount       TEXT,                       -- decimal-as-string; NULL = no budget tracking
    expected_merchants  TEXT,                       -- JSON array of strings
    is_active           INTEGER NOT NULL DEFAULT 1,
    closed_at           TIMESTAMP,
    closeout_json       TEXT,                       -- snapshot at close (totals, splits)
    notes               TEXT,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS projects_entity_idx ON projects (entity_slug);
CREATE INDEX IF NOT EXISTS projects_dates_idx  ON projects (start_date, end_date);
CREATE INDEX IF NOT EXISTS projects_active_idx ON projects (is_active);

-- Cache of txns the classifier decided belonged to a project.
-- Reconstructable from ledger + projects.expected_merchants.

CREATE TABLE IF NOT EXISTS project_txns (
    project_slug        TEXT NOT NULL,
    txn_hash            TEXT NOT NULL,
    txn_date            DATE NOT NULL,
    txn_amount          TEXT NOT NULL,
    merchant_text       TEXT,
    decided_by          TEXT NOT NULL DEFAULT 'ai',  -- 'ai' | 'user' | 'rule'
    decided_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_slug, txn_hash),
    FOREIGN KEY (project_slug) REFERENCES projects(slug) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS project_txns_txn_idx ON project_txns (txn_hash);
CREATE INDEX IF NOT EXISTS project_txns_date_idx ON project_txns (txn_date);
