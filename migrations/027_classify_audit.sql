-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Classification audit: run AI against resolved txns, surface
-- disagreements with ledger's current target, let user accept
-- or dismiss.
--
-- `audit_runs` tracks each batch so the audit page can show
-- history + stats. `audit_items` holds per-txn disagreements
-- + user decisions (accept = override written; dismiss = the
-- original was right, silence future passes on this merchant
-- target).
--
-- `audit_dismissals` is the long-term "user confirmed original"
-- memory — keyed by (merchant_text, current_account) so a
-- different txn at the same merchant with the same current
-- classification doesn't re-surface.

CREATE TABLE IF NOT EXISTS audit_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TIMESTAMP,
    sampled             INTEGER NOT NULL DEFAULT 0,
    classified          INTEGER NOT NULL DEFAULT 0,
    disagreements       INTEGER NOT NULL DEFAULT 0,
    errors              INTEGER NOT NULL DEFAULT 0,
    sample_mode         TEXT NOT NULL DEFAULT 'random',
    sample_size         INTEGER NOT NULL DEFAULT 20,
    notes               TEXT
);

CREATE TABLE IF NOT EXISTS audit_items (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    audit_run_id        INTEGER NOT NULL,
    txn_hash            TEXT NOT NULL,
    txn_date            DATE NOT NULL,
    txn_amount          TEXT NOT NULL,          -- decimal-as-string
    merchant_text       TEXT,                   -- payee + narration concat
    current_account     TEXT NOT NULL,
    ai_proposed_account TEXT NOT NULL,
    ai_confidence       REAL NOT NULL,
    ai_reasoning        TEXT,
    ai_decision_id      INTEGER,
    status              TEXT NOT NULL DEFAULT 'open',  -- open | accepted | dismissed
    decided_at          TIMESTAMP,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (audit_run_id) REFERENCES audit_runs(id)
);

CREATE INDEX IF NOT EXISTS audit_items_status_idx
    ON audit_items (status);
CREATE INDEX IF NOT EXISTS audit_items_run_idx
    ON audit_items (audit_run_id);
CREATE INDEX IF NOT EXISTS audit_items_txn_idx
    ON audit_items (txn_hash);

CREATE TABLE IF NOT EXISTS audit_dismissals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    merchant_text       TEXT NOT NULL,
    current_account     TEXT NOT NULL,
    reason              TEXT,
    dismissed_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (merchant_text, current_account)
);
