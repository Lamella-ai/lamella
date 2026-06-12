-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 042_account_balance_anchors — per-account balance anchor audit surface.
--
-- Generalizes loan_balance_anchors to every account (checking, credit,
-- brokerage, etc.). A "known balance" on a given date. Segments between
-- consecutive anchors form the audit surface: for each segment, compute
-- (next.balance - prev.balance) vs (sum of postings to this account in
-- [prev.date, next.date]). Drift = asserted − computed; non-zero means
-- the ledger's accounting of that segment doesn't match reality.
--
-- Source column records provenance so the UI can weight "statement"
-- and "simplefin" anchors more heavily than "manual" ones.

CREATE TABLE IF NOT EXISTS account_balance_anchors (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    account_path  TEXT NOT NULL,
    as_of_date    DATE NOT NULL,
    balance       TEXT NOT NULL,      -- string for Decimal fidelity
    currency      TEXT NOT NULL DEFAULT 'USD',
    source        TEXT,               -- 'statement' | 'simplefin' | 'manual' | 'payoff' | ...
    notes         TEXT,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (account_path, as_of_date)
);

CREATE INDEX IF NOT EXISTS account_balance_anchors_path_date_idx
    ON account_balance_anchors (account_path, as_of_date);
