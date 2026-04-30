-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 9.5f — loan balance anchors.
--
-- An anchor is the user's statement-observed balance on a specific
-- date: "on 2020-01-01, the mortgage balance was $183,412.05". Use
-- cases:
--   * Extra-principal / bonus payments broke the textbook amortization
--     schedule, so the "remaining principal" we compute from APR + term
--     drifts from reality. The anchor pins reality to a date.
--   * Loan predates the ledger (25-year mortgage opened 1996, ledger
--     only goes back to 2020). The 2020 anchor becomes the effective
--     starting balance for ledger-based tracking; pre-2020 history
--     remains a gap you don't try to backfill.
--   * Refi or servicer change that reset the balance display.
--
-- Multiple anchors per loan are fine. The detail page uses the latest
-- anchor with as_of_date <= today, then walks ledger postings to the
-- liability account forward from that date to get the current
-- ledger-true remaining balance.

CREATE TABLE IF NOT EXISTS loan_balance_anchors (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    loan_slug   TEXT NOT NULL REFERENCES loans(slug) ON DELETE CASCADE,
    as_of_date  DATE NOT NULL,
    balance     TEXT NOT NULL,    -- stored as string for Decimal fidelity
    source      TEXT,             -- "statement" | "payoff letter" | "user" | ...
    notes       TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (loan_slug, as_of_date)
);

CREATE INDEX IF NOT EXISTS loan_balance_anchors_slug_date_idx
    ON loan_balance_anchors(loan_slug, as_of_date);
