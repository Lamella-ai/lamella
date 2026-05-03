-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 9.5b — loans (mortgages, auto, student, HELOC).
--
-- Each loan has a liability account in the ledger (auto-created) plus
-- interest + escrow expense buckets. The card UX detects payments to
-- the loan's SimpleFIN account and offers an amortization-aware split
-- pre-fill.

CREATE TABLE IF NOT EXISTS loans (
    slug                     TEXT PRIMARY KEY,
    display_name             TEXT,
    loan_type                TEXT NOT NULL,       -- mortgage | auto | student | personal | heloc | other
    entity_slug              TEXT REFERENCES entities(slug),
    institution              TEXT,
    original_principal       TEXT NOT NULL,
    funded_date              DATE NOT NULL,
    first_payment_date       DATE,
    payment_due_day          INTEGER,
    term_months              INTEGER,
    interest_rate_apr        TEXT,
    monthly_payment_estimate TEXT,
    escrow_monthly           TEXT,
    simplefin_account_id     TEXT,
    payoff_date              DATE,
    payoff_amount            TEXT,
    is_active                INTEGER NOT NULL DEFAULT 1,
    notes                    TEXT,
    created_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS loans_simplefin_idx ON loans(simplefin_account_id);
CREATE INDEX IF NOT EXISTS loans_entity_idx    ON loans(entity_slug);
