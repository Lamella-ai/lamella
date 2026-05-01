-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- WP12 — forbearance / payment pause cache.
--
-- A loan in forbearance has a window where no payments are expected;
-- the coverage engine should skip generating expected rows for those
-- months instead of marking them as missing. Without this table, the
-- WP3 graceful-degradation safety net collapses 3+ consecutive
-- "missing" rows into one "long-payment-gap" attention item — which
-- is actually-correct-by-design for a real pause. Once WP12 lands,
-- the coverage engine knows about pauses and stays silent on
-- legitimately-skipped months.
--
-- Reconstruct (step23_loan_pauses) rebuilds rows from
-- `custom "loan-pause"` directives in connector_config.bean. State
-- table — verify treats any drift as a bug.

CREATE TABLE IF NOT EXISTS loan_pauses (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    loan_slug        TEXT NOT NULL REFERENCES loans(slug) ON DELETE CASCADE,
    start_date       DATE NOT NULL,
    end_date         DATE,                          -- NULL = open-ended
    reason           TEXT,                          -- 'forbearance' | 'covid' | 'hardship' | 'other'
    notes            TEXT,
    accrued_interest TEXT,                          -- Decimal fidelity; servicer's stated amount
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (loan_slug, start_date)
);

CREATE INDEX IF NOT EXISTS loan_pauses_slug_dates_idx
    ON loan_pauses (loan_slug, start_date, end_date);
