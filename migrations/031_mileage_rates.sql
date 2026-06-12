-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Date-ranged IRS standard mileage deduction rates.
--
-- The IRS updates the federal standard mileage rate roughly
-- every 6 months (sometimes mid-year for fuel shocks). Storing
-- a single fixed config value silently values every trip at
-- today's rate — wrong for any trip before the latest update.
--
-- Rate lookup for a given trip date returns the rate whose
-- `effective_from` is the latest date <= the trip date. Null
-- fallback: use the single `settings.mileage_rate` value if no
-- rows are set up yet (back-compat for existing deploys).

CREATE TABLE IF NOT EXISTS mileage_rates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    effective_from  DATE NOT NULL,
    rate_per_mile   REAL NOT NULL,
    notes           TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (effective_from)
);

CREATE INDEX IF NOT EXISTS mileage_rates_effective_idx
    ON mileage_rates (effective_from);
