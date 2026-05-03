-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 10 — property value history.
--
-- A property's cost basis is purchase_price + closing costs + capital
-- improvements. Its market value is something the owner looks up
-- periodically (Zillow, appraisal, 1099 from a sale). Track both so
-- the equity calc has a consistent reference point per date.

ALTER TABLE properties ADD COLUMN closing_costs   TEXT;
ALTER TABLE properties ADD COLUMN asset_account_path TEXT;

CREATE TABLE IF NOT EXISTS property_valuations (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    property_slug  TEXT NOT NULL REFERENCES properties(slug) ON DELETE CASCADE,
    as_of_date     DATE NOT NULL,
    value          TEXT NOT NULL,
    source         TEXT,          -- "zillow" | "appraisal" | "county" | "sale" | "manual"
    notes          TEXT,
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (property_slug, as_of_date)
);

CREATE INDEX IF NOT EXISTS property_valuations_slug_date_idx
    ON property_valuations(property_slug, as_of_date);
