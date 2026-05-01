-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 10 — vehicle cost basis + value history.
--
-- Mirrors property_valuations. Vehicles lose value over time so the
-- book-vs-market delta is especially informative, and rentals /
-- business vehicles that use actual-cost deduction need a basis that
-- includes title/tag/doc fees beyond the sticker price.

ALTER TABLE vehicles ADD COLUMN purchase_fees       TEXT;
ALTER TABLE vehicles ADD COLUMN asset_account_path  TEXT;

CREATE TABLE IF NOT EXISTS vehicle_valuations (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_slug  TEXT NOT NULL REFERENCES vehicles(slug) ON DELETE CASCADE,
    as_of_date    DATE NOT NULL,
    value         TEXT NOT NULL,
    source        TEXT,            -- "kbb" | "nada" | "edmunds" | "appraisal" | "sale" | "manual"
    notes         TEXT,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (vehicle_slug, as_of_date)
);

CREATE INDEX IF NOT EXISTS vehicle_valuations_slug_date_idx
    ON vehicle_valuations(vehicle_slug, as_of_date);
