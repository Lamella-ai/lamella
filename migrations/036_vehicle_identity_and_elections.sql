-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 036 — vehicle identity fields + Section 179 / bonus elections capture
-- + disposal records.
--
-- Phase 4 of the vehicles expansion. Purely capture — no computation.
-- The user (or their CPA) decides the values; we store what was
-- entered so the Phase 5 Form 4562 / Schedule C Part IV worksheets
-- can pre-fill from existing rows, and so a future disposal
-- transaction can reference the recorded basis + accumulated
-- depreciation without a manual reconstruction.
--
-- Three pieces:
--
--   1. Identity columns on `vehicles` that Form 4562 asks about:
--      GVWR, placed-in-service date (distinct from purchase date —
--      "a truck bought in December and first used in January was
--      placed in service in January"), and fuel type. Plus a
--      disposal_txn_hash pointer so the card can badge
--      "Sold (ledger written)" vs "Sold (pending write)".
--
--   2. vehicle_elections — one row per (vehicle_slug, tax_year)
--      holding §179 amount, bonus depreciation amount, method,
--      basis at placed-in-service, business-use-pct override,
--      listed-property flag, and notes. The plan references IRS
--      Pub 463 / Form 4562 from the UI; we do NOT compute §179
--      eligibility or caps.
--
--   3. vehicle_disposals — disposal_id is the PK and matches the
--      `bcg-disposal-id` metadata on the ledger transaction written
--      to connector_overrides.bean. Partial unique index lets a
--      revoke + replacement share the same (vehicle, date) without
--      blocking the pair.
--
-- Plan document referenced this as migration 035. Phase 3 claimed
-- 035 for user_ui_state; this lands as 036.

ALTER TABLE vehicles ADD COLUMN gvwr_lbs               INTEGER;
ALTER TABLE vehicles ADD COLUMN placed_in_service_date DATE;
ALTER TABLE vehicles ADD COLUMN fuel_type              TEXT;
-- fuel_type enum: 'gasoline' | 'diesel' | 'ev' | 'phev' | 'hybrid' | 'other' | NULL
ALTER TABLE vehicles ADD COLUMN disposal_txn_hash      TEXT;

CREATE TABLE IF NOT EXISTS vehicle_elections (
    vehicle_slug                TEXT NOT NULL REFERENCES vehicles(slug) ON DELETE CASCADE,
    tax_year                    INTEGER NOT NULL,
    depreciation_method         TEXT,      -- 'MACRS-5YR' | 'MACRS-SL' | 'bonus' | 'section-179' | NULL
    section_179_amount          TEXT,      -- decimal string, informational only
    bonus_depreciation_amount   TEXT,      -- decimal string
    basis_at_placed_in_service  TEXT,      -- decimal string
    business_use_pct_override   REAL,      -- user-asserted, if differs from trip rollup
    listed_property_qualified   INTEGER,   -- 0/1, heavy-vehicle §280F exception etc.
    notes                       TEXT,
    created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (vehicle_slug, tax_year)
);

CREATE TABLE IF NOT EXISTS vehicle_disposals (
    disposal_id              TEXT PRIMARY KEY,      -- matches bcg-disposal-id
    vehicle_slug             TEXT NOT NULL REFERENCES vehicles(slug) ON DELETE CASCADE,
    disposal_date            DATE NOT NULL,
    disposal_type            TEXT NOT NULL,         -- 'sale' | 'trade-in' | 'total-loss' | 'gift' | 'scrap' | 'other'
    proceeds_amount          TEXT,                  -- decimal string
    buyer_or_party           TEXT,
    proceeds_account         TEXT,                  -- e.g. Assets:Personal:Checking
    gain_loss_account        TEXT,                  -- e.g. Income:CapitalGains:VehicleSale
    adjusted_basis           TEXT,                  -- decimal string, user best-guess
    accumulated_depreciation TEXT,                  -- decimal string, informational
    revokes_disposal_id      TEXT,                  -- NULL for originals; set on revoke rows
    revoked_by_disposal_id   TEXT,                  -- NULL when live; set once revoke lands
    notes                    TEXT,
    created_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- At most one live disposal per (vehicle, date). A revoke + replacement
-- pair can share the date because the revoke row has
-- revokes_disposal_id set, taking it out of this index.
CREATE UNIQUE INDEX IF NOT EXISTS vehicle_disposals_live_idx
    ON vehicle_disposals (vehicle_slug, disposal_date)
    WHERE revokes_disposal_id IS NULL
      AND revoked_by_disposal_id IS NULL;
