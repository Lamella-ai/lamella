-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 038 — fuel log per vehicle.
--
-- Records fuel / charging events separately from the ledger
-- postings. The ledger already captures $ under
-- Expenses:...:Vehicles:{slug}:Fuel; this sidecar captures the
-- physical quantities (gallons, kWh, odometer) that derive MPG
-- and cost-per-mile.
--
-- Cache with best-effort reconstruct (see FEATURE_VEHICLES_PLAN §5):
-- Phase 7's reconstruct pass will rebuild rows from receipt-linked
-- Fuel postings + Paperless enrichments. Events that lack odometer
-- or gallons in the receipt land as warnings rather than corruption.
--
-- Unit enum: 'gallon' (gas/diesel) or 'kwh' (EV charging). Other
-- exotic fuels record under 'other' with free-form notes.

CREATE TABLE IF NOT EXISTS vehicle_fuel_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_slug    TEXT NOT NULL REFERENCES vehicles(slug) ON DELETE CASCADE,
    as_of_date      DATE NOT NULL,
    as_of_time      TEXT,                             -- optional HH:MM
    fuel_type       TEXT NOT NULL,                    -- 'gasoline' | 'diesel' | 'ev' | 'phev' | 'hybrid' | 'other'
    quantity        REAL NOT NULL,                    -- gallons OR kWh
    unit            TEXT NOT NULL,                    -- 'gallon' | 'kwh'
    cost_cents      INTEGER,                          -- optional; EV home-charging often NULL
    odometer        INTEGER,                          -- optional; used for MPG derivation
    location        TEXT,
    paperless_id    INTEGER,                          -- optional link to receipt
    notes           TEXT,
    source          TEXT NOT NULL DEFAULT 'manual',   -- 'manual' | 'receipt-auto' | 'import'
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS vehicle_fuel_log_vehicle_date_idx
    ON vehicle_fuel_log (vehicle_slug, as_of_date);
