-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 9.d — per-trip mileage splits + AI-parsed trip provenance.
--
-- We store splits in a sidecar table (not mileage_entries directly)
-- because mileage_entries is a cache rebuilt from vehicles.csv on each
-- mtime change — in-place ALTER TABLE columns would be wiped on the
-- next refresh. Keys match mileage_entries.id by content
-- (entry_date + vehicle + miles) so splits survive cache rebuilds.

CREATE TABLE IF NOT EXISTS mileage_trip_meta (
    id              INTEGER PRIMARY KEY,
    entry_date      DATE NOT NULL,
    vehicle         TEXT NOT NULL,
    miles           REAL NOT NULL,          -- total miles (for matching)
    business_miles  REAL,
    personal_miles  REAL,
    purpose_parsed  TEXT,                   -- AI-derived purpose / entity
    entity_parsed   TEXT,
    auto_from_ai    INTEGER NOT NULL DEFAULT 0,
    free_text       TEXT,                   -- original NL input if any
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entry_date, vehicle, miles)
);

CREATE INDEX IF NOT EXISTS mileage_trip_meta_date_idx
    ON mileage_trip_meta (entry_date);
