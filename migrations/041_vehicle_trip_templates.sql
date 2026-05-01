-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 040 — recurring trip templates.
--
-- Users drive the same routes repeatedly (office, rental property,
-- recurring client). Templates capture the constants and spawn a
-- prefilled trip in one tap. is_round_trip=1 doubles the miles on
-- spawn and appends " (round trip)" to the purpose.

CREATE TABLE IF NOT EXISTS vehicle_trip_templates (
    slug              TEXT PRIMARY KEY,
    display_name      TEXT NOT NULL,
    vehicle_slug      TEXT REFERENCES vehicles(slug) ON DELETE SET NULL,
    entity            TEXT,
    default_from      TEXT,
    default_to        TEXT,
    default_purpose   TEXT,
    default_miles     REAL,                  -- one-way distance
    default_category  TEXT,                  -- business | commuting | personal | mixed
    is_round_trip     INTEGER NOT NULL DEFAULT 0,
    is_active         INTEGER NOT NULL DEFAULT 1,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
