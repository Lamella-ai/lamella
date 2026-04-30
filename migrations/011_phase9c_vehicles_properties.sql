-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 9.c — vehicles get an owner entity (personal or business) and
-- first-class real property tracking.

-- Vehicles can be owned by a business (Expenses:{Entity}:Vehicles:{slug})
-- or personal (Expenses:Vehicles:{slug}). The entity_slug column
-- captures which — NULL means personal, not attributed to any business.
ALTER TABLE vehicles ADD COLUMN entity_slug TEXT REFERENCES entities(slug);

-- Real property: houses, land, buildings. Like vehicles, each property
-- auto-creates an asset book-value account and (optionally) expense
-- sub-accounts for insurance, taxes, maintenance, etc.
CREATE TABLE IF NOT EXISTS properties (
    slug            TEXT PRIMARY KEY,
    display_name    TEXT,
    property_type   TEXT NOT NULL,                -- house | land | building | condo | rental | other
    entity_slug     TEXT REFERENCES entities(slug),
    address         TEXT,
    city            TEXT,
    state           TEXT,
    postal_code     TEXT,
    purchase_date   DATE,
    purchase_price  TEXT,
    sale_date       DATE,
    sale_price      TEXT,
    is_primary_residence INTEGER NOT NULL DEFAULT 0,
    is_rental       INTEGER NOT NULL DEFAULT 0,
    is_active       INTEGER NOT NULL DEFAULT 1,
    notes           TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS properties_entity_idx ON properties(entity_slug);
