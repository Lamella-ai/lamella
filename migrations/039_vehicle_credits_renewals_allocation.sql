-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 039 — Phase 6 schema: credits, renewals, per-trip attribution
-- override.
--
-- vehicle_credits: free-form capture of tax credits / incentives the
-- user is tracking per vehicle + tax year. We do NOT maintain a
-- knowledge base of eligibility — amount / status / notes are all
-- user-entered.
--
-- vehicle_renewals: registration / inspection / insurance due dates
-- with optional cadence_months for auto-advance. Surfaced in the
-- detail-page renewal section + data-health when past-due.
--
-- mileage_trip_meta.attributed_entity: a per-trip override of the
-- trip's entity, used when a single vehicle's usage splits across
-- multiple entities. **State, not cache** (per plan §6 decision 4):
-- Phase 7 stamps it as a custom "mileage-attribution" directive so
-- the override survives a DB wipe.

CREATE TABLE IF NOT EXISTS vehicle_credits (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_slug    TEXT NOT NULL REFERENCES vehicles(slug) ON DELETE CASCADE,
    tax_year        INTEGER NOT NULL,
    credit_label    TEXT NOT NULL,   -- "Federal EV § 30D", "Utility rebate", etc.
    amount          TEXT,            -- decimal string, optional
    status          TEXT,             -- 'claimed' | 'pending' | 'ineligible' | free text
    notes           TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS vehicle_credits_vehicle_year_idx
    ON vehicle_credits (vehicle_slug, tax_year);


CREATE TABLE IF NOT EXISTS vehicle_renewals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_slug    TEXT NOT NULL REFERENCES vehicles(slug) ON DELETE CASCADE,
    renewal_kind    TEXT NOT NULL,   -- 'registration' | 'inspection' | 'insurance' | 'other'
    due_date        DATE NOT NULL,
    cadence_months  INTEGER,         -- NULL = one-shot; 12 = annual
    last_completed  DATE,
    notes           TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS vehicle_renewals_due_idx
    ON vehicle_renewals (is_active, due_date);


ALTER TABLE mileage_trip_meta ADD COLUMN attributed_entity TEXT;
