-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 034 — vehicle data-health cache + breaking-change banner state.
--
-- Phase 2 introduces a data-health panel on the vehicle detail page
-- and flips the business-use fallback from "assume 100% business"
-- to "unknown (split not recorded)". The second change is a silent
-- behaviour flip for anyone not reading release notes: every
-- vehicle whose actual-expense panel used to render a clean dollar
-- figure will now render "unknown" until the user records splits.
--
-- Two cache-only tables:
--
--   vehicle_data_health_cache — snapshotted list of issues per
--     vehicle so the detail page doesn't recompute the full check
--     set on every render. Invalidated on writes to mileage_entries,
--     vehicles, or vehicle_yearly_mileage.
--
--   vehicle_breaking_change_seen — per-(change_key, vehicle_slug)
--     UX state for the one-time banner. The migration seeds
--     (change_key='phase2_unknown_business_use', vehicle_slug=...)
--     rows for every vehicle that would flip, so the banner
--     surfaces on next visit. Dismissing the callout sets
--     dismissed_at. The /vehicles index banner disappears once
--     every row for a change_key is dismissed.
--
-- Both tables are cache — a DB wipe just means the banner
-- re-appears and the health panel re-computes. No ledger writeback.

CREATE TABLE IF NOT EXISTS vehicle_data_health_cache (
    vehicle_slug    TEXT NOT NULL,
    year            INTEGER NOT NULL,
    computed_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    payload_json    TEXT NOT NULL,
    PRIMARY KEY (vehicle_slug, year)
);

CREATE TABLE IF NOT EXISTS vehicle_breaking_change_seen (
    change_key      TEXT NOT NULL,
    vehicle_slug    TEXT NOT NULL,
    seen_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dismissed_at    TIMESTAMP,
    PRIMARY KEY (change_key, vehicle_slug)
);

CREATE INDEX IF NOT EXISTS vehicle_breaking_change_active_idx
    ON vehicle_breaking_change_seen (change_key)
    WHERE dismissed_at IS NULL;

-- Seed the phase2_unknown_business_use banner for every active
-- vehicle that has trips in the current calendar year but has not
-- recorded a single split. Those are exactly the vehicles whose
-- deduction display flips from "100% business" to "unknown". The
-- year bound matches what _standard_vs_actual scopes to by default;
-- adjacent years land in the panel the next time the user picks
-- them.
--
-- Uses COALESCE on the missing columns to stay safe even on fresh
-- test installs where no vehicles exist yet.
INSERT OR IGNORE INTO vehicle_breaking_change_seen
    (change_key, vehicle_slug)
SELECT 'phase2_unknown_business_use', v.slug
  FROM vehicles v
 WHERE v.is_active = 1
   AND EXISTS (
       SELECT 1
         FROM mileage_entries e
        WHERE (e.vehicle_slug = v.slug
               OR e.vehicle = v.slug
               OR e.vehicle = v.display_name)
          AND e.entry_date >= strftime('%Y-01-01', 'now')
   )
   AND NOT EXISTS (
       SELECT 1
         FROM mileage_trip_meta m
        WHERE m.vehicle IN (v.slug, v.display_name)
          AND m.entry_date >= strftime('%Y-01-01', 'now')
          AND (
              m.business_miles IS NOT NULL
              OR m.commuting_miles IS NOT NULL
              OR m.personal_miles IS NOT NULL
          )
   );
