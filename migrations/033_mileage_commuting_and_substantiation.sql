-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 033 — commuting bucket + trip substantiation fields.
--
-- Schedule C Part IV line 44 wants three mileage buckets:
-- business / commuting / other personal. Today we only split
-- business vs personal. Add commuting as a first-class category
-- at the trip-meta (per-trip) level and normalize the yearly-row
-- column name so the stack talks about "commuting" consistently.
--
-- Trip substantiation (purpose, from_loc, to_loc, notes) is
-- ALREADY captured on mileage_entries as of migration 032 — this
-- migration does not re-add those columns. The Phase 1 UI pass
-- exposes them as separate form fields, but no schema change is
-- needed.
--
-- Existing rows are preserved untouched. New columns default to
-- NULL so "commuting miles not recorded" stays distinct from
-- "commuting miles = 0". The business-use fallback fix (Phase 2)
-- is the consumer of NULL-vs-0 semantics.

-- 1. Trip-level commuting + category enum.
ALTER TABLE mileage_trip_meta ADD COLUMN commuting_miles REAL;
ALTER TABLE mileage_trip_meta ADD COLUMN category TEXT;
-- category ∈ {'business','commuting','personal','mixed',NULL}. NULL
-- for rows that predate the radio (user split by entering numbers
-- directly), populated when the simplified radio was used.

-- 2. Denormalized per-trip convenience copy so readers that only
-- touch mileage_entries can see the category without the sidecar
-- join. mileage_trip_meta.category is authoritative on conflict.
ALTER TABLE mileage_entries ADD COLUMN purpose_category TEXT;

-- 3. Normalize the yearly-row column name to match the rest of the
-- stack ("commuting_miles", not "commute_miles"). Preserves data.
-- SQLite 3.25+ supports RENAME COLUMN.
ALTER TABLE vehicle_yearly_mileage RENAME COLUMN commute_miles TO commuting_miles;
