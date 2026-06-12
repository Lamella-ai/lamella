-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 035 — per-scope KV store for UI state that shouldn't live in a
-- cookie.
--
-- Single-user app, but the user hits it from phone + desktop + tablet.
-- Cookie-based "last vehicle" on /mileage/quick would ping-pong
-- between devices as each one learns independently. A tiny DB table
-- makes the state device-independent.
--
-- Scope is a free-form string — `('mileage-quick', 'last_vehicle_slug')`
-- for Phase 3; future UI-state keys (last-used template, last-picked
-- category filter, etc.) share the same shape. Cache-only: a DB wipe
-- just means the form defaults to empty on next visit.
--
-- Plan document referenced this as migration 034a, piggy-backed on
-- the Phase 2 series — 034 was already claimed by that phase's
-- health-cache tables, so this lands as 035.

CREATE TABLE IF NOT EXISTS user_ui_state (
    scope       TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       TEXT,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (scope, key)
);
