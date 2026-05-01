-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 051 — first-run wizard state.
--
-- Stores the user's progress through the first-run onboarding wizard
-- so closing a tab, hitting refresh, or crashing the browser doesn't
-- lose answers. Single row keyed on slug='default' — single-user app,
-- but the column-shape avoids assuming that forever.
--
-- This is NOT a state-of-truth table. The actual entities, accounts,
-- properties, and vehicles created during the wizard live in their
-- canonical tables (entities, accounts_meta, properties, vehicles)
-- and in the ledger directives. This row exists only to (a) remember
-- which step the user is on, (b) remember their first-step answers
-- (name + intent) so step 2 can scaffold from them, and (c) flag the
-- install as setup_complete when they finish.
--
-- payload is a JSON blob with the shape (see beancounter_glue.wizard.state):
--   {
--     "step":   "welcome|entities|bank|accounts|propvehicle|done",
--     "name":   "Jane",
--     "intent": "personal|business|both|household|everything|manual",
--     "businesses_planned":  ["EntityA", "EntityB"],
--     "individuals_planned": ["Jane"],
--     "simplefin_connected": false,
--     "completed_at":        null
--   }
--
-- Cleared by the same migration on any future install reset (the
-- table itself is idempotent — IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS setup_wizard_state (
    slug         TEXT NOT NULL PRIMARY KEY,
    payload_json TEXT NOT NULL DEFAULT '{}',
    started_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);
