-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Per-entity dashboard widget cache.
--
-- The /businesses/{slug} dashboard recomputes revenue, expenses, KPIs, and
-- rolling charts on every page load. Walking the full ledger N times for
-- the index page is expensive once a few entities are in play, so we cache
-- per (entity, widget, period). Invalidation is implicit: every cached
-- payload carries the ledger mtime it was computed against; a mismatch on
-- read forces a recompute.
--
-- payload_json is opaque to SQLite — each compute_* helper picks its own
-- schema (a list, a struct, a chart-data triple) and serializes accordingly.
CREATE TABLE IF NOT EXISTS business_cache (
    entity_slug    TEXT NOT NULL,
    widget_key     TEXT NOT NULL,                 -- 'kpis' | 'pnl_monthly' | 'expense_composition' | ...
    period_key     TEXT NOT NULL,                 -- '30d' | 'mtd' | '1mo' | 'ytd' | '1yr' | 'all' | 'custom:YYYY-MM-DD:YYYY-MM-DD'
    payload_json   TEXT NOT NULL,
    computed_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ledger_mtime   INTEGER NOT NULL,              -- max(mtime) across all loaded ledger files, as integer seconds
    PRIMARY KEY (entity_slug, widget_key, period_key)
);

CREATE INDEX IF NOT EXISTS business_cache_widget_idx ON business_cache(widget_key);
