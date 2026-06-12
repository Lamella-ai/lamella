-- Copyright 2026 Lamella LLC
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 064_paperless_link_health.sql — accumulator for "is this Paperless
-- doc still there?" sweeps.
--
-- One row per distinct paperless_id seen in receipt_links. Each
-- sweep updates the counters; only after >=3 consecutive 404
-- responses (and >=7 days since first_404_at) does the link surface
-- as "dangling" in the report.
--
-- Why a counter, not a single-shot 404→delete: a transient Paperless
-- outage / restart / 5xx must NOT cause a mass-unlink of every
-- receipt. The sweep treats only confirmed 404s as evidence; transport
-- errors leave the row unchanged. Multiple consecutive 404s spaced
-- over time = real deletion vs. one-off blip.

CREATE TABLE IF NOT EXISTS paperless_link_health (
    paperless_id     INTEGER PRIMARY KEY,
    last_seen_at     TEXT,    -- ISO-8601 UTC; most recent 200 OK
    last_404_at      TEXT,    -- most recent confirmed 404
    first_404_at     TEXT,    -- first 404 of current consecutive run; NULL after a 200 resets
    last_check_at    TEXT,    -- last sweep that touched this row (200 or 404; not transport errors)
    consecutive_404s INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_paperless_link_health_dangling
    ON paperless_link_health(consecutive_404s, first_404_at)
    WHERE consecutive_404s >= 3;
