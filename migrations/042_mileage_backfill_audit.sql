-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 042 — mileage back-fill audit cache.
--
-- Pure cache. Rebuildable in one SQL pass from mileage_entries —
-- a row exists here iff at least one mileage_entries row for that
-- entry_date was inserted >= BACKFILL_THRESHOLD_DAYS after the
-- trip happened. When the user back-fills a years-old trip log,
-- same-day already-classified transactions may have been decided
-- without that context; this table surfaces the candidate dates
-- so an audit page can join to ai_decisions / classifications
-- and propose a re-classify.
--
-- Not state — if this table is dropped, rebuild_mileage_backfill_audit()
-- repopulates it from mileage_entries. Reconstruct-from-ledger is a
-- pure no-op for this table since its contents derive from other
-- cache state (mileage_entries itself is the source).

CREATE TABLE IF NOT EXISTS mileage_backfill_audit (
    entry_date            TEXT NOT NULL PRIMARY KEY,
    backfill_latest_at    TEXT NOT NULL,   -- MAX(created_at) of back-filled rows on this date
    backfill_entry_count  INTEGER NOT NULL,-- # of back-filled rows on this date
    gap_days_max          INTEGER NOT NULL,-- largest (created_at - entry_date) in days
    refreshed_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS mileage_backfill_audit_latest_idx
    ON mileage_backfill_audit(backfill_latest_at DESC);
