-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 044_calendar_day_reviews — calendar feature tables.
--
-- day_reviews: per-day review state surfaced on the calendar grid
-- and day view. Mirrored to the ledger via custom "day-review"
-- directives so a DB wipe can be reconstructed without loss.
--
-- Notable shape decisions:
--   * No `most_recent_activity_at` column — derived per query from
--     the live data sources, because a cached column would drift
--     from reality whenever a new txn / doc / mileage entry lands
--     and the ingest path forgot to bump it. Cheap enough to compute.
--   * AI summary / audit columns are present but unused in the
--     MVP — phase 2 consumers populate them. All nullable so the
--     absence doesn't block reviews.
--   * The free-text day note lives in `notes` (single-day unscoped
--     rows), NOT in this table. Treating day notes as just "a
--     single-day, unscoped note" keeps one storage + one pipeline
--     into classify_txn, and lets users keep multiple notes per day
--     without a special-case editor.
--
-- txn_classification_modified: cache of per-txn override-write
-- timestamps, populated by OverrideWriter.append() and rebuilt by
-- reconstruct step19 from bcg-modified-at metadata on override
-- blocks in connector_overrides.bean. Calendar dirty-check joins
-- on this table in a single indexed query.

CREATE TABLE IF NOT EXISTS day_reviews (
    review_date          DATE PRIMARY KEY,
    last_reviewed_at     TIMESTAMP,
    ai_summary           TEXT,
    ai_summary_at        TIMESTAMP,
    ai_audit_result      TEXT,
    ai_audit_result_at   TIMESTAMP,
    created_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS day_reviews_last_reviewed_idx
    ON day_reviews (last_reviewed_at);

CREATE TABLE IF NOT EXISTS txn_classification_modified (
    txn_hash     TEXT PRIMARY KEY,
    txn_date     DATE NOT NULL,
    modified_at  TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS txn_classification_modified_date_idx
    ON txn_classification_modified (txn_date, modified_at);
