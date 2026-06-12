-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 5: mileage cache + notifications log.
-- vehicles.csv stays the source of truth for mileage rows; the cache table
-- is rebuilt from the file on startup and on mtime change.
-- notifications records every dispatch attempt (delivered, deduped, errored)
-- so the audit trail is honest and dedup decisions are inspectable.

CREATE TABLE IF NOT EXISTS mileage_entries (
    id              INTEGER PRIMARY KEY,
    entry_date      DATE NOT NULL,
    vehicle         TEXT NOT NULL,
    odometer_start  INTEGER,
    odometer_end    INTEGER,
    miles           REAL NOT NULL,
    purpose         TEXT,
    entity          TEXT NOT NULL,
    from_loc        TEXT,
    to_loc          TEXT,
    notes           TEXT,
    csv_row_index   INTEGER NOT NULL,
    csv_mtime       TIMESTAMP NOT NULL,
    UNIQUE(csv_mtime, csv_row_index)
);

CREATE INDEX IF NOT EXISTS mileage_entries_year_entity_idx
    ON mileage_entries (entry_date, entity);

CREATE TABLE IF NOT EXISTS notifications (
    id              INTEGER PRIMARY KEY,
    sent_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    channel         TEXT NOT NULL,
    priority        TEXT NOT NULL,
    dedup_key       TEXT NOT NULL,
    title           TEXT NOT NULL,
    body            TEXT NOT NULL,
    delivered       BOOLEAN NOT NULL DEFAULT 0,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS notifications_dedup_idx
    ON notifications (dedup_key, sent_at DESC);
CREATE INDEX IF NOT EXISTS notifications_recent_idx
    ON notifications (sent_at DESC);
