-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 032 — mileage_entries becomes the primary store for trip rows.
--
-- Before 032, vehicles.csv was source of truth and mileage_entries was
-- a cache keyed on (csv_mtime, csv_row_index). That made the CSV the
-- only durable store, the write path fragile (rewrite the whole CSV
-- on every append), and importing a log impossible without mangling
-- the file. After 032:
--
--   * mileage_entries is the primary store (id is its own PK, nothing
--     is keyed to file mtime).
--   * The CSV at mileage_csv_resolved exists as a daily backup only,
--     written from the DB — the DB never reads from it except for a
--     one-shot legacy bootstrap.
--   * A new mileage_imports table tracks each import batch (CSV
--     upload or pasted text) so rows can be grouped / undone.
--   * The legacy csv_row_index / csv_mtime columns are kept NULLABLE
--     only so existing test fixtures that seed mileage_entries
--     directly keep working. New rows leave them NULL.
--
-- Legacy rows (everything in mileage_entries pre-032) are marked
-- source='csv_legacy' so the reconstruct / import flows can tell
-- what came from where.

ALTER TABLE mileage_entries RENAME TO mileage_entries_v1;

CREATE TABLE mileage_entries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_date      DATE NOT NULL,
    entry_time      TEXT,               -- optional HH:MM for intra-day markers
    vehicle         TEXT NOT NULL,      -- display_name or slug (historical)
    vehicle_slug    TEXT,               -- FK-ish to vehicles.slug when known
    odometer_start  INTEGER,
    odometer_end    INTEGER,
    miles           REAL NOT NULL,
    purpose         TEXT,
    entity          TEXT NOT NULL,
    from_loc        TEXT,
    to_loc          TEXT,
    notes           TEXT,
    source          TEXT NOT NULL DEFAULT 'manual',
    import_batch_id INTEGER,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- legacy compat (nullable) — pre-032 rows stamp these; post-032 rows leave NULL
    csv_row_index   INTEGER,
    csv_mtime       TIMESTAMP
);

INSERT INTO mileage_entries
    (entry_date, vehicle, odometer_start, odometer_end, miles,
     purpose, entity, from_loc, to_loc, notes, source,
     csv_row_index, csv_mtime)
SELECT entry_date, vehicle, odometer_start, odometer_end, miles,
       purpose, entity, from_loc, to_loc, notes, 'csv_legacy',
       csv_row_index, csv_mtime
  FROM mileage_entries_v1;

DROP TABLE mileage_entries_v1;

CREATE INDEX mileage_entries_year_entity_idx ON mileage_entries (entry_date, entity);
CREATE INDEX mileage_entries_vehicle_idx ON mileage_entries (vehicle);
CREATE INDEX mileage_entries_vehicle_date_idx ON mileage_entries (vehicle, entry_date);
CREATE INDEX mileage_entries_batch_idx ON mileage_entries (import_batch_id)
    WHERE import_batch_id IS NOT NULL;

CREATE TABLE mileage_imports (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    imported_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    vehicle_slug     TEXT,
    source_filename  TEXT,
    source_format    TEXT NOT NULL DEFAULT 'csv',  -- 'csv' | 'text_anchor' | 'text_range'
    row_count        INTEGER NOT NULL DEFAULT 0,
    skipped_count    INTEGER NOT NULL DEFAULT 0,
    conflict_count   INTEGER NOT NULL DEFAULT 0,
    notes            TEXT
);

CREATE INDEX mileage_imports_recent_idx ON mileage_imports (imported_at DESC);
