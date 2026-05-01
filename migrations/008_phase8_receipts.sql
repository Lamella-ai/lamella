-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 8 — txn-first receipt workflow.
--   * paperless_field_map: editable mapping from Paperless custom field ids/names
--     to canonical roles (total, subtotal, tax, vendor, payment_last_four,
--     receipt_date, ignore). Auto-seeded by keyword for new fields; never
--     overwrites a role the user has set.
--   * paperless_doc_index: local cache of Paperless documents. Candidate
--     lookups join on this table so we don't pound the Paperless API for
--     every row in the queue.
--   * paperless_sync_state: singleton row tracking the incremental sync
--     cursor (max modified timestamp seen) and the last full-sync time.
--   * receipt_dismissals: per-transaction "no receipt expected" acks, so
--     cash tips / parking meters / donations don't clog the queue.

CREATE TABLE IF NOT EXISTS paperless_field_map (
    paperless_field_id    INTEGER PRIMARY KEY,
    paperless_field_name  TEXT NOT NULL,
    canonical_role        TEXT NOT NULL DEFAULT 'ignore',
    auto_assigned         INTEGER NOT NULL DEFAULT 1,   -- 1 = seeded by keyword; flipped to 0 when user saves an explicit choice
    updated_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS paperless_field_map_role_idx ON paperless_field_map (canonical_role);


CREATE TABLE IF NOT EXISTS paperless_doc_index (
    paperless_id          INTEGER PRIMARY KEY,
    title                 TEXT,
    correspondent_id      INTEGER,
    correspondent_name    TEXT,
    document_type_id      INTEGER,
    document_type_name    TEXT,
    created_date          DATE,
    modified_at           TIMESTAMP,
    content_excerpt       TEXT,                          -- first ~4 KB of OCR content, used for substring matching
    -- canonical fields extracted via paperless_field_map
    total_amount          TEXT,                          -- decimal-as-text
    subtotal_amount       TEXT,
    tax_amount            TEXT,
    vendor                TEXT,
    payment_last_four     TEXT,
    receipt_date          DATE,
    -- housekeeping
    last_synced_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tags_json             TEXT
);

CREATE INDEX IF NOT EXISTS paperless_doc_index_created_date_idx  ON paperless_doc_index (created_date);
CREATE INDEX IF NOT EXISTS paperless_doc_index_receipt_date_idx  ON paperless_doc_index (receipt_date);
CREATE INDEX IF NOT EXISTS paperless_doc_index_total_idx         ON paperless_doc_index (total_amount);
CREATE INDEX IF NOT EXISTS paperless_doc_index_correspondent_idx ON paperless_doc_index (correspondent_id);
CREATE INDEX IF NOT EXISTS paperless_doc_index_modified_idx      ON paperless_doc_index (modified_at);


CREATE TABLE IF NOT EXISTS paperless_sync_state (
    id                          INTEGER PRIMARY KEY CHECK (id = 1),
    last_full_sync_at           TIMESTAMP,
    last_incremental_sync_at    TIMESTAMP,
    last_modified_cursor        TIMESTAMP,
    doc_count                   INTEGER NOT NULL DEFAULT 0,
    last_error                  TEXT,
    last_status                 TEXT
);

INSERT OR IGNORE INTO paperless_sync_state (id) VALUES (1);


CREATE TABLE IF NOT EXISTS receipt_dismissals (
    txn_hash       TEXT PRIMARY KEY,
    reason         TEXT,
    dismissed_by   TEXT NOT NULL DEFAULT 'user',
    dismissed_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
