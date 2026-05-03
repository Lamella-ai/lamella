-- Copyright 2026 Lamella LLC
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 067_documents_rename.sql — ADR-0061 Phase 2: rename receipt_* tables
-- to document_* and add a document_type discriminator.
--
-- Renames:
--   receipt_links            → document_links
--   receipt_dismissals       → document_dismissals
--   receipt_link_blocks      → document_link_blocks
--   paperless_doc_index.receipt_date → paperless_doc_index.document_date
--
-- Adds:
--   paperless_doc_index.document_type TEXT
--     Canonical values per ADR-0061 §4: receipt | invoice | order |
--     statement | tax | other.
--     Backfilled to 'receipt' for legacy rows; left NULL for new rows
--     until the Paperless sync populates it from doc_type roles.
--
-- Indexes carrying the old table or column names are dropped and
-- recreated under the new names so query planners and DROP-by-name
-- semantics behave identically post-rename.
--
-- Forward-only (ADR-0026). Idempotent via the schema_migrations version
-- gate in core/db.py — this script is only ever run once because
-- migrate() short-circuits when version 67 is already recorded. There
-- is no in-script idempotency guard because ALTER TABLE RENAME on a
-- non-existent table would raise — exactly what we want to surface if
-- someone replays this manually outside the framework.

-- ─── Tables ──────────────────────────────────────────────────────────
ALTER TABLE receipt_links       RENAME TO document_links;
ALTER TABLE receipt_dismissals  RENAME TO document_dismissals;
ALTER TABLE receipt_link_blocks RENAME TO document_link_blocks;

-- ─── Column rename inside paperless_doc_index ────────────────────────
ALTER TABLE paperless_doc_index RENAME COLUMN receipt_date TO document_date;

-- ─── New discriminator column ────────────────────────────────────────
ALTER TABLE paperless_doc_index ADD COLUMN document_type TEXT;

-- Backfill legacy rows to 'receipt' — every row that landed before this
-- migration was treated as a receipt by the matcher, so that's the
-- correct baseline. Future rows get NULL until the sync upserts the
-- canonical role from paperless_doc_type_roles.
UPDATE paperless_doc_index SET document_type = 'receipt' WHERE document_type IS NULL;

-- ─── Indexes — drop old names, recreate under new names ──────────────
-- 001_init.sql:  receipt_links_paperless_idx ON receipt_links (paperless_id)
-- 017_receipt_paperless_hash.sql: receipt_links_hash_idx ON receipt_links (paperless_hash)
-- 065_receipt_link_blocks.sql: idx_receipt_link_blocks_txn ON receipt_link_blocks (txn_hash)
-- 008_phase8_receipts.sql: paperless_doc_index_receipt_date_idx ON paperless_doc_index (receipt_date)
--
-- SQLite preserves the index *definitions* across RENAME TABLE / RENAME
-- COLUMN (the master schema is rewritten in place), but the index
-- *names* still carry the old noun. Drop and recreate so future
-- maintainers grepping for "document_*" find them.

DROP INDEX IF EXISTS receipt_links_paperless_idx;
CREATE INDEX IF NOT EXISTS document_links_paperless_idx
    ON document_links (paperless_id);

DROP INDEX IF EXISTS receipt_links_hash_idx;
CREATE INDEX IF NOT EXISTS document_links_hash_idx
    ON document_links (paperless_hash);

DROP INDEX IF EXISTS idx_receipt_link_blocks_txn;
CREATE INDEX IF NOT EXISTS idx_document_link_blocks_txn
    ON document_link_blocks (txn_hash);

DROP INDEX IF EXISTS paperless_doc_index_receipt_date_idx;
CREATE INDEX IF NOT EXISTS paperless_doc_index_document_date_idx
    ON paperless_doc_index (document_date);
