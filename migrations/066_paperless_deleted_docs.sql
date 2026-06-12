-- Copyright 2026 Lamella LLC
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 066_paperless_deleted_docs.sql — tombstone for Paperless docs confirmed
-- deleted after the dangling-link gate (3 consecutive 404s + 7-day cooldown).
--
-- Once a row lands here the dangling-link sweeper will NOT re-ingest the
-- document from a future sync (the sync upsert path checks this table).
-- The txn_matcher also excludes tombstoned paperless_ids so they cannot
-- be auto-linked to a transaction.
--
-- A companion "paperless-doc-deleted" custom directive in
-- connector_links.bean is the Beancount-layer record. The directive is the
-- source of truth for reconstruct; this table is the fast-query cache.
--
-- purged_at   — wall-clock UTC when the dangling sweeper deleted the row.
-- first_404_at — the first_404_at from paperless_link_health at purge time,
--               preserved for audit / reporting.

CREATE TABLE IF NOT EXISTS paperless_deleted_docs (
    paperless_id   INTEGER PRIMARY KEY,
    purged_at      TEXT NOT NULL DEFAULT (datetime('now')),
    first_404_at   TEXT
);
