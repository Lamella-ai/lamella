-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Paperless verify-and-writeback (Slice A+B+C).
--
-- Adds `mime_type` so we can tell whether a doc needs vision
-- re-OCR (image/png, application/pdf) or is native-text
-- (text/plain, docx, xlsx) where the stored content is already
-- accurate and verification would be wasted tokens.
--
-- Also adds `writeback_log` to track what we pushed to Paperless
-- for each doc — one row per correction/enrichment, used for
-- dedup (don't enrich twice with the same note body) and for
-- audit (paired with the row in ai_decisions that produced the
-- writeback).

ALTER TABLE paperless_doc_index ADD COLUMN mime_type TEXT;

CREATE TABLE IF NOT EXISTS paperless_writeback_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    paperless_id      INTEGER NOT NULL,
    -- 'verify_correction' when we pushed field corrections after a
    -- vision re-extract; 'enrichment_note' when we pushed a
    -- contextual note/custom-field from classify signals.
    kind              TEXT NOT NULL,
    -- Stable dedup key so a repeated run with the same derived
    -- note doesn't stamp twice. e.g., hash of the note body, or
    -- "field:receipt_date:2026-04-18" for a corrected field.
    dedup_key         TEXT NOT NULL,
    -- Full JSON payload of what we sent (before/after for
    -- corrections, note body / custom_fields for enrichments).
    payload_json      TEXT NOT NULL,
    ai_decision_id    INTEGER,                 -- nullable: manual user triggers don't have one
    applied_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (paperless_id, kind, dedup_key)
);

CREATE INDEX IF NOT EXISTS paperless_writeback_log_doc_idx
    ON paperless_writeback_log (paperless_id);
