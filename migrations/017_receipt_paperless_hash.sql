-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Add content-hash column to receipt_links so Paperless cross-references
-- survive a Paperless reinstall (paperless_id is not stable across those).
-- Values are stored with an algorithm prefix ("md5:<hex>" or "sha256:<hex>")
-- so a future pass can upgrade without breaking lookups.

ALTER TABLE receipt_links ADD COLUMN paperless_hash TEXT;

CREATE INDEX IF NOT EXISTS receipt_links_hash_idx
    ON receipt_links (paperless_hash);

-- Cache the Paperless MD5 in our doc index so link writers don't need
-- an extra HTTP roundtrip just to stamp bcg-paperless-hash. Populated by
-- paperless.sync on every sync pass. NULL until re-sync.
ALTER TABLE paperless_doc_index ADD COLUMN original_checksum TEXT;

CREATE INDEX IF NOT EXISTS paperless_doc_index_checksum_idx
    ON paperless_doc_index (original_checksum);
