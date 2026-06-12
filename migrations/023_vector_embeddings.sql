-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase H — vector index over resolved transactions + user corrections.
--
-- Stores one embedding per (source, identity) pair:
--   * source='ledger' — one row per resolved ledger Transaction,
--     keyed by identity=<txn_hash>. Weight 1.0.
--   * source='correction' — one row per user_corrected=1 entry in
--     ai_decisions, keyed by identity=<decision_id>. Weight > 1.0
--     so the user's correction outranks the original ledger embed
--     when both are present.
--
-- Classification is cache: the whole table can be wiped and
-- rebuilt from the ledger + ai_decisions. Per NEXTGEN.md §2.5
-- no reconstruct pass is registered.
--
-- The ``embedding`` column stores a raw float32 buffer (no
-- JSON), 4 bytes × dim per row. For all-MiniLM-L6-v2 that's
-- 1536 bytes per row. At 10k transactions that's ~15MB — fits
-- in SQLite comfortably.

CREATE TABLE IF NOT EXISTS txn_embeddings (
    id              INTEGER PRIMARY KEY,
    source          TEXT NOT NULL,          -- 'ledger' | 'correction'
    identity        TEXT NOT NULL,          -- txn_hash OR decision_id
    file            TEXT,                   -- for ledger rows
    lineno          INTEGER,                -- for ledger rows
    merchant_text   TEXT NOT NULL,          -- the string that was embedded
    target_account  TEXT,                   -- the classification endpoint
    posting_date    TEXT,                   -- ISO YYYY-MM-DD
    amount          TEXT,                   -- Decimal as string, absolute value
    weight          REAL NOT NULL DEFAULT 1.0,
    embedding       BLOB NOT NULL,          -- packed float32
    dims            INTEGER NOT NULL,       -- dimensionality of the embedding
    model_name      TEXT NOT NULL,          -- exact model used; re-embed when this changes
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source, identity)
);

CREATE INDEX IF NOT EXISTS txn_embeddings_source_idx      ON txn_embeddings(source);
CREATE INDEX IF NOT EXISTS txn_embeddings_date_idx        ON txn_embeddings(posting_date);
CREATE INDEX IF NOT EXISTS txn_embeddings_file_line_idx   ON txn_embeddings(file, lineno);
CREATE INDEX IF NOT EXISTS txn_embeddings_target_idx      ON txn_embeddings(target_account);


-- Build metadata — one row per (source, model_name) tracking when
-- the index was last rebuilt and the ledger signature at that time.
-- A rebuild runs when the current ledger signature differs from the
-- stored one.
CREATE TABLE IF NOT EXISTS txn_embeddings_build (
    id                INTEGER PRIMARY KEY,
    source            TEXT NOT NULL,
    model_name        TEXT NOT NULL,
    ledger_signature  TEXT NOT NULL,
    built_at          TEXT NOT NULL DEFAULT (datetime('now')),
    row_count         INTEGER NOT NULL,
    UNIQUE(source, model_name)
);
