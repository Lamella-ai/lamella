-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 4: SimpleFIN takeover.
-- simplefin_ingests records what each fetch did. There is NO transactions
-- mirror — the ledger stays authoritative for money. simplefin-id dedup
-- is derived from the ledger itself (see simplefin/dedup.py).

CREATE TABLE IF NOT EXISTS simplefin_ingests (
    id                   INTEGER PRIMARY KEY,
    started_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at          TIMESTAMP,
    trigger              TEXT NOT NULL,
    bridge_response_hash TEXT,
    new_txns             INTEGER NOT NULL DEFAULT 0,
    duplicate_txns       INTEGER NOT NULL DEFAULT 0,
    classified_by_rule   INTEGER NOT NULL DEFAULT 0,
    classified_by_ai     INTEGER NOT NULL DEFAULT 0,
    fixme_txns           INTEGER NOT NULL DEFAULT 0,
    bean_check_ok        BOOLEAN NOT NULL DEFAULT 0,
    error                TEXT,
    result_summary       TEXT
);

CREATE INDEX IF NOT EXISTS simplefin_ingests_recent_idx
    ON simplefin_ingests (started_at DESC);
