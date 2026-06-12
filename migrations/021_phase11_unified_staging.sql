-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 11 — Unified staging pipeline (NEXTGEN.md Phase A).
--
-- The universal pending-transaction surface every data source
-- (SimpleFIN, CSV/ODS/XLSX, pasted text, reboot re-ingest) writes
-- to before anything touches Beancount. Purpose:
--
--  * Cross-source visibility — the transfer matcher (Phase C) needs
--    to see a PayPal CSV row and its Bank One SimpleFIN row in
--    one place to pair them.
--  * Source-agnostic classification — rules, AI, and notes work the
--    same way regardless of where a row came from.
--  * Staging before bean-write — rows only land in Beancount once
--    confidence is high enough. Ambiguous rows live here until the
--    human resolves them.
--
-- Reconstruct classification: **cache**, not state. If the DB is
-- wiped mid-pipeline, the ledger still contains everything that
-- was committed; anything in staging gets re-fetched from the
-- original source (SimpleFIN by date range, CSVs by re-upload).
-- No reconstruct pass is registered for these tables.
--
-- Coexistence with existing importer tables (raw_rows, classifications,
-- categorizations, row_pairs from migration 007): the importer keeps
-- writing to its own tables as today. Phase A2 will mirror those
-- writes into staged_transactions so every new source uses the same
-- downstream pipeline. Dropping the importer-specific tables is a
-- later decision, gated on all consumers having migrated to the
-- unified surface.


-- -----------------------------------------------------------------------------
-- Every pending transaction from every source.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staged_transactions (
    id                INTEGER PRIMARY KEY,
    source            TEXT NOT NULL,       -- simplefin | csv | ods | xlsx | paste | reboot
    source_ref        TEXT NOT NULL,       -- JSON: source-specific identifiers (see schema)
    source_ref_hash   TEXT NOT NULL,       -- sha1 of source + normalized source_ref — dedup key
    session_id        TEXT,                -- groups rows from the same ingest run
    posting_date      TEXT NOT NULL,       -- ISO YYYY-MM-DD of the transaction
    amount            TEXT NOT NULL,       -- Decimal as string (preserves precision)
    currency          TEXT NOT NULL DEFAULT 'USD',
    payee             TEXT,                -- best-guess merchant text
    description       TEXT,                -- longer narration if source has it
    memo              TEXT,                -- optional memo line
    raw_json          TEXT NOT NULL,       -- full source record verbatim
    status            TEXT NOT NULL DEFAULT 'new',
                                           -- new | classified | matched | promoted | failed | dismissed
    promoted_to_file  TEXT,                -- path of .bean file on promotion
    promoted_txn_hash TEXT,                -- identifier of the emitted ledger transaction
    promoted_at       TEXT,                -- ISO timestamp
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source, source_ref_hash)
);

CREATE INDEX IF NOT EXISTS staged_tx_source_status_idx ON staged_transactions(source, status);
CREATE INDEX IF NOT EXISTS staged_tx_status_idx        ON staged_transactions(status);
CREATE INDEX IF NOT EXISTS staged_tx_session_idx       ON staged_transactions(session_id);
CREATE INDEX IF NOT EXISTS staged_tx_date_idx          ON staged_transactions(posting_date);
CREATE INDEX IF NOT EXISTS staged_tx_date_amount_idx   ON staged_transactions(posting_date, amount);


-- -----------------------------------------------------------------------------
-- Classification outcome per staged row. Collapses the importer's
-- `classifications` + `categorizations` into a single decision record.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staged_decisions (
    staged_id          INTEGER PRIMARY KEY REFERENCES staged_transactions(id) ON DELETE CASCADE,
    account            TEXT,                -- chosen target account (nullable when unresolved)
    confidence         TEXT NOT NULL,       -- high | medium | low | unresolved
    confidence_score   REAL,                -- 0.0 … 1.0 when available
    decided_by         TEXT NOT NULL,       -- rule | ai | human | card-binding | auto
    rule_id            INTEGER,             -- FK target is soft — rules live in multiple tables
    ai_decision_id     INTEGER,             -- optional ref to ai_decisions.id
    rationale          TEXT,                -- human-readable explanation
    needs_review       INTEGER NOT NULL DEFAULT 0,
    decided_at         TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS staged_dec_account_idx    ON staged_decisions(account);
CREATE INDEX IF NOT EXISTS staged_dec_confidence_idx ON staged_decisions(confidence);
CREATE INDEX IF NOT EXISTS staged_dec_review_idx     ON staged_decisions(needs_review);


-- -----------------------------------------------------------------------------
-- Detected pair between two staged rows (transfer, duplicate). One side
-- may point into the already-committed ledger via ``b_ledger_hash``
-- instead of a second staged row — that's how cross-fetch pairing
-- (today's SimpleFIN row vs. last week's already-committed CSV row)
-- lands here.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staged_pairs (
    id              INTEGER PRIMARY KEY,
    kind            TEXT NOT NULL,       -- transfer | duplicate
    confidence      TEXT NOT NULL,       -- high | medium | low
    a_staged_id     INTEGER NOT NULL REFERENCES staged_transactions(id) ON DELETE CASCADE,
    b_staged_id     INTEGER          REFERENCES staged_transactions(id) ON DELETE CASCADE,
    b_ledger_hash   TEXT,                -- set when the other side is already in the ledger
    reason          TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS staged_pairs_a_idx        ON staged_pairs(a_staged_id);
CREATE INDEX IF NOT EXISTS staged_pairs_b_idx        ON staged_pairs(b_staged_id);
CREATE INDEX IF NOT EXISTS staged_pairs_kind_idx     ON staged_pairs(kind);
CREATE INDEX IF NOT EXISTS staged_pairs_ledger_idx   ON staged_pairs(b_ledger_hash);


-- -----------------------------------------------------------------------------
-- Convenience view: currently-pending work visible to review + UI.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS v_staged_pending;
CREATE VIEW v_staged_pending AS
SELECT
    t.id,
    t.source,
    t.source_ref,
    t.session_id,
    t.posting_date,
    t.amount,
    t.currency,
    t.payee,
    t.description,
    t.status,
    d.account       AS proposed_account,
    d.confidence    AS proposed_confidence,
    d.decided_by    AS proposed_by,
    d.rationale     AS proposed_rationale,
    d.needs_review  AS needs_review,
    t.created_at
FROM staged_transactions t
LEFT JOIN staged_decisions d ON d.staged_id = t.id
WHERE t.status IN ('new', 'classified', 'matched')
ORDER BY t.posting_date DESC, t.id DESC;
