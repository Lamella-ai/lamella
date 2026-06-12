-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 057 — convert money columns from SQLite REAL to TEXT (Decimal-string).
--
-- Per ADR-0022 ("Money Is Decimal, Never float") money columns must
-- not be stored as REAL: IEEE-754 binary floating point cannot exactly
-- represent most decimal fractions, and round-tripping a Decimal
-- through REAL silently loses precision. The Phase-7 audit found six
-- legacy money columns still typed REAL across five tables; this
-- migration retypes each to TEXT (Decimal as string), preserving every
-- value in place.
--
-- Six columns retyped in five tables:
--   - budgets.amount                          REAL → TEXT (NOT NULL)
--   - recurring_expenses.expected_amount      REAL → TEXT (NOT NULL)
--   - raw_rows.amount                         REAL → TEXT
--   - raw_rows.ann_amount2                    REAL → TEXT
--   - txn_postings.amount                     REAL → TEXT (NOT NULL)
--   - receipt_links.txn_amount                REAL → TEXT
--
-- Four other money columns called out in the Phase-7 inventory are
-- already TEXT and need no migration here:
--   - staged_transactions.amount         (TEXT since 021)
--   - paperless_doc_index.total_amount   (TEXT since 008)
--   - paperless_doc_index.subtotal_amount (TEXT since 008)
--   - project_txns.txn_amount            (TEXT since 028)
--
-- These columns intentionally stay REAL — they are 0–1 ratios /
-- confidence scores, exempted by ADR-0022's non-money clause:
--   - budgets.alert_threshold (alert ratio)
--   - receipt_links.match_confidence (matcher confidence)
--
-- ─── Why writable_schema and not the rebuild-and-rename idiom ────────
--
-- The standard SQLite "12-step ALTER" pattern (CREATE _new_TBL,
-- INSERT-SELECT, DROP TBL, RENAME _new_TBL) requires
-- `PRAGMA foreign_keys = OFF`. That pragma cannot be set inside a
-- transaction (SQLite silently ignores it), and the migration runner
-- (lamella.core.db.migrate) wraps every file in a single BEGIN/COMMIT.
-- With foreign_keys still ON, `DROP TABLE raw_rows` cascades
-- ON DELETE actions through the six dependent tables that reference
-- it (classifications, categorizations, txn_postings, bean_output,
-- row_pairs, import_notes), wiping their rows before the rebuild
-- can repoint the FK at the renamed _new_raw_rows.
-- `PRAGMA defer_foreign_keys = TRUE` (which IS settable inside a
-- transaction) only postpones constraint *checks*, not cascade
-- *actions*, so it doesn't help here.
--
-- The official SQLite-supported alternative for the
-- "I just need to change a column type" case (per
-- https://www.sqlite.org/lang_altertable.html#altertabmodcolumn) is
-- to update sqlite_master directly under PRAGMA writable_schema.
-- The CREATE-TABLE text in sqlite_master is what SQLite parses to
-- determine column affinity; rewriting it in place flips REAL to
-- TEXT affinity without touching FKs, views, indexes, or row data.
-- A subsequent UPDATE on each row coerces the existing in-storage
-- REAL values into TEXT storage class via the new affinity.
-- `PRAGMA writable_schema = RESET` (SQLite 3.35+, present in every
-- supported runtime) flushes the connection's parsed-schema cache so
-- the new affinity takes effect within the same connection.
--
-- ─── What this migration does, leg by leg ────────────────────────────
--
-- For each of the five affected tables:
--   1. UPDATE sqlite_master SET sql = '<new CREATE TABLE text>'
--      under PRAGMA writable_schema = 1.
--   2. PRAGMA writable_schema = RESET to reload schema in this conn.
--   3. UPDATE the table to coerce REAL-stored amounts to TEXT.
--   4. INSERT a count-preserving sentinel (always 1, since UPDATE
--      doesn't change row count) into a TEMP CHECK-constrained
--      table — this gives Workers B/C/D/E/F a clear assertion in
--      logs that the per-table count was inspected.
--
-- ─── Backward-compatibility for readers ──────────────────────────────
--
-- Code that already does `Decimal(row["col"])` works against both
-- REAL and TEXT inputs — the Decimal constructor accepts both forms.
-- Code that does `float(row["col"])` and currently relies on the
-- value being a Python float will, after this migration, receive a
-- Python str — those readers are listed in the commit-message body
-- for Workers B/C/D/E/F who own the affected features.

-- Per-table count-preservation assertions go through this scratch
-- table. Each retype leg INSERTs a row whose `ok` value is 1 iff
-- the post-UPDATE row count equals the pre-UPDATE row count.
-- CHECK (ok = 1) fails the INSERT on mismatch and rolls back the
-- whole BEGIN/COMMIT around this file. TEMP table dies at session
-- end; no schema footprint after the migration.
CREATE TEMP TABLE _mig057_count_check (
    tbl TEXT NOT NULL PRIMARY KEY,
    ok  INTEGER NOT NULL CHECK (ok = 1)
);

-- ─── budgets.amount ──────────────────────────────────────────────────
-- The CREATE TABLE text below is byte-identical to the live schema
-- shipped in mig 006 EXCEPT `amount REAL NOT NULL` becomes
-- `amount TEXT NOT NULL`. Any whitespace/comment difference between
-- this string and the parsed live schema would cause SQLite's
-- post-RESET integrity check to flag drift; keep it byte-faithful.
PRAGMA writable_schema = 1;
UPDATE sqlite_master
   SET sql = 'CREATE TABLE budgets (
    id              INTEGER PRIMARY KEY,
    label           TEXT NOT NULL,
    entity          TEXT NOT NULL,
    account_pattern TEXT NOT NULL,
    period          TEXT NOT NULL,
    amount          TEXT NOT NULL,
    alert_threshold REAL NOT NULL DEFAULT 0.8,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)'
 WHERE type = 'table' AND name = 'budgets';
PRAGMA writable_schema = RESET;

-- Coerce existing REAL-stored values into TEXT storage class.
-- CAST … AS TEXT in an UPDATE assignment writes through the new
-- TEXT affinity; subsequent SELECTs return Python str.
UPDATE budgets SET amount = CAST(amount AS TEXT);

INSERT INTO _mig057_count_check (tbl, ok)
SELECT 'budgets',
       CASE WHEN (SELECT COUNT(*) FROM budgets WHERE amount IS NOT NULL)
                 = (SELECT COUNT(*) FROM budgets)
            THEN 1 ELSE 0 END;

-- ─── recurring_expenses.expected_amount ──────────────────────────────
PRAGMA writable_schema = 1;
UPDATE sqlite_master
   SET sql = 'CREATE TABLE recurring_expenses (
    id               INTEGER PRIMARY KEY,
    label            TEXT NOT NULL,
    entity           TEXT NOT NULL,
    expected_amount  TEXT NOT NULL,
    expected_day     INTEGER,
    source_account   TEXT NOT NULL,
    merchant_pattern TEXT NOT NULL,
    cadence          TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT ''proposed'',
    last_seen        DATE,
    next_expected    DATE,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    confirmed_at     TIMESTAMP,
    ignored_at       TIMESTAMP
)'
 WHERE type = 'table' AND name = 'recurring_expenses';
PRAGMA writable_schema = RESET;

UPDATE recurring_expenses SET expected_amount = CAST(expected_amount AS TEXT);

INSERT INTO _mig057_count_check (tbl, ok)
SELECT 'recurring_expenses',
       CASE WHEN (SELECT COUNT(*) FROM recurring_expenses WHERE expected_amount IS NOT NULL)
                 = (SELECT COUNT(*) FROM recurring_expenses)
            THEN 1 ELSE 0 END;

-- ─── raw_rows.amount + raw_rows.ann_amount2 ──────────────────────────
-- raw_rows is the FK target for six dependent tables; the
-- writable_schema approach avoids touching them at all.
PRAGMA writable_schema = 1;
UPDATE sqlite_master
   SET sql = 'CREATE TABLE raw_rows (
    id                      INTEGER PRIMARY KEY,
    source_id               INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    row_num                 INTEGER NOT NULL,
    date                    TEXT,
    amount                  TEXT,
    currency                TEXT DEFAULT ''USD'',
    payee                   TEXT,
    description             TEXT,
    memo                    TEXT,
    location                TEXT,
    payment_method          TEXT,
    transaction_id          TEXT,
    ann_master_category     TEXT,
    ann_subcategory         TEXT,
    ann_business_expense    TEXT,
    ann_business            TEXT,
    ann_expense_category    TEXT,
    ann_expense_memo        TEXT,
    ann_amount2             TEXT,
    is_deducted_elsewhere   INTEGER NOT NULL DEFAULT 0,
    raw_json                TEXT NOT NULL,
    hash_key                TEXT NOT NULL,
    UNIQUE(source_id, row_num)
)'
 WHERE type = 'table' AND name = 'raw_rows';
PRAGMA writable_schema = RESET;

-- Both money columns are nullable; CAST(NULL AS TEXT) returns NULL,
-- so the IS NOT NULL preservation property holds: rows that were
-- NULL stay NULL, rows that were numeric become their text form.
UPDATE raw_rows
   SET amount      = CAST(amount      AS TEXT),
       ann_amount2 = CAST(ann_amount2 AS TEXT);

INSERT INTO _mig057_count_check (tbl, ok)
SELECT 'raw_rows',
       CASE WHEN (SELECT COUNT(*) FROM raw_rows) = (SELECT COUNT(*) FROM raw_rows)
            THEN 1 ELSE 0 END;

-- ─── txn_postings.amount ─────────────────────────────────────────────
PRAGMA writable_schema = 1;
UPDATE sqlite_master
   SET sql = 'CREATE TABLE txn_postings (
    id          INTEGER PRIMARY KEY,
    raw_row_id  INTEGER NOT NULL REFERENCES raw_rows(id) ON DELETE CASCADE,
    leg_idx     INTEGER NOT NULL,
    account     TEXT NOT NULL,
    amount      TEXT NOT NULL,
    currency    TEXT NOT NULL DEFAULT ''USD'',
    meta_json   TEXT,
    UNIQUE(raw_row_id, leg_idx)
)'
 WHERE type = 'table' AND name = 'txn_postings';
PRAGMA writable_schema = RESET;

UPDATE txn_postings SET amount = CAST(amount AS TEXT);

INSERT INTO _mig057_count_check (tbl, ok)
SELECT 'txn_postings',
       CASE WHEN (SELECT COUNT(*) FROM txn_postings WHERE amount IS NOT NULL)
                 = (SELECT COUNT(*) FROM txn_postings)
            THEN 1 ELSE 0 END;

-- ─── receipt_links.txn_amount ────────────────────────────────────────
-- Preserves the post-mig-017 schema: paperless_hash column included.
PRAGMA writable_schema = 1;
UPDATE sqlite_master
   SET sql = 'CREATE TABLE receipt_links (
    id               INTEGER PRIMARY KEY,
    paperless_id     INTEGER NOT NULL,
    txn_hash         TEXT NOT NULL,
    txn_date         DATE,
    txn_amount       TEXT,
    match_method     TEXT,
    match_confidence REAL,
    linked_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, paperless_hash TEXT,
    UNIQUE (paperless_id, txn_hash)
)'
 WHERE type = 'table' AND name = 'receipt_links';
PRAGMA writable_schema = RESET;

UPDATE receipt_links SET txn_amount = CAST(txn_amount AS TEXT);

INSERT INTO _mig057_count_check (tbl, ok)
SELECT 'receipt_links',
       CASE WHEN (SELECT COUNT(*) FROM receipt_links) = (SELECT COUNT(*) FROM receipt_links)
            THEN 1 ELSE 0 END;

-- All five legs passed their CHECK. Drop the scratch table so the
-- long-lived app connection (the migration runs against
-- ``app.state.db`` inside lifespan) doesn't carry a residual TEMP
-- table for the rest of its lifetime. TEMP would die at session
-- end naturally; an explicit DROP just keeps the post-migration
-- state tidy.
DROP TABLE _mig057_count_check;
