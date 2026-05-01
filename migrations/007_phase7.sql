-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 7: spreadsheet import with AI column mapping.
-- Adapts importer_bundle/importers/schema.sql into the Connector's DB:
--   - renames `notes` -> `import_notes` (Phase 1 already has user notes).
--   - renames sources.imported_at -> sources.discovered_at.
--   - adds sources.upload_id, ties every source to a single /import upload.
--   - adds the `imports` table: state machine per upload.
-- Everything else (raw_rows, classifications, categorizations, payee_rules,
-- txn_postings, bean_output, row_pairs, views) is copied from the bundle.

-- -----------------------------------------------------------------------------
-- Per-upload state machine
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS imports (
    id              INTEGER PRIMARY KEY,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    filename        TEXT NOT NULL,
    content_sha256  TEXT NOT NULL,
    stored_path     TEXT NOT NULL,
    status          TEXT NOT NULL,
    source_class    TEXT,
    entity          TEXT,
    rows_imported   INTEGER NOT NULL DEFAULT 0,
    rows_committed  INTEGER NOT NULL DEFAULT 0,
    error           TEXT,
    committed_at    TIMESTAMP,
    UNIQUE(content_sha256)
);

CREATE INDEX IF NOT EXISTS imports_status_idx ON imports (status);

-- -----------------------------------------------------------------------------
-- Per-sheet registry (adapted from bundle). upload_id ties a sheet to an
-- /import upload; discovered_at replaces the bundle's imported_at.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sources (
    id              INTEGER PRIMARY KEY,
    upload_id       INTEGER NOT NULL REFERENCES imports(id) ON DELETE CASCADE,
    year            INTEGER,
    path            TEXT NOT NULL,
    sheet_name      TEXT NOT NULL,
    sheet_type      TEXT NOT NULL,
    source_class    TEXT NOT NULL,
    entity          TEXT,
    notes           TEXT,
    rows_read       INTEGER NOT NULL DEFAULT 0,
    discovered_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(upload_id, sheet_name)
);

CREATE INDEX IF NOT EXISTS sources_upload_idx ON sources(upload_id);
CREATE INDEX IF NOT EXISTS sources_year_idx   ON sources(year);
CREATE INDEX IF NOT EXISTS sources_entity_idx ON sources(entity);
CREATE INDEX IF NOT EXISTS sources_class_idx  ON sources(source_class);

-- -----------------------------------------------------------------------------
-- Reusable categorization rules (row-level LIKE patterns). Must exist before
-- categorizations because categorizations.rule_id references it.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payee_rules (
    id                      INTEGER PRIMARY KEY,
    priority                INTEGER NOT NULL DEFAULT 100,
    pattern                 TEXT NOT NULL,
    description_pattern     TEXT,
    payment_method_pattern  TEXT,
    source_class_filter     TEXT,
    entity_filter           TEXT,
    year_start              INTEGER,
    year_end                INTEGER,
    account                 TEXT NOT NULL,
    entity                  TEXT,
    schedule_c_category     TEXT,
    needs_review            INTEGER NOT NULL DEFAULT 0,
    reason                  TEXT,
    enabled                 INTEGER NOT NULL DEFAULT 1,
    created_at              TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at              TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS payee_rules_pattern_idx  ON payee_rules(pattern);
CREATE INDEX IF NOT EXISTS payee_rules_priority_idx ON payee_rules(priority);

-- -----------------------------------------------------------------------------
-- Every row we ingest from every sheet.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_rows (
    id                      INTEGER PRIMARY KEY,
    source_id               INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    row_num                 INTEGER NOT NULL,
    date                    TEXT,
    amount                  REAL,
    currency                TEXT DEFAULT 'USD',
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
    ann_amount2             REAL,
    is_deducted_elsewhere   INTEGER NOT NULL DEFAULT 0,
    raw_json                TEXT NOT NULL,
    hash_key                TEXT NOT NULL,
    UNIQUE(source_id, row_num)
);

CREATE INDEX IF NOT EXISTS raw_rows_date_idx      ON raw_rows(date);
CREATE INDEX IF NOT EXISTS raw_rows_amount_idx    ON raw_rows(amount);
CREATE INDEX IF NOT EXISTS raw_rows_payee_idx     ON raw_rows(payee);
CREATE INDEX IF NOT EXISTS raw_rows_txnid_idx     ON raw_rows(transaction_id);
CREATE INDEX IF NOT EXISTS raw_rows_hash_idx      ON raw_rows(hash_key);
CREATE INDEX IF NOT EXISTS raw_rows_ann_biz_idx   ON raw_rows(ann_business);
CREATE INDEX IF NOT EXISTS raw_rows_ann_exp_idx   ON raw_rows(ann_expense_category);
CREATE INDEX IF NOT EXISTS raw_rows_dedelse_idx   ON raw_rows(is_deducted_elsewhere);

-- -----------------------------------------------------------------------------
-- What we decided to do with each raw row.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS classifications (
    raw_row_id           INTEGER PRIMARY KEY REFERENCES raw_rows(id) ON DELETE CASCADE,
    status               TEXT NOT NULL,
    tx_type              TEXT,
    source_account       TEXT,
    counterparty_account TEXT,
    clearing_account     TEXT,
    pair_key             TEXT,
    pair_id              INTEGER,
    dedup_canonical_id   INTEGER,
    dedup_reason         TEXT,
    skip_reason          TEXT,
    notes                TEXT,
    decided_at           TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS classifications_status_idx ON classifications(status);
CREATE INDEX IF NOT EXISTS classifications_type_idx   ON classifications(tx_type);
CREATE INDEX IF NOT EXISTS classifications_pair_idx   ON classifications(pair_key);

-- -----------------------------------------------------------------------------
-- Final account assignment per row.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS categorizations (
    raw_row_id          INTEGER PRIMARY KEY REFERENCES raw_rows(id) ON DELETE CASCADE,
    entity              TEXT,
    schedule_c_category TEXT,
    account             TEXT NOT NULL,
    confidence          TEXT NOT NULL,
    needs_review        INTEGER NOT NULL DEFAULT 0,
    rule_id             INTEGER REFERENCES payee_rules(id) ON DELETE SET NULL,
    reason              TEXT,
    decided_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS categorizations_account_idx ON categorizations(account);
CREATE INDEX IF NOT EXISTS categorizations_review_idx  ON categorizations(needs_review);
CREATE INDEX IF NOT EXISTS categorizations_confid_idx  ON categorizations(confidence);
CREATE INDEX IF NOT EXISTS categorizations_entity_idx  ON categorizations(entity);

-- -----------------------------------------------------------------------------
-- The actual beancount postings we'll emit.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS txn_postings (
    id          INTEGER PRIMARY KEY,
    raw_row_id  INTEGER NOT NULL REFERENCES raw_rows(id) ON DELETE CASCADE,
    leg_idx     INTEGER NOT NULL,
    account     TEXT NOT NULL,
    amount      REAL NOT NULL,
    currency    TEXT NOT NULL DEFAULT 'USD',
    meta_json   TEXT,
    UNIQUE(raw_row_id, leg_idx)
);

CREATE INDEX IF NOT EXISTS txn_postings_account_idx ON txn_postings(account);
CREATE INDEX IF NOT EXISTS txn_postings_rawrow_idx  ON txn_postings(raw_row_id);

-- -----------------------------------------------------------------------------
-- Output tracking: which raw rows got written to which .bean file.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bean_output (
    id           INTEGER PRIMARY KEY,
    raw_row_id   INTEGER NOT NULL REFERENCES raw_rows(id) ON DELETE CASCADE,
    year         INTEGER NOT NULL,
    written_to   TEXT NOT NULL,
    written_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS bean_output_year_idx   ON bean_output(year);
CREATE INDEX IF NOT EXISTS bean_output_rawrow_idx ON bean_output(raw_row_id);

-- -----------------------------------------------------------------------------
-- Transfer / duplicate row-pair lookups (populated by importer.transfers).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS row_pairs (
    id          INTEGER PRIMARY KEY,
    row_a_id    INTEGER NOT NULL REFERENCES raw_rows(id) ON DELETE CASCADE,
    row_b_id    INTEGER NOT NULL REFERENCES raw_rows(id) ON DELETE CASCADE,
    kind        TEXT NOT NULL,
    confidence  TEXT NOT NULL,
    reason      TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS row_pairs_a_idx    ON row_pairs(row_a_id);
CREATE INDEX IF NOT EXISTS row_pairs_b_idx    ON row_pairs(row_b_id);
CREATE INDEX IF NOT EXISTS row_pairs_kind_idx ON row_pairs(kind);

-- -----------------------------------------------------------------------------
-- Import pipeline's own notes log (renamed from bundle `notes` to avoid
-- collision with the user-facing `notes` table from Phase 1).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS import_notes (
    id           INTEGER PRIMARY KEY,
    import_id    INTEGER REFERENCES imports(id) ON DELETE CASCADE,
    raw_row_id   INTEGER REFERENCES raw_rows(id) ON DELETE CASCADE,
    source_id    INTEGER REFERENCES sources(id) ON DELETE CASCADE,
    topic        TEXT NOT NULL,
    body         TEXT NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS import_notes_topic_idx  ON import_notes(topic);
CREATE INDEX IF NOT EXISTS import_notes_import_idx ON import_notes(import_id);

-- -----------------------------------------------------------------------------
-- Views (same as bundle but quoted against this schema).
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS v_import_transfer_candidates;
CREATE VIEW v_import_transfer_candidates AS
SELECT
    c.raw_row_id,
    c.pair_key,
    c.pair_id,
    c.source_account,
    c.counterparty_account,
    r.date,
    r.amount,
    r.payee,
    r.description,
    s.path AS source_path,
    s.sheet_name,
    s.upload_id
FROM classifications c
JOIN raw_rows r ON r.id = c.raw_row_id
JOIN sources  s ON s.id = r.source_id
WHERE c.tx_type = 'transfer';

DROP VIEW IF EXISTS v_import_review_queue;
CREATE VIEW v_import_review_queue AS
SELECT
    r.id AS raw_row_id,
    r.date,
    r.amount,
    r.payee,
    r.description,
    r.payment_method,
    cat.account,
    cat.entity,
    cat.confidence,
    cat.reason,
    s.year,
    s.path AS source_path,
    s.sheet_name,
    s.upload_id
FROM categorizations cat
JOIN raw_rows r ON r.id = cat.raw_row_id
JOIN sources  s ON s.id = r.source_id
WHERE cat.needs_review = 1
ORDER BY r.date;

DROP VIEW IF EXISTS v_import_clearing_balance;
CREATE VIEW v_import_clearing_balance AS
SELECT
    p.account,
    ROUND(SUM(p.amount), 2) AS balance,
    COUNT(*) AS posting_count,
    MIN(r.date) AS first_date,
    MAX(r.date) AS last_date
FROM txn_postings p
JOIN raw_rows r ON r.id = p.raw_row_id
WHERE p.account LIKE 'Assets:Clearing:%'
   OR p.account LIKE 'Liabilities:Clearing:%'
GROUP BY p.account;

DROP VIEW IF EXISTS v_import_source_summary;
CREATE VIEW v_import_source_summary AS
SELECT
    s.id,
    s.upload_id,
    s.year,
    s.entity,
    s.source_class,
    s.sheet_type,
    s.path,
    s.sheet_name,
    s.rows_read,
    (SELECT COUNT(*) FROM raw_rows r  WHERE r.source_id = s.id) AS rows_in_db,
    (SELECT COUNT(*) FROM raw_rows r
       JOIN classifications c ON c.raw_row_id = r.id
       WHERE r.source_id = s.id AND c.status = 'imported') AS imported,
    (SELECT COUNT(*) FROM raw_rows r
       JOIN classifications c ON c.raw_row_id = r.id
       WHERE r.source_id = s.id AND c.status = 'skipped')  AS skipped,
    (SELECT COUNT(*) FROM raw_rows r
       JOIN classifications c ON c.raw_row_id = r.id
       WHERE r.source_id = s.id AND c.status = 'deduped')  AS deduped,
    (SELECT COUNT(*) FROM raw_rows r
       JOIN categorizations c2 ON c2.raw_row_id = r.id
       WHERE r.source_id = s.id AND c2.needs_review = 1)   AS pending_review
FROM sources s;
