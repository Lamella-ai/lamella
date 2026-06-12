-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 9 — the humanization pass.
--
-- Registry of entities (businesses / farms / personal), accounts (with
-- human-readable names, kind, last-four, entity link), vehicles (with
-- purchase/sale dates + per-year mileage), and the supporting tables for
-- merchant memory, split postings on FIXME overrides, and undoable review
-- actions.
--
-- Slugs in `entities` and `vehicles` are never hardcoded — they're
-- discovered from the ledger's Open directives on boot and inserted here
-- with placeholder display names. Users fill in metadata through the admin
-- pages (/settings/entities, /settings/vehicles, /settings/accounts).


-- Businesses, farms, personal. One row per "who owns this money."
CREATE TABLE IF NOT EXISTS entities (
    slug            TEXT PRIMARY KEY,
    display_name    TEXT,                       -- NULL = unlabeled, still needs admin input
    entity_type     TEXT,                       -- personal | sole_prop | llc | s_corp | partnership | farm
    tax_schedule    TEXT,                       -- "C" | "F" | NULL
    start_date      DATE,                       -- activity before this is excluded from reports
    ceased_date     DATE,                       -- activity after this is excluded from reports
    is_active       INTEGER NOT NULL DEFAULT 1,
    sort_order      INTEGER NOT NULL DEFAULT 100,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes           TEXT
);


-- One row per account the UI cares to label. Keyed by raw Beancount path.
-- Auto-populated from Open directives on migration apply; edited via the
-- accounts admin later.
CREATE TABLE IF NOT EXISTS accounts_meta (
    account_path           TEXT PRIMARY KEY,
    display_name           TEXT NOT NULL,       -- best-guess on seed, user-editable
    kind                   TEXT,                -- checking | savings | credit_card | line_of_credit
                                                -- | loan | brokerage | cash | asset | virtual | NULL
    institution            TEXT,                -- "Chase", "Bank One"
    last_four              TEXT,
    entity_slug            TEXT REFERENCES entities(slug),
    simplefin_account_id   TEXT,                -- auto-routes future ingests
    icon                   TEXT,
    is_active              INTEGER NOT NULL DEFAULT 1,
    opened_on              DATE,
    closed_on              DATE,
    seeded_from_ledger     INTEGER NOT NULL DEFAULT 0,  -- 1 = auto-seeded, 0 = user-created
    notes                  TEXT,
    updated_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS accounts_meta_entity_idx     ON accounts_meta(entity_slug);
CREATE INDEX IF NOT EXISTS accounts_meta_lastfour_idx   ON accounts_meta(last_four);
CREATE INDEX IF NOT EXISTS accounts_meta_simplefin_idx  ON accounts_meta(simplefin_account_id);
CREATE INDEX IF NOT EXISTS accounts_meta_kind_idx       ON accounts_meta(kind);


-- Vehicles — tracked independently; mileage isn't per-business.
CREATE TABLE IF NOT EXISTS vehicles (
    slug            TEXT PRIMARY KEY,
    display_name    TEXT,                       -- NULL = discovered but unlabeled
    year            INTEGER,
    make            TEXT,
    model           TEXT,
    vin             TEXT,
    license_plate   TEXT,
    purchase_date   DATE,                       -- excludes prior activity
    purchase_price  TEXT,
    sale_date       DATE,                       -- excludes later activity
    sale_price      TEXT,
    current_mileage INTEGER,
    is_active       INTEGER NOT NULL DEFAULT 1,
    notes           TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- Per-year mileage log. Schedule C Part IV needs business/commute/personal
-- split per vehicle.
CREATE TABLE IF NOT EXISTS vehicle_yearly_mileage (
    vehicle_slug    TEXT NOT NULL REFERENCES vehicles(slug) ON DELETE CASCADE,
    year            INTEGER NOT NULL,
    start_mileage   INTEGER,
    end_mileage     INTEGER,
    business_miles  INTEGER,
    commute_miles   INTEGER,
    personal_miles  INTEGER,
    PRIMARY KEY (vehicle_slug, year)
);


-- Per-merchant memory: what accounts the user tends to send each merchant
-- to. Powers the "recent for this merchant" top-of-dropdown on the card.
CREATE TABLE IF NOT EXISTS merchant_memory (
    merchant_key     TEXT NOT NULL,             -- normalized token from narration / payee
    target_account   TEXT NOT NULL,
    entity_slug      TEXT,
    last_used_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    use_count        INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (merchant_key, target_account)
);

CREATE INDEX IF NOT EXISTS merchant_memory_recent_idx
    ON merchant_memory(merchant_key, last_used_at DESC);


-- Optional per-leg split on FIXME overrides. One row per split line;
-- OverrideWriter reads these when emitting postings so the override
-- balances across multiple target accounts. When absent the override is
-- single-line (backward compatible with existing overrides).
CREATE TABLE IF NOT EXISTS fixme_override_splits (
    txn_hash        TEXT NOT NULL,
    leg_idx         INTEGER NOT NULL,
    target_account  TEXT NOT NULL,
    amount          TEXT NOT NULL,              -- decimal-as-text; signed
    note            TEXT,
    PRIMARY KEY (txn_hash, leg_idx)
);


-- Recently-completed review actions. Lets the card UI offer in-place
-- undo for 10 seconds after save and a "recently categorized" drawer.
CREATE TABLE IF NOT EXISTS review_actions (
    id              INTEGER PRIMARY KEY,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    review_item_id  INTEGER NOT NULL,
    txn_hash        TEXT,
    action_type     TEXT NOT NULL,              -- categorize | transfer | skip | dismiss | split_categorize
    payload_json    TEXT NOT NULL,              -- full snapshot for reversal
    undone_at       TIMESTAMP
);

CREATE INDEX IF NOT EXISTS review_actions_created_idx ON review_actions(created_at DESC);
CREATE INDEX IF NOT EXISTS review_actions_item_idx    ON review_actions(review_item_id);


-- Defer counter. Lets the dashboard highlight transactions the user keeps
-- skipping. Uses an ALTER because review_queue already exists from phase 1.
-- Wrapped so the migration is re-runnable: if the column already exists
-- (e.g., on a partial rollout), SQLite returns an error that bubbles up
-- and makes migrate() abort — acceptable since migrations run exactly
-- once per version anyway.
ALTER TABLE review_queue ADD COLUMN deferred_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE review_queue ADD COLUMN deferred_until TIMESTAMP;
