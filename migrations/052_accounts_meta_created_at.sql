-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 052 — accounts_meta.created_at: first-seen timestamp.
--
-- Background: when a user imports a skeleton ledger, every Open
-- directive seeds an accounts_meta row. On the *next* boot, any
-- additional Open directives reachable via main.bean's `include`
-- chain (historical files, generated subtrees) get seeded too — so
-- the "needs labeling" list grows after import without an obvious
-- reason. The /setup/recovery editor wants to group these:
--
--   - "from your import" (created during the import event)
--   - "discovered on a later boot"  (seeded by routine boot discovery)
--   - "manually added"              (seeded_from_ledger=0)
--
-- accounts_meta.updated_at already exists (auto-bumped on every
-- save) so it can't double as a creation marker. Add a sibling
-- created_at column with the same DEFAULT CURRENT_TIMESTAMP shape.
-- For pre-existing rows we backfill from updated_at — those rows
-- predate this migration, so their first-seen time is at best the
-- last-edit time, but using updated_at is strictly better than
-- NULL (and better than the migration's own apply time, which
-- would lump every row together as "discovered today").

-- SQLite quirk: ALTER TABLE ADD COLUMN with DEFAULT CURRENT_TIMESTAMP
-- requires the default to be a constant, not a non-deterministic
-- expression. We add the column with no default, backfill existing
-- rows from updated_at, then enforce the default through application
-- code (seed_accounts_meta + any manual inserts). New rows that go
-- through INSERT INTO accounts_meta (...) VALUES (...) without an
-- explicit created_at will fall back to NULL — caller responsibility.
ALTER TABLE accounts_meta ADD COLUMN created_at TIMESTAMP;

UPDATE accounts_meta
   SET created_at = updated_at
 WHERE created_at IS NULL;

CREATE INDEX IF NOT EXISTS accounts_meta_created_at_idx
    ON accounts_meta(created_at);
