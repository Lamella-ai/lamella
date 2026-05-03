-- Copyright 2026 Lamella LLC
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 068_tag_workflow_bindings.sql — ADR-0065: user-defined tag→action bindings.
--
-- The ``tag_workflow_bindings`` table is a reconstructible cache of the
-- ``custom "lamella-tag-binding"`` directives in connector_config.bean.
-- It is the DB half of the ADR-0065 binding contract; the ledger
-- half is the source of truth (ADR-0001 / ADR-0015).
--
-- No seed data. Empty table on fresh install — workflows do nothing
-- until the user creates the first binding via the settings UI.
-- This is the opt-in default behavior required by the user (the old
-- hardcoded DEFAULT_RULES auto-fired on every doc, which was wrong).
--
-- Forward-only (ADR-0026). Idempotent via the schema_migrations version
-- gate in core/db.py.

CREATE TABLE IF NOT EXISTS tag_workflow_bindings (
    tag_name      TEXT PRIMARY KEY,
    action_name   TEXT NOT NULL,
    enabled       INTEGER NOT NULL DEFAULT 1,
    config_json   TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_tag_workflow_bindings_enabled
    ON tag_workflow_bindings(enabled);
