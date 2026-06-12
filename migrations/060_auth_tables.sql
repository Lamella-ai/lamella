-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 060 — auth tables (ADR-0050).
--
-- Lamella shipped without authentication through Stage 1; ADR-0050
-- introduces optional auth with financial-grade defaults. The same
-- migration also lays the SaaS-day shape (accounts table, password
-- reset token slot, MFA secret slot) so the cutover from single-
-- tenant to multi-tenant is incremental wiring rather than a schema
-- rewrite.
--
-- Tables added:
--   accounts                — tenant root; single-row seed (id=1).
--   users                   — credentials + lockout state.
--   auth_sessions           — server-side session store (cookie holds session_id).
--   auth_events             — append-only audit log.
--   password_reset_tokens   — schema slot only; no UI today.
--   mfa_secrets             — schema slot only; no UI today.
--
-- All tables are tenant-scoped through accounts.id. Single-tenant
-- mode populates accounts(id=1) and every user / session / event
-- carries account_id=1. SaaS-day adds rows to accounts and the
-- foreign keys ensure isolation if every read filters on
-- account_id (the cutover is a separate work-stream).
--
-- Per ADR-0001 the ledger is the source of truth and SQLite is a
-- disposable cache. Auth state is correctly cached: blowing away the
-- DB and re-bootstrapping from env vars is a supported path.

CREATE TABLE IF NOT EXISTS accounts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

INSERT OR IGNORE INTO accounts (id, name) VALUES (1, 'local');

CREATE TABLE IF NOT EXISTS users (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id          INTEGER NOT NULL REFERENCES accounts(id),
    username            TEXT NOT NULL,
    password_hash       TEXT NOT NULL,
    role                TEXT NOT NULL DEFAULT 'owner',
    failed_login_count  INTEGER NOT NULL DEFAULT 0,
    last_failed_at      TEXT,
    locked_until        TEXT,
    last_login_at       TEXT,
    password_changed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS users_account_username_idx
    ON users(account_id, username);

CREATE TABLE IF NOT EXISTS auth_sessions (
    session_id   TEXT PRIMARY KEY,
    user_id      INTEGER NOT NULL REFERENCES users(id),
    account_id   INTEGER NOT NULL REFERENCES accounts(id),
    csrf_token   TEXT NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_seen    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    expires_at   TEXT NOT NULL,
    revoked_at   TEXT,
    ua           TEXT,
    ip           TEXT
);

CREATE INDEX IF NOT EXISTS auth_sessions_user_idx
    ON auth_sessions(user_id) WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS auth_sessions_expires_idx
    ON auth_sessions(expires_at) WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS auth_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    user_id      INTEGER REFERENCES users(id),
    account_id   INTEGER REFERENCES accounts(id),
    event_type   TEXT NOT NULL,
    success      INTEGER NOT NULL DEFAULT 1,
    ip           TEXT,
    ua           TEXT,
    detail       TEXT
);

CREATE INDEX IF NOT EXISTS auth_events_ts_idx ON auth_events(ts);
CREATE INDEX IF NOT EXISTS auth_events_user_idx ON auth_events(user_id, ts);

CREATE TABLE IF NOT EXISTS password_reset_tokens (
    token_hash   TEXT PRIMARY KEY,
    user_id      INTEGER NOT NULL REFERENCES users(id),
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    expires_at   TEXT NOT NULL,
    used_at      TEXT
);

CREATE TABLE IF NOT EXISTS mfa_secrets (
    user_id        INTEGER PRIMARY KEY REFERENCES users(id),
    secret_encrypted TEXT NOT NULL,
    enabled        INTEGER NOT NULL DEFAULT 0,
    last_used_at   TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
