-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 040 — generic job runner (progress-tracked background jobs).
--
-- Any HTTP handler that takes > a few seconds to run — AI batch
-- classifications, receipt hunts, SimpleFIN pulls, audit passes,
-- rewrite-with-bean-check, etc. — enqueues a job via JobRunner
-- and returns the job id. The browser subscribes to progress via
-- /jobs/{id}/partial polling or /jobs/{id}/stream (SSE). See
-- beancounter_glue.jobs.runner for the Python-side contract.
--
-- This is a CACHE table per the reconstruct rule: if the DB is
-- wiped, the jobs history is lost and that's fine. No user state
-- lives here — every job's downstream writes (ledger directives,
-- receipt links, Paperless writebacks) are persisted through the
-- normal channels and survive DB rebuild.
--
-- Terminology:
--   status: queued | running | done | cancelled | error | interrupted
--   outcome (per event): success | failure | not_found | error | info
--
-- "interrupted" is set on startup for any job that was still
-- running when the process died — the worker is gone, so we mark
-- the stale rows and the UI can display them as terminated.

CREATE TABLE IF NOT EXISTS jobs (
    id                  TEXT PRIMARY KEY,
    kind                TEXT NOT NULL,
    title               TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'queued',
    total               INTEGER,
    completed           INTEGER NOT NULL DEFAULT 0,
    success_count       INTEGER NOT NULL DEFAULT 0,
    failure_count       INTEGER NOT NULL DEFAULT 0,
    not_found_count     INTEGER NOT NULL DEFAULT 0,
    error_count         INTEGER NOT NULL DEFAULT 0,
    info_count          INTEGER NOT NULL DEFAULT 0,
    cancel_requested    INTEGER NOT NULL DEFAULT 0,
    meta_json           TEXT,
    result_json         TEXT,
    error_message       TEXT,
    return_url          TEXT,
    started_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TIMESTAMP,
    last_progress_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs (status);
CREATE INDEX IF NOT EXISTS jobs_started_at_idx ON jobs (started_at DESC);

CREATE TABLE IF NOT EXISTS job_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    seq         INTEGER NOT NULL,
    ts          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    message     TEXT NOT NULL,
    outcome     TEXT,
    detail_json TEXT,
    UNIQUE (job_id, seq)
);

CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events (job_id, seq);
