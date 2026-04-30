-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Vector-index run state for progress reporting + prevent double-clicks.
--
-- A single row per rebuild attempt. `state='building'` marks a
-- rebuild in flight; the UI uses this to disable rebuild buttons
-- and show an estimated-time indicator. state flips to 'complete'
-- on success or 'error' on failure. No retention policy here —
-- the history is useful context ("last 3 builds took 18s, 22s,
-- 19s — today's is on pace").

CREATE TABLE IF NOT EXISTS vector_index_runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at   TIMESTAMP,
    state         TEXT NOT NULL DEFAULT 'building',    -- building | complete | error
    total         INTEGER NOT NULL DEFAULT 0,           -- expected row count
    processed     INTEGER NOT NULL DEFAULT 0,           -- rows embedded so far
    trigger       TEXT,                                 -- startup | manual | classify
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS vector_index_runs_state_idx
    ON vector_index_runs (state);
