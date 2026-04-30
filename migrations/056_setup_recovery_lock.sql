-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 056 — recovery in-flight lock for /setup/recovery/apply.
--
-- Resolves gap §11.7 of RECOVERY_SYSTEM.md: two browser tabs running
-- /setup/recovery actions concurrently can interleave bean-snapshot
-- envelopes through ``run_bulk_apply``. Each apply pipeline takes its
-- own snapshot of every connector-owned ``.bean`` it intends to
-- write, runs the heal chain, bean-checks, and either commits or
-- restores. Two of those running in parallel race the snapshot/restore
-- ordering: snapshot A → write A → snapshot B (now sees A's writes
-- as the baseline) → write B → restore A would clobber B's commits;
-- the per-action atomicity guarantees don't extend across two
-- workers tagging through the same files.
--
-- The pre-existing JobRunner ``runner.active()`` check inside
-- ``recovery_apply`` catches the cross-tab double-submit when the
-- worker has already been queued, but it doesn't cover:
--
--   * the brief window between request arrival and ``runner.submit``,
--   * a server restart that loses the in-memory job state, or
--   * future entry points (a CLI, a scheduled scan) that bypass the
--     route layer entirely.
--
-- This table is the durable answer: a single-row latch keyed on
-- ``"current"`` (mirrors ``setup_repair_state``'s session-id
-- convention) with a holder/acquired_at pair so the rendered error
-- can tell the user *who* holds it and *when* they acquired it.
--
-- Released on worker exit via try/finally — a crashed worker that
-- never executes the release branch leaves the row behind, which the
-- ``release`` helper handles via a stale-after timeout the holder
-- code can claim back. Single-user single-container deploy: a
-- restart with a stuck row + nothing actually running is the failure
-- mode, and the helper documents the manual ``DELETE FROM
-- setup_recovery_lock`` recovery path.
--
-- ``IF NOT EXISTS`` guards both clauses so a re-run of db.migrate
-- after an upgrade re-applies the file but doesn't blow away an
-- existing in-flight row mid-apply.

CREATE TABLE IF NOT EXISTS setup_recovery_lock (
    session_id    TEXT NOT NULL PRIMARY KEY,
    holder        TEXT NOT NULL,
    acquired_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
