-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- WP5 — multi-leg payment groups cache.
--
-- When a loan payment arrives as several transactions (e.g. separate
-- principal / escrow / insurance pulls from the servicer, or the user
-- chunks a payment across two checking draws), the FIXME-candidates
-- panel currently surfaces them as N individual rows and the
-- auto-classifier preempts each one individually. The proposer scans
-- sliding date windows for subsets whose amounts sum to the
-- configured monthly payment (± tolerance) and offers them as a
-- single resolvable group.
--
-- Source of truth is the bcg-loan-group-members meta stamped on each
-- override block in connector_overrides.bean. This cache exists so the
-- coverage engine and the FIXME panel can do a single-table lookup to
-- answer "is this FIXME already part of a confirmed group?" without
-- walking the ledger every render.
--
-- Reconstruct (step22_loan_payment_groups) rebuilds status='confirmed'
-- rows from the ledger; status='proposed' and status='dismissed' are
-- ephemeral UI state that reconstruct does not re-populate. The
-- proposer re-runs on each render, so proposed-state drift fixes
-- itself; dismissed-state (user clicked "not a group") is fine to
-- lose on rebuild — next render will just propose again and the user
-- can re-dismiss.

CREATE TABLE IF NOT EXISTS loan_payment_groups (
    group_id          TEXT PRIMARY KEY,                -- sha256(sorted member hashes)[:16]
    loan_slug         TEXT NOT NULL REFERENCES loans(slug) ON DELETE CASCADE,
    member_hashes     TEXT NOT NULL,                   -- comma-separated, lex-sorted
    aggregate_amount  TEXT NOT NULL,                   -- TEXT for Decimal fidelity
    date_span_start   DATE NOT NULL,
    date_span_end     DATE NOT NULL,
    primary_hash      TEXT,                            -- the member whose block carries the real split
    status            TEXT NOT NULL,                   -- 'proposed' | 'confirmed' | 'dismissed'
    confirmed_at      TIMESTAMP,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS loan_payment_groups_slug_status_idx
    ON loan_payment_groups(loan_slug, status);
