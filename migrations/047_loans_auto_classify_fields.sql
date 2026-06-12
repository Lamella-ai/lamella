-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 9g / WP6 — auto-classification of loan payments.
--
-- When a FIXME payment arrives on a configured loan account, the
-- loan module has enough structured information (amortization model
-- + configured escrow/tax/insurance) to split it correctly without
-- asking the user. This migration adds the per-loan master switch,
-- the overflow-destination default, and a cache table for the
-- per-classification audit trail. The ledger-side source of truth
-- is the bcg-loan-autoclass-* meta stamped on each override block;
-- the cache just makes "what did we auto-classify" queryable
-- without walking the full ledger.

-- Per-loan master switch; default on for new loans so users who
-- don't know about auto-classify still benefit. Existing rows
-- inherit the default.
ALTER TABLE loans ADD COLUMN auto_classify_enabled INTEGER NOT NULL DEFAULT 1;

-- Where tier-"over" overflow lands. Values:
--   'bonus_principal' | 'bonus_escrow' | 'ask'
-- 'ask' downgrades the tier to "surface, don't auto-post" regardless
-- of size, so the user is prompted each time.
ALTER TABLE loans ADD COLUMN overflow_default TEXT NOT NULL DEFAULT 'bonus_principal';

-- Cache — every auto-classification decision. Reconstructable from
-- the bcg-loan-autoclass-* meta keys on connector_overrides.bean,
-- so this table can be wiped and rebuilt without data loss. The
-- reconstructor is left as a follow-up (tracked alongside the
-- entity/account description reconstruct in FUTURE.md).
CREATE TABLE IF NOT EXISTS loan_autoclass_log (
    decision_id          TEXT PRIMARY KEY,         -- uuid4; matches bcg-loan-autoclass-decision-id
    loan_slug            TEXT NOT NULL REFERENCES loans(slug) ON DELETE CASCADE,
    txn_hash             TEXT NOT NULL,            -- bean txn_hash of the claimed FIXME
    tier                 TEXT NOT NULL,            -- 'exact' | 'over' | 'under' | 'far'
    expected_total       TEXT NOT NULL,            -- TEXT for Decimal fidelity
    actual_total         TEXT NOT NULL,
    overflow_amount      TEXT,                     -- only set for tier='over'
    overflow_dest        TEXT,                     -- resolved account path
    overflow_dest_source TEXT,                     -- 'default' (loans.overflow_default)
                                                    -- | 'user' (explicit per-payment reassign)
    decided_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS loan_autoclass_log_slug_idx ON loan_autoclass_log(loan_slug);
CREATE INDEX IF NOT EXISTS loan_autoclass_log_txn_idx  ON loan_autoclass_log(txn_hash);
