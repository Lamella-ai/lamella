-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- WP13 — revolving / HELOC support.
--
-- A revolving loan (HELOC, line of credit, credit card-style debt) has
-- a different lifecycle than an amortizing loan: balance changes via
-- draws (more debt) and payments (less debt) against a credit limit,
-- not a fixed amortization schedule. The auto-classify path already
-- handles this — `ClaimKind.REVOLVING_SKIP` returns wrote_override=False
-- so AI is preempted and the user hand-categorizes via record-payment.
-- This migration just lets a row carry the bit so the routing actually
-- fires.
--
-- credit_limit is informational today — used in the property-loans
-- aggregation panel to surface available headroom. Variable-rate APR
-- + interest-only schedules + credit-limit history are
-- DEFERRED-WP13-PHASE2 (see comments in amortization.py + writer.py).
--
-- Both columns are reconstructable from `bcg-loan-is-revolving` /
-- `bcg-loan-credit-limit` meta on the loan directive (see
-- read_loans + append_loan).

ALTER TABLE loans ADD COLUMN is_revolving INTEGER NOT NULL DEFAULT 0;
ALTER TABLE loans ADD COLUMN credit_limit TEXT;
