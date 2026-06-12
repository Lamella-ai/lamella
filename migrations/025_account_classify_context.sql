-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- User-written plain-English description for an account, surfaced
-- to the AI classifier as context.
--
-- When you create a narrow new account like
-- Expenses:Acme:OtherExpenses:StampsPostage, the classifier has
-- no history pointing at it and the leaf name alone is a weak
-- signal. Add a description here ("Stamps.com SaaS charges for
-- shipping labels") and the classify prompt surfaces it alongside
-- the whitelist entry — giving the AI immediate context it can
-- use from the very first txn.
--
-- Keyed by account_path so it works for Expenses, Income, Assets,
-- Liabilities uniformly. Reconstructable: a `custom
-- "account-description"` directive in connector_config.bean
-- survives a SQLite wipe.

CREATE TABLE IF NOT EXISTS account_classify_context (
    account_path    TEXT PRIMARY KEY,
    description     TEXT NOT NULL,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
