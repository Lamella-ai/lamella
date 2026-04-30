-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Cache of accounts most recently returned by the SimpleFIN bridge, so the
-- /simplefin page can render the "unmapped discovered accounts" dropdown
-- without re-hitting the bridge on every page load. Refreshed whenever the
-- user clicks "Discover accounts" or a scheduled fetch completes.
CREATE TABLE IF NOT EXISTS simplefin_discovered_accounts (
    account_id     TEXT PRIMARY KEY,
    name           TEXT,
    org_name       TEXT,
    org_domain     TEXT,
    currency       TEXT,
    balance        TEXT,
    discovered_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
