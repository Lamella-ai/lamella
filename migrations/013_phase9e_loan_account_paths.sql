-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- Phase 9.5e — loans: wire up linked account paths, escrow add-ons,
-- and property linkage so historical (sold-house) mortgages can be
-- closed out and present mortgages can be tied to the asset that
-- secures them. `payoff_date` / `payoff_amount` already exist from
-- migration 010.

ALTER TABLE loans ADD COLUMN liability_account_path TEXT;
ALTER TABLE loans ADD COLUMN interest_account_path  TEXT;
ALTER TABLE loans ADD COLUMN escrow_account_path    TEXT;
ALTER TABLE loans ADD COLUMN property_tax_monthly   TEXT;
ALTER TABLE loans ADD COLUMN insurance_monthly      TEXT;

-- A loan optionally secures against a property we track. A property
-- can have multiple loans (first mortgage + HELOC, or refis that
-- chained), so this is loan.property_slug, not the reverse.
ALTER TABLE loans ADD COLUMN property_slug TEXT REFERENCES properties(slug);

CREATE INDEX IF NOT EXISTS loans_property_idx ON loans(property_slug);
