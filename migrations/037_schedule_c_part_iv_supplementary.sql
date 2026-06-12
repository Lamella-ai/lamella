-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai

-- 037 — Schedule C Part IV supplementary fields on the yearly row.
--
-- Lines 44b / 45 / 46 / 47a / 47b on Schedule C Part IV ask for
-- information the trip rollup alone can't provide:
--
--   44b — average daily commute distance (needs a commute-days
--         divisor that isn't derivable from trip miles)
--   45  — "Do you (or your spouse) have another vehicle available
--         for personal use?" (yes / no)
--   46  — "Was your vehicle available for personal use during
--         off-duty hours?" (yes / no)
--   47a — "Do you have evidence to support your deduction?"
--         (yes / no)
--   47b — "If 'Yes', is the evidence written?" (yes / no)
--
-- Capture only — Phase 5D worksheet pre-fills from these columns.
-- Unanswered (NULL) renders as an empty checkbox on the worksheet so
-- the user or their CPA can mark it by hand.

ALTER TABLE vehicle_yearly_mileage ADD COLUMN commute_days                     INTEGER;
ALTER TABLE vehicle_yearly_mileage ADD COLUMN other_vehicle_available_personal INTEGER;  -- 0/1/NULL — line 45
ALTER TABLE vehicle_yearly_mileage ADD COLUMN vehicle_available_off_duty       INTEGER;  -- 0/1/NULL — line 46
ALTER TABLE vehicle_yearly_mileage ADD COLUMN has_evidence                     INTEGER;  -- 0/1/NULL — line 47a
ALTER TABLE vehicle_yearly_mileage ADD COLUMN evidence_is_written              INTEGER;  -- 0/1/NULL — line 47b
