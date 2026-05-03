# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the mileage import parser — covers the three input
shapes the user throws at /mileage/import:
 * Format A single-anchor odometer + optional time
 * Format B two-anchor trip lines
 * Canonical CSV (vehicles.csv header)

Conflict and error-path cases are explicit here because the user
wants to see data-entry mistakes in the preview, not silent drops.
"""
from __future__ import annotations

from datetime import date

from lamella.features.mileage.import_parser import (
    detect_csv,
    parse_free_text,
    parse_input,
    parse_csv_text,
    resolve_anchors_to_trips,
)


def test_detect_csv_on_canonical_header():
    text = "date,vehicle,odometer_start,odometer_end,miles\n2026-01-01,SuvA,10,20,10\n"
    assert detect_csv(text) is True


def test_detect_csv_rejects_freetext():
    text = "2026-01-01 08:00 69001 drove to costco\n"
    assert detect_csv(text) is False


def test_parse_csv_text_canonical_row():
    text = (
        "date,vehicle,odometer_start,odometer_end,miles,purpose,entity\n"
        "2026-01-01,SuvA,69000,69010,10,Errand,Acme\n"
    )
    rows = parse_csv_text(text)
    assert len(rows) == 1
    assert rows[0].entry_date == date(2026, 1, 1)
    assert rows[0].odometer_start == 69000
    assert rows[0].odometer_end == 69010
    assert rows[0].miles == 10.0
    assert rows[0].error is None


def test_parse_csv_text_derives_miles_from_odometer():
    text = (
        "date,odometer_start,odometer_end,description\n"
        "2026-01-01,100,115,Trip 1\n"
    )
    rows = parse_csv_text(text)
    assert rows[0].miles == 15.0


def test_parse_csv_text_flags_invalid_date():
    text = "date,odometer_start,odometer_end\nnot-a-date,100,115\n"
    rows = parse_csv_text(text)
    assert rows[0].error is not None
    assert "invalid date" in rows[0].error


def test_parse_free_text_format_b_two_anchor_commas():
    rows = parse_free_text("2026-01-01,69001,69011,Home to Warehouse Club\n")
    assert len(rows) == 1
    assert rows[0].entry_date == date(2026, 1, 1)
    assert rows[0].odometer_start == 69001
    assert rows[0].odometer_end == 69011
    assert rows[0].miles == 10.0
    assert "Warehouse Club" in (rows[0].description or "")


def test_parse_free_text_format_a_single_anchor_with_time():
    rows = parse_free_text("2025-01-01 08:00 69001 Set out for Warehouse Club\n")
    assert len(rows) == 1
    assert rows[0].entry_date == date(2025, 1, 1)
    assert rows[0].entry_time == "08:00"
    assert rows[0].odometer_end == 69001
    assert rows[0].odometer_start is None
    assert rows[0].miles is None  # derivation happens in resolve_anchors_to_trips


def test_parse_free_text_us_date_and_12_hour_time():
    rows = parse_free_text("01/01/2025 12:00 PM 69011 Done shopping\n")
    assert rows[0].entry_date == date(2025, 1, 1)
    assert rows[0].entry_time == "12:00"
    assert rows[0].odometer_end == 69011


def test_parse_free_text_strips_commas_in_numbers():
    rows = parse_free_text("2025-01-01 08:00 69,001 miles set out\n")
    assert rows[0].odometer_end == 69001


def test_resolve_anchors_fills_in_start_and_miles():
    rows = parse_free_text(
        "2025-01-01 08:00 69001 depart\n"
        "2025-01-01 12:00 PM 69011 arrive\n"
        "2025-01-02 16:00 69011 leave\n"
        "2025-01-02 16:10 69015 arrive PO\n"
    )
    resolved = resolve_anchors_to_trips(rows, starting_odometer=None)
    # First anchor has no prior — stored as 0-mile marker with conflict.
    assert resolved[0].miles == 0.0
    assert resolved[0].conflict is not None
    # Second line: 69001 → 69011 = 10 miles.
    assert resolved[1].odometer_start == 69001
    assert resolved[1].miles == 10.0
    # Third line: 69011 → 69011 = 0-mile "still parked" marker.
    assert resolved[2].miles == 0.0
    # Fourth line: 69011 → 69015 = 4 miles.
    assert resolved[3].miles == 4.0


def test_resolve_anchors_uses_starting_odometer():
    rows = parse_free_text("2025-01-01 08:00 69011 first real entry\n")
    resolved = resolve_anchors_to_trips(rows, starting_odometer=69001)
    assert resolved[0].odometer_start == 69001
    assert resolved[0].miles == 10.0
    assert resolved[0].conflict is None


def test_resolve_anchors_flags_backward_odometer():
    rows = parse_free_text(
        "2025-01-01 08:00 69100 ok\n"
        "2025-01-01 10:00 69050 backward\n"
    )
    resolved = resolve_anchors_to_trips(rows, starting_odometer=69000)
    # First: 69000 → 69100 OK.
    assert resolved[0].miles == 100.0
    # Second: 69100 → 69050 is backward — flagged as error, miles=None.
    assert resolved[1].error is not None
    assert "backward" in resolved[1].error


def test_resolve_anchors_flags_huge_jump_as_conflict():
    rows = parse_free_text(
        "2025-01-01 08:00 69000 ok\n"
        "2025-01-02 08:00 95000 suspicious\n"
    )
    resolved = resolve_anchors_to_trips(rows, starting_odometer=None)
    # Second line has > 10k jump vs first — conflict, not error.
    assert resolved[1].miles == 26_000.0
    assert resolved[1].conflict is not None
    assert "suspicious" in resolved[1].conflict.lower()


def test_parse_input_csv_bytes_decodes_utf8_bom():
    csv_bytes = ("﻿" + "date,odometer_start,odometer_end\n2026-01-01,100,110\n").encode("utf-8")
    rows, fmt = parse_input(text=None, csv_bytes=csv_bytes)
    assert fmt == "csv"
    assert len(rows) == 1
    assert rows[0].miles == 10.0


def test_parse_input_text_only_pure_anchor_format_labels_text_anchor():
    text = (
        "2025-01-01 08:00 69001 set out\n"
        "2025-01-01 12:00 PM 69011 done\n"
    )
    rows, fmt = parse_input(text=text, csv_bytes=None)
    assert fmt == "text_anchor"
    assert len(rows) == 2


def test_parse_input_detects_csv_in_pasted_text():
    text = (
        "date,odometer_start,odometer_end,description\n"
        "2026-01-01,100,115,Trip 1\n"
    )
    rows, fmt = parse_input(text=text, csv_bytes=None)
    assert fmt == "csv"
    assert len(rows) == 1


def test_parse_free_text_accepts_long_month_name_date():
    rows = parse_free_text("December 3, 2024 212632 Gas, Maverick\n")
    assert rows[0].entry_date == date(2024, 12, 3)
    assert rows[0].odometer_end == 212632


def test_parse_free_text_accepts_abbreviated_month_with_period():
    rows = parse_free_text("Nov. 30 2024 211999 Set out\n")
    assert rows[0].entry_date == date(2024, 11, 30)
    assert rows[0].odometer_end == 211999


def test_parse_free_text_accepts_day_first_month_name():
    rows = parse_free_text("3 December 2024 212632 Gas\n")
    assert rows[0].entry_date == date(2024, 12, 3)
    assert rows[0].odometer_end == 212632


def test_parse_free_text_collapses_day_log_to_single_row():
    """Real-world day-log line with multiple odometer readings mixed
    with descriptive text + dollar amounts. Parser emits ONE row per
    dated line: start = first odometer, end = last, miles = delta,
    description = the full line text with odometer numbers stripped.
    The user prefers one row per dated entry — intermediary readings
    are informational and live in the description."""
    rows = parse_free_text(
        "November 30, 2024 211,999 – START Gas, Chevron $77.07 – "
        "212,039 Gas, Maverick $49.06 – 212,284 → Warehouse 212,523\n"
    )
    assert len(rows) == 1
    r = rows[0]
    assert r.entry_date == date(2024, 11, 30)
    assert r.error is None
    assert (r.odometer_start, r.odometer_end) == (211999, 212523)
    assert r.miles == 524.0
    # Full-line description retains every stop the user logged.
    desc = r.description or ""
    assert "Chevron" in desc
    assert "Maverick" in desc
    assert "Warehouse" in desc


def test_parse_free_text_ignores_dollar_amounts_as_odometers():
    rows = parse_free_text(
        "2024-12-03 212,632 Gas $33.91 – 212,824 Gas $40.62 – 213,044\n"
    )
    # One row per dated line: 212632 → 213044.
    assert len(rows) == 1
    assert (rows[0].odometer_start, rows[0].odometer_end) == (212632, 213044)
    assert rows[0].miles == 412.0


def test_parse_input_one_row_per_date_across_day_log_block():
    """Multi-day day-log block: each dated line collapses to a single
    row whose miles equal (last odometer − first odometer) on that
    line. Intermediary readings are informational only."""
    text = (
        "November 30, 2024 211,999 – START Logan Chevron $77.07 – "
        "212,039 Maverick Rawlings $49.06 – 212,284 → Warehouse 212,523\n"
        "December 1, 2024 212,523 Warehouse Club Riverpoint $31.83 - 212,525 "
        "Harvey Park Car Wash → PERSONAL (29.5 MILES) → Warehouse\n"
        "December 3, 2024 212,632 Maverick Torrington $33.91 – 212,824 "
        "Rapid Auto Salvage – Exxon Rapid City $40.62 – 213,044 "
        "Essential Fuels Scottsbluff $40.37 213,464\n"
    )
    rows, fmt = parse_input(text=text, csv_bytes=None)
    assert fmt == "text_range"
    nov30 = [r for r in rows if r.entry_date == date(2024, 11, 30)]
    dec1 = [r for r in rows if r.entry_date == date(2024, 12, 1)]
    dec3 = [r for r in rows if r.entry_date == date(2024, 12, 3)]
    assert len(nov30) == 1 and len(dec1) == 1 and len(dec3) == 1
    assert nov30[0].miles == 524.0
    assert dec1[0].miles == 2.0
    assert dec3[0].miles == 832.0


def test_parse_free_text_flags_out_of_order_odometers():
    """If anchors aren't monotonically non-decreasing in source order
    we still collapse to min→max for the date, but flag the conflict
    so the user can fix the source."""
    rows = parse_free_text(
        "2024-12-03 213,044 then went back 212,632 and arrived 213,464\n"
    )
    assert len(rows) == 1
    assert rows[0].odometer_start == 212632
    assert rows[0].odometer_end == 213464
    assert rows[0].conflict is not None
    assert "ascending" in rows[0].conflict.lower()


def test_parse_free_text_two_digit_miles_parenthetical_excluded():
    """'(29.5 MILES)' should not be treated as an odometer reading."""
    rows = parse_free_text(
        "2024-12-01 212,523 Warehouse Club $31.83 - 212,525 "
        "PERSONAL (29.5 MILES) → Warehouse\n"
    )
    assert rows[0].odometer_start == 212523
    assert rows[0].odometer_end == 212525


def test_parse_input_detects_cross_day_gap_as_conflict():
    """Day N's end and Day N+1's start should match. A gap (missing
    EOD reading on N or missing start on N+1) gets flagged."""
    text = (
        "2025-01-01 1000 – 1100 Day 1\n"
        "2025-01-02 1200 – 1300 Day 2\n"  # gap of 100 vs Day 1 end
    )
    rows, _ = parse_input(text=text, csv_bytes=None)
    assert rows[0].conflict is None
    assert rows[1].conflict is not None
    assert "gap" in rows[1].conflict.lower()


def test_resolve_anchors_ignores_stale_seed_on_backfill():
    """Seed from the DB's latest reading must not corrupt a backfill:
    if the earliest anchor in the batch is far below the seed, treat
    it as backfill and derive the chain from the batch itself. This
    is the real-world case where the user imports older trips after
    newer ones are already logged."""
    rows = parse_free_text(
        "2024-11-30 211999 Day 1\n"
        "2024-12-01 212523 Day 2\n"
        "2024-12-03 212632 Day 3\n"
    )
    resolved = resolve_anchors_to_trips(rows, starting_odometer=232159)
    # Day 1 is the first anchor — 0-mile seed, NOT a backward error.
    assert resolved[0].error is None
    assert resolved[0].miles == 0.0
    # Day 2 derives from Day 1 correctly: 212523 - 211999 = 524.
    assert resolved[1].odometer_start == 211999
    assert resolved[1].miles == 524.0
    assert resolved[1].error is None
    # Day 3 derives from Day 2: 212632 - 212523 = 109.
    assert resolved[2].miles == 109.0
    assert resolved[2].error is None
