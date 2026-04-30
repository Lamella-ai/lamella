# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for mileage context + receipt-date sanity checks.

Two additions to the classify context pipeline:
1. Mileage log entries near the txn date feed into the prompt.
2. Linked receipts with implausible OCR'd dates get a
   date_mismatch_note so the AI treats the date with skepticism.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.features.ai_cascade.mileage_context import mileage_context_for_txn
from lamella.features.ai_cascade.receipt_context import (
    ReceiptContext,
    fetch_receipt_context,
)
from lamella.core.db import connect, migrate


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _seed_mileage(
    conn, *,
    entry_date: str, vehicle: str, entity: str, miles: float = 30.0,
    purpose: str | None = None, from_loc: str | None = None,
    to_loc: str | None = None, notes: str | None = None,
    csv_row: int = 0, csv_mtime: str = "2026-04-20 00:00:00",
):
    conn.execute(
        """
        INSERT INTO mileage_entries
            (entry_date, vehicle, odometer_start, odometer_end,
             miles, purpose, entity, from_loc, to_loc, notes,
             csv_row_index, csv_mtime)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (entry_date, vehicle, None, None, miles, purpose, entity,
         from_loc, to_loc, notes, csv_row, csv_mtime),
    )


# --- mileage context ------------------------------------------------


class TestMileageContext:
    def test_returns_entries_within_proximity(self, conn):
        _seed_mileage(
            conn, entry_date="2026-04-17", vehicle="Acme Cargo Van",
            entity="Acme", purpose="Drove to Warehouse Club for gas",
        )
        entries = mileage_context_for_txn(
            conn, txn_date=date(2026, 4, 18),
        )
        assert len(entries) == 1
        assert entries[0].vehicle == "Acme Cargo Van"
        assert entries[0].purpose == "Drove to Warehouse Club for gas"

    def test_entries_outside_window_excluded(self, conn):
        _seed_mileage(
            conn, entry_date="2026-04-01", vehicle="Personal Work SUV",
            entity="Personal", purpose="random trip",
        )
        entries = mileage_context_for_txn(
            conn, txn_date=date(2026, 4, 20),
        )
        assert entries == []

    def test_entity_ranking_puts_matching_entity_first(self, conn):
        """Entity filter is a ranking preference, not a strict
        exclusion — cross-entity entries still surface (that's the
        wrong-card signal), but the card's entity's entries
        appear first."""
        _seed_mileage(
            conn, entry_date="2026-04-17", vehicle="V1",
            entity="Acme", purpose="Acme trip", csv_row=1,
        )
        _seed_mileage(
            conn, entry_date="2026-04-18", vehicle="V2",
            entity="Personal", purpose="personal errand", csv_row=2,
        )
        entries = mileage_context_for_txn(
            conn, txn_date=date(2026, 4, 18), entity="Acme",
        )
        # Both surface; Acme first.
        assert len(entries) == 2
        assert entries[0].entity == "Acme"

    def test_costco_fuel_multi_vehicle_scenario(self, conn):
        """The motivating case: multiple Warehouse Club-fuel mileage entries
        for different vehicles over the same few days. The classify
        prompt needs to see ALL of them so the AI can pick the one
        whose date matches the txn."""
        _seed_mileage(
            conn, entry_date="2026-04-16", vehicle="Work SUV",
            entity="Personal", purpose="drove to Warehouse Club for gas",
            csv_row=1,
        )
        _seed_mileage(
            conn, entry_date="2026-04-17", vehicle="Cargo Van",
            entity="Acme", purpose="drove to Warehouse Club for gas",
            csv_row=2,
        )
        _seed_mileage(
            conn, entry_date="2026-04-18", vehicle="Farm Tractor",
            entity="FarmCo",
            purpose="drove to Warehouse Club for gas", csv_row=3,
        )
        entries = mileage_context_for_txn(
            conn, txn_date=date(2026, 4, 17), entity="Acme",
        )
        assert len(entries) == 3
        # Acme-entity entry ranks first.
        assert entries[0].entity == "Acme"

    def test_zero_mile_entries_with_notes_surface_for_fuel_attribution(self, conn):
        """Zero-mile days can still be the correct attribution target.

        Scenario: two vehicles, one fuel transaction.
          - Car A (stationary): miles=0, notes="Filled up with gas from
            gas can". Car A is what got the gas.
          - Car B (the one that drove): miles=15, purpose/notes
            describe fetching gas FOR Car A.

        Both rows must flow into context so the AI can infer that the
        fuel charge is attributed to Car A, not to the vehicle that
        physically drove.
        """
        _seed_mileage(
            conn, entry_date="2026-04-17", vehicle="Car A",
            entity="Personal", miles=0.0,
            notes="Filled up with gas from gas can",
            csv_row=1,
        )
        _seed_mileage(
            conn, entry_date="2026-04-17", vehicle="Car B",
            entity="Personal", miles=15.0,
            purpose="Went to gas station to get gas for Car A",
            csv_row=2,
        )
        entries = mileage_context_for_txn(
            conn, txn_date=date(2026, 4, 17),
        )
        assert len(entries) == 2
        by_vehicle = {e.vehicle: e for e in entries}
        assert by_vehicle["Car A"].miles == 0.0
        assert by_vehicle["Car A"].notes == "Filled up with gas from gas can"
        assert by_vehicle["Car B"].miles == 15.0
        assert "gas for Car A" in (by_vehicle["Car B"].purpose or "")

    def test_zero_mile_no_activity_marker_still_surfaces(self, conn):
        """A pure "vehicle sat still today" marker (miles=0, no notes)
        is also signal: it rules the vehicle OUT of attribution for a
        fuel / tolls / parking txn on that date."""
        _seed_mileage(
            conn, entry_date="2026-04-17", vehicle="Car A",
            entity="Personal", miles=0.0,
        )
        entries = mileage_context_for_txn(
            conn, txn_date=date(2026, 4, 17),
        )
        assert len(entries) == 1
        assert entries[0].miles == 0.0

    def test_missing_mileage_table_returns_empty(self, tmp_path: Path):
        """Defensive: if the mileage_entries table doesn't exist,
        the helper returns [] rather than crashing."""
        import sqlite3 as _sqlite
        bare = _sqlite.connect(":memory:")
        bare.row_factory = _sqlite.Row
        assert mileage_context_for_txn(
            bare, txn_date=date(2026, 4, 18),
        ) == []


# --- per-vehicle log density ---------------------------------------


class TestVehicleLogDensity:
    def _seed_vehicle(self, conn, slug, display, entity_slug=None):
        # entity_slug is FK-to-entities; leave NULL to avoid seeding
        # an entities row just for density tests (density only uses
        # the vehicle list, not entity validation).
        conn.execute(
            "INSERT INTO vehicles (slug, display_name, entity_slug, "
            "is_active) VALUES (?, ?, NULL, 1)",
            (slug, display),
        )

    def test_dense_vs_sparse_vehicles(self, conn):
        """One vehicle logged daily (30 entries), another logged twice
        in the window. Density reflects the divergence — the AI can
        then calibrate "absent today" differently per vehicle."""
        from lamella.features.ai_cascade.mileage_context import vehicle_log_density

        self._seed_vehicle(conn, "VA", "Car A")
        self._seed_vehicle(conn, "VB", "Car B")
        # Car A — one entry per day for 28 days ending 2026-04-30.
        for i in range(28):
            d = date(2026, 4, 30).toordinal() - i
            iso = date.fromordinal(d).isoformat()
            _seed_mileage(
                conn, entry_date=iso, vehicle="Car A", entity="Personal",
                miles=10.0, csv_row=i,
            )
        # Car B — two sparse entries in the same window.
        _seed_mileage(
            conn, entry_date="2026-04-25", vehicle="Car B",
            entity="Personal", miles=20.0, csv_row=100,
        )
        _seed_mileage(
            conn, entry_date="2026-04-10", vehicle="Car B",
            entity="Personal", miles=15.0, csv_row=101,
        )

        rows = vehicle_log_density(
            conn, as_of_date=date(2026, 4, 30), window_days=30,
        )
        by = {r.vehicle: r for r in rows}
        assert by["Car A"].days_with_entries == 28
        assert by["Car A"].total_entries == 28
        assert by["Car B"].days_with_entries == 2
        assert by["Car B"].total_entries == 2
        assert by["Car A"].window_days == 30

    def test_inactive_vehicle_excluded(self, conn):
        from lamella.features.ai_cascade.mileage_context import vehicle_log_density

        conn.execute(
            "INSERT INTO vehicles (slug, display_name, is_active) "
            "VALUES ('VOLD', 'Retired Car', 0)"
        )
        _seed_mileage(
            conn, entry_date="2026-04-20", vehicle="Retired Car",
            entity="Personal",
        )
        rows = vehicle_log_density(conn, as_of_date=date(2026, 4, 30))
        assert rows == []

    def test_zero_density_vehicle_still_reported(self, conn):
        """A vehicle with no entries in the window still gets a row
        (count=0). Absence-of-row would hide the vehicle from the
        prompt entirely — we want the AI to know it exists."""
        from lamella.features.ai_cascade.mileage_context import vehicle_log_density

        self._seed_vehicle(conn, "VQ", "Quiet Car")
        rows = vehicle_log_density(conn, as_of_date=date(2026, 4, 30))
        assert len(rows) == 1
        assert rows[0].vehicle == "Quiet Car"
        assert rows[0].days_with_entries == 0
        assert rows[0].total_entries == 0
        assert rows[0].last_entry_date is None


# --- receipt date mismatch -----------------------------------------


def _seed_paperless_doc(
    conn, *,
    paperless_id: int = 1, vendor: str = "Warehouse Club",
    total: str = "58.12",
    receipt_date: str,
    content: str = "Warehouse Club Wholesale\nFuel 15.2 gal $58.12\n",
):
    conn.execute(
        """
        INSERT INTO paperless_doc_index
            (paperless_id, vendor, total_amount, receipt_date,
             content_excerpt, title)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (paperless_id, vendor, total, receipt_date, content, "Receipt"),
    )


def _seed_link(conn, *, paperless_id: int, txn_hash: str):
    conn.execute(
        """
        INSERT INTO receipt_links
            (paperless_id, txn_hash, txn_date, txn_amount, match_method)
        VALUES (?, ?, ?, ?, 'manual')
        """,
        (paperless_id, txn_hash, "2026-04-18", 58.12),
    )


class TestReceiptDateSanity:
    def test_plausible_date_has_no_mismatch_note(self, conn):
        _seed_paperless_doc(
            conn, paperless_id=1, receipt_date="2026-04-17",
        )
        _seed_link(conn, paperless_id=1, txn_hash="abc")
        ctx = fetch_receipt_context(
            conn, txn_hash="abc", posting_date=date(2026, 4, 18),
        )
        assert ctx is not None
        assert ctx.date_mismatch_note is None

    def test_year_off_date_flagged_as_ocr_error(self, conn):
        """The Warehouse Club-2064 scenario. Receipt OCR'd as year 2064,
        txn posted in 2026 — the date is junk, everything else
        (vendor, total, line items) is fine. The AI gets a warning
        to trust the content but not the date."""
        _seed_paperless_doc(
            conn, paperless_id=1, receipt_date="2064-01-08",
        )
        _seed_link(conn, paperless_id=1, txn_hash="abc")
        ctx = fetch_receipt_context(
            conn, txn_hash="abc", posting_date=date(2026, 4, 18),
        )
        assert ctx is not None
        assert ctx.date_mismatch_note is not None
        assert "OCR error" in ctx.date_mismatch_note

    def test_month_off_date_flagged_as_possible(self, conn):
        """45 days off: could be OCR, delayed settlement, or the
        wrong receipt linked. Flag softly so the AI weighs content
        over dates."""
        _seed_paperless_doc(
            conn, paperless_id=1, receipt_date="2026-03-01",
        )
        _seed_link(conn, paperless_id=1, txn_hash="abc")
        ctx = fetch_receipt_context(
            conn, txn_hash="abc", posting_date=date(2026, 4, 18),
        )
        assert ctx is not None
        assert ctx.date_mismatch_note is not None
        # Softer warning than the year-off case; not the "likely
        # OCR error" phrasing.
        assert "likely an OCR error" not in ctx.date_mismatch_note
        assert "days off" in ctx.date_mismatch_note

    def test_missing_posting_date_skips_check(self, conn):
        _seed_paperless_doc(
            conn, paperless_id=1, receipt_date="2064-01-08",
        )
        _seed_link(conn, paperless_id=1, txn_hash="abc")
        ctx = fetch_receipt_context(conn, txn_hash="abc")  # no posting_date
        assert ctx is not None
        assert ctx.date_mismatch_note is None

    def test_candidate_path_also_flags_mismatch(self, conn):
        """A candidate-match receipt with a plausible date+amount
        match shouldn't be flagged. But if the SAME total matches
        a receipt dated 2064 and a txn in 2026, the date window
        rules it out before we even get to flagging — verifying
        that negative path."""
        _seed_paperless_doc(
            conn, paperless_id=1, receipt_date="2064-01-08",
            total="58.12",
        )
        ctx = fetch_receipt_context(
            conn, posting_date=date(2026, 4, 18),
            amount=Decimal("-58.12"),
            tolerance_days=3,
        )
        # Outside the ±3-day window → not returned as a candidate.
        assert ctx is None


# --- classify wire returns 10-tuple (mileage + density) ----------


class TestClassifyWire:
    def test_build_classify_context_returns_10_tuple(self, conn):
        from beancount.core import data as bdata
        from beancount.core.amount import Amount
        from beancount.core.number import D

        _seed_mileage(
            conn, entry_date="2026-04-17", vehicle="Cargo Van",
            entity="Acme", purpose="Warehouse Club gas",
        )
        posting_card = bdata.Posting(
            account="Liabilities:Acme:Card:0123",
            units=Amount(D("-58.12"), "USD"),
            cost=None, price=None, flag=None, meta=None,
        )
        posting_fixme = bdata.Posting(
            account="Expenses:Acme:FIXME",
            units=Amount(D("58.12"), "USD"),
            cost=None, price=None, flag=None, meta=None,
        )
        txn = bdata.Transaction(
            meta={"filename": "x", "lineno": 1},
            date=date(2026, 4, 18), flag="*",
            payee="Warehouse Club", narration="fuel",
            tags=frozenset(), links=frozenset(),
            postings=[posting_card, posting_fixme],
        )
        from lamella.features.ai_cascade.classify import build_classify_context
        result = build_classify_context(
            entries=[], txn=txn, conn=conn,
        )
        assert len(result) == 10
        mileage_entries = result[8]
        assert len(mileage_entries) == 1
        assert mileage_entries[0].purpose == "Warehouse Club gas"
        vehicle_density = result[9]
        # vehicles table is empty in this fixture, so density is empty.
        assert vehicle_density == []
