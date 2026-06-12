# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 1 — commuting bucket across upsert_trip_meta, add_entry,
yearly_summary, and the writer's ledger output.

Schedule C Part IV line 44 wants three buckets: business, commuting,
personal. Phase 1 adds the commuting column to the trip-meta sidecar
plus a `category` enum for the simplified radio UX.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from lamella.features.mileage.beancount_writer import MileageBeancountWriter
from lamella.features.mileage.service import (
    ImportPreviewRow,
    MileageService,
    MileageValidationError,
    YearlySummaryRow,
)


def _seed_entity(db) -> None:
    """A yearly_summary fixture needs entity rows to exist so
    Expenses:<entity>:Mileage references a valid account. We don't
    need the ledger open directives here — the tests stop at the
    service + writer layer."""
    db.execute(
        "INSERT OR IGNORE INTO entities (slug, display_name) VALUES ('Acme', 'Acme')"
    )


def test_upsert_trip_meta_records_three_buckets(db):
    service = MileageService(conn=db, csv_path=None)
    service.upsert_trip_meta(
        entry_date=date(2026, 3, 10),
        vehicle="SuvA",
        miles=40.0,
        business_miles=25.0,
        commuting_miles=10.0,
        personal_miles=5.0,
        category="mixed",
    )
    row = service.trip_meta_for(
        entry_date=date(2026, 3, 10), vehicle="SuvA", miles=40.0,
    )
    assert row is not None
    assert row["business_miles"] == 25.0
    assert row["commuting_miles"] == 10.0
    assert row["personal_miles"] == 5.0
    assert row["category"] == "mixed"


def test_upsert_trip_meta_rejects_unknown_category(db):
    service = MileageService(conn=db, csv_path=None)
    with pytest.raises(MileageValidationError):
        service.upsert_trip_meta(
            entry_date=date(2026, 3, 10),
            vehicle="SuvA",
            miles=40.0,
            category="bogus",
        )


def test_add_entry_forwards_commuting_to_sidecar(db):
    service = MileageService(conn=db, csv_path=None)
    service.add_entry(
        entry_date=date(2026, 3, 11),
        vehicle="SuvA",
        entity="Acme",
        miles=20.0,
        business_miles=15.0,
        commuting_miles=5.0,
        category="mixed",
        known_entities=["Acme", "Personal"],
    )
    row = service.trip_meta_for(
        entry_date=date(2026, 3, 11), vehicle="SuvA", miles=20.0,
    )
    assert row is not None
    assert row["business_miles"] == 15.0
    assert row["commuting_miles"] == 5.0
    assert row["category"] == "mixed"
    # And the denormalized copy on mileage_entries.
    entry = db.execute(
        "SELECT purpose_category FROM mileage_entries "
        "WHERE entry_date = ? AND vehicle = ? AND miles = ?",
        ("2026-03-11", "SuvA", 20.0),
    ).fetchone()
    assert entry["purpose_category"] == "mixed"


def test_yearly_summary_returns_three_buckets(db):
    service = MileageService(conn=db, csv_path=None)
    service.add_entry(
        entry_date=date(2026, 2, 1),
        vehicle="SuvA",
        entity="Acme",
        miles=30.0,
        business_miles=20.0,
        commuting_miles=8.0,
        personal_miles=2.0,
        category="mixed",
        known_entities=["Acme"],
    )
    service.add_entry(
        entry_date=date(2026, 2, 2),
        vehicle="SuvA",
        entity="Acme",
        miles=10.0,
        business_miles=10.0,
        category="business",
        known_entities=["Acme"],
    )
    rows = service.yearly_summary(2026, rate_per_mile=0.67)
    assert len(rows) == 1
    summary = rows[0]
    assert isinstance(summary, YearlySummaryRow)
    assert summary.vehicle == "SuvA"
    assert summary.entity == "Acme"
    assert summary.miles == pytest.approx(40.0)
    assert summary.business_miles == pytest.approx(30.0)
    assert summary.commuting_miles == pytest.approx(8.0)
    assert summary.personal_miles == pytest.approx(2.0)
    # Phase 2 narrowing: deduction = business miles × rate.
    # Commuting and personal are never deductible.
    assert summary.deduction_usd == pytest.approx(round(30.0 * 0.67, 2))


def test_yearly_summary_missing_split_keeps_zero_buckets(db):
    """When no split is recorded the three buckets stay at 0 so the
    Phase 2 data-health panel can surface 'unknown'."""
    service = MileageService(conn=db, csv_path=None)
    service.add_entry(
        entry_date=date(2026, 2, 1),
        vehicle="SuvA",
        entity="Acme",
        miles=30.0,
        known_entities=["Acme"],
    )
    rows = service.yearly_summary(2026, rate_per_mile=0.67)
    assert len(rows) == 1
    summary = rows[0]
    assert summary.miles == pytest.approx(30.0)
    assert summary.business_miles == 0.0
    assert summary.commuting_miles == 0.0
    assert summary.personal_miles == 0.0


def test_mileage_writer_emits_commuting_metadata_key(tmp_path: Path):
    """When yearly summary rows carry commuting miles, the ledger
    writer emits `lamella-mileage-commuting-miles` on the corresponding
    transaction block."""
    main_bean = tmp_path / "main.bean"
    main_bean.write_text(
        '2020-01-01 open Expenses:Acme:Mileage USD\n'
        '2020-01-01 open Equity:MileageDeductions USD\n',
        encoding="utf-8",
    )
    summary_path = tmp_path / "mileage_summary.bean"

    writer = MileageBeancountWriter(
        main_bean=main_bean,
        summary_path=summary_path,
        run_check=False,
    )
    rows = [
        YearlySummaryRow(
            vehicle="SuvA",
            entity="Acme",
            miles=100.0,
            deduction_usd=67.0,
            business_miles=70.0,
            commuting_miles=20.0,
            personal_miles=10.0,
        ),
    ]
    writer.write_year(year=2026, rows=rows, rate_per_mile=0.67)
    text = summary_path.read_text(encoding="utf-8")
    assert "lamella-mileage-business-miles: 70.00" in text
    assert "lamella-mileage-commuting-miles: 20.00" in text
    assert "lamella-mileage-personal-miles: 10.00" in text


def test_mileage_writer_omits_zero_bucket_keys(tmp_path: Path):
    """Personal-only deploys shouldn't bloat `mileage_summary.bean`
    with zero commuting/business keys."""
    main_bean = tmp_path / "main.bean"
    main_bean.write_text(
        '2020-01-01 open Expenses:Personal:Mileage USD\n'
        '2020-01-01 open Equity:MileageDeductions USD\n',
        encoding="utf-8",
    )
    summary_path = tmp_path / "mileage_summary.bean"

    writer = MileageBeancountWriter(
        main_bean=main_bean, summary_path=summary_path, run_check=False,
    )
    rows = [
        YearlySummaryRow(
            vehicle="SuvA",
            entity="Personal",
            miles=50.0,
            deduction_usd=33.50,
            business_miles=0.0,
            commuting_miles=0.0,
            personal_miles=50.0,
        ),
    ]
    writer.write_year(year=2026, rows=rows, rate_per_mile=0.67)
    text = summary_path.read_text(encoding="utf-8")
    assert "lamella-mileage-business-miles" not in text
    assert "lamella-mileage-commuting-miles" not in text
    assert "lamella-mileage-personal-miles: 50.00" in text


def test_import_preview_row_carries_commuting_and_category(db):
    """Import path round-trips commuting + category through
    `write_import_rows` into mileage_trip_meta + mileage_entries."""
    service = MileageService(conn=db, csv_path=None)
    batch = service.create_import_batch(
        vehicle_slug="suvone", source_filename=None, source_format="csv",
    )
    previews = [
        ImportPreviewRow(
            line_no=2,
            entry_date=date(2026, 3, 1),
            entry_time=None,
            vehicle="SuvA",
            odometer_start=None,
            odometer_end=None,
            miles=40.0,
            description="home → office → site visit",
            business_miles=25.0,
            commuting_miles=10.0,
            personal_miles=5.0,
            category="mixed",
        ),
    ]
    result = service.write_import_rows(
        batch_id=batch,
        vehicle="SuvA",
        vehicle_slug="suvone",
        entity="Acme",
        rows=previews,
    )
    assert result.rows_written == 1
    # Sidecar carries the split.
    meta = service.trip_meta_for(
        entry_date=date(2026, 3, 1), vehicle="SuvA", miles=40.0,
    )
    assert meta["business_miles"] == 25.0
    assert meta["commuting_miles"] == 10.0
    assert meta["personal_miles"] == 5.0
    assert meta["category"] == "mixed"
    # Denormalized purpose_category lands on mileage_entries.
    row = db.execute(
        "SELECT purpose_category FROM mileage_entries "
        "WHERE entry_date = '2026-03-01' AND vehicle = 'SuvA'"
    ).fetchone()
    assert row["purpose_category"] == "mixed"
