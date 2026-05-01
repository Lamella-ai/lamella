# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pytest

from lamella.features.mileage.service import MileageService, MileageValidationError


FIXTURES = Path(__file__).parent / "fixtures" / "mileage"


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    src = FIXTURES / "vehicles_two_vehicles.csv"
    dest = tmp_path / "vehicles.csv"
    shutil.copy(src, dest)
    return dest


def _service_with_bootstrap(db, csv_path: Path) -> MileageService:
    """Build a service and bootstrap mileage_entries from the fixture
    CSV — simulating a fresh-install first boot where vehicles.csv
    existed before migration 032."""
    service = MileageService(conn=db, csv_path=csv_path)
    service.bootstrap_from_csv_if_empty()
    return service


def test_bootstrap_fills_table_from_csv(db, csv_path: Path):
    service = MileageService(conn=db, csv_path=csv_path)
    n = service.bootstrap_from_csv_if_empty()
    assert n == 5
    rows = service.list_entries()
    assert len(rows) == 5
    vehicles = {r.vehicle for r in rows}
    assert vehicles == {"SuvA", "TruckB"}


def test_bootstrap_skips_when_already_populated(db, csv_path: Path):
    service = MileageService(conn=db, csv_path=csv_path)
    assert service.bootstrap_from_csv_if_empty() == 5
    # Second call should be a no-op; the row count stays at 5.
    assert service.bootstrap_from_csv_if_empty() == 0
    assert service.refresh_cache() == 5


def test_yearly_summary_groups_by_vehicle_entity(db, csv_path: Path):
    service = _service_with_bootstrap(db, csv_path)
    rows = service.yearly_summary(2026, rate_per_mile=0.67)
    by_pair = {(r.vehicle, r.entity): r for r in rows}
    assert by_pair[("SuvA", "Acme")].miles == pytest.approx(140.0)
    assert by_pair[("TruckB", "Personal")].miles == pytest.approx(55.0)
    # Phase 2: deduction = business miles × rate. The CSV fixture
    # doesn't record any split, so business_miles = 0 for every trip
    # and the deduction is $0. Users see the gap in the data-health
    # banner on /vehicles and log splits to unlock a non-zero figure.
    assert by_pair[("SuvA", "Acme")].business_miles == pytest.approx(0.0)
    assert by_pair[("SuvA", "Acme")].deduction_usd == pytest.approx(0.0)


def test_yearly_summary_deduction_uses_business_miles_only(db, csv_path: Path):
    """After a split is recorded, the deduction uses business miles
    only — commuting + personal stay excluded per Schedule C rules."""
    service = _service_with_bootstrap(db, csv_path)
    # Add a split to the Jan 12 SuvA/Acme trip (65 mi).
    service.upsert_trip_meta(
        entry_date=date(2026, 1, 12),
        vehicle="SuvA",
        miles=65.0,
        business_miles=50.0,
        commuting_miles=10.0,
        personal_miles=5.0,
    )
    rows = service.yearly_summary(2026, rate_per_mile=0.67)
    by_pair = {(r.vehicle, r.entity): r for r in rows}
    # Business miles only from the one trip with a recorded split.
    assert by_pair[("SuvA", "Acme")].business_miles == pytest.approx(50.0)
    assert by_pair[("SuvA", "Acme")].deduction_usd == pytest.approx(round(50.0 * 0.67, 2))


def test_add_entry_with_odometer_computes_miles(db, csv_path: Path):
    service = _service_with_bootstrap(db, csv_path)
    row = service.add_entry(
        entry_date=date(2026, 4, 1),
        vehicle="SuvA",
        entity="Acme",
        odometer_end=84200,
        purpose="Pickup",
    )
    # Last SuvA odometer in fixture = 84140. Delta = 60.
    assert row.miles == 60.0
    assert row.odometer_start == 84140
    assert row.odometer_end == 84200


def test_add_entry_odometer_without_prior_rejects(db, tmp_path: Path):
    # No CSV, no prior entries.
    service = MileageService(conn=db, csv_path=tmp_path / "vehicles.csv")
    with pytest.raises(MileageValidationError, match="no prior"):
        service.add_entry(
            entry_date=date(2026, 4, 1),
            vehicle="NewCar",
            entity="Acme",
            odometer_end=15000,
        )


def test_add_entry_negative_odometer_delta_rejects(db, csv_path: Path):
    service = _service_with_bootstrap(db, csv_path)
    with pytest.raises(MileageValidationError, match=">= prior reading"):
        service.add_entry(
            entry_date=date(2026, 4, 1),
            vehicle="SuvA",
            entity="Acme",
            odometer_end=80000,  # less than fixture's 84140
        )




def test_add_entry_equal_odometer_delta_allows_zero_miles(db, csv_path: Path):
    service = _service_with_bootstrap(db, csv_path)
    row = service.add_entry(
        entry_date=date(2026, 4, 2),
        vehicle="SuvA",
        entity="Acme",
        odometer_end=84140,  # equal to latest fixture reading
        purpose="Note-only stop",
    )
    assert row.odometer_start == 84140
    assert row.odometer_end == 84140
    assert row.miles == 0.0
def test_add_entry_miles_only_path(db, tmp_path: Path):
    service = MileageService(conn=db, csv_path=tmp_path / "vehicles.csv")
    row = service.add_entry(
        entry_date=date(2026, 4, 1),
        vehicle="NewCar",
        entity="Acme",
        miles=15.0,
    )
    assert row.miles == 15.0
    assert row.odometer_start is None
    assert row.odometer_end is None
    rows = service.list_entries()
    assert len(rows) == 1


def test_add_entry_unknown_entity_rejects(db, csv_path: Path):
    service = _service_with_bootstrap(db, csv_path)
    with pytest.raises(MileageValidationError, match="not in the known"):
        service.add_entry(
            entry_date=date(2026, 4, 1),
            vehicle="SuvA",
            entity="UnknownLLC",
            miles=10,
            known_entities=["Acme", "Personal"],
        )


def test_add_entry_both_odometer_and_miles_rejects(db, csv_path: Path):
    service = _service_with_bootstrap(db, csv_path)
    with pytest.raises(MileageValidationError, match="not both"):
        service.add_entry(
            entry_date=date(2026, 4, 1),
            vehicle="SuvA",
            entity="Acme",
            odometer_end=84200,
            miles=60.0,
        )


def test_backup_to_csv_round_trip(db, tmp_path: Path):
    """Add a few entries with no seed CSV; backup_to_csv should
    write a fresh file that round-trips back through the parser."""
    csv_path = tmp_path / "vehicles.csv"
    service = MileageService(conn=db, csv_path=csv_path)
    service.add_entry(
        entry_date=date(2026, 1, 1), vehicle="SuvA", entity="Acme", miles=10.0,
    )
    service.add_entry(
        entry_date=date(2026, 1, 2), vehicle="SuvA", entity="Acme",
        miles=20.0, purpose="Delivery",
    )
    # add_entry already calls backup_to_csv internally, but check it
    # again explicitly.
    service.backup_to_csv()
    assert csv_path.exists()

    # Drop the DB rows and re-bootstrap from the CSV we just wrote.
    db.execute("DELETE FROM mileage_entries")
    fresh = MileageService(conn=db, csv_path=csv_path)
    assert fresh.bootstrap_from_csv_if_empty() == 2
    entries = fresh.list_entries()
    assert len(entries) == 2
    assert sum(e.miles for e in entries) == pytest.approx(30.0)


def test_delete_entry_by_id(db, csv_path: Path):
    service = _service_with_bootstrap(db, csv_path)
    rows = service.list_entries()
    # csv_row_index on MileageRow now carries the DB id.
    first_id = rows[0].csv_row_index
    assert service.delete_entry(first_id) is True
    assert service.delete_entry(first_id) is False
    assert len(service.list_entries()) == 4


def test_import_batch_round_trip(db, tmp_path: Path):
    """Create an import batch, write rows against it, then undo
    the batch — every row should be removed."""
    from lamella.features.mileage.service import ImportPreviewRow
    csv_path = tmp_path / "vehicles.csv"
    service = MileageService(conn=db, csv_path=csv_path)
    batch_id = service.create_import_batch(
        vehicle_slug="suvone", source_filename="log.txt",
        source_format="text_range",
    )
    rows = [
        ImportPreviewRow(
            line_no=1, entry_date=date(2026, 1, 1), entry_time=None,
            vehicle=None, odometer_start=10_000, odometer_end=10_010,
            miles=10.0, description="Trip 1",
        ),
        ImportPreviewRow(
            line_no=2, entry_date=date(2026, 1, 2), entry_time=None,
            vehicle=None, odometer_start=10_010, odometer_end=10_025,
            miles=15.0, description="Trip 2",
        ),
    ]
    result = service.write_import_rows(
        batch_id=batch_id,
        vehicle="SuvA", vehicle_slug="suvone", entity="Acme",
        rows=rows,
    )
    assert result.rows_written == 2
    assert len(service.list_entries()) == 2

    removed = service.delete_import_batch(batch_id)
    assert removed == 2
    assert len(service.list_entries()) == 0
