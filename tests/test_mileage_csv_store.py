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

from lamella.features.mileage.csv_store import (
    CSV_HEADER,
    MileageCsvError,
    MileageCsvStore,
    MileageRow,
)


FIXTURES = Path(__file__).parent / "fixtures" / "mileage"


def _new_row() -> MileageRow:
    return MileageRow(
        entry_date=date(2026, 4, 1),
        vehicle="SuvA",
        odometer_start=84140,
        odometer_end=84200,
        miles=60.0,
        purpose="Pickup",
        entity="Acme",
        from_loc="Shop",
        to_loc="Supplier",
        notes=None,
        csv_row_index=-1,
    )


def test_read_two_vehicle_csv(tmp_path: Path):
    src = FIXTURES / "vehicles_two_vehicles.csv"
    dest = tmp_path / "vehicles.csv"
    shutil.copy(src, dest)
    rows, warnings = MileageCsvStore(dest).read_all()
    assert warnings == []
    assert len(rows) == 5
    assert rows[0].vehicle == "SuvA"
    assert rows[0].entry_date == date(2026, 1, 5)
    assert rows[0].miles == 25.0
    assert rows[2].vehicle == "TruckB"


def test_read_malformed_row_collects_warning(tmp_path: Path):
    src = FIXTURES / "vehicles_malformed_row.csv"
    dest = tmp_path / "vehicles.csv"
    shutil.copy(src, dest)
    rows, warnings = MileageCsvStore(dest).read_all()
    # Two valid rows, one warning for the bad date.
    assert len(rows) == 2
    assert any("invalid date" in w.lower() for w in warnings)


def test_read_strict_raises_on_malformed_row(tmp_path: Path):
    src = FIXTURES / "vehicles_malformed_row.csv"
    dest = tmp_path / "vehicles.csv"
    shutil.copy(src, dest)
    with pytest.raises(MileageCsvError):
        MileageCsvStore(dest).read_all(strict=True)


def test_append_round_trip(tmp_path: Path):
    src = FIXTURES / "vehicles_two_vehicles.csv"
    dest = tmp_path / "vehicles.csv"
    shutil.copy(src, dest)
    store = MileageCsvStore(dest)
    new_index = store.append(_new_row())
    assert new_index == 5
    rows, _ = store.read_all()
    assert len(rows) == 6
    assert rows[-1].vehicle == "SuvA"
    assert rows[-1].miles == 60.0
    assert rows[-1].csv_row_index == 5


def test_ensure_creates_with_header(tmp_path: Path):
    path = tmp_path / "mileage" / "vehicles.csv"
    store = MileageCsvStore(path)
    assert not path.exists()
    store.ensure()
    assert path.exists()
    contents = path.read_text(encoding="utf-8")
    assert contents.startswith(",".join(CSV_HEADER))


def test_rewrite_replaces_file(tmp_path: Path):
    src = FIXTURES / "vehicles_two_vehicles.csv"
    dest = tmp_path / "vehicles.csv"
    shutil.copy(src, dest)
    store = MileageCsvStore(dest)
    rows, _ = store.read_all()
    keep = [r for r in rows if r.vehicle == "TruckB"]
    n = store.rewrite(keep)
    assert n == 2
    fresh, _ = store.read_all()
    assert all(r.vehicle == "TruckB" for r in fresh)


def test_missing_columns_raise(tmp_path: Path):
    dest = tmp_path / "vehicles.csv"
    dest.write_text("date,vehicle,miles\n2026-01-01,SuvA,10\n", encoding="utf-8")
    with pytest.raises(MileageCsvError):
        MileageCsvStore(dest).read_all()
