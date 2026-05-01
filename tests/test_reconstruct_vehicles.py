# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 7 — reconstruct pass for vehicle state.

Wipe state tables, load a fixture ledger carrying every directive
type, run step8_vehicles, and confirm the SQLite tables match.
"""
from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

from beancount.loader import load_file

from lamella.core.transform.steps.step8_vehicles import reconstruct_vehicles
from lamella.features.vehicles.writer import (
    append_mileage_attribution,
    append_vehicle,
    append_vehicle_credit,
    append_vehicle_election,
    append_vehicle_renewal,
    append_vehicle_trip_template,
    append_vehicle_valuation,
    append_vehicle_yearly_mileage,
)


def _seed_ledger(tmp_path: Path) -> Path:
    main = tmp_path / "main.bean"
    config = tmp_path / "connector_config.bean"
    main.write_text(
        "2020-01-01 open Assets:Vehicles:SuvA USD\n"
        "2020-01-01 open Assets:Personal:Checking USD\n",
        encoding="utf-8",
    )
    append_vehicle(
        connector_config=config, main_bean=main,
        slug="SuvA", display_name="2015 SuvA",
        year=2015, make="Chevrolet", model="SuvA",
        vin=None, license_plate=None, entity_slug=None,
        purchase_date="2024-01-15", purchase_price="20000",
        purchase_fees="500", asset_account="Assets:Vehicles:SuvA",
        gvwr_lbs=7100, placed_in_service="2024-01-15",
        fuel_type="gasoline", notes=None, is_active=True,
        run_check=False,
    )
    append_vehicle_yearly_mileage(
        connector_config=config, main_bean=main,
        slug="SuvA", year=2026,
        start_mileage=84000, end_mileage=96000,
        business_miles=9000, commuting_miles=1500, personal_miles=1500,
        run_check=False,
    )
    append_vehicle_valuation(
        connector_config=config, main_bean=main,
        slug="SuvA", as_of_date=date(2026, 3, 1),
        value="18500", source="kbb", notes=None, run_check=False,
    )
    append_vehicle_election(
        connector_config=config, main_bean=main,
        slug="SuvA", tax_year=2026,
        depreciation_method="section-179",
        section_179_amount="12000",
        bonus_depreciation_amount=None,
        basis_at_placed_in_service="20500",
        business_use_pct_override=0.85,
        listed_property_qualified=1,
        notes=None, run_check=False,
    )
    append_vehicle_credit(
        connector_config=config, main_bean=main,
        slug="SuvA", tax_year=2026,
        credit_label="Federal EV § 30D",
        amount="7500", status="claimed", notes=None,
        run_check=False,
    )
    append_vehicle_renewal(
        connector_config=config, main_bean=main,
        slug="SuvA", renewal_kind="registration",
        due_date=date(2026, 10, 15), cadence_months=12,
        notes=None, run_check=False,
    )
    append_vehicle_trip_template(
        connector_config=config, main_bean=main,
        slug="ClientA", display_name="Office → Client A",
        vehicle_slug="SuvA", entity="Acme",
        default_from="Office", default_to="Client A",
        default_purpose="Client visit",
        default_miles=18.5, default_category="business",
        is_round_trip=True, is_active=True,
        run_check=False,
    )
    append_mileage_attribution(
        connector_config=config, main_bean=main,
        entry_date=date(2026, 1, 10),
        vehicle="SuvA", miles=100.0,
        attributed_entity="Ranch", run_check=False,
    )
    return main


def _fresh_db(tmp_path: Path) -> sqlite3.Connection:
    from lamella.core.db import connect, migrate
    conn = connect(tmp_path / "test.sqlite")
    migrate(conn)
    return conn


def test_reconstruct_writes_every_state_table(tmp_path: Path):
    main = _seed_ledger(tmp_path)
    entries, _errors, _opts = load_file(str(main))
    conn = _fresh_db(tmp_path / "db")
    try:
        report = reconstruct_vehicles(conn, entries)
    finally:
        pass

    # Every state table should have at least one row.
    for table in (
        "vehicles", "vehicle_yearly_mileage", "vehicle_valuations",
        "vehicle_elections", "vehicle_credits", "vehicle_renewals",
        "vehicle_trip_templates",
    ):
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert n >= 1, f"{table} not populated"

    # Attribution column on mileage_trip_meta.
    row = conn.execute(
        "SELECT attributed_entity FROM mileage_trip_meta "
        "WHERE entry_date = '2026-01-10' AND vehicle = 'SuvA' AND miles = 100.0"
    ).fetchone()
    assert row is not None
    assert row["attributed_entity"] == "Ranch"

    # Specific values landed correctly.
    vrow = conn.execute(
        "SELECT year, gvwr_lbs, fuel_type FROM vehicles WHERE slug = 'SuvA'"
    ).fetchone()
    assert vrow["year"] == 2015
    assert vrow["gvwr_lbs"] == 7100
    assert vrow["fuel_type"] == "gasoline"

    # The report carries descriptive notes.
    assert any("vehicles" in note for note in report.notes)
    conn.close()


def test_reconstruct_is_idempotent(tmp_path: Path):
    main = _seed_ledger(tmp_path)
    entries, _errors, _opts = load_file(str(main))
    conn = _fresh_db(tmp_path / "db")
    reconstruct_vehicles(conn, entries)
    counts_before = {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in (
            "vehicles", "vehicle_yearly_mileage", "vehicle_valuations",
            "vehicle_elections", "vehicle_credits", "vehicle_renewals",
            "vehicle_trip_templates",
        )
    }
    # Second pass should not add rows.
    reconstruct_vehicles(conn, entries)
    counts_after = {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in counts_before
    }
    assert counts_before == counts_after
    conn.close()
