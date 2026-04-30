# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 7 — directive write ↔ read round-trip.

For each directive type, render to disk, parse back, and confirm the
row shape matches. Exercises the writer + reader contract so the
reconstruct step's behaviour is predictable.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from beancount.loader import load_file

from lamella.features.vehicles.reader import (
    read_mileage_attributions,
    read_vehicle_credits,
    read_vehicle_elections,
    read_vehicle_renewals,
    read_vehicle_trip_templates,
    read_vehicle_valuations,
    read_vehicle_yearly_mileage,
    read_vehicles,
)
from lamella.features.vehicles.writer import (
    append_mileage_attribution,
    append_mileage_attribution_revoked,
    append_vehicle,
    append_vehicle_credit,
    append_vehicle_election,
    append_vehicle_renewal,
    append_vehicle_trip_template,
    append_vehicle_valuation,
    append_vehicle_yearly_mileage,
)


def _prep_ledger(tmp_path: Path) -> tuple[Path, Path]:
    main = tmp_path / "main.bean"
    config = tmp_path / "connector_config.bean"
    main.write_text(
        "option \"title\" \"Test ledger\"\n"
        "2020-01-01 open Assets:Vehicles:SuvA USD\n",
        encoding="utf-8",
    )
    return main, config


def _load(main: Path):
    entries, _errors, _opts = load_file(str(main))
    return entries


def test_vehicle_round_trip(tmp_path: Path):
    main, config = _prep_ledger(tmp_path)
    append_vehicle(
        connector_config=config, main_bean=main,
        slug="SuvA", display_name="2015 SuvA",
        year=2015, make="Chevrolet", model="SuvA",
        vin="1GNSKBE08FR000000", license_plate="ABC 123",
        entity_slug=None,
        purchase_date="2024-01-15", purchase_price="20000",
        purchase_fees="500",
        asset_account="Assets:Vehicles:SuvA",
        gvwr_lbs=7100, placed_in_service="2024-01-15",
        fuel_type="gasoline", notes="Service rig",
        is_active=True, run_check=False,
    )
    entries = _load(main)
    rows = read_vehicles(entries)
    assert len(rows) == 1
    r = rows[0]
    assert r["slug"] == "SuvA"
    assert r["year"] == 2015
    assert r["make"] == "Chevrolet"
    assert r["gvwr_lbs"] == 7100
    assert r["fuel_type"] == "gasoline"
    assert r["is_active"] == 1


def test_yearly_mileage_round_trip(tmp_path: Path):
    main, config = _prep_ledger(tmp_path)
    append_vehicle_yearly_mileage(
        connector_config=config, main_bean=main,
        slug="SuvA", year=2026,
        start_mileage=84000, end_mileage=96000,
        business_miles=9000, commuting_miles=1500, personal_miles=1500,
        run_check=False,
    )
    rows = read_vehicle_yearly_mileage(_load(main))
    assert len(rows) == 1
    assert rows[0]["year"] == 2026
    assert rows[0]["business_miles"] == 9000


def test_valuation_round_trip(tmp_path: Path):
    main, config = _prep_ledger(tmp_path)
    append_vehicle_valuation(
        connector_config=config, main_bean=main,
        slug="SuvA", as_of_date=date(2026, 3, 1),
        value="18500", source="kbb", notes="clean retail",
        run_check=False,
    )
    rows = read_vehicle_valuations(_load(main))
    assert len(rows) == 1
    assert rows[0]["source"] == "kbb"
    assert rows[0]["value"] == "18500"


def test_election_round_trip(tmp_path: Path):
    main, config = _prep_ledger(tmp_path)
    append_vehicle_election(
        connector_config=config, main_bean=main,
        slug="SuvA", tax_year=2026,
        depreciation_method="section-179",
        section_179_amount="12000",
        bonus_depreciation_amount="5000",
        basis_at_placed_in_service="20500",
        business_use_pct_override=0.85,
        listed_property_qualified=1,
        notes="heavy SUV",
        run_check=False,
    )
    rows = read_vehicle_elections(_load(main))
    assert len(rows) == 1
    r = rows[0]
    assert r["depreciation_method"] == "section-179"
    assert r["business_use_pct_override"] == 0.85
    assert r["listed_property_qualified"] == 1


def test_credit_round_trip(tmp_path: Path):
    main, config = _prep_ledger(tmp_path)
    append_vehicle_credit(
        connector_config=config, main_bean=main,
        slug="SuvA", tax_year=2026,
        credit_label="Federal EV § 30D",
        amount="7500", status="claimed", notes="picked up at dealer",
        run_check=False,
    )
    rows = read_vehicle_credits(_load(main))
    assert len(rows) == 1
    assert rows[0]["credit_label"] == "Federal EV § 30D"


def test_renewal_round_trip(tmp_path: Path):
    main, config = _prep_ledger(tmp_path)
    append_vehicle_renewal(
        connector_config=config, main_bean=main,
        slug="SuvA", renewal_kind="registration",
        due_date=date(2026, 10, 15), cadence_months=12,
        notes="DMV online",
        run_check=False,
    )
    rows = read_vehicle_renewals(_load(main))
    assert len(rows) == 1
    assert rows[0]["renewal_kind"] == "registration"
    assert rows[0]["cadence_months"] == 12


def test_trip_template_round_trip(tmp_path: Path):
    main, config = _prep_ledger(tmp_path)
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
    rows = read_vehicle_trip_templates(_load(main))
    assert len(rows) == 1
    t = rows[0]
    assert t["slug"] == "ClientA"
    assert t["is_round_trip"] == 1
    assert t["default_miles"] == 18.5


def test_mileage_attribution_round_trip(tmp_path: Path):
    main, config = _prep_ledger(tmp_path)
    append_mileage_attribution(
        connector_config=config, main_bean=main,
        entry_date=date(2026, 1, 10),
        vehicle="SuvA", miles=100.0, attributed_entity="Ranch",
        run_check=False,
    )
    rows = read_mileage_attributions(_load(main))
    assert len(rows) == 1
    assert rows[0]["attributed_entity"] == "Ranch"


def test_mileage_attribution_revoke_suppresses(tmp_path: Path):
    main, config = _prep_ledger(tmp_path)
    append_mileage_attribution(
        connector_config=config, main_bean=main,
        entry_date=date(2026, 1, 10),
        vehicle="SuvA", miles=100.0, attributed_entity="Ranch",
        run_check=False,
    )
    append_mileage_attribution_revoked(
        connector_config=config, main_bean=main,
        entry_date=date(2026, 1, 10),
        vehicle="SuvA", miles=100.0,
        run_check=False,
    )
    rows = read_mileage_attributions(_load(main))
    assert rows == []
