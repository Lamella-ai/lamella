# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 6 — credits + renewals CRUD."""
from __future__ import annotations

import sqlite3
from datetime import date

from lamella.features.vehicles.credits import (
    add_credit, delete_credit, list_credits,
)
from lamella.features.vehicles.renewals import (
    add_renewal, complete_renewal, delete_renewal,
    list_due_soon, list_renewals,
)


def _seed_vehicle(db, slug="SuvA"):
    db.execute(
        "INSERT INTO vehicles (slug, display_name, is_active) "
        "VALUES (?, ?, 1)", (slug, slug),
    )


# -------------- credits ---------------------------------------------


def test_credits_crud_round_trip(db):
    _seed_vehicle(db)
    cid = add_credit(
        db, vehicle_slug="SuvA", tax_year=2026,
        credit_label="Federal EV § 30D", amount="7500", status="claimed",
    )
    rows = list_credits(db, "SuvA")
    assert len(rows) == 1
    assert rows[0].id == cid
    assert rows[0].amount == "7500"
    assert delete_credit(db, cid)
    assert list_credits(db, "SuvA") == []


def test_credits_http_route(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles/SuvA/credits",
        data={
            "tax_year": "2026",
            "credit_label": "Federal EV § 30D",
            "amount": "7500",
            "status": "claimed",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT credit_label, amount, status FROM vehicle_credits"
    ).fetchone()
    db.close()
    assert row["credit_label"] == "Federal EV § 30D"
    assert row["amount"] == "7500"
    assert row["status"] == "claimed"


def test_credits_unknown_vehicle_404(app_client):
    resp = app_client.post(
        "/vehicles/Nothing/credits",
        data={"tax_year": "2026", "credit_label": "foo"},
        follow_redirects=False,
    )
    assert resp.status_code == 404


# -------------- renewals --------------------------------------------


def test_renewals_complete_advances_due_date_by_cadence(db):
    _seed_vehicle(db)
    rid = add_renewal(
        db, vehicle_slug="SuvA", renewal_kind="registration",
        due_date=date(2026, 10, 15), cadence_months=12,
    )
    updated = complete_renewal(db, rid, completed_on=date(2026, 10, 14))
    assert updated is not None
    assert updated.due_date == date(2027, 10, 15)
    assert updated.last_completed == date(2026, 10, 14)
    assert updated.is_active


def test_renewals_one_shot_completes_to_inactive(db):
    _seed_vehicle(db)
    rid = add_renewal(
        db, vehicle_slug="SuvA", renewal_kind="inspection",
        due_date=date(2026, 6, 1), cadence_months=None,
    )
    updated = complete_renewal(db, rid)
    assert updated is not None
    assert not updated.is_active
    assert updated.last_completed is not None


def test_renewals_due_soon_within_14_days(db):
    _seed_vehicle(db)
    today = date(2026, 10, 1)
    add_renewal(
        db, vehicle_slug="SuvA", renewal_kind="registration",
        due_date=date(2026, 10, 10),
    )  # within 14d
    add_renewal(
        db, vehicle_slug="SuvA", renewal_kind="inspection",
        due_date=date(2026, 12, 1),
    )  # outside 14d
    due = list_due_soon(db, within_days=14, today=today)
    kinds = {r.renewal_kind for r in due}
    assert "registration" in kinds
    assert "inspection" not in kinds


def test_renewals_http_round_trip(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles/SuvA/renewals",
        data={
            "renewal_kind": "registration",
            "due_date": "2026-10-15",
            "cadence_months": "12",
            "notes": "DMV online",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT renewal_kind, due_date, cadence_months, is_active "
        "FROM vehicle_renewals"
    ).fetchone()
    db.close()
    assert row["renewal_kind"] == "registration"
    assert row["due_date"] == "2026-10-15"
    assert row["cadence_months"] == 12
    assert row["is_active"] == 1


def test_renewals_delete_route(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    app_client.post(
        "/vehicles/SuvA/renewals",
        data={"renewal_kind": "registration", "due_date": "2026-10-15"},
    )
    db = sqlite3.connect(str(settings.db_path))
    rid = db.execute("SELECT id FROM vehicle_renewals").fetchone()[0]
    db.close()

    app_client.post(f"/vehicles/SuvA/renewals/{rid}/delete")
    db = sqlite3.connect(str(settings.db_path))
    n = db.execute("SELECT COUNT(*) FROM vehicle_renewals").fetchone()[0]
    db.close()
    assert n == 0
