# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 1 — trip substantiation (purpose, from_loc, to_loc, notes).

IRS substantiation requires a contemporaneous log that records the
business purpose and destination of each trip. The `mileage_entries`
columns for purpose / from_loc / to_loc / notes already exist; Phase
1's job is to make sure the UI captures them and the recent-trips
partial surfaces them so the user sees what's been substantiated.

The empty-purpose path is NOT rejected here — the Phase 2 data-health
panel flags missing substantiation instead of blocking writes.
"""
from __future__ import annotations

import re
from datetime import date


def _open_entity(ledger_dir):
    """Append an Open for the entity used by these tests so the
    `_known_entities` check in the mileage route accepts the POST."""
    main = ledger_dir / "main.bean"
    text = main.read_text(encoding="utf-8")
    if "Expenses:Acme" not in text:
        main.write_text(
            text
            + "\n2020-01-01 open Expenses:Acme:Mileage USD\n",
            encoding="utf-8",
        )


def _seed_vehicle(app_client):
    """Insert a vehicle row directly so the mileage form accepts the
    display name. Uses the running app's DB connection."""
    conn = app_client.app.state.db
    conn.execute(
        """
        INSERT OR IGNORE INTO vehicles
            (slug, display_name, entity_slug, is_active)
        VALUES ('suvone', 'SuvA', 'Acme', 1)
        """
    )


def test_post_mileage_stores_purpose_from_to_notes(app_client, ledger_dir):
    _open_entity(ledger_dir)
    _seed_vehicle(app_client)

    today = date.today().isoformat()
    resp = app_client.post(
        "/mileage",
        data={
            "entry_date": today,
            "vehicle": "SuvA",
            "entity": "Acme",
            "miles": "12.5",
            "purpose": "Site visit + Hardware Store",
            "from_loc": "Office",
            "to_loc": "Jobsite",
            "notes": "Picked up rebar + stakes",
            "category": "business",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    row = app_client.app.state.db.execute(
        "SELECT purpose, from_loc, to_loc, notes, purpose_category "
        "FROM mileage_entries WHERE entry_date = ? AND vehicle = ?",
        (today, "SuvA"),
    ).fetchone()
    assert row is not None
    assert row["purpose"] == "Site visit + Hardware Store"
    assert row["from_loc"] == "Office"
    assert row["to_loc"] == "Jobsite"
    assert row["notes"] == "Picked up rebar + stakes"
    assert row["purpose_category"] == "business"


def test_post_mileage_accepts_empty_purpose(app_client, ledger_dir):
    """Empty substantiation is allowed at write time. The Phase 2
    data-health panel flags it as 'missing_purpose'."""
    _open_entity(ledger_dir)
    _seed_vehicle(app_client)

    today = date.today().isoformat()
    resp = app_client.post(
        "/mileage",
        data={
            "entry_date": today,
            "vehicle": "SuvA",
            "entity": "Acme",
            "miles": "3.0",
            # purpose, from_loc, to_loc, notes all omitted
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


def test_post_mileage_rejects_unknown_category(app_client, ledger_dir):
    _open_entity(ledger_dir)
    _seed_vehicle(app_client)

    today = date.today().isoformat()
    resp = app_client.post(
        "/mileage",
        data={
            "entry_date": today,
            "vehicle": "SuvA",
            "entity": "Acme",
            "miles": "5",
            "purpose": "test",
            "category": "bogus",
        },
        follow_redirects=False,
    )
    # Unknown category returns the page with an error, not a redirect.
    assert resp.status_code != 303


def test_mileage_page_renders_from_to_purpose(app_client, ledger_dir):
    _open_entity(ledger_dir)
    _seed_vehicle(app_client)

    today = date.today().isoformat()
    app_client.post(
        "/mileage",
        data={
            "entry_date": today,
            "vehicle": "SuvA",
            "entity": "Acme",
            "miles": "12.5",
            "purpose": "Office to jobsite",
            "from_loc": "Office",
            "to_loc": "Jobsite",
            "category": "business",
        },
    )
    resp = app_client.get("/mileage")
    assert resp.status_code == 200
    body = resp.text
    # The partial shows from → to and the purpose.
    assert "Office" in body
    assert "Jobsite" in body
    assert "Office to jobsite" in body
    # Arrow separator between from/to.
    assert re.search(r"Office\s*→\s*Jobsite", body)
