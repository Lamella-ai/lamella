# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0066 — form validation errors must preserve user input.

Upstream report (Lamella-ai/lamella issue 8): a validation failure on
the mileage quick-log form redirected to a fresh GET, wiping every
field — including long typed-out trip notes. These tests pin the new
contract: on error the form re-renders with the submitted values and
a readable message; only success redirects.
"""
from __future__ import annotations


_NOTE = (
    "Drove to the Example LLC site at 123 Main St, Anytown to deliver "
    "the signed paperwork, then a second stop for materials pickup."
)


def _seed_vehicle(app_client, slug="workvan", name="Work Van"):
    db = app_client.app.state.db
    db.execute(
        "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
        "VALUES (?, ?, ?, ?)",
        (slug, name, "Personal", 1),
    )
    db.commit()
    return name


def test_quick_form_bad_odometer_keeps_input(app_client):
    _seed_vehicle(app_client)
    r = app_client.post(
        "/mileage/quick",
        data={
            "entry_date": "2026-06-01",
            "vehicle_slug": "Work Van",
            "odometer_end": "not-a-number",
            "what": _NOTE,
            "category": "commuting",
        },
    )
    assert r.status_code == 422
    # Error surfaced...
    assert "Invalid odometer" in r.text
    # ...and every submitted value survives the re-render.
    assert _NOTE in r.text
    assert 'value="2026-06-01"' in r.text
    assert 'value="not-a-number"' in r.text
    assert 'value="commuting" checked' in r.text


def test_quick_form_unknown_vehicle_keeps_input(app_client):
    _seed_vehicle(app_client)
    r = app_client.post(
        "/mileage/quick",
        data={
            "entry_date": "2026-06-01",
            "vehicle_slug": "Imaginary Car",
            "odometer_end": "84320",
            "what": _NOTE,
            "category": "business",
        },
    )
    assert r.status_code == 422
    assert "Unknown vehicle" in r.text
    assert _NOTE in r.text
    assert 'value="Imaginary Car"' in r.text
    assert 'value="84320"' in r.text


def test_quick_form_success_still_redirects(app_client):
    _seed_vehicle(app_client)
    # The quick form derives odometer_start from the vehicle's last
    # trip — seed one through the full form first.
    r = app_client.post(
        "/mileage",
        data={
            "entry_date": "2026-05-30",
            "vehicle": "Work Van",
            "entity": "Personal",
            "odometer_start": "84000",
            "odometer_end": "84100",
            "purpose": "Prior trip",
            "category": "business",
        },
        follow_redirects=False,
    )
    assert r.status_code in (200, 303), r.text
    r = app_client.post(
        "/mileage/quick",
        data={
            "entry_date": "2026-06-01",
            "vehicle_slug": "Work Van",
            "odometer_end": "84320",
            "what": "Site visit",
            "category": "business",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/mileage/quick?saved=1"
    row = app_client.app.state.db.execute(
        "SELECT * FROM mileage_entries ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row["odometer_end"] == 84320


def test_full_form_bad_date_keeps_input(app_client):
    """The /mileage New-trip form's vanilla (non-HTMX) error path
    re-renders with the submission instead of an empty form."""
    _seed_vehicle(app_client)
    r = app_client.post(
        "/mileage",
        data={
            "entry_date": "junk",
            "vehicle": "Work Van",
            "entity": "Personal",
            "odometer_end": "84320",
            "purpose": _NOTE,
            "category": "business",
        },
    )
    assert r.status_code == 200
    assert "invalid date" in r.text
    assert _NOTE in r.text
    assert 'value="84320"' in r.text


# ─── ADR-0066 conversions: edit form, rates form, opening balance ──


def test_edit_form_bad_date_keeps_edits(app_client):
    """POST /mileage/{id} with an invalid date re-renders the edit
    page with the user's edits — not the stored row, not a redirect."""
    _seed_vehicle(app_client)
    r = app_client.post(
        "/mileage",
        data={"entry_date": "2026-05-30", "vehicle": "Work Van",
              "entity": "Personal", "odometer_start": "84000",
              "odometer_end": "84100", "purpose": "Original purpose",
              "category": "business"},
        follow_redirects=False,
    )
    assert r.status_code in (200, 303)
    row = app_client.app.state.db.execute(
        "SELECT id FROM mileage_entries ORDER BY id DESC LIMIT 1"
    ).fetchone()
    r = app_client.post(
        f"/mileage/{row['id']}",
        data={"entry_date": "junk", "vehicle": "Work Van",
              "entity": "Personal", "miles": "100",
              "purpose": "Heavily edited purpose text", "notes": _NOTE},
    )
    assert r.status_code == 422
    assert "Invalid date" in r.text
    # The user's edits, not the stored row values.
    assert "Heavily edited purpose text" in r.text
    assert _NOTE in r.text
    assert 'value="junk"' in r.text


def test_rates_form_bad_input_keeps_values(app_client):
    r = app_client.post(
        "/settings/mileage-rates",
        data={"effective_from": "2026-01-01", "rate_per_mile": "abc",
              "notes": "IRS Rev. Proc. note text"},
    )
    assert r.status_code == 422
    assert "Invalid input" in r.text
    assert 'value="abc"' in r.text
    assert "IRS Rev. Proc. note text" in r.text


def test_opening_balance_bad_amount_keeps_values(app_client):
    db = app_client.app.state.db
    db.execute(
        "INSERT OR IGNORE INTO accounts_meta (account_path) VALUES (?)",
        ("Assets:Personal:Checking",),
    )
    db.commit()
    r = app_client.post(
        "/accounts/Assets:Personal:Checking/opening-balance",
        data={"as_of_date": "2026-01-01", "amount": "not-money",
              "currency": "USD", "source_note": "Jan statement note"},
    )
    assert r.status_code == 422
    assert "Invalid amount" in r.text
    assert 'value="not-money"' in r.text
    assert "Jan statement note" in r.text
