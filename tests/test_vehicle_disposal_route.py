# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 4 — disposal flow end-to-end: form → preview → commit → revoke.

Covers request/response plumbing, Open-directive scaffolding, the
idempotency contract (repeat commit with same lamella-disposal-id is a
no-op), and the revoke round-trip.
"""
from __future__ import annotations

import sqlite3

import pytest


def _ledger_prep(ledger_dir, *, entity: str | None = None):
    """Seed main.bean with the accounts the disposal flow needs to
    reference. The route scaffolds missing ones through
    AccountsWriter; pre-opening them in main.bean just keeps the
    tests self-contained and independent of the writer."""
    main = ledger_dir / "main.bean"
    text = main.read_text(encoding="utf-8")
    additions: list[str] = []
    wants = [
        "Assets:Vehicles:SuvA",
        "Assets:Personal:Checking",
        "Income:Personal:CapitalGains:VehicleSale",
        "Expenses:Personal:CapitalLoss:VehicleSale",
    ]
    if entity:
        wants.extend([
            f"Assets:{entity}:Vehicles:SuvA",
            f"Assets:{entity}:Checking",
            f"Income:{entity}:CapitalGains:VehicleSale",
        ])
    for acct in wants:
        if acct not in text:
            additions.append(f"2020-01-01 open {acct} USD")
    if additions:
        main.write_text(
            text + "\n" + "\n".join(additions) + "\n", encoding="utf-8",
        )


def _seed_vehicle(db, *, slug="SuvA", display_name="2015 SuvA"):
    db.execute(
        "INSERT INTO vehicles "
        "(slug, display_name, is_active, purchase_price, purchase_fees) "
        "VALUES (?, ?, 1, '20000', '500')",
        (slug, display_name),
    )


def test_dispose_form_renders(app_client, settings, ledger_dir):
    _ledger_prep(ledger_dir)
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/SuvA/dispose")
    assert resp.status_code == 200
    body = resp.text
    assert "Dispose of" in body
    assert 'name="disposal_date"' in body
    assert 'name="proceeds_amount"' in body
    assert 'name="proceeds_account"' in body
    assert 'name="gain_loss_account"' in body
    # Default proceeds account surfaced as the input value.
    assert "Assets:Personal:Checking" in body


def test_dispose_preview_shows_balanced_block(app_client, settings, ledger_dir):
    _ledger_prep(ledger_dir)
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles/SuvA/dispose",
        data={
            "disposal_date": "2026-04-01",
            "disposal_type": "sale",
            "proceeds_amount": "15000",
            "adjusted_basis": "20000",
            "accumulated_depreciation": "8000",
            "proceeds_account": "Assets:Personal:Checking",
            "gain_loss_account": "Income:Personal:CapitalGains:VehicleSale",
            "buyer_or_party": "J. Smith",
        },
    )
    assert resp.status_code == 200
    body = resp.text
    # Preview contains the rendered block — three postings, with USD.
    assert "Assets:Vehicles:SuvA" in body
    assert "15000.00 USD" in body
    assert "-12000.00 USD" in body         # basis (20000) - dep (8000) = 12000 out
    assert "#lamella-vehicle-disposal" in body
    # Gain: 15000 - (20000 - 8000) = 3000.
    assert "3000" in body


def test_dispose_commit_writes_transaction_and_updates_vehicle(
    app_client, settings, ledger_dir,
):
    _ledger_prep(ledger_dir)
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    # Drive commit directly — preview renders the hidden disposal_id
    # for the user; here we construct the POST body as the preview
    # page's hidden form would.
    from lamella.features.vehicles.disposal_writer import new_disposal_id
    disposal_id = new_disposal_id()

    resp = app_client.post(
        "/vehicles/SuvA/dispose/commit",
        data={
            "disposal_id": disposal_id,
            "disposal_date": "2026-04-01",
            "disposal_type": "sale",
            "proceeds_amount": "15000",
            "adjusted_basis": "20000",
            "accumulated_depreciation": "8000",
            "proceeds_account": "Assets:Personal:Checking",
            "gain_loss_account": "Income:Personal:CapitalGains:VehicleSale",
            "buyer_or_party": "J. Smith",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "saved=disposal_committed" in resp.headers["location"]

    # Ledger file got the block + tag.
    overrides = (settings.ledger_dir / "connector_overrides.bean").read_text(
        encoding="utf-8",
    )
    assert "#lamella-vehicle-disposal" in overrides
    assert f'lamella-disposal-id: "{disposal_id}"' in overrides

    # Cache row persisted.
    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT * FROM vehicle_disposals WHERE disposal_id = ?",
        (disposal_id,),
    ).fetchone()
    assert row is not None
    assert row["disposal_type"] == "sale"
    assert row["proceeds_amount"] == "15000"
    assert row["revokes_disposal_id"] is None
    assert row["revoked_by_disposal_id"] is None

    # Vehicle marked sold with disposal_txn_hash pointing at the id.
    v = db.execute(
        "SELECT sale_date, sale_price, disposal_txn_hash, is_active "
        "FROM vehicles WHERE slug = 'SuvA'"
    ).fetchone()
    db.close()
    assert v["sale_date"] == "2026-04-01"
    assert v["sale_price"] == "15000"
    assert v["disposal_txn_hash"] == disposal_id
    assert v["is_active"] == 0


def test_dispose_commit_is_idempotent(app_client, settings, ledger_dir):
    """Repeat commit with the same lamella-disposal-id redirects without
    writing a second ledger transaction."""
    _ledger_prep(ledger_dir)
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    from lamella.features.vehicles.disposal_writer import new_disposal_id
    disposal_id = new_disposal_id()
    data = {
        "disposal_id": disposal_id,
        "disposal_date": "2026-04-01",
        "disposal_type": "sale",
        "proceeds_amount": "15000",
        "adjusted_basis": "20000",
        "accumulated_depreciation": "8000",
        "proceeds_account": "Assets:Personal:Checking",
        "gain_loss_account": "Income:Personal:CapitalGains:VehicleSale",
    }
    app_client.post("/vehicles/SuvA/dispose/commit", data=data)
    first_ov = (settings.ledger_dir / "connector_overrides.bean").read_text(
        encoding="utf-8",
    )
    app_client.post("/vehicles/SuvA/dispose/commit", data=data)
    second_ov = (settings.ledger_dir / "connector_overrides.bean").read_text(
        encoding="utf-8",
    )
    assert first_ov == second_ov


def test_dispose_commit_requires_disposal_id(app_client, settings, ledger_dir):
    _ledger_prep(ledger_dir)
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles/SuvA/dispose/commit",
        data={
            "disposal_date": "2026-04-01",
            "disposal_type": "sale",
            "proceeds_amount": "15000",
            "proceeds_account": "Assets:Personal:Checking",
            "gain_loss_account": "Income:Personal:CapitalGains:VehicleSale",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 400


def test_dispose_revoke_writes_offset_and_clears_sale(
    app_client, settings, ledger_dir,
):
    _ledger_prep(ledger_dir)
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    from lamella.features.vehicles.disposal_writer import new_disposal_id
    disposal_id = new_disposal_id()
    app_client.post(
        "/vehicles/SuvA/dispose/commit",
        data={
            "disposal_id": disposal_id,
            "disposal_date": "2026-04-01",
            "disposal_type": "sale",
            "proceeds_amount": "15000",
            "adjusted_basis": "20000",
            "accumulated_depreciation": "8000",
            "proceeds_account": "Assets:Personal:Checking",
            "gain_loss_account": "Income:Personal:CapitalGains:VehicleSale",
        },
    )
    resp = app_client.post(
        f"/vehicles/SuvA/dispose/{disposal_id}/revoke",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "saved=disposal_revoked" in resp.headers["location"]

    # Overrides file carries BOTH the original and the revoke, with
    # lamella-disposal-revokes metadata linking them.
    overrides = (settings.ledger_dir / "connector_overrides.bean").read_text(
        encoding="utf-8",
    )
    assert overrides.count("#lamella-vehicle-disposal") >= 2
    assert f'lamella-disposal-revokes: "{disposal_id}"' in overrides

    # Cache: the original gets revoked_by_disposal_id; the revoke
    # row has revokes_disposal_id pointing at the original.
    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    orig = db.execute(
        "SELECT revoked_by_disposal_id FROM vehicle_disposals "
        "WHERE disposal_id = ?", (disposal_id,),
    ).fetchone()
    rev = db.execute(
        "SELECT disposal_id, revokes_disposal_id FROM vehicle_disposals "
        "WHERE revokes_disposal_id = ?", (disposal_id,),
    ).fetchone()
    v = db.execute(
        "SELECT sale_date, sale_price, disposal_txn_hash, is_active "
        "FROM vehicles WHERE slug = 'SuvA'"
    ).fetchone()
    db.close()
    assert orig["revoked_by_disposal_id"] == rev["disposal_id"]
    assert rev["revokes_disposal_id"] == disposal_id
    # Vehicle restored to active — revoke nets out the disposal.
    assert v["sale_date"] is None
    assert v["sale_price"] is None
    assert v["disposal_txn_hash"] is None
    assert v["is_active"] == 1


def test_dispose_revoke_rejects_already_revoked(
    app_client, settings, ledger_dir,
):
    _ledger_prep(ledger_dir)
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    from lamella.features.vehicles.disposal_writer import new_disposal_id
    disposal_id = new_disposal_id()
    data = {
        "disposal_id": disposal_id,
        "disposal_date": "2026-04-01",
        "disposal_type": "sale",
        "proceeds_amount": "15000",
        "adjusted_basis": "20000",
        "accumulated_depreciation": "8000",
        "proceeds_account": "Assets:Personal:Checking",
        "gain_loss_account": "Income:Personal:CapitalGains:VehicleSale",
    }
    app_client.post("/vehicles/SuvA/dispose/commit", data=data)
    app_client.post(f"/vehicles/SuvA/dispose/{disposal_id}/revoke")
    resp = app_client.post(
        f"/vehicles/SuvA/dispose/{disposal_id}/revoke",
        follow_redirects=False,
    )
    assert resp.status_code == 400


def test_dispose_header_action_visible_when_vehicle_active(
    app_client, settings, ledger_dir,
):
    _ledger_prep(ledger_dir)
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/SuvA")
    assert "Record disposal" in resp.text


@pytest.mark.xfail(
    reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
    strict=False,
)
def test_disposals_section_renders_after_commit(
    app_client, settings, ledger_dir,
):
    _ledger_prep(ledger_dir)
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    from lamella.features.vehicles.disposal_writer import new_disposal_id
    disposal_id = new_disposal_id()
    app_client.post(
        "/vehicles/SuvA/dispose/commit",
        data={
            "disposal_id": disposal_id,
            "disposal_date": "2026-04-01",
            "disposal_type": "sale",
            "proceeds_amount": "15000",
            "adjusted_basis": "20000",
            "accumulated_depreciation": "8000",
            "proceeds_account": "Assets:Personal:Checking",
            "gain_loss_account": "Income:Personal:CapitalGains:VehicleSale",
        },
    )
    resp = app_client.get("/vehicles/SuvA")
    body = resp.text
    assert 'id="disposals"' in body
    assert "Disposals" in body
    assert "sale" in body
    # "Record disposal" action is suppressed once the vehicle has a
    # live disposal (is_active flipped to 0 + disposal_txn_hash set).
    assert "Record disposal" not in body
