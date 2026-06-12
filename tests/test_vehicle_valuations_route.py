# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import sqlite3


def _seed_vehicle(db, slug="SuvA"):
    db.execute(
        "INSERT INTO vehicles (slug, display_name, is_active) VALUES (?, ?, 1)",
        (slug, slug),
    )


def test_valuation_notes_trims_confirm_modal_html_tail(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    leaked_tail = (
        'Value history ... kbb $5,500.00)? Depreciation calcs will fall '
        'back to the next-most-recent valuation." data-confirm-button="Remove valuation"> Remove'
    )
    app_client.post(
        "/vehicles/SuvA/valuations",
        data={
            "as_of_date": "2026-04-27",
            "value": "5500",
            "source": "kbb",
            "notes": leaked_tail,
        },
    )

    db = sqlite3.connect(str(settings.db_path))
    row = db.execute(
        "SELECT notes FROM vehicle_valuations WHERE vehicle_slug='SuvA'"
    ).fetchone()
    db.close()
    assert row is not None
    assert row[0] == (
        "Value history ... kbb $5,500.00)? Depreciation calcs will fall "
        "back to the next-most-recent valuation.\""
    )
