# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Regression test for ``MileageService.link_unlinked_entries`` —
the backfill pass that maps ``mileage_entries.vehicle`` (free-form
display from CSV) to ``vehicle_slug`` via the ``vehicles`` table.

Bug: the backfill matched purely on the normalized vehicle name and
ignored entity attribution. If two entities had vehicles with the
same display name, OR an entity had a vehicle whose display name
collided with another entity's CSV row text, the wrong slug was
written across the entity boundary."""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.mileage.service import MileageService


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _seed_entity(conn, slug):
    conn.execute(
        "INSERT OR IGNORE INTO entities (slug) VALUES (?)", (slug,),
    )


def _seed_vehicle(conn, *, slug, display_name, entity_slug):
    if entity_slug is not None:
        _seed_entity(conn, entity_slug)
    conn.execute(
        "INSERT INTO vehicles (slug, display_name, entity_slug) "
        "VALUES (?, ?, ?)",
        (slug, display_name, entity_slug),
    )


def _seed_mileage(conn, *, vehicle, entity, csv_row):
    conn.execute(
        """
        INSERT INTO mileage_entries
            (entry_date, vehicle, miles, entity, csv_row_index, csv_mtime)
        VALUES ('2026-04-01', ?, 10.0, ?, ?, '2026-04-01 00:00:00')
        """,
        (vehicle, entity, csv_row),
    )


def test_link_does_not_cross_entity_on_shared_display_name(conn):
    # Two entities both have a vehicle whose CSV display name is the
    # same human string. Slugs are globally unique (PK) — Acme owns
    # AcmeSUV, Personal owns PersonalSUV — but the user typed
    # "Work SUV" in both CSVs.
    _seed_vehicle(
        conn, slug="AcmeSUV", display_name="Work SUV",
        entity_slug="Acme",
    )
    _seed_vehicle(
        conn, slug="PersonalSUV", display_name="Work SUV",
        entity_slug="Personal",
    )
    _seed_mileage(conn, vehicle="Work SUV", entity="Acme", csv_row=1)
    _seed_mileage(conn, vehicle="Work SUV", entity="Personal", csv_row=2)

    svc = MileageService(conn=conn)
    svc.link_unlinked_entries()

    rows = list(conn.execute(
        "SELECT entity, vehicle_slug FROM mileage_entries ORDER BY entity"
    ))
    pairs = {(r["entity"], r["vehicle_slug"]) for r in rows}
    # Each entity's row must point to its OWN entity's vehicle slug.
    assert ("Acme", "AcmeSUV") in pairs
    assert ("Personal", "PersonalSUV") in pairs
    # And specifically NOT the cross-entity slug.
    assert ("Acme", "PersonalSUV") not in pairs
    assert ("Personal", "AcmeSUV") not in pairs


def test_link_does_not_attribute_orphan_entity_to_anothers_vehicle(conn):
    # Acme owns the only vehicle named "Work SUV". A Personal CSV
    # row also names its vehicle "Work SUV" but Personal hasn't
    # registered any vehicle yet. The backfill must not silently
    # attribute Personal's mileage to Acme's vehicle.
    _seed_vehicle(
        conn, slug="AcmeSUV", display_name="Work SUV",
        entity_slug="Acme",
    )
    _seed_mileage(conn, vehicle="Work SUV", entity="Personal", csv_row=1)

    svc = MileageService(conn=conn)
    svc.link_unlinked_entries()

    row = conn.execute(
        "SELECT vehicle_slug FROM mileage_entries WHERE entity = 'Personal'"
    ).fetchone()
    assert row["vehicle_slug"] in (None, "")


def test_link_still_works_in_single_entity_case(conn):
    # The backfill must still backfill the obvious case: one vehicle
    # per entity, name collides with the CSV string.
    _seed_vehicle(
        conn, slug="AcmeSUV", display_name="Work SUV",
        entity_slug="Acme",
    )
    _seed_mileage(conn, vehicle="Work SUV", entity="Acme", csv_row=1)

    svc = MileageService(conn=conn)
    linked = svc.link_unlinked_entries()
    assert linked == 1
    row = conn.execute(
        "SELECT vehicle_slug FROM mileage_entries WHERE entity = 'Acme'"
    ).fetchone()
    assert row["vehicle_slug"] == "AcmeSUV"
