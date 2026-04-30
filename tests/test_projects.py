# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for Projects MVP: service, routes, classify integration."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.projects.service import (
    ProjectService,
    active_projects_for_txn,
    is_valid_project_slug,
)


@pytest.fixture
def db(tmp_path: Path):
    conn = connect(tmp_path / "test.sqlite")
    migrate(conn)
    yield conn
    conn.close()


class TestProjectService:
    def test_upsert_and_get(self, db):
        svc = ProjectService(db)
        svc.upsert(
            slug="retaining-wall",
            display_name="Retaining wall — MainResidence",
            description="Build a retaining wall for business-vehicle parking.",
            entity_slug="Personal",
            property_slug="main-residence",
            project_type="home_improvement",
            start_date="2026-05-01",
            budget_amount="6500.00",
            expected_merchants=["Hardware Store", "Home Center", "Supplier B"],
        )
        p = svc.get("retaining-wall")
        assert p is not None
        assert p.display_name == "Retaining wall — MainResidence"
        assert p.entity_slug == "Personal"
        assert p.property_slug == "main-residence"
        assert p.project_type == "home_improvement"
        assert p.start_date == date(2026, 5, 1)
        assert p.end_date is None
        assert p.budget_amount == Decimal("6500.00")
        assert "Hardware Store" in p.expected_merchants
        assert p.is_active is True

    def test_active_for_matches_merchant_in_window(self, db):
        svc = ProjectService(db)
        svc.upsert(
            slug="fence", display_name="Fence",
            description="Farm fence", entity_slug="Personal",
            start_date="2026-05-01", end_date="2026-09-01",
            expected_merchants=["Hardware Store", "Tractor Supply"],
        )
        matches = svc.active_for(
            txn_date=date(2026, 6, 15),
            merchant_text="Hardware Store #4521 — Lumber and concrete",
        )
        assert len(matches) == 1
        assert matches[0].slug == "fence"

    def test_active_for_rejects_out_of_window(self, db):
        svc = ProjectService(db)
        svc.upsert(
            slug="fence", display_name="Fence",
            start_date="2026-05-01", end_date="2026-09-01",
            expected_merchants=["Hardware Store"],
        )
        # After the end date.
        assert svc.active_for(
            txn_date=date(2026, 10, 15),
            merchant_text="Hardware Store",
        ) == []
        # Before the start date.
        assert svc.active_for(
            txn_date=date(2026, 4, 15),
            merchant_text="Hardware Store",
        ) == []

    def test_active_for_rejects_merchant_mismatch(self, db):
        svc = ProjectService(db)
        svc.upsert(
            slug="fence", display_name="Fence",
            start_date="2026-05-01",
            expected_merchants=["Hardware Store", "Home Center"],
        )
        assert svc.active_for(
            txn_date=date(2026, 6, 15),
            merchant_text="Grocery Store #123",
        ) == []

    def test_active_for_empty_merchant_returns_nothing(self, db):
        """No merchant text → can't match, should NOT return all
        active projects on every classify."""
        svc = ProjectService(db)
        svc.upsert(
            slug="p", display_name="P", start_date="2026-05-01",
            expected_merchants=["Hardware Store"],
        )
        assert svc.active_for(
            txn_date=date(2026, 6, 15), merchant_text="",
        ) == []

    def test_overlapping_projects_both_returned(self, db):
        """Fence + shelving both use Hardware Store, windows overlap.
        active_for returns both; classify prompt tells the AI to
        reduce confidence when multiple claim the same txn."""
        svc = ProjectService(db)
        svc.upsert(
            slug="fence", display_name="Fence",
            start_date="2026-05-01", end_date="2026-12-01",
            expected_merchants=["Hardware Store"],
        )
        svc.upsert(
            slug="shelving", display_name="Business shelving",
            start_date="2026-06-01",
            expected_merchants=["Hardware Store", "Staples"],
        )
        matches = svc.active_for(
            txn_date=date(2026, 7, 15),
            merchant_text="Hardware Store #4521",
        )
        slugs = {m.slug for m in matches}
        assert slugs == {"fence", "shelving"}

    def test_close_sets_is_active_false(self, db):
        svc = ProjectService(db)
        svc.upsert(
            slug="p", display_name="P", start_date="2026-05-01",
            expected_merchants=["Hardware Store"],
        )
        svc.close("p", closeout={"actual_total": 4200.00})
        p = svc.get("p")
        assert p.is_active is False
        assert p.closed_at is not None
        assert p.closeout_json == {"actual_total": 4200.0}

    def test_continuation_chain(self, db):
        """Paused-then-restarted fence: two projects linked via
        previous_project_slug. chain() returns both, oldest
        first, regardless of which slug is queried."""
        svc = ProjectService(db)
        svc.upsert(
            slug="fence-2025", display_name="Fence — summer 2025",
            start_date="2025-07-01", end_date="2025-09-15",
            expected_merchants=["Hardware Store", "Tractor Supply"],
            is_active=False,
        )
        svc.upsert(
            slug="fence-2025-resume",
            display_name="Fence — winter 2025",
            start_date="2025-11-01",
            expected_merchants=["Hardware Store"],
            previous_project_slug="fence-2025",
        )
        chain_from_tail = svc.chain("fence-2025-resume")
        assert [p.slug for p in chain_from_tail] == [
            "fence-2025", "fence-2025-resume",
        ]
        chain_from_head = svc.chain("fence-2025")
        assert [p.slug for p in chain_from_head] == [
            "fence-2025", "fence-2025-resume",
        ]

    def test_chain_solo(self, db):
        svc = ProjectService(db)
        svc.upsert(
            slug="solo", display_name="Solo", start_date="2026-05-01",
            expected_merchants=["X"],
        )
        chain = svc.chain("solo")
        assert [p.slug for p in chain] == ["solo"]


class TestSlugValidation:
    def test_good_slugs(self):
        assert is_valid_project_slug("fence")
        assert is_valid_project_slug("retaining-wall")
        assert is_valid_project_slug("home-office-2026")

    def test_bad_slugs(self):
        assert not is_valid_project_slug("")
        assert not is_valid_project_slug("Bad Spaces")
        assert not is_valid_project_slug("-leading-dash")
        assert not is_valid_project_slug("has/slash")

    def test_caps_normalized(self):
        """Uppercase input is friendly — validator accepts it,
        routes lowercase before inserting. `CAPS_BAD` becomes
        `caps_bad` which is a legal slug."""
        assert is_valid_project_slug("CAPS_BAD")


def test_active_projects_helper_handles_null_conn():
    """Classify integration helper must tolerate conn=None."""
    assert active_projects_for_txn(
        None, txn_date=date(2026, 6, 15), merchant_text="Hardware Store",
    ) == []


def test_projects_page_renders(app_client):
    resp = app_client.get("/projects")
    assert resp.status_code == 200
    assert "Projects" in resp.text


def test_project_create_and_detail_round_trip(app_client):
    resp = app_client.post(
        "/projects",
        data={
            "slug": "test-project",
            "display_name": "Test project",
            "description": "Description here.",
            "entity_slug": "",
            "property_slug": "",
            "project_type": "home_improvement",
            "start_date": "2026-05-01",
            "end_date": "",
            "budget_amount": "1000",
            "expected_merchants": "Hardware Store, Home Center",
            "notes": "",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/projects/test-project"

    detail = app_client.get("/projects/test-project")
    assert detail.status_code == 200
    assert "Test project" in detail.text
    assert "Hardware Store" in detail.text
    assert "1000" in detail.text
