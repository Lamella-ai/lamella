# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Reports matrix overhaul: /reports renders a per-entity year matrix
where every cell is a real link (View / PDF / CSV / Detail CSV); no
form-fill ceremony before clicking through. Site-chromed view at
/reports/schedule-c?entity=&year= renders inside base.html with the
normal nav."""
from __future__ import annotations


def _seed_entity(app_client, slug: str, *, schedule: str = "C") -> None:
    """Insert or upsert an entity with the given tax_schedule. Uses
    ON CONFLICT so a pre-seeded row from conftest gets its
    tax_schedule overwritten, not silently ignored."""
    conn = app_client.app.state.db
    conn.execute(
        "INSERT INTO entities "
        "(slug, display_name, entity_type, tax_schedule, is_active) "
        "VALUES (?, ?, ?, ?, 1) "
        "ON CONFLICT(slug) DO UPDATE SET "
        "  display_name = excluded.display_name, "
        "  entity_type = excluded.entity_type, "
        "  tax_schedule = excluded.tax_schedule, "
        "  is_active = 1",
        (slug, slug + " Co.", "llc", schedule),
    )
    conn.commit()


class TestMatrix:
    def test_renders_with_no_form(self, app_client):
        r = app_client.get("/reports")
        assert r.status_code == 200
        # The old gate-with-a-form pattern is gone — cells are
        # plain links, no need to fill out an entity/year form.
        assert "report-form" not in r.text

    def test_lists_entity_with_schedule(self, app_client):
        _seed_entity(app_client, "Acme", schedule="C")
        r = app_client.get("/reports")
        assert r.status_code == 200
        assert "Acme" in r.text
        # Per-cell links. View uses the entity-first URL
        # (/reports/{slug}/{year}); PDF / CSV remain query-style
        # (separate routes that accept ?entity=&year= for the form-shaped
        # download endpoints).
        body = r.text
        assert "/reports/Acme/" in body
        assert "/reports/schedule-c.pdf?entity=Acme" in body
        assert "/reports/schedule-c.csv?entity=Acme" in body

    def test_unscheduled_entity_appears_in_other_section(self, app_client):
        # Insert an entity with NO tax_schedule — should land in the
        # "Other entities" section, not the Schedule C/F matrix.
        conn = app_client.app.state.db
        conn.execute(
            "INSERT OR IGNORE INTO entities "
            "(slug, display_name, entity_type, is_active) "
            "VALUES (?, ?, ?, 1)",
            ("Personal", "Personal", "personal"),
        )
        conn.commit()
        r = app_client.get("/reports")
        assert r.status_code == 200
        # Entity is listed but no Schedule C link is generated for it.
        assert "Personal" in r.text


class TestSiteChromedView:
    def test_schedule_c_view_renders_with_chrome(self, app_client):
        _seed_entity(app_client, "Acme", schedule="C")
        r = app_client.get("/reports/schedule-c?entity=Acme&year=2024")
        assert r.status_code == 200
        # Site chrome: nav + sidebar are part of base.html. Use a
        # known sidebar string.
        assert "Dashboard" in r.text or "sidebar" in r.text
        # Page-head buttons link out to PDF / preview / CSVs (HTML
        # encodes & as &amp;).
        body = r.text
        assert "/reports/schedule-c.pdf?entity=Acme" in body
        assert "/reports/schedule-c.preview.html?entity=Acme" in body


class TestEntityFirstUrl:
    """``/reports/{entity_slug}/{year}`` looks up the entity's
    ``tax_schedule`` and dispatches to the matching schedule view —
    the URL no longer encodes the schedule because the schedule is a
    property of the entity, not of the URL."""

    def test_entity_first_url_dispatches_schedule_c(self, app_client):
        _seed_entity(app_client, "Acme", schedule="C")
        r = app_client.get("/reports/Acme/2024")
        assert r.status_code == 200
        assert "Schedule C" in r.text
        assert "Acme" in r.text

    def test_entity_first_url_dispatches_schedule_f(self, app_client):
        _seed_entity(app_client, "Farmco", schedule="F")
        r = app_client.get("/reports/Farmco/2024")
        assert r.status_code == 200
        assert "Schedule F" in r.text

    def test_entity_first_url_unknown_entity_404(self, app_client):
        r = app_client.get("/reports/Nonexistent/2024")
        assert r.status_code == 404

    def test_entity_first_url_no_schedule_404(self, app_client):
        # Entity with no tax_schedule — dispatch can't pick a report.
        conn = app_client.app.state.db
        conn.execute(
            "INSERT OR IGNORE INTO entities "
            "(slug, display_name, entity_type, is_active) "
            "VALUES (?, ?, ?, 1)",
            ("Personal", "Personal", "personal"),
        )
        conn.commit()
        r = app_client.get("/reports/Personal/2024")
        assert r.status_code == 404

    def test_legacy_query_url_still_works(self, app_client):
        # Old /reports/schedule-c?entity=&year= URL must keep rendering
        # for bookmark stability.
        _seed_entity(app_client, "Acme", schedule="C")
        r = app_client.get("/reports/schedule-c?entity=Acme&year=2024")
        assert r.status_code == 200
        assert "Schedule C" in r.text


class TestEstimatedTaxQuarterStillWorks:
    def test_current_year_quarter_picker_present(self, app_client):
        _seed_entity(app_client, "Acme", schedule="C")
        r = app_client.get("/reports")
        assert r.status_code == 200
        # The estimated-tax form is the only one keeping a quarter
        # picker; verify it's still wired.
        assert "esttax-quarter" in r.text
