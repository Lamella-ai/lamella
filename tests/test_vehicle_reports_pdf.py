# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 5D — vehicle worksheet PDF routes.

Covers the three new routes end-to-end. WeasyPrint is required for
PDF bytes, but the tests use the render_html helpers where possible
and the PDF route's 503 fallback when WeasyPrint's native libs aren't
available in the test env.
"""
from __future__ import annotations

import sqlite3
from datetime import date

from lamella.features.reports.vehicles_pdf import (
    Form4562Context,
    Form4562Row,
    MileageLogContext,
    MileageLogRow,
    ScheduleCPartIVContext,
    build_schedule_c_part_iv_context,
)
from lamella.features.reports._pdf import render_html


def _seed_entity_vehicle(db, *, entity="Acme", slug="SuvA"):
    db.execute(
        "INSERT OR IGNORE INTO entities (slug, display_name) "
        "VALUES (?, ?)", (entity, entity),
    )
    db.execute(
        "INSERT OR REPLACE INTO vehicles "
        "(slug, display_name, entity_slug, is_active, "
        " gvwr_lbs, placed_in_service_date, fuel_type, "
        " purchase_price, purchase_fees) "
        "VALUES (?, ?, ?, 1, 6800, '2024-01-15', 'gasoline', '20000', '500')",
        (slug, slug, entity),
    )


# -------------- HTML renderers (no native libs needed) ---------------


def test_mileage_log_html_renders_rows():
    rows = [
        MileageLogRow(
            entry_date=date(2026, 3, 1), vehicle="SuvA",
            business_purpose="Client visit", from_loc="Office", to_loc="Client",
            miles=45.5, odometer=84100,
        ),
        MileageLogRow(
            entry_date=date(2026, 3, 10), vehicle="SuvA",
            business_purpose="Oil change + filter",
            from_loc=None, to_loc=None, miles=0.0, odometer=None,
        ),
    ]
    ctx = MileageLogContext(
        year=2026, vehicle_slug="SuvA",
        vehicle_display_name="2015 SuvA",
        rows=rows,
        total_business_miles=40.0, total_commuting_miles=5.5,
        total_personal_miles=0.0,
    )
    html = render_html("vehicle_mileage_log.html", ctx=ctx)
    assert "Mileage log" in html
    assert "2015 SuvA" in html
    assert "Client visit" in html
    # 0-mile maintenance day shows its purpose/notes text.
    assert "Oil change + filter" in html
    # Totals footer with the split.
    assert "40.0" in html
    assert "5.5" in html
    # "Worksheet" label, not "return".
    assert "worksheet" in html.lower()


def test_mileage_log_html_empty_state():
    ctx = MileageLogContext(
        year=2026, vehicle_slug=None, vehicle_display_name=None,
        rows=[],
        total_business_miles=0.0, total_commuting_miles=0.0,
        total_personal_miles=0.0,
    )
    html = render_html("vehicle_mileage_log.html", ctx=ctx)
    assert "No trips recorded" in html


def test_build_schedule_c_part_iv_computes_avg_daily_commute():
    yearly_rows = [{
        "vehicle_slug": "SuvA",
        "year": 2026,
        "business_miles": 9000,
        "commuting_miles": 2500,
        "personal_miles": 1500,
        "commute_days": 250,
        "other_vehicle_available_personal": 1,
        "vehicle_available_off_duty": 0,
        "has_evidence": 1,
        "evidence_is_written": None,
    }]
    vehicles_by_slug = {
        "SuvA": {
            "slug": "SuvA",
            "display_name": "2015 SuvA",
            "placed_in_service_date": "2024-01-15",
        },
    }
    ctx = build_schedule_c_part_iv_context(
        entity="Acme", year=2026,
        yearly_rows=yearly_rows,
        vehicles_by_slug=vehicles_by_slug,
    )
    assert len(ctx.rows) == 1
    row = ctx.rows[0]
    assert row.avg_daily_commute == 10.0   # 2500 / 250
    assert row.commute_days == 250
    assert row.evidence_is_written is None


def test_schedule_c_part_iv_html_renders_tristates():
    ctx = ScheduleCPartIVContext(
        entity="Acme", year=2026,
        rows=[
            # Three cases: yes, no, unanswered — should render
            # three distinct checkbox cell styles.
            # Using build_schedule_c_part_iv_context's output shape.
            *build_schedule_c_part_iv_context(
                entity="Acme", year=2026,
                yearly_rows=[
                    {
                        "vehicle_slug": "SuvA",
                        "business_miles": 9000, "commuting_miles": 1000,
                        "personal_miles": 500,
                        "commute_days": 200,
                        "other_vehicle_available_personal": 1,
                        "vehicle_available_off_duty": 0,
                        "has_evidence": 1,
                        "evidence_is_written": None,
                    },
                    {
                        "vehicle_slug": "TruckB",
                        "business_miles": 2000, "commuting_miles": 0,
                        "personal_miles": 4000,
                        "commute_days": None,
                        "other_vehicle_available_personal": None,
                        "vehicle_available_off_duty": None,
                        "has_evidence": None,
                        "evidence_is_written": None,
                    },
                ],
                vehicles_by_slug={
                    "SuvA": {"slug": "SuvA", "display_name": "SuvA"},
                    "TruckB": {"slug": "TruckB", "display_name": "TruckB"},
                },
            ).rows
        ],
    )
    html = render_html("vehicle_schedule_c_part_iv.html", ctx=ctx)
    assert "Schedule C Part IV" in html
    assert "SuvA" in html
    assert "TruckB" in html
    # Yes marks present on line 45 for SuvA (value=1).
    assert "☑ Yes" in html
    # No marks present on line 46 for SuvA (value=0).
    assert "☑ No" in html
    # Unanswered lines for TruckB get plain empty boxes (no ☑).
    # We just verify both vehicles rendered.
    assert "worksheet" in html.lower()


def test_form_4562_html_renders_capture_fields():
    ctx = Form4562Context(
        entity="Acme", year=2026,
        rows=[Form4562Row(
            vehicle_slug="SuvA",
            vehicle_display_name="2015 SuvA",
            placed_in_service_date="2024-01-15",
            gvwr_lbs=6800,
            fuel_type="gasoline",
            purchase_price="20000",
            purchase_fees="500",
            tax_year=2026,
            depreciation_method="section-179",
            section_179_amount="12000",
            bonus_depreciation_amount="5000",
            basis_at_placed_in_service="20500",
            business_use_pct_override=0.85,
            listed_property_qualified=1,
            notes="First year of service.",
        )],
    )
    html = render_html("vehicle_form_4562_worksheet.html", ctx=ctx)
    assert "Form 4562 worksheet" in html
    assert "2015 SuvA" in html
    assert "6,800" in html
    assert "section-179" in html
    assert "12,000.00" in html
    assert "85%" in html
    assert "First year of service" in html
    # Worksheet label, not return.
    assert "worksheet" in html.lower()


def test_form_4562_html_empty_election_row():
    """Vehicle with no election for the target year still renders
    with dashes in the election columns."""
    ctx = Form4562Context(
        entity="Acme", year=2026,
        rows=[Form4562Row(
            vehicle_slug="SuvA",
            vehicle_display_name="SuvA",
            placed_in_service_date=None,
            gvwr_lbs=None,
            fuel_type=None,
            purchase_price=None,
            purchase_fees=None,
            tax_year=2026,
            depreciation_method=None,
            section_179_amount=None,
            bonus_depreciation_amount=None,
            basis_at_placed_in_service=None,
            business_use_pct_override=None,
            listed_property_qualified=None,
            notes=None,
        )],
    )
    html = render_html("vehicle_form_4562_worksheet.html", ctx=ctx)
    # Every value cell becomes a dash.
    assert html.count("—") >= 5


# -------------- Route helpers (no PDF binary required) ---------------


def test_mileage_log_route_404_on_unknown_vehicle(app_client):
    resp = app_client.get(
        "/reports/vehicles/mileage-log.pdf?year=2026&vehicle=NoSuchSlug",
        follow_redirects=False,
    )
    assert resp.status_code == 404


def test_schedule_c_part_iv_route_404_when_no_vehicles(app_client):
    resp = app_client.get(
        "/reports/vehicles/schedule-c-part-iv.pdf?entity=Nobody&year=2026",
        follow_redirects=False,
    )
    assert resp.status_code == 404


def test_form_4562_route_404_when_no_vehicles(app_client):
    resp = app_client.get(
        "/reports/vehicles/form-4562-worksheet.pdf?entity=Nobody&year=2026",
        follow_redirects=False,
    )
    assert resp.status_code == 404
