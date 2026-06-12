# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Vehicle worksheet PDFs — IRS mileage log, Schedule C Part IV,
Form 4562.

Every PDF is labeled "worksheet — transcribe to your return", never
"return". We capture + format the user's entries; we do NOT compute
authoritative depreciation or determine §179 eligibility.

Three renderers:
  - render_mileage_log_pdf           — IRS substantiation log
  - render_schedule_c_part_iv_pdf    — Part IV grid, one row / vehicle
  - render_form_4562_worksheet_pdf   — capture block per vehicle
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date as date_t
from decimal import Decimal
from typing import Any

from lamella.features.reports._pdf import render_html, render_pdf


@dataclass(frozen=True)
class MileageLogRow:
    entry_date: date_t
    vehicle: str
    business_purpose: str
    from_loc: str | None
    to_loc: str | None
    miles: float
    odometer: int | None


@dataclass(frozen=True)
class MileageLogContext:
    year: int
    vehicle_slug: str | None           # None = fleet-wide
    vehicle_display_name: str | None
    rows: list[MileageLogRow]
    total_business_miles: float
    total_commuting_miles: float
    total_personal_miles: float


def render_mileage_log_pdf(ctx: MileageLogContext) -> bytes:
    html = render_html("vehicle_mileage_log.html", ctx=ctx)
    return render_pdf(html)


@dataclass(frozen=True)
class ScheduleCPartIVRow:
    vehicle_slug: str
    vehicle_display_name: str
    # Placed in service (line 43 / Part IV date).
    placed_in_service_date: str | None
    # Line 44 buckets (a / b / c).
    business_miles: int
    commuting_miles: int
    personal_miles: int
    # Line 44b divisor.
    commute_days: int | None
    # Derived line 44b value — average daily commute distance.
    avg_daily_commute: float | None
    # Tri-states (1/0/None) — lines 45 / 46 / 47a / 47b.
    other_vehicle_available_personal: int | None
    vehicle_available_off_duty: int | None
    has_evidence: int | None
    evidence_is_written: int | None


@dataclass(frozen=True)
class ScheduleCPartIVContext:
    entity: str
    year: int
    rows: list[ScheduleCPartIVRow]


def build_schedule_c_part_iv_context(
    *, entity: str, year: int, yearly_rows: list[dict],
    vehicles_by_slug: dict[str, dict],
) -> ScheduleCPartIVContext:
    """Assemble context from the raw `vehicle_yearly_mileage` rows.
    Computes average daily commute = commuting_miles / commute_days
    when both present."""
    out_rows: list[ScheduleCPartIVRow] = []
    for row in yearly_rows:
        slug = row["vehicle_slug"]
        vehicle = vehicles_by_slug.get(slug) or {}
        com = row.get("commuting_miles") or 0
        cd = row.get("commute_days") or 0
        avg_daily = (com / cd) if (com and cd) else None
        out_rows.append(ScheduleCPartIVRow(
            vehicle_slug=slug,
            vehicle_display_name=vehicle.get("display_name") or slug,
            placed_in_service_date=vehicle.get("placed_in_service_date"),
            business_miles=int(row.get("business_miles") or 0),
            commuting_miles=int(com),
            personal_miles=int(row.get("personal_miles") or 0),
            commute_days=int(cd) if cd else None,
            avg_daily_commute=round(avg_daily, 1) if avg_daily else None,
            other_vehicle_available_personal=row.get(
                "other_vehicle_available_personal",
            ),
            vehicle_available_off_duty=row.get("vehicle_available_off_duty"),
            has_evidence=row.get("has_evidence"),
            evidence_is_written=row.get("evidence_is_written"),
        ))
    return ScheduleCPartIVContext(entity=entity, year=year, rows=out_rows)


def render_schedule_c_part_iv_pdf(ctx: ScheduleCPartIVContext) -> bytes:
    html = render_html("vehicle_schedule_c_part_iv.html", ctx=ctx)
    return render_pdf(html)


@dataclass(frozen=True)
class Form4562Row:
    """Per-vehicle capture for Form 4562 Parts I–V. We surface what
    the user entered; line numbers on the worksheet are a reference
    aid, not a filing claim."""
    vehicle_slug: str
    vehicle_display_name: str
    placed_in_service_date: str | None
    gvwr_lbs: int | None
    fuel_type: str | None
    purchase_price: str | None
    purchase_fees: str | None
    # Election fields for the target year.
    tax_year: int
    depreciation_method: str | None
    section_179_amount: str | None
    bonus_depreciation_amount: str | None
    basis_at_placed_in_service: str | None
    business_use_pct_override: float | None
    listed_property_qualified: int | None
    notes: str | None


@dataclass(frozen=True)
class Form4562Context:
    entity: str
    year: int
    rows: list[Form4562Row]


def render_form_4562_worksheet_pdf(ctx: Form4562Context) -> bytes:
    html = render_html("vehicle_form_4562_worksheet.html", ctx=ctx)
    return render_pdf(html)
