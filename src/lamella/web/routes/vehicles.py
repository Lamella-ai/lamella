# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Vehicles — dashboard index, per-vehicle detail, add/edit forms.

The user-facing surface lives under ``/vehicles``:

- ``GET  /vehicles``                     dashboard index (cards)
- ``GET  /vehicles/new``                 add-vehicle form
- ``GET  /vehicles/{slug}``              per-vehicle dashboard
- ``GET  /vehicles/{slug}/edit``         edit-vehicle form
- ``POST /vehicles``                     create/update (from add or edit form)
- ``POST /vehicles/{slug}/mileage``      upsert Schedule C Part IV yearly row
- ``POST /vehicles/{slug}/valuations``   log a KBB/NADA/appraisal
- ``POST /vehicles/{slug}/valuations/{id}/delete``

Legacy ``/settings/vehicles*`` URLs still resolve — they 307-redirect to
the new canonical paths so existing bookmarks and internal links keep
working during the rename window.

Mileage aggregation reads ``mileage_entries`` as the source of truth
(``vehicle_slug`` column, with a display-name fallback for legacy rows
imported from the CSV before migration 032 stamped slugs).
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import date as date_t
from decimal import Decimal, InvalidOperation
from pathlib import Path

import yaml
from beancount.core.data import Transaction
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.features.mileage.service import MileageService
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.registry.accounts_writer import AccountsWriter
from lamella.features.vehicles.allocation import (
    allocation_for_year,
    set_trip_attribution,
)
from lamella.features.vehicles.credits import (
    add_credit,
    delete_credit,
    list_credits,
)
from lamella.features.vehicles.forecasting import (
    cost_per_mile_series,
    project_miles_for_year,
    yoy_miles_overlay,
)
from lamella.features.vehicles.method_lock import advisory_for_vehicle
from lamella.features.vehicles.renewals import (
    VALID_RENEWAL_KINDS,
    add_renewal,
    complete_renewal,
    delete_renewal,
    list_renewals,
)
from lamella.features.vehicles.templates import (
    delete_template,
    list_templates,
    upsert_template,
)
from lamella.features.vehicles.fuel import (
    FuelValidationError,
    add_event as add_fuel_event,
    compute_stats as compute_fuel_stats,
    delete_event as delete_fuel_event,
    list_events as list_fuel_events,
)
from lamella.features.vehicles.disposal_writer import (
    VALID_DISPOSAL_TYPES,
    DisposalDraft,
    compute_gain_loss,
    new_disposal_id,
    render_disposal_block,
    write_disposal,
    write_revoke,
)
from lamella.features.vehicles.health import compute_health


PHASE2_BREAKING_CHANGE_KEY = "phase2_unknown_business_use"
from lamella.core.registry.service import (
    is_valid_slug,
    list_entities,
    normalize_slug,
    suggest_slug,
)

log = logging.getLogger(__name__)

router = APIRouter()
_FUEL_DESC_RE = re.compile(r"\b(gas|fuel|fill[\s-]?up|petrol|diesel|charging|charge)\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_categories(settings: Settings) -> list[str]:
    yaml_path = settings.config_dir / "vehicle_categories.yml"
    if not yaml_path.exists():
        return ["Fuel", "Insurance", "Maintenance", "Repairs", "Tires",
                "Registration", "Depreciation"]
    try:
        data = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
        cats = data.get("categories") or []
        return [str(c) for c in cats if c]
    except Exception:
        return []


def _vehicle_expense_paths(
    slug: str, categories: list[str], entity_slug: str | None
) -> list[str]:
    """DEPRECATED — writes the legacy plural-`Vehicles` path shape
    that triggers phantom-entity resurrection at boot. All callers
    are routed through ``registry.vehicle_companion.ensure_vehicle_chart``
    which uses the canonical singular `Vehicle` shape. This function
    remains only for any external caller we haven't tracked down; it
    now returns the CANONICAL paths so it can't write the bad shape
    even if someone finds it.
    """
    if not entity_slug:
        # Refuse — the canonical chart requires an owning entity.
        return []
    expense_root = f"Expenses:{entity_slug}:Vehicle:{slug}"
    asset_path = f"Assets:{entity_slug}:Vehicle:{slug}"
    paths = [f"{expense_root}:{c}" for c in categories]
    paths.append(asset_path)
    return paths


def _fuel_import_candidates(
    conn,
    *,
    vehicle_slug: str,
    entity_slug: str | None,
    year: int,
) -> list[dict]:
    if not entity_slug:
        return []
    rows = conn.execute(
        """
        SELECT rr.id AS raw_row_id, rr.date AS txn_date, rr.amount AS amount,
               COALESCE(rr.payee, '') AS payee, COALESCE(rr.description, '') AS description,
               COALESCE(c.schedule_c_category, '') AS schedule_c_category
          FROM raw_rows rr
          JOIN categorizations c ON c.raw_row_id = rr.id
         WHERE c.entity = ?
           AND rr.date >= ?
           AND rr.date < ?
           AND rr.amount IS NOT NULL
        ORDER BY rr.date DESC, rr.id DESC
        """,
        (entity_slug, f"{year:04d}-01-01", f"{year+1:04d}-01-01"),
    ).fetchall()
    existing = {
        str(r["notes"]).strip() for r in conn.execute(
            "SELECT notes FROM vehicle_fuel_log WHERE vehicle_slug = ? AND source = 'schedule_c_import'",
            (vehicle_slug,),
        ).fetchall()
        if r["notes"]
    }
    out: list[dict] = []
    for r in rows:
        cat = str(r["schedule_c_category"] or "").lower()
        memo = f"{r['payee']} {r['description']}".strip()
        is_sched_c_vehicle = "car" in cat or "truck" in cat or "vehicle" in cat or "auto" in cat
        is_fuelish = bool(_FUEL_DESC_RE.search(memo))
        if not (is_sched_c_vehicle or is_fuelish):
            continue
        note = f"raw_row_id:{int(r['raw_row_id'])}"
        if note in existing:
            continue
        amt = abs(float(r["amount"] or 0.0))
        if amt <= 0:
            continue
        out.append({
            "raw_row_id": int(r["raw_row_id"]),
            "txn_date": str(r["txn_date"])[:10],
            "amount": amt,
            "payee": str(r["payee"] or ""),
            "description": str(r["description"] or ""),
            "source_hint": "schedule_c" if is_sched_c_vehicle else "mileage_keyword",
        })
    return out[:25]


def _decimal(s) -> Decimal | None:
    if s is None or s == "":
        return None
    raw = str(s).strip().replace(",", "").replace("$", "").replace(" ", "")
    if not raw:
        return None
    try:
        return Decimal(raw)
    except Exception:
        return None


def _vehicle_expense_prefix(vehicle: dict) -> str:
    """Prefix that every vehicle expense posting should sit under.
    Canonical shape is `Expenses:<Entity>:Vehicle:<slug>:` (singular
    `Vehicle`) per CLAUDE.md + LEDGER_LAYOUT. The old plural shape
    `Expenses:<Entity>:Vehicles:<slug>:` only survives in legacy
    pre-migration ledgers; the migrate flow on /setup/vehicles
    cleans them up."""
    entity = vehicle.get("entity_slug")
    slug = vehicle["slug"]
    if entity:
        return f"Expenses:{entity}:Vehicle:{slug}:"
    return f"Expenses:Vehicle:{slug}:"


def _mileage_filter_values(vehicle: dict) -> list[str]:
    """Values we'll accept as a match on mileage_entries.vehicle.

    mileage_entries.vehicle stores the *display name* by convention
    (set in service.write_import_rows / add_entry), but older csv_legacy
    rows may hold either slug or display_name, and some historical
    imports predate vehicle_slug. Match on any of them.
    """
    values = {vehicle["slug"]}
    if vehicle.get("display_name"):
        values.add(vehicle["display_name"])
    return [v for v in values if v]


def _year_mileage_breakdown(conn, vehicle: dict, year: int) -> dict:
    """Return total / business / personal miles for a calendar year.

    Joins mileage_entries (the trip log — source of truth) with
    mileage_trip_meta (the business/personal split sidecar) by
    (entry_date, vehicle, miles).
    """
    slug = vehicle["slug"]
    names = _mileage_filter_values(vehicle)
    placeholders = ",".join(["?"] * len(names))
    start = f"{year:04d}-01-01"
    end = f"{year + 1:04d}-01-01"

    total_row = conn.execute(
        f"""
        SELECT COALESCE(SUM(miles), 0) AS total
          FROM mileage_entries
         WHERE entry_date >= ? AND entry_date < ?
           AND (vehicle_slug = ? OR vehicle IN ({placeholders}))
        """,
        (start, end, slug, *names),
    ).fetchone()

    split_row = conn.execute(
        f"""
        SELECT COALESCE(SUM(business_miles), 0) AS biz,
               COALESCE(SUM(commuting_miles), 0) AS com,
               COALESCE(SUM(personal_miles), 0) AS per
          FROM mileage_trip_meta
         WHERE entry_date >= ? AND entry_date < ?
           AND vehicle IN ({placeholders})
        """,
        (start, end, *names),
    ).fetchone()

    return {
        "year": year,
        "total": float(total_row["total"] or 0),
        "business": float(split_row["biz"] or 0),
        "commuting": float(split_row["com"] or 0),
        "personal": float(split_row["per"] or 0),
    }


def _mileage_by_year(conn, vehicle: dict) -> list[dict]:
    """Every year we have mileage for, newest first. Includes splits."""
    slug = vehicle["slug"]
    names = _mileage_filter_values(vehicle)
    placeholders = ",".join(["?"] * len(names))

    rows = conn.execute(
        f"""
        SELECT CAST(strftime('%Y', entry_date) AS INTEGER) AS yr,
               ROUND(SUM(miles), 1) AS total_miles
          FROM mileage_entries
         WHERE vehicle_slug = ? OR vehicle IN ({placeholders})
         GROUP BY yr
         ORDER BY yr DESC
        """,
        (slug, *names),
    ).fetchall()
    if not rows:
        return []

    split_q = conn.execute(
        f"""
        SELECT CAST(strftime('%Y', entry_date) AS INTEGER) AS yr,
               ROUND(COALESCE(SUM(business_miles), 0), 1) AS biz,
               ROUND(COALESCE(SUM(commuting_miles), 0), 1) AS com,
               ROUND(COALESCE(SUM(personal_miles), 0), 1) AS per
          FROM mileage_trip_meta
         WHERE vehicle IN ({placeholders})
         GROUP BY yr
        """,
        tuple(names),
    ).fetchall()
    split_by_year = {
        r["yr"]: (
            float(r["biz"] or 0),
            float(r["com"] or 0),
            float(r["per"] or 0),
        )
        for r in split_q
    }
    out: list[dict] = []
    for r in rows:
        biz, com, per = split_by_year.get(r["yr"], (0.0, 0.0, 0.0))
        out.append({
            "yr": r["yr"],
            "total_miles": float(r["total_miles"] or 0),
            "business_miles": biz,
            "commuting_miles": com,
            "personal_miles": per,
        })
    return out


def _recent_trips(conn, vehicle: dict, limit: int = 10) -> list[dict]:
    names = _mileage_filter_values(vehicle)
    placeholders = ",".join(["?"] * len(names))
    rows = conn.execute(
        f"""
        SELECT entry_date, entry_time, miles, odometer_end,
               purpose, from_loc, to_loc, notes, entity
          FROM mileage_entries
         WHERE vehicle_slug = ? OR vehicle IN ({placeholders})
         ORDER BY entry_date DESC,
                  COALESCE(entry_time, '') DESC,
                  id DESC
         LIMIT ?
        """,
        (vehicle["slug"], *names, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def _mileage_monthly_series(conn, vehicle: dict, year: int) -> list[dict]:
    """12-point monthly series for the mileage chart."""
    names = _mileage_filter_values(vehicle)
    placeholders = ",".join(["?"] * len(names))
    rows = conn.execute(
        f"""
        SELECT CAST(strftime('%m', entry_date) AS INTEGER) AS mo,
               ROUND(SUM(miles), 1) AS total
          FROM mileage_entries
         WHERE entry_date >= ? AND entry_date < ?
           AND (vehicle_slug = ? OR vehicle IN ({placeholders}))
         GROUP BY mo
        """,
        (f"{year:04d}-01-01", f"{year + 1:04d}-01-01",
         vehicle["slug"], *names),
    ).fetchall()
    by_mo = {int(r["mo"]): float(r["total"] or 0) for r in rows}
    return [{"month": m, "miles": by_mo.get(m, 0.0)} for m in range(1, 13)]


def _standard_vs_actual(
    *,
    conn,
    loaded_entries,
    vehicle: dict,
    year: int,
    settings: Settings,
) -> dict:
    """IRS standard-mileage vs per-posting actual-expense deduction.

    - Standard: per-trip business miles × IRS rate in effect on that
      date (honors mid-year rate changes via the mileage_rates table;
      falls back to settings.mileage_rate when uncovered).
    - Actual: sum of postings under the vehicle's expense subtree for
      the year. ``depreciation_key`` bucket is surfaced separately
      because IRS actual-expense treatment splits it from operating.
    """
    service = MileageService(conn=conn, csv_path=None)
    names = _mileage_filter_values(vehicle)
    placeholders = ",".join(["?"] * len(names))
    start = f"{year:04d}-01-01"
    end = f"{year + 1:04d}-01-01"

    rows = conn.execute(
        f"""
        SELECT e.entry_date, e.miles,
               m.business_miles, m.commuting_miles, m.personal_miles
          FROM mileage_entries e
          LEFT JOIN mileage_trip_meta m
                 ON m.entry_date = e.entry_date
                AND m.vehicle = e.vehicle
                AND m.miles = e.miles
         WHERE e.entry_date >= ? AND e.entry_date < ?
           AND (e.vehicle_slug = ? OR e.vehicle IN ({placeholders}))
        """,
        (start, end, vehicle["slug"], *names),
    ).fetchall()

    business_miles = 0.0
    commuting_miles = 0.0
    personal_miles = 0.0
    total_miles = 0.0
    standard_usd = Decimal("0")
    # Track whether ANY trip in the year has a recorded split. If
    # none do, business_pct flips to None ("unknown") — Phase 2
    # breaking-change replacing the silent "100% business" fallback.
    any_split_recorded = False
    for r in rows:
        try:
            d = date_t.fromisoformat(str(r["entry_date"])[:10])
        except ValueError:
            continue
        miles = float(r["miles"] or 0)
        total_miles += miles
        biz = r["business_miles"]
        com = r["commuting_miles"]
        per = r["personal_miles"]
        if biz is not None or com is not None or per is not None:
            any_split_recorded = True
        business_miles += float(biz or 0)
        commuting_miles += float(com or 0)
        personal_miles += float(per or 0)
        rate = service.rate_for_date(d, fallback=settings.mileage_rate)
        standard_usd += Decimal(str(rate)) * Decimal(str(biz or 0))

    prefix = _vehicle_expense_prefix(vehicle)
    actual_by_category: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    ytd_cutoff = date_t(year, 1, 1)
    year_end = date_t(year + 1, 1, 1)
    for entry in loaded_entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date < ytd_cutoff or entry.date >= year_end:
            continue
        for p in entry.postings:
            acct = p.account or ""
            if not acct.startswith(prefix):
                continue
            if p.units is None or p.units.number is None:
                continue
            leaf = acct[len(prefix):].split(":")[0] or "Other"
            actual_by_category[leaf] += Decimal(p.units.number)

    actual_total = sum(actual_by_category.values(), Decimal("0"))
    # Phase 2 business-use correctness: when no split was recorded
    # anywhere in the year, business_pct is None ("unknown") and the
    # actual-expense dollar figure is suppressed. When a split WAS
    # recorded, the ratio is (recorded business miles / total miles);
    # unsplit trips in a mixed year effectively count as 0% business
    # for the ratio, which is conservative — surfacing the drift is
    # the data-health panel's job (via missing_splits).
    depreciation_usd = actual_by_category.get("Depreciation", Decimal("0"))
    operating_usd = actual_total - depreciation_usd

    business_pct_unknown = (
        total_miles > 0 and not any_split_recorded
    )
    if business_pct_unknown:
        business_pct: float | None = None
        actual_business_usd: Decimal | None = None
    elif total_miles > 0:
        business_pct = business_miles / total_miles
        actual_business_usd = (
            (operating_usd + depreciation_usd) * Decimal(str(business_pct))
        ).quantize(Decimal("0.01"))
    else:
        # No trips at all in the year — not "unknown", just empty.
        business_pct = 0.0
        actual_business_usd = Decimal("0.00")

    if actual_business_usd is None:
        better = "standard" if standard_usd > 0 else None
    else:
        better = (
            "standard" if standard_usd >= actual_business_usd
            else "actual"
        )

    return {
        "year": year,
        "total_miles": round(total_miles, 1),
        "business_miles": round(business_miles, 1),
        "commuting_miles": round(commuting_miles, 1),
        "personal_miles": round(personal_miles, 1),
        "business_pct": (
            round(business_pct * 100, 1) if business_pct is not None else None
        ),
        "business_pct_unknown": business_pct_unknown,
        "standard_deduction": standard_usd.quantize(Decimal("0.01")),
        "actual_operating": operating_usd,
        "actual_depreciation": depreciation_usd,
        "actual_total": actual_total,
        "actual_business_deduction": actual_business_usd,
        "actual_by_category": sorted(
            actual_by_category.items(), key=lambda kv: kv[1], reverse=True,
        ),
        "better": better,
    }


def _card_for_vehicle(
    *,
    conn,
    v: dict,
    year: int,
    rate,
    loans_all: list[dict],
    ytd_expense: Decimal,
) -> dict:
    """Build the card-shaped dict consumed by partials/_vehicle_card.html.
    Used by both the index dashboard render and the modal-edit save
    response so the in-place swap stays in sync with full-page rendering."""
    breakdown = _year_mileage_breakdown(conn, v, year)
    standard_est = Decimal(str(rate)) * Decimal(str(breakdown["business"]))
    needle = (v.get("display_name") or v["slug"]).lower()
    slug_l = v["slug"].lower()
    linked = [
        loan for loan in loans_all
        if (loan.get("display_name")
            and needle in (loan["display_name"] or "").lower())
        or (loan.get("slug") and slug_l in loan["slug"].lower())
    ]
    return {
        "slug": v["slug"],
        "display_name": v.get("display_name") or v["slug"],
        "year": v.get("year"),
        "make": v.get("make"),
        "model": v.get("model"),
        "license_plate": v.get("license_plate"),
        "entity_slug": v.get("entity_slug"),
        "entity_display_name": v.get("entity_display_name"),
        "is_active": bool(v.get("is_active", 1)),
        "sale_date": v.get("sale_date"),
        "current_mileage": v.get("current_mileage"),
        "purchase_price": v.get("purchase_price"),
        "ytd_total_miles": breakdown["total"],
        "ytd_business_miles": breakdown["business"],
        "ytd_personal_miles": breakdown["personal"],
        "ytd_standard_deduction": standard_est.quantize(Decimal("0.01")),
        "ytd_expense_total": ytd_expense.quantize(Decimal("0.01")),
        "loan_count": len(linked),
        "mileage_rate": rate,
    }


def _phase2_banner_active_for(conn, slug: str) -> bool:
    """True when the phase2_unknown_business_use banner is still
    undismissed for this vehicle. Silent if the table doesn't exist
    (fresh test DB without migration 034)."""
    try:
        row = conn.execute(
            "SELECT dismissed_at FROM vehicle_breaking_change_seen "
            "WHERE change_key = ? AND vehicle_slug = ?",
            (PHASE2_BREAKING_CHANGE_KEY, slug),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return False
    return bool(row) and row["dismissed_at"] is None


def _phase2_banner_index_count(conn) -> int:
    """How many vehicles still have the undismissed Phase 2 banner.
    Drives the index-page summary callout."""
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM vehicle_breaking_change_seen "
            "WHERE change_key = ? AND dismissed_at IS NULL",
            (PHASE2_BREAKING_CHANGE_KEY,),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return 0
    return int(row["n"] or 0) if row else 0


def _linked_loans_for(conn, vehicle: dict) -> list[dict]:
    """Find auto loans that reference this vehicle by name or slug."""
    loans_all = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, loan_type, institution, "
            "       original_principal, liability_account_path "
            "FROM loans WHERE loan_type = 'auto' AND is_active = 1"
        ).fetchall()
    ]
    slug = vehicle["slug"]
    needle = (vehicle.get("display_name") or slug).lower()
    return [
        loan for loan in loans_all
        if (loan.get("display_name")
            and needle in (loan["display_name"] or "").lower())
        or (loan.get("slug") and slug.lower() in loan["slug"].lower())
    ]


def _current_debt(loaded_entries, linked_loans: list[dict]) -> Decimal:
    liability_paths = {
        l.get("liability_account_path") for l in linked_loans
        if l.get("liability_account_path")
    }
    if not liability_paths:
        return Decimal("0")
    balance = Decimal("0")
    for entry in loaded_entries:
        if not isinstance(entry, Transaction):
            continue
        for p in entry.postings:
            if (p.account in liability_paths and p.units
                    and p.units.number is not None):
                balance += Decimal(p.units.number)
    return abs(balance)


def _book_value_and_expenses(
    loaded_entries, vehicle: dict,
) -> tuple[Decimal, list[tuple[str, Decimal]]]:
    asset_path = vehicle.get("asset_account_path")
    slug = vehicle["slug"]
    book = Decimal("0")
    expenses: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for entry in loaded_entries:
        if not isinstance(entry, Transaction):
            continue
        for p in entry.postings:
            if p.units is None or p.units.number is None:
                continue
            acct = p.account or ""
            amt = Decimal(p.units.number)
            if asset_path and acct == asset_path:
                book += amt
            if acct.startswith("Expenses:") and f":Vehicles:{slug}:" in f":{acct}:":
                expenses[acct] += abs(amt)
    return book, sorted(expenses.items(), key=lambda kv: kv[1], reverse=True)


# ---------------------------------------------------------------------------
# Dashboard index
# ---------------------------------------------------------------------------


@router.get("/vehicles", response_class=HTMLResponse)
def vehicles_index(
    request: Request,
    conn=Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Dashboard — one card per vehicle, with at-a-glance stats.

    Each card shows the basics (display name, year/make/model, owner),
    current-year miles (total + business), IRS standard-mileage
    deduction for those business miles, YTD expenses under the
    vehicle's subtree, and linked-loan count.
    """
    today = date_t.today()
    year = today.year

    rows = conn.execute(
        """
        SELECT v.*, e.display_name AS entity_display_name
          FROM vehicles v
          LEFT JOIN entities e ON e.slug = v.entity_slug
         ORDER BY v.is_active DESC,
                  COALESCE(v.year, 0) DESC,
                  COALESCE(v.display_name, v.slug)
        """
    ).fetchall()
    vehicles = [dict(r) for r in rows]

    loaded_entries = reader.load().entries

    loan_counts: dict[str, int] = defaultdict(int)
    loans_all = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, liability_account_path "
            "FROM loans WHERE loan_type = 'auto' AND is_active = 1"
        ).fetchall()
    ]

    # Single ledger pass: fold YTD expense totals into a prefix → Decimal
    # dict so we don't re-walk entries per vehicle.
    prefixes = {v["slug"]: _vehicle_expense_prefix(v) for v in vehicles}
    ytd_by_prefix: dict[str, Decimal] = {p: Decimal("0") for p in prefixes.values()}
    for entry in loaded_entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date.year != year:
            continue
        for p in entry.postings:
            acct = p.account or ""
            if p.units is None or p.units.number is None:
                continue
            for prefix in ytd_by_prefix:
                if acct.startswith(prefix):
                    ytd_by_prefix[prefix] += abs(Decimal(p.units.number))
                    break

    service = MileageService(conn=conn, csv_path=None)
    rate = service.rate_for_date(today, fallback=settings.mileage_rate)

    cards: list[dict] = []
    for v in vehicles:
        ytd_expense = ytd_by_prefix[prefixes[v["slug"]]]
        cards.append(_card_for_vehicle(
            conn=conn, v=v, year=year, rate=rate,
            loans_all=loans_all, ytd_expense=ytd_expense,
        ))

    ctx = {
        "cards": cards,
        "year": year,
        "phase2_banner_count": _phase2_banner_index_count(conn),
    }
    return request.app.state.templates.TemplateResponse(
        request, "vehicles_index.html", ctx,
    )


# ---------------------------------------------------------------------------
# Per-vehicle dashboard
# ---------------------------------------------------------------------------


@router.get("/vehicles/{slug}/edit-modal", response_class=HTMLResponse)
def vehicle_edit_modal(
    slug: str,
    request: Request,
    conn=Depends(get_db),
):
    """HTMX fragment — quick-edit modal for one vehicle. Mirrors
    /entities/{slug}/edit-modal: the form posts back to /vehicles, the
    modal targets the dashboard card by id for in-place swap, and the
    server fires HX-Trigger=vehicle-saved to close the modal. Detailed
    fields (VIN, GVWR, fuel type, depreciation, disposal) stay on the
    focused detail page at /vehicles/{slug}/edit."""
    row = conn.execute(
        "SELECT v.*, e.display_name AS entity_display_name "
        "FROM vehicles v "
        "LEFT JOIN entities e ON e.slug = v.entity_slug "
        "WHERE v.slug = ?",
        (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    return request.app.state.templates.TemplateResponse(
        request, "partials/_vehicle_modal_edit.html",
        {"vehicle": dict(row)},
    )


@router.get("/vehicles/new-modal", response_class=HTMLResponse)
def vehicle_new_modal(
    request: Request,
    conn=Depends(get_db),
):
    """HTMX fragment — "+ Add vehicle" modal. Slim form: entity,
    display name, year/make/model. Slug auto-derives from display
    name. After save, the page reloads via HX-Refresh and the new
    vehicle's card appears in the dashboard. Detailed fields (VIN,
    plate, GVWR, fuel type, purchase, etc.) are edited on
    /vehicles/{slug}/edit."""
    entities = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name FROM entities "
            "WHERE is_active = 1 ORDER BY display_name, slug"
        ).fetchall()
    ]
    return request.app.state.templates.TemplateResponse(
        request, "partials/_vehicle_modal_new.html",
        {"entities": entities},
    )


@router.get("/vehicles/new", response_class=HTMLResponse)
def vehicle_new(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn=Depends(get_db),
):
    entities = list_entities(conn, include_inactive=False)
    ctx = {
        "entities": entities,
        "categories": _load_categories(settings),
    }
    return request.app.state.templates.TemplateResponse(
        request, "vehicle_new.html", ctx,
    )


@router.get("/vehicles/{slug}", response_class=HTMLResponse)
def vehicle_detail(
    slug: str,
    request: Request,
    year: int | None = None,
    saved: str | None = None,
    conn=Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    row = conn.execute(
        """
        SELECT v.*, e.display_name AS entity_display_name
          FROM vehicles v
          LEFT JOIN entities e ON e.slug = v.entity_slug
         WHERE v.slug = ?
        """,
        (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    vehicle = dict(row)

    today = date_t.today()
    target_year = year or today.year

    purchase_price = _decimal(vehicle.get("purchase_price")) or Decimal("0")
    purchase_fees = _decimal(vehicle.get("purchase_fees")) or Decimal("0")
    cost_basis = purchase_price + purchase_fees

    loaded_entries = reader.load().entries
    book_value, expense_rollup = _book_value_and_expenses(
        loaded_entries, vehicle,
    )

    linked_loans = _linked_loans_for(conn, vehicle)
    current_debt = _current_debt(loaded_entries, linked_loans)

    valuations = [
        dict(r) for r in conn.execute(
            "SELECT id, as_of_date, value, source, notes "
            "FROM vehicle_valuations WHERE vehicle_slug = ? "
            "ORDER BY as_of_date DESC",
            (slug,),
        ).fetchall()
    ]
    current_value = _decimal(valuations[0]["value"]) if valuations else None
    equity = (current_value - current_debt) if current_value is not None else None

    deduction = _standard_vs_actual(
        conn=conn,
        loaded_entries=loaded_entries,
        vehicle=vehicle,
        year=target_year,
        settings=settings,
    )

    mileage_years = _mileage_by_year(conn, vehicle)
    monthly_series = _mileage_monthly_series(conn, vehicle, target_year)
    recent_trips = _recent_trips(conn, vehicle, limit=10)

    yearly_rows = [
        dict(r) for r in conn.execute(
            "SELECT * FROM vehicle_yearly_mileage WHERE vehicle_slug = ? "
            "ORDER BY year DESC",
            (slug,),
        ).fetchall()
    ]

    years_with_data = sorted(
        {m["yr"] for m in mileage_years} | {target_year},
        reverse=True,
    )

    health_issues = compute_health(conn, vehicle=vehicle, year=target_year)
    phase2_banner_active = _phase2_banner_active_for(conn, slug)

    elections = [
        dict(r) for r in conn.execute(
            "SELECT * FROM vehicle_elections "
            "WHERE vehicle_slug = ? ORDER BY tax_year DESC",
            (slug,),
        ).fetchall()
    ]
    disposals = [
        dict(r) for r in conn.execute(
            "SELECT * FROM vehicle_disposals "
            "WHERE vehicle_slug = ? ORDER BY disposal_date DESC, created_at DESC",
            (slug,),
        ).fetchall()
    ]
    fuel_events = list_fuel_events(conn, vehicle_slug=slug, limit=25)
    fuel_stats = compute_fuel_stats(
        list_fuel_events(conn, vehicle_slug=slug, year=target_year, limit=1000)
    )
    fuel_import_candidates = _fuel_import_candidates(
        conn, vehicle_slug=slug, entity_slug=vehicle.get("entity_slug"), year=target_year,
    )

    # Phase 5E forecasting — all pure view-time derivations.
    forecast = project_miles_for_year(
        conn, vehicle=vehicle, year=target_year,
        rate_per_mile=settings.mileage_rate,
    )
    cost_per_mile = cost_per_mile_series(
        conn, vehicle=vehicle, year=target_year,
    )
    yoy_overlay = yoy_miles_overlay(
        conn, vehicle=vehicle, year=target_year, prior_years=3,
    )

    # Phase 6 — credits, renewals, allocation, method-lock.
    credits = list_credits(conn, slug)
    renewals = list_renewals(conn, slug)
    allocation = allocation_for_year(
        conn, vehicle=vehicle, year=target_year,
    )
    method_lock = advisory_for_vehicle(
        conn, vehicle_slug=slug, target_year=target_year,
    )

    ctx = {
        "vehicle": vehicle,
        "cost_basis": cost_basis,
        "book_value": book_value,
        "current_value": current_value,
        "current_debt": current_debt,
        "equity": equity,
        "linked_loans": linked_loans,
        "valuations": valuations,
        "expense_rollup": expense_rollup,
        "deduction": deduction,
        "mileage_years": mileage_years,
        "monthly_series": monthly_series,
        "recent_trips": recent_trips,
        "yearly_rows": yearly_rows,
        "target_year": target_year,
        "year_options": years_with_data,
        "saved": saved,
        "health_issues": health_issues,
        "phase2_banner_active": phase2_banner_active,
        "elections": elections,
        "disposals": disposals,
        "fuel_events": fuel_events,
        "fuel_stats": fuel_stats,
        "fuel_import_candidates": fuel_import_candidates,
        "forecast": forecast,
        "cost_per_mile": cost_per_mile,
        "yoy_overlay": yoy_overlay,
        "credits": credits,
        "renewals": renewals,
        "allocation": allocation,
        "method_lock": method_lock,
    }
    return request.app.state.templates.TemplateResponse(
        request, "vehicle_detail.html", ctx,
    )


_TRIPS_PAGE_SIZE = 50

_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


@router.get("/vehicles-backfill-audit", response_class=HTMLResponse)
def backfill_audit_index(
    request: Request,
    conn=Depends(get_db),
):
    """List dates that had mileage data back-filled meaningfully
    after the trip happened (default >= 2 days). Any transaction on
    that date that was classified before the back-fill landed was
    decided without the benefit of that mileage context and may
    deserve a re-review.

    This page is intentionally the cheap-and-honest view: it shows
    the dates and when the back-fill was recorded. A detail view
    that joins against ai_decisions for per-txn re-classify proposals
    is a follow-up once we see how this gets used in practice."""
    from lamella.features.mileage.backfill_audit import (
        list_backfill_dates,
    )

    rows = list_backfill_dates(conn, limit=500)
    # Group by year for compactness — back-filling years of old logs
    # will produce dozens of rows otherwise.
    by_year: dict[int, list] = defaultdict(list)
    for r in rows:
        by_year[r.entry_date.year].append(r)
    years = sorted(by_year.keys(), reverse=True)

    # Same-day ledger-txn join is expensive and depends on reader
    # load; skip for v1 and surface it on a per-date detail page.
    total_dates = len(rows)
    total_entries = sum(r.backfill_entry_count for r in rows)
    oldest = min((r.entry_date for r in rows), default=None)
    newest = max((r.entry_date for r in rows), default=None)

    ctx = {
        "rows": rows,
        "years": years,
        "by_year": dict(by_year),
        "total_dates": total_dates,
        "total_entries": total_entries,
        "oldest": oldest,
        "newest": newest,
    }
    return request.app.state.templates.TemplateResponse(
        request, "vehicle_backfill_audit.html", ctx,
    )


@router.get("/vehicles/{slug}/trips", response_class=HTMLResponse)
def vehicle_trips(
    slug: str,
    request: Request,
    year: int | None = None,
    month: int | None = None,
    page: int = 1,
    conn=Depends(get_db),
):
    """Paginated, filterable trip log for a single vehicle. Uses the
    same tolerant vehicle predicate as the detail page's recent-trips
    widget (vehicle_slug OR vehicle matches slug/display_name) so
    legacy rows imported before migration 032 stamped vehicle_slug are
    still included."""
    row = conn.execute(
        "SELECT * FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    vehicle = dict(row)

    names = _mileage_filter_values(vehicle)
    placeholders = ",".join(["?"] * len(names))

    if month is not None and (month < 1 or month > 12):
        month = None
    if month is not None and year is None:
        # month without year is nonsensical — drop the filter rather than 500.
        month = None

    clauses = [f"(e.vehicle_slug = ? OR e.vehicle IN ({placeholders}))"]
    params: list = [vehicle["slug"], *names]
    if year is not None and month is not None:
        start = date_t(year, month, 1)
        if month == 12:
            end = date_t(year + 1, 1, 1)
        else:
            end = date_t(year, month + 1, 1)
        clauses.append("e.entry_date >= ? AND e.entry_date < ?")
        params.extend([start.isoformat(), end.isoformat()])
    elif year is not None:
        clauses.append("e.entry_date >= ? AND e.entry_date < ?")
        params.extend([f"{year:04d}-01-01", f"{year + 1:04d}-01-01"])

    where = "WHERE " + " AND ".join(clauses)

    total_row = conn.execute(
        f"SELECT COUNT(*) AS n, COALESCE(SUM(e.miles), 0) AS total_miles "
        f"FROM mileage_entries e {where}",
        tuple(params),
    ).fetchone()
    total = int(total_row["n"] or 0)
    total_miles = float(total_row["total_miles"] or 0)

    page = max(1, int(page))
    total_pages = max(1, (total + _TRIPS_PAGE_SIZE - 1) // _TRIPS_PAGE_SIZE)
    if page > total_pages:
        page = total_pages
    offset = (page - 1) * _TRIPS_PAGE_SIZE

    rows = conn.execute(
        f"""
        SELECT e.id, e.entry_date, e.entry_time, e.miles,
               e.odometer_start, e.odometer_end,
               e.purpose, e.purpose_category, e.entity,
               e.from_loc, e.to_loc, e.notes, e.source,
               m.business_miles, m.commuting_miles, m.personal_miles,
               m.category AS meta_category
          FROM mileage_entries e
          LEFT JOIN mileage_trip_meta m
                 ON m.entry_date = e.entry_date
                AND m.vehicle = e.vehicle
                AND m.miles = e.miles
          {where}
      ORDER BY e.entry_date DESC,
               COALESCE(e.entry_time, '') DESC,
               e.id DESC
         LIMIT ? OFFSET ?
        """,
        (*params, _TRIPS_PAGE_SIZE, offset),
    ).fetchall()
    trips = [dict(r) for r in rows]

    # Available years + months for the filter dropdowns (scoped to this
    # vehicle only — the /mileage/all page is for all-vehicle filtering).
    year_rows = conn.execute(
        f"""
        SELECT DISTINCT CAST(strftime('%Y', entry_date) AS INTEGER) AS y
          FROM mileage_entries e
         WHERE e.vehicle_slug = ? OR e.vehicle IN ({placeholders})
         ORDER BY y DESC
        """,
        (vehicle["slug"], *names),
    ).fetchall()
    years = [int(r["y"]) for r in year_rows if r["y"] is not None]

    months_for_year: list[int] = []
    if year is not None:
        month_rows = conn.execute(
            f"""
            SELECT DISTINCT CAST(strftime('%m', entry_date) AS INTEGER) AS m
              FROM mileage_entries e
             WHERE (e.vehicle_slug = ? OR e.vehicle IN ({placeholders}))
               AND e.entry_date >= ? AND e.entry_date < ?
             ORDER BY m ASC
            """,
            (vehicle["slug"], *names,
             f"{year:04d}-01-01", f"{year + 1:04d}-01-01"),
        ).fetchall()
        months_for_year = [int(r["m"]) for r in month_rows if r["m"] is not None]

    ctx = {
        "vehicle": vehicle,
        "trips": trips,
        "total": total,
        "total_miles": total_miles,
        "page": page,
        "total_pages": total_pages,
        "page_size": _TRIPS_PAGE_SIZE,
        "year": year,
        "month": month,
        "years": years,
        "months_for_year": months_for_year,
        "month_names": _MONTH_NAMES,
    }
    return request.app.state.templates.TemplateResponse(
        request, "vehicle_trips.html", ctx,
    )


@router.post("/vehicles/{slug}/banner/{change_key}/dismiss")
async def dismiss_breaking_change_banner(
    slug: str,
    change_key: str,
    request: Request,
    conn=Depends(get_db),
):
    """Record dismissal of a one-time breaking-change banner for this
    vehicle. Silent no-op if the row doesn't exist (migration didn't
    seed it or already dismissed)."""
    conn.execute(
        """
        UPDATE vehicle_breaking_change_seen
           SET dismissed_at = CURRENT_TIMESTAMP
         WHERE change_key = ? AND vehicle_slug = ?
           AND dismissed_at IS NULL
        """,
        (change_key, slug),
    )
    # Redirect back to the detail page (or stay put for HTMX).
    return RedirectResponse(f"/vehicles/{slug}", status_code=303)


@router.get("/vehicles/{slug}/change-ownership", response_class=HTMLResponse)
def vehicle_change_ownership_page(
    slug: str,
    request: Request,
    conn=Depends(get_db),
):
    """Entry point for the ownership-change flow. Offers two paths:

      (A) Misattribution rename — you picked the wrong entity at
          create time; all postings/paths get renamed to the right
          entity. Only safe when the asset is still effectively owned
          by the same taxpayer but was labeled wrong.

      (B) Disposal + re-acquisition — the asset genuinely changed
          hands. Original vehicle record gets a disposal (sale date
          + sale price) posted on the old entity. A new vehicle
          record is created under the new entity with the disposal
          proceeds as the new cost basis (user can override).

    Both flows are stubbed; this page exists to gate the choice.
    """
    row = conn.execute(
        "SELECT slug, display_name, entity_slug, year, make, model, "
        "       current_mileage, purchase_date, purchase_price "
        "  FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    entities = list_entities(conn, include_inactive=False)
    # Count historical postings to warn about blast radius.
    prefix = f"Expenses:{row['entity_slug']}:Vehicle:{slug}:" if row["entity_slug"] else ""
    posting_count = 0
    if prefix:
        try:
            # Best-effort; if the reader isn't warm yet, skip
            reader = getattr(request.app.state, "ledger_reader", None)
            if reader is not None:
                from beancount.core.data import Transaction
                for e in reader.load().entries:
                    if not isinstance(e, Transaction):
                        continue
                    for p in e.postings or ():
                        if p.account and p.account.startswith(prefix):
                            posting_count += 1
        except Exception:  # noqa: BLE001
            posting_count = 0
    return request.app.state.templates.TemplateResponse(
        request, "vehicle_change_ownership.html",
        {
            "vehicle": dict(row),
            "entities": entities,
            "posting_count": posting_count,
        },
    )


@router.post("/vehicles/{slug}/change-ownership/rename")
def vehicle_change_ownership_rename(
    slug: str,
    request: Request,
    new_entity_slug: str = Form(...),
    conn = Depends(get_db),
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Misattribution-fix: rewrite every posting on
    ``Expenses:<old>:Vehicle:<slug>:*`` and ``Assets:<old>:Vehicle:<slug>``
    to point at the new entity. Two cases:

    * **Case A — direct postings on old vehicle paths.** A new
      override block is written redirecting from old → new path.
    * **Case B — existing migration overrides whose ``to_account``
      already points at the old vehicle path.** The override block's
      account names are textually rewritten so the override now
      points at the new path. (Replacing via OverrideWriter would
      stack a new override under the same ``lamella-override-of`` hash
      and mid-migration would break balances.)

    After the rewrites: open the canonical chart under the new entity
    (idempotent), close the old-entity vehicle accounts, and bump
    ``vehicles.entity_slug`` in SQLite. All wrapped in
    ``_recovery_write_envelope`` so any failure rolls back every
    file. **No disposal is recorded** — the assumption is that the
    user labeled the wrong entity, not that the vehicle changed
    hands. The change-ownership form refuses transfer (the
    tax-implication-bearing flow) without explicit user input."""
    import re
    from datetime import date as _date_t
    from urllib.parse import quote as _q

    from fastapi.responses import RedirectResponse

    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash as _txn_hash
    from lamella.features.vehicles.vehicle_companion import ensure_vehicle_chart
    from lamella.features.rules.overrides import OverrideWriter
    from lamella.features.setup.posting_counts import (
        is_override_txn,
        open_paths,
    )
    from lamella.features.setup.recovery import recovery_write_envelope

    new_entity_slug = (new_entity_slug or "").strip()
    if not new_entity_slug:
        return RedirectResponse(
            f"/vehicles/{slug}/change-ownership?error=missing-new-entity",
            status_code=303,
        )

    row = conn.execute(
        "SELECT slug, display_name, entity_slug FROM vehicles WHERE slug = ?",
        (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"vehicle '{slug}' not found")
    old_entity = row["entity_slug"]
    if not old_entity:
        return RedirectResponse(
            f"/vehicles/{slug}/change-ownership?error=current-entity-missing",
            status_code=303,
        )
    if old_entity == new_entity_slug:
        return RedirectResponse(
            f"/vehicles/{slug}/change-ownership?error=same-entity",
            status_code=303,
        )
    if not conn.execute(
        "SELECT 1 FROM entities WHERE slug = ? AND is_active = 1",
        (new_entity_slug,),
    ).fetchone():
        return RedirectResponse(
            f"/vehicles/{slug}/change-ownership?error=unknown-new-entity"
            f"&detail={_q(new_entity_slug)}",
            status_code=303,
        )

    old_expense_prefix = f"Expenses:{old_entity}:Vehicle:{slug}:"
    new_expense_prefix = f"Expenses:{new_entity_slug}:Vehicle:{slug}:"
    # Income paths cover the vehicle-rental-business case (rental
    # fleet, ride-share, equipment rental, etc.) — every customer
    # rental posts to Income:<Entity>:Vehicle:<slug>:* on the entity
    # that owns the vehicle. A misattribution rename has to move
    # those legs along with the asset/expenses, otherwise income
    # reports split across two entities silently.
    old_income_prefix = f"Income:{old_entity}:Vehicle:{slug}:"
    new_income_prefix = f"Income:{new_entity_slug}:Vehicle:{slug}:"
    old_asset = f"Assets:{old_entity}:Vehicle:{slug}"
    new_asset = f"Assets:{new_entity_slug}:Vehicle:{slug}"

    overrides_path = settings.connector_overrides_path
    accounts_path = settings.connector_accounts_path

    # Counters for the redirect summary banner.
    counters = {"case_a": 0, "case_b": 0, "closed": 0}

    def _do_rename() -> None:
        from beancount.core.data import Open as _Open
        from lamella.core.registry.accounts_writer import (
            AccountsWriter as _AccountsWriter,
        )
        # 1. Open canonical chart under new entity (backdates to
        # earliest historical posting on this slug — see
        # ensure_vehicle_chart docstring). Plus any non-canonical
        # Income paths (rental fleets etc.) — vehicle_companion
        # doesn't include Income in the canonical chart, so we
        # scan the old entity's existing Income paths for this slug
        # and open mirrored paths on the new entity.
        ensure_vehicle_chart(
            conn=conn, settings=settings, reader=reader,
            vehicle_slug=slug, entity_slug=new_entity_slug,
        )
        reader.invalidate()
        live = list(reader.load().entries)
        existing_paths = {
            getattr(e, "account", None)
            for e in live
            if isinstance(e, _Open)
        }
        old_income_paths = {
            p for p in existing_paths
            if isinstance(p, str) and p.startswith(old_income_prefix)
        }
        new_income_paths = {
            new_income_prefix + p[len(old_income_prefix):]
            for p in old_income_paths
        }
        income_to_open = [
            p for p in new_income_paths if p not in existing_paths
        ]
        if income_to_open:
            # Find earliest income-leg posting date so the new
            # Open is backdated correctly.
            from beancount.core.data import Transaction as _Txn
            earliest = None
            from datetime import date as _D
            for e in live:
                if not isinstance(e, _Txn):
                    continue
                for p in e.postings or ():
                    acct = p.account or ""
                    if acct in old_income_paths:
                        d = (
                            e.date if isinstance(e.date, _D)
                            else _D.fromisoformat(str(e.date))
                        )
                        if earliest is None or d < earliest:
                            earliest = d
                        break
            opener = _AccountsWriter(
                main_bean=settings.ledger_main,
                connector_accounts=accounts_path,
            )
            opener.write_opens(
                income_to_open,
                comment=(
                    f"Vehicle rename — open Income paths under "
                    f"{new_entity_slug}:Vehicle:{slug} for rental "
                    f"income carryover"
                ),
                existing_paths=existing_paths,
                earliest_ref_by_path=(
                    {p: earliest for p in income_to_open}
                    if earliest else None
                ),
            )
            reader.invalidate()

        # 2. Case B — textual rewrite of existing override blocks.
        # Word-boundary regex keyed on negative lookbehind/-ahead for
        # the account-name character class so we never match a
        # substring of a longer account path. Three patterns: asset,
        # expense prefix, and income prefix (rental businesses).
        if overrides_path.exists():
            text = overrides_path.read_text(encoding="utf-8")
            asset_re = re.compile(
                rf"(?<![A-Za-z0-9:_\-]){re.escape(old_asset)}"
                rf"(?![A-Za-z0-9:_\-])"
            )
            expense_re = re.compile(
                rf"(?<![A-Za-z0-9:_\-]){re.escape(old_expense_prefix)}"
            )
            income_re = re.compile(
                rf"(?<![A-Za-z0-9:_\-]){re.escape(old_income_prefix)}"
            )
            new_text, n_asset = asset_re.subn(new_asset, text)
            new_text, n_expense = expense_re.subn(new_expense_prefix, new_text)
            new_text, n_income = income_re.subn(new_income_prefix, new_text)
            if new_text != text:
                overrides_path.write_text(new_text, encoding="utf-8")
                counters["case_b"] = n_asset + n_expense + n_income
                reader.invalidate()

        # 3. Case A — direct postings on old paths in original txns.
        entries = list(reader.load().entries)
        writer = OverrideWriter(
            main_bean=settings.ledger_main,
            overrides=overrides_path,
            conn=conn,
            run_check=False,  # envelope handles cross-file bean-check
        )
        for entry in entries:
            if not isinstance(entry, Transaction):
                continue
            if is_override_txn(entry):
                continue
            for p in entry.postings or ():
                acct = p.account or ""
                if acct == old_asset:
                    new_acct = new_asset
                elif acct.startswith(old_expense_prefix):
                    new_acct = new_expense_prefix + acct[len(old_expense_prefix):]
                elif acct.startswith(old_income_prefix):
                    new_acct = new_income_prefix + acct[len(old_income_prefix):]
                else:
                    continue
                if p.units is None or p.units.number is None:
                    continue
                amt = Decimal(p.units.number)
                # Credit-side originals (Income, Liability) post
                # negative; OverrideWriter's default `from: -amt,
                # to: +amt` would double them. Swap from/to so the
                # override neutralizes the original. Same fix as in
                # property rename. Vehicles WITH rental fleets need
                # this — vehicles without never trigger the branch.
                if amt >= 0:
                    swap_from, swap_to = acct, new_acct
                else:
                    swap_from, swap_to = new_acct, acct
                writer.append(
                    txn_date=(
                        entry.date
                        if isinstance(entry.date, _date_t)
                        else _date_t.fromisoformat(str(entry.date))
                    ),
                    txn_hash=_txn_hash(entry),
                    amount=abs(amt),
                    from_account=swap_from,
                    to_account=swap_to,
                    currency=p.units.currency or "USD",
                    narration=(
                        entry.narration
                        or f"vehicle rename {old_entity}→{new_entity_slug}"
                    ),
                    replace_existing=False,  # don't clobber prior pairs on same txn
                )
                counters["case_a"] += 1

        # 4. Close old-entity vehicle accounts that are still Open.
        # Use post-rewrite open_paths so we know what's actually open.
        # Includes Income paths (rental fleet) along with asset +
        # expense paths.
        reader.invalidate()
        entries = list(reader.load().entries)
        opens = open_paths(entries)
        old_paths = {
            p for p in opens
            if p == old_asset
            or p.startswith(old_expense_prefix)
            or p.startswith(old_income_prefix)
        }
        if old_paths:
            accounts_path.parent.mkdir(parents=True, exist_ok=True)
            if not accounts_path.exists():
                accounts_path.write_text(
                    "; connector_accounts.bean — managed by Lamella.\n",
                    encoding="utf-8",
                )
            today = _date_t.today().isoformat()
            close_block = "\n".join(
                f"{today} close {p}" for p in sorted(old_paths)
            )
            with accounts_path.open("a", encoding="utf-8") as fh:
                fh.write(f"\n; rename — close old-entity paths for {slug}\n")
                fh.write(close_block + "\n")
            counters["closed"] = len(old_paths)

        # 5. SQLite update.
        conn.execute(
            "UPDATE vehicles SET entity_slug = ? WHERE slug = ?",
            (new_entity_slug, slug),
        )
        conn.commit()

    try:
        recovery_write_envelope(
            main_bean=settings.ledger_main,
            files_to_snapshot=[
                settings.ledger_main, accounts_path, overrides_path,
            ],
            write_fn=_do_rename,
        )
    except BeanCheckError as exc:
        # SQLite was committed inside the envelope — but the envelope
        # rolled back the file writes. Roll back SQLite too so the DB
        # doesn't claim the vehicle moved when the ledger says
        # otherwise.
        conn.execute(
            "UPDATE vehicles SET entity_slug = ? WHERE slug = ?",
            (old_entity, slug),
        )
        conn.commit()
        return RedirectResponse(
            f"/vehicles/{slug}/change-ownership"
            f"?error=bean-check-rejected&detail={_q(str(exc)[:200])}",
            status_code=303,
        )
    except Exception as exc:  # noqa: BLE001
        conn.execute(
            "UPDATE vehicles SET entity_slug = ? WHERE slug = ?",
            (old_entity, slug),
        )
        conn.commit()
        log.exception("vehicle rename failed for %s", slug)
        return RedirectResponse(
            f"/vehicles/{slug}/change-ownership"
            f"?error={type(exc).__name__}&detail={_q(str(exc)[:200])}",
            status_code=303,
        )

    reader.invalidate()
    return RedirectResponse(
        f"/vehicles/{slug}?renamed_to={_q(new_entity_slug)}"
        f"&case_a={counters['case_a']}"
        f"&case_b={counters['case_b']}"
        f"&closed={counters['closed']}",
        status_code=303,
    )


@router.post("/vehicles/{slug}/change-ownership/transfer")
async def vehicle_change_ownership_transfer(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Intercompany transfer — vehicle changes hands between two
    entities the user owns. Records two ledger transactions atomically:

    * **OLD entity** — vehicle leaves at book value, cash leg lands in
      ``Assets:<Old>:Vehicle:<slug>:SaleClearing`` (the bank deposit
      reconciles here when SimpleFIN brings it), equity leg posts to
      ``Equity:<Old>:Vehicle:<slug>:SaleEquity``. Any gap between book
      value and (cash + equity) plugs to ``...:SaleRecapture`` for the
      CPA to reconcile (gain, loss, or recapture).
    * **NEW entity** — canonical vehicle chart scaffolded under the
      new entity, asset arrives at ``new_basis`` (carryover NBV / sale
      price / explicit), cash leg lands in ``...:PurchaseClearing``,
      equity leg posts to ``...:PurchaseEquity``.

    Form fields:
      * ``new_entity_slug`` — required, must be a different active entity
      * ``transfer_date`` — ISO date
      * ``cash_amount`` — Decimal ≥ 0
      * ``equity_amount`` — Decimal ≥ 0
      * ``basis_choice`` — ``carryover`` / ``sale_price`` / ``explicit``
      * ``basis_explicit`` — Decimal, required only when basis_choice=explicit

    Per the bookkeeper-not-tax product directive (see
    ``FEATURE_SETUP_4.2_4.3_QUESTIONS.md``): the system records facts;
    a CPA decides §1031 vs taxable-sale treatment, depreciation
    recapture characterization, etc. ``SaleRecapture`` is the
    visibly-named CPA-touchpoint for any gap.
    """
    from datetime import date as _date_t
    from decimal import Decimal as _D, InvalidOperation as _DInv
    from urllib.parse import quote as _q
    from beancount.core.data import Open
    from lamella.core.registry.accounts_writer import AccountsWriter
    from lamella.features.vehicles.vehicle_companion import (
        ensure_vehicle_chart,
    )
    from lamella.features.setup.recovery import recovery_write_envelope
    from lamella.features.vehicles.transfer_writer import (
        TransferDraft, new_transfer_id, render_acquisition_block,
        render_disposal_block, required_open_paths,
        vehicle_asset_path,
    )

    form = await request.form()
    new_entity_slug = (form.get("new_entity_slug") or "").strip()
    transfer_date_raw = (form.get("transfer_date") or "").strip()
    cash_raw = (form.get("cash_amount") or "0").strip() or "0"
    equity_raw = (form.get("equity_amount") or "0").strip() or "0"
    basis_choice = (form.get("basis_choice") or "").strip().lower()
    basis_explicit_raw = (form.get("basis_explicit") or "").strip()
    notes = (form.get("notes") or "").strip() or None

    base_redirect = f"/vehicles/{slug}/change-ownership"

    def _err(code: str, detail: str | None = None) -> RedirectResponse:
        url = f"{base_redirect}?error={code}"
        if detail:
            url += f"&detail={_q(detail[:200])}"
        return RedirectResponse(url, status_code=303)

    if not new_entity_slug:
        return _err("missing-new-entity")
    if not transfer_date_raw:
        return _err("missing-transfer-date")
    try:
        transfer_date = _date_t.fromisoformat(transfer_date_raw)
    except ValueError:
        return _err("invalid-transfer-date", transfer_date_raw)
    try:
        cash_amount = _D(cash_raw)
        equity_amount = _D(equity_raw)
    except _DInv:
        return _err("non-numeric-amount")
    if cash_amount < 0 or equity_amount < 0:
        return _err("negative-amount")
    if cash_amount + equity_amount == 0:
        return _err(
            "zero-transaction-value",
            "Either cash or equity must be > 0 — otherwise this is a "
            "rename, not a transfer. Use Option A on the same page.",
        )
    if basis_choice not in ("carryover", "sale_price", "explicit"):
        return _err("missing-basis-choice")

    row = conn.execute(
        "SELECT slug, display_name, entity_slug FROM vehicles WHERE slug = ?",
        (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"vehicle '{slug}' not found")
    old_entity = row["entity_slug"]
    if not old_entity:
        return _err("current-entity-missing")
    if old_entity == new_entity_slug:
        return _err("same-entity")
    if not conn.execute(
        "SELECT 1 FROM entities WHERE slug = ? AND is_active = 1",
        (new_entity_slug,),
    ).fetchone():
        return _err("unknown-new-entity", new_entity_slug)

    # Read current NBV from the ledger — the asset account's running
    # balance at transfer_date. Postings on or after transfer_date
    # don't count (the disposal we're about to write would be one).
    asset_account = vehicle_asset_path(old_entity, slug)
    entries = list(reader.load().entries)
    book_value = _D("0")
    from beancount.core.data import Transaction
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        e_date = (
            e.date if isinstance(e.date, _date_t)
            else _date_t.fromisoformat(str(e.date))
        )
        if e_date > transfer_date:
            continue
        for p in e.postings or ():
            if p.account == asset_account and p.units and p.units.number is not None:
                book_value += _D(p.units.number)
    if book_value < 0:
        # Negative NBV means the asset has been depreciated past zero
        # or already partially disposed. Refuse rather than silently
        # post a negative number.
        return _err(
            "negative-book-value",
            f"book value on {asset_account} is {book_value}; manual "
            "adjustment required before transfer",
        )

    if basis_choice == "carryover":
        new_basis = book_value
    elif basis_choice == "sale_price":
        new_basis = cash_amount + equity_amount
    else:  # explicit
        try:
            new_basis = _D(basis_explicit_raw or "0")
        except _DInv:
            return _err("non-numeric-basis")
        if new_basis < 0:
            return _err("negative-basis")

    transfer_id = new_transfer_id()
    draft = TransferDraft(
        transfer_id=transfer_id,
        vehicle_slug=slug,
        vehicle_display_name=row["display_name"],
        transfer_date=transfer_date,
        old_entity=old_entity,
        new_entity=new_entity_slug,
        book_value=book_value,
        cash_amount=cash_amount,
        equity_amount=equity_amount,
        new_basis=new_basis,
        notes=notes,
    )

    overrides_path = settings.connector_overrides_path
    accounts_path = settings.connector_accounts_path

    def _do_transfer() -> None:
        # 1. Open the canonical vehicle chart on the new entity (asset
        # + every Expenses sub-account). Idempotent; backdates to
        # earliest historical posting on this slug if any.
        ensure_vehicle_chart(
            conn=conn, settings=settings, reader=reader,
            vehicle_slug=slug, entity_slug=new_entity_slug,
        )
        reader.invalidate()

        # 2. Open every disposal/acquisition sub-account this draft
        # actually uses. AccountsWriter is idempotent against the live
        # set — already-open paths skip.
        live = list(reader.load().entries)
        existing_paths = {
            getattr(e, "account", None)
            for e in live
            if isinstance(e, Open)
        }
        wanted = required_open_paths(draft)
        to_open = [p for p in wanted if p and p not in existing_paths]
        if to_open:
            opener = AccountsWriter(
                main_bean=settings.ledger_main,
                connector_accounts=accounts_path,
            )
            opener.write_opens(
                to_open,
                comment=f"Vehicle transfer scaffolds for {slug}",
                existing_paths=existing_paths,
            )
            reader.invalidate()

        # 3. Append both ledger transactions to connector_overrides.bean
        # (same file the existing disposal_writer + override_writer use).
        from lamella.features.rules.overrides import (
            ensure_overrides_exists,
        )
        from lamella.core.ledger_writer import ensure_include_in_main
        ensure_overrides_exists(overrides_path)
        ensure_include_in_main(settings.ledger_main, overrides_path)
        with overrides_path.open("a", encoding="utf-8") as fh:
            fh.write(render_disposal_block(draft))
            fh.write(render_acquisition_block(draft))

        # 4. Mark old vehicle inactive; create new vehicle row on the
        # target entity. Carry over the existing display_name + meta
        # so reports tie out across the transfer date.
        transaction_value = cash_amount + equity_amount
        conn.execute(
            "UPDATE vehicles SET is_active = 0, sale_date = ?, "
            "sale_price = ?, disposal_txn_hash = ? WHERE slug = ?",
            (
                transfer_date.isoformat(),
                str(transaction_value),
                transfer_id,
                slug,
            ),
        )
        # On the new entity, register a fresh vehicle row reusing the
        # same slug. The slug is unique per entity in user perception
        # but the vehicles table keys on slug alone — so for now we
        # disambiguate via slug suffix to avoid the UNIQUE collision.
        # The natural disambiguation: appended-letter form (e.g.
        # V2009FabrikamSuvB on the new entity), which is what
        # disambiguate_slug already does for create-collisions. The
        # display_name stays the same so the user still recognizes
        # the vehicle.
        from lamella.core.registry.service import disambiguate_slug
        new_row_slug = disambiguate_slug(conn, slug, "vehicles") or (slug + "B")
        cols = conn.execute(
            "PRAGMA table_info(vehicles)",
        ).fetchall()
        col_names = [c["name"] for c in cols]

        # Pull source row as dict for selective copy.
        source = conn.execute(
            "SELECT * FROM vehicles WHERE slug = ?", (slug,),
        ).fetchone()
        new_values = {c: source[c] for c in col_names if c != "id"}
        new_values["slug"] = new_row_slug
        new_values["entity_slug"] = new_entity_slug
        new_values["is_active"] = 1
        new_values["sale_date"] = None
        new_values["sale_price"] = None
        new_values["disposal_txn_hash"] = None
        # Don't carry forward depreciation election or ownership
        # history rows — those are tied to the old entity's tax
        # context. Leave at SQL defaults.
        for nullable in (
            "purchase_date", "purchase_price", "purchase_fees",
        ):
            if nullable in new_values:
                # Carry purchase metadata so the new entity has source
                # data for its own basis tracking.
                pass
        placeholders = ", ".join("?" for _ in new_values)
        col_list = ", ".join(new_values.keys())
        try:
            conn.execute(
                f"INSERT INTO vehicles ({col_list}) VALUES ({placeholders})",
                tuple(new_values.values()),
            )
        except Exception:  # noqa: BLE001
            # If INSERT fails (schema drift / NOT NULL on a column
            # we didn't anticipate), surface as bean-check error so
            # the envelope rolls back the ledger writes too.
            log.exception("transfer: INSERT new vehicle row failed")
            raise
        conn.commit()

    try:
        recovery_write_envelope(
            main_bean=settings.ledger_main,
            files_to_snapshot=[
                settings.ledger_main, accounts_path, overrides_path,
            ],
            write_fn=_do_transfer,
        )
    except BeanCheckError as exc:
        # Roll back SQLite (the envelope rolled back files; SQLite
        # was committed inside _do_transfer).
        conn.execute(
            "UPDATE vehicles SET is_active = 1, sale_date = NULL, "
            "sale_price = NULL, disposal_txn_hash = NULL "
            "WHERE slug = ? AND disposal_txn_hash = ?",
            (slug, transfer_id),
        )
        conn.execute(
            "DELETE FROM vehicles WHERE entity_slug = ? "
            "AND display_name = ? AND is_active = 1 "
            "AND sale_date IS NULL",
            (new_entity_slug, row["display_name"]),
        )
        conn.commit()
        return _err("bean-check-rejected", str(exc))
    except Exception as exc:  # noqa: BLE001
        log.exception("vehicle transfer failed for %s", slug)
        # Same rollback as above.
        conn.execute(
            "UPDATE vehicles SET is_active = 1, sale_date = NULL, "
            "sale_price = NULL, disposal_txn_hash = NULL "
            "WHERE slug = ? AND disposal_txn_hash = ?",
            (slug, transfer_id),
        )
        conn.execute(
            "DELETE FROM vehicles WHERE entity_slug = ? "
            "AND display_name = ? AND is_active = 1 "
            "AND sale_date IS NULL",
            (new_entity_slug, row["display_name"]),
        )
        conn.commit()
        return _err(type(exc).__name__, str(exc))

    reader.invalidate()
    return RedirectResponse(
        f"/vehicles/{slug}?transferred_to={_q(new_entity_slug)}"
        f"&transfer_id={transfer_id}",
        status_code=303,
    )


@router.get("/vehicles/{slug}/edit", response_class=HTMLResponse)
def vehicle_edit(
    slug: str,
    request: Request,
    conn=Depends(get_db),
):
    row = conn.execute(
        "SELECT * FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    vehicle = dict(row)
    entities = list_entities(conn, include_inactive=False)
    ctx = {"vehicle": vehicle, "entities": entities}
    return request.app.state.templates.TemplateResponse(
        request, "vehicle_edit.html", ctx,
    )


# ---------------------------------------------------------------------------
# Write endpoints
# ---------------------------------------------------------------------------


@router.post("/vehicles")
async def save_vehicle(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn=Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    form = await request.form()
    raw_slug = (form.get("slug") or "").strip()
    display_name = (form.get("display_name") or "").strip() or None
    entity_slug_in = (form.get("entity_slug") or "").strip() or None

    slug = normalize_slug(raw_slug, fallback_display_name=display_name)
    if not slug:
        raise HTTPException(
            status_code=400,
            detail=(
                "Couldn't derive a valid slug — type a display name or "
                "an explicit slug starting with a capital letter."
            ),
        )

    year = form.get("year")
    make = (form.get("make") or "").strip() or None
    model = (form.get("model") or "").strip() or None
    vin = (form.get("vin") or "").strip() or None
    plate = (form.get("license_plate") or "").strip() or None
    purchase_date = (form.get("purchase_date") or "").strip() or None
    purchase_price = (form.get("purchase_price") or "").strip() or None
    sale_date = (form.get("sale_date") or "").strip() or None
    sale_price = (form.get("sale_price") or "").strip() or None
    current_mileage = form.get("current_mileage")
    is_active = form.get("is_active", "1")
    notes = (form.get("notes") or "").strip() or None
    purchase_fees = (form.get("purchase_fees") or "").strip() or None
    asset_account_path = (form.get("asset_account_path") or "").strip() or None
    # Phase 4 identity fields — optional, default NULL.
    gvwr_raw = (form.get("gvwr_lbs") or "").strip()
    gvwr_lbs = int(gvwr_raw) if gvwr_raw else None
    placed_in_service_date = (
        (form.get("placed_in_service_date") or "").strip() or None
    )
    fuel_type_raw = (form.get("fuel_type") or "").strip().lower() or None
    _FUEL_TYPES = {"gasoline", "diesel", "ev", "phev", "hybrid", "other"}
    fuel_type = fuel_type_raw if (
        fuel_type_raw is None or fuel_type_raw in _FUEL_TYPES
    ) else None

    # If the POST came from the New-Vehicle form (carries
    # intent=create), refuse to silently clobber an existing record.
    # Auto-suggest a numeric suffix so the user can confirm the
    # second Ram VanOne / identical-year vehicle is intentional.
    intent = (form.get("intent") or "").strip().lower()
    existing = conn.execute(
        "SELECT slug, entity_slug FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if existing and intent == "create":
        from lamella.core.registry.service import disambiguate_slug
        suggested = disambiguate_slug(conn, slug, "vehicles")
        raise HTTPException(
            status_code=409,
            detail=(
                f"Vehicle slug {slug!r} is already taken. "
                f"Try {suggested!r} instead — or use the edit page "
                f"if you meant to update the existing record."
            ),
        )
    if existing:
        # Entity reassignment is NOT allowed through the regular edit
        # form — it's a real-world event (sale or misattribution fix)
        # that needs the dedicated change-ownership flow. Silently
        # preserve the existing entity_slug even if the POST body
        # tries to change it.
        locked_entity_slug = existing["entity_slug"]
        if locked_entity_slug and entity_slug_in != locked_entity_slug:
            log.warning(
                "save_vehicle refused entity-slug change on %s: "
                "tried %r, kept %r (use /vehicles/%s/change-ownership)",
                slug, entity_slug_in, locked_entity_slug, slug,
            )
        effective_entity_slug = locked_entity_slug or entity_slug_in
        conn.execute(
            """
            UPDATE vehicles SET
                display_name = ?, year = ?, make = ?, model = ?, vin = ?,
                license_plate = ?, purchase_date = ?, purchase_price = ?,
                purchase_fees = ?,
                asset_account_path = COALESCE(NULLIF(?, ''), asset_account_path),
                sale_date = ?, sale_price = ?, current_mileage = ?,
                is_active = ?, notes = ?, entity_slug = ?,
                gvwr_lbs = ?, placed_in_service_date = ?, fuel_type = ?
            WHERE slug = ?
            """,
            (
                display_name, int(year) if year else None, make, model, vin,
                plate, purchase_date, purchase_price, purchase_fees,
                asset_account_path or "",
                sale_date, sale_price,
                int(current_mileage) if current_mileage else None,
                1 if is_active == "1" else 0, notes, effective_entity_slug,
                gvwr_lbs, placed_in_service_date, fuel_type,
                slug,
            ),
        )
        # Modal-edit response shape: when the form posted from the
        # /vehicles dashboard quick-edit modal, the HX-Target is
        # "vehicle-card-{slug}" and the modal expects the card partial
        # back with HX-Trigger=vehicle-saved so the page-level handler
        # closes the modal. Detected via HX-Target prefix; everything
        # else (focused /vehicles/{slug}/edit form, /vehicles/new) keeps
        # the legacy 303 redirect behavior.
        headers = {k.lower(): v for k, v in request.headers.items()}
        hx_target = headers.get("hx-target", "")
        if "hx-request" in headers and hx_target.startswith("vehicle-card-"):
            row = conn.execute(
                "SELECT v.*, e.display_name AS entity_display_name "
                "FROM vehicles v "
                "LEFT JOIN entities e ON e.slug = v.entity_slug "
                "WHERE v.slug = ?",
                (slug,),
            ).fetchone()
            today = date_t.today()
            year_now = today.year
            from lamella.features.mileage.service import MileageService
            service = MileageService(conn=conn, csv_path=None)
            rate_now = service.rate_for_date(today, fallback=settings.mileage_rate)
            loans_all = [
                dict(r) for r in conn.execute(
                    "SELECT slug, display_name, liability_account_path "
                    "FROM loans WHERE loan_type = 'auto' AND is_active = 1"
                ).fetchall()
            ]
            v_dict = dict(row)
            prefix = _vehicle_expense_prefix(v_dict)
            ytd_expense = Decimal("0")
            for entry in reader.load().entries:
                if not isinstance(entry, Transaction):
                    continue
                if entry.date.year != year_now:
                    continue
                for p in entry.postings:
                    if p.units is None or p.units.number is None:
                        continue
                    if (p.account or "").startswith(prefix):
                        ytd_expense += abs(Decimal(p.units.number))
            card = _card_for_vehicle(
                conn=conn, v=v_dict, year=year_now, rate=rate_now,
                loans_all=loans_all, ytd_expense=ytd_expense,
            )
            return request.app.state.templates.TemplateResponse(
                request, "partials/_vehicle_card.html", {"c": card},
                headers={"HX-Trigger": "vehicle-saved"},
            )
        return RedirectResponse(
            f"/vehicles/{slug}?saved=1", status_code=303,
        )

    computed_asset = asset_account_path or (
        f"Assets:{entity_slug_in}:Vehicles:{slug}" if entity_slug_in
        else f"Assets:Vehicles:{slug}"
    )
    conn.execute(
        """
        INSERT INTO vehicles
            (slug, display_name, year, make, model, vin, license_plate,
             purchase_date, purchase_price, purchase_fees, asset_account_path,
             sale_date, sale_price,
             current_mileage, is_active, notes, entity_slug,
             gvwr_lbs, placed_in_service_date, fuel_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            slug, display_name, int(year) if year else None, make, model, vin,
            plate, purchase_date, purchase_price, purchase_fees, computed_asset,
            sale_date, sale_price,
            int(current_mileage) if current_mileage else None,
            1 if is_active == "1" else 0, notes, entity_slug_in,
            gvwr_lbs, placed_in_service_date, fuel_type,
        ),
    )

    # Auto-scaffold the canonical per-vehicle chart. The old path
    # (gated on a `create_expense_tree` checkbox, using the legacy
    # `Expenses:Vehicles:<slug>:*` shape) wrote non-canonical paths
    # with "Vehicles" as segment 1 — which triggered discovery to
    # auto-register a phantom "Vehicles" entity on every boot. The
    # canonical builder in registry/vehicle_companion emits
    # `Expenses:<Entity>:Vehicle:<slug>:<cat>` (singular "Vehicle")
    # per CLAUDE.md and LEDGER_LAYOUT.md.
    create_tree = form.get("create_expense_tree") != "0"  # default on
    if create_tree and entity_slug_in:
        from lamella.features.vehicles.vehicle_companion import (
            ensure_vehicle_chart,
        )
        try:
            ensure_vehicle_chart(
                conn=conn, settings=settings, reader=reader,
                vehicle_slug=slug, entity_slug=entity_slug_in,
            )
        except BeanCheckError as exc:
            conn.execute("DELETE FROM vehicles WHERE slug = ?", (slug,))
            raise HTTPException(
                status_code=500, detail=f"bean-check failed: {exc}",
            )
    # Modal-add path: caller is HTMX with HX-Target == 'entities-grid'
    # equivalent or similar. Respond with HX-Refresh so the dashboard
    # reloads cleanly and the new card appears in its right section.
    # Falls through to the legacy 303 redirect for non-HTMX callers
    # (e.g. the focused /vehicles/new full-page form).
    headers = {k.lower(): v for k, v in request.headers.items()}
    if "hx-request" in headers:
        return HTMLResponse(
            "", status_code=200,
            headers={"HX-Refresh": "true"},
        )
    return RedirectResponse(f"/vehicles/{slug}?saved=1", status_code=303)


@router.post("/vehicles/{slug}/mileage")
async def save_mileage(
    slug: str,
    request: Request,
    conn=Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    form = await request.form()
    year = form.get("year")
    if not year:
        raise HTTPException(status_code=400, detail="year required")
    # Schedule C Part IV supplementary fields — all optional tri-state
    # (yes / no / unanswered). An empty string from the form becomes
    # None, which renders as an empty checkbox on the worksheet.
    def _tristate(name: str) -> int | None:
        v = (form.get(name) or "").strip()
        if v == "1":
            return 1
        if v == "0":
            return 0
        return None

    commute_days_raw = (form.get("commute_days") or "").strip()
    commute_days = int(commute_days_raw) if commute_days_raw else None

    conn.execute(
        """
        INSERT INTO vehicle_yearly_mileage
            (vehicle_slug, year, start_mileage, end_mileage,
             business_miles, commuting_miles, personal_miles,
             commute_days, other_vehicle_available_personal,
             vehicle_available_off_duty, has_evidence, evidence_is_written)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (vehicle_slug, year) DO UPDATE SET
            start_mileage                    = excluded.start_mileage,
            end_mileage                      = excluded.end_mileage,
            business_miles                   = excluded.business_miles,
            commuting_miles                  = excluded.commuting_miles,
            personal_miles                   = excluded.personal_miles,
            commute_days                     = excluded.commute_days,
            other_vehicle_available_personal = excluded.other_vehicle_available_personal,
            vehicle_available_off_duty       = excluded.vehicle_available_off_duty,
            has_evidence                     = excluded.has_evidence,
            evidence_is_written              = excluded.evidence_is_written
        """,
        (
            slug, int(year),
            int(form.get("start_mileage") or 0) or None,
            int(form.get("end_mileage") or 0) or None,
            int(form.get("business_miles") or 0) or None,
            int(form.get("commuting_miles") or 0) or None,
            int(form.get("personal_miles") or 0) or None,
            commute_days,
            _tristate("other_vehicle_available_personal"),
            _tristate("vehicle_available_off_duty"),
            _tristate("has_evidence"),
            _tristate("evidence_is_written"),
        ),
    )
    try:
        from lamella.features.vehicles.writer import append_vehicle_yearly_mileage
        append_vehicle_yearly_mileage(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            slug=slug, year=int(year),
            start_mileage=(int(form.get("start_mileage") or 0) or None),
            end_mileage=(int(form.get("end_mileage") or 0) or None),
            business_miles=(int(form.get("business_miles") or 0) or None),
            commuting_miles=(int(form.get("commuting_miles") or 0) or None),
            personal_miles=(int(form.get("personal_miles") or 0) or None),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("vehicle-yearly-mileage directive write failed for %s %s: %s", slug, year, exc)
    return RedirectResponse(f"/vehicles/{slug}?saved=mileage", status_code=303)


@router.post("/vehicles/{slug}/elections")
async def save_vehicle_election(
    slug: str,
    request: Request,
    conn=Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Upsert a per-(vehicle, tax_year) §179 / bonus / MACRS election
    record. Capture only — we don't validate amounts against §179
    caps or determine eligibility."""
    row = conn.execute(
        "SELECT slug FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    form = await request.form()
    try:
        tax_year = int((form.get("tax_year") or "").strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="tax_year is required")

    _METHODS = {"", "MACRS-5YR", "MACRS-SL", "bonus", "section-179"}
    method = (form.get("depreciation_method") or "").strip() or None
    if method is not None and method not in _METHODS:
        method = None

    def _opt(name: str) -> str | None:
        v = (form.get(name) or "").strip()
        return v or None

    business_pct_raw = (form.get("business_use_pct_override") or "").strip()
    try:
        business_pct = float(business_pct_raw) if business_pct_raw else None
    except ValueError:
        business_pct = None

    listed_raw = form.get("listed_property_qualified")
    listed = 1 if listed_raw == "1" else (0 if listed_raw == "0" else None)

    conn.execute(
        """
        INSERT INTO vehicle_elections
            (vehicle_slug, tax_year, depreciation_method,
             section_179_amount, bonus_depreciation_amount,
             basis_at_placed_in_service, business_use_pct_override,
             listed_property_qualified, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (vehicle_slug, tax_year) DO UPDATE SET
            depreciation_method         = excluded.depreciation_method,
            section_179_amount          = excluded.section_179_amount,
            bonus_depreciation_amount   = excluded.bonus_depreciation_amount,
            basis_at_placed_in_service  = excluded.basis_at_placed_in_service,
            business_use_pct_override   = excluded.business_use_pct_override,
            listed_property_qualified   = excluded.listed_property_qualified,
            notes                       = excluded.notes
        """,
        (
            slug, tax_year, method,
            _opt("section_179_amount"),
            _opt("bonus_depreciation_amount"),
            _opt("basis_at_placed_in_service"),
            business_pct,
            listed,
            _opt("notes"),
        ),
    )
    try:
        from lamella.features.vehicles.writer import append_vehicle_election
        append_vehicle_election(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            slug=slug, tax_year=tax_year,
            depreciation_method=method,
            section_179_amount=_opt("section_179_amount"),
            bonus_depreciation_amount=_opt("bonus_depreciation_amount"),
            basis_at_placed_in_service=_opt("basis_at_placed_in_service"),
            business_use_pct_override=business_pct,
            listed_property_qualified=listed,
            notes=_opt("notes"),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("vehicle-election directive write failed for %s %s: %s", slug, tax_year, exc)
    return RedirectResponse(
        f"/vehicles/{slug}?saved=election#elections", status_code=303,
    )


@router.post("/vehicles/{slug}/elections/{tax_year}/delete")
async def delete_vehicle_election(
    slug: str, tax_year: int, conn=Depends(get_db),
):
    conn.execute(
        "DELETE FROM vehicle_elections "
        "WHERE vehicle_slug = ? AND tax_year = ?",
        (slug, int(tax_year)),
    )
    return RedirectResponse(
        f"/vehicles/{slug}?saved=election_deleted#elections",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Disposal flow — GET form → POST preview → POST commit → optional revoke.
# ---------------------------------------------------------------------------


def _load_vehicle_or_404(conn, slug: str) -> dict:
    row = conn.execute(
        "SELECT * FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    return dict(row)


def _default_proceeds_account(vehicle: dict) -> str:
    entity = vehicle.get("entity_slug")
    return (
        f"Assets:{entity}:Checking" if entity
        else "Assets:Personal:Checking"
    )


def _default_gain_loss_account(vehicle: dict) -> str:
    entity = vehicle.get("entity_slug")
    return (
        f"Income:{entity}:CapitalGains:VehicleSale" if entity
        else "Income:Personal:CapitalGains:VehicleSale"
    )


def _latest_election_basis(conn, slug: str) -> Decimal | None:
    row = conn.execute(
        "SELECT basis_at_placed_in_service FROM vehicle_elections "
        "WHERE vehicle_slug = ? AND basis_at_placed_in_service IS NOT NULL "
        "ORDER BY tax_year DESC LIMIT 1",
        (slug,),
    ).fetchone()
    if row is None or not row["basis_at_placed_in_service"]:
        return None
    try:
        return Decimal(row["basis_at_placed_in_service"])
    except Exception:  # noqa: BLE001
        return None


@router.get("/vehicles/{slug}/dispose", response_class=HTMLResponse)
def dispose_form(
    slug: str, request: Request, conn=Depends(get_db),
):
    vehicle = _load_vehicle_or_404(conn, slug)
    cost_basis_default = (
        _latest_election_basis(conn, slug)
        or (
            (_decimal(vehicle.get("purchase_price")) or Decimal("0"))
            + (_decimal(vehicle.get("purchase_fees")) or Decimal("0"))
        )
    )
    return request.app.state.templates.TemplateResponse(
        request, "vehicle_disposal_form.html",
        {
            "vehicle": vehicle,
            "today": date_t.today().isoformat(),
            "default_proceeds_account": _default_proceeds_account(vehicle),
            "default_gain_loss_account": _default_gain_loss_account(vehicle),
            "default_basis": cost_basis_default,
            "valid_types": sorted(VALID_DISPOSAL_TYPES),
        },
    )


def _build_draft_from_form(
    *, vehicle: dict, form, disposal_id: str,
) -> DisposalDraft:
    try:
        d = date_t.fromisoformat((form.get("disposal_date") or "").strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="disposal_date is required")
    dtype = (form.get("disposal_type") or "").strip().lower()
    if dtype not in VALID_DISPOSAL_TYPES:
        raise HTTPException(status_code=400, detail="invalid disposal_type")
    try:
        proceeds = Decimal((form.get("proceeds_amount") or "0").strip() or "0")
        basis = Decimal((form.get("adjusted_basis") or "0").strip() or "0")
        accum_dep = Decimal(
            (form.get("accumulated_depreciation") or "0").strip() or "0"
        )
    except Exception:  # noqa: BLE001
        raise HTTPException(
            status_code=400, detail="proceeds / basis / depreciation must be numeric",
        )
    proceeds_account = (form.get("proceeds_account") or "").strip()
    gain_loss_account = (form.get("gain_loss_account") or "").strip()
    if not proceeds_account or not gain_loss_account:
        raise HTTPException(
            status_code=400,
            detail="proceeds_account and gain_loss_account are required",
        )
    asset_account = (
        vehicle.get("asset_account_path")
        or (
            f"Assets:{vehicle['entity_slug']}:Vehicles:{vehicle['slug']}"
            if vehicle.get("entity_slug")
            else f"Assets:Vehicles:{vehicle['slug']}"
        )
    )
    # Book-value out = adjusted_basis − accumulated_depreciation.
    # That's what actually comes off the Assets account.
    asset_amount_out = (basis - accum_dep)
    # Plug = proceeds − (basis − depreciation). Positive = gain
    # (income account); negative = loss (goes negative on the income
    # account, or positive on an expense account depending on how the
    # user wired it).
    gain_loss = compute_gain_loss(
        proceeds=proceeds,
        adjusted_basis=basis,
        accumulated_depreciation=accum_dep,
    )
    # Beancount convention: Income accounts carry negative sign for
    # income entries. A positive plug reads as a gain; to post to an
    # Income account we negate.
    if gain_loss_account.startswith("Income:"):
        gain_loss_post = -gain_loss
    else:
        gain_loss_post = gain_loss
    return DisposalDraft(
        disposal_id=disposal_id,
        vehicle_slug=vehicle["slug"],
        vehicle_display_name=vehicle.get("display_name"),
        disposal_date=d,
        disposal_type=dtype,
        proceeds_amount=proceeds,
        proceeds_account=proceeds_account,
        asset_account=asset_account,
        asset_amount_out=asset_amount_out,
        gain_loss_account=gain_loss_account,
        gain_loss_amount=gain_loss_post,
        buyer_or_party=(form.get("buyer_or_party") or "").strip() or None,
        notes=(form.get("notes") or "").strip() or None,
    )


@router.post("/vehicles/{slug}/dispose", response_class=HTMLResponse)
async def dispose_preview(
    slug: str, request: Request, conn=Depends(get_db),
):
    vehicle = _load_vehicle_or_404(conn, slug)
    form = await request.form()
    # Generate the disposal_id now and pass it through to /commit as
    # a hidden input so the two renderings — preview and commit —
    # share one identity.
    disposal_id = new_disposal_id()
    draft = _build_draft_from_form(
        vehicle=vehicle, form=form, disposal_id=disposal_id,
    )
    preview_block = render_disposal_block(draft)
    gain_loss_raw = compute_gain_loss(
        proceeds=draft.proceeds_amount,
        adjusted_basis=Decimal((form.get("adjusted_basis") or "0").strip() or "0"),
        accumulated_depreciation=Decimal(
            (form.get("accumulated_depreciation") or "0").strip() or "0",
        ),
    )
    return request.app.state.templates.TemplateResponse(
        request, "vehicle_disposal_preview.html",
        {
            "vehicle": vehicle,
            "draft": draft,
            "preview_block": preview_block,
            "gain_loss_raw": gain_loss_raw,
            # Mirror every form field back as a hidden input so /commit
            # re-renders the same draft from the same data.
            "form_data": {k: form.get(k) for k in [
                "disposal_date", "disposal_type", "proceeds_amount",
                "proceeds_account", "gain_loss_account", "adjusted_basis",
                "accumulated_depreciation", "buyer_or_party", "notes",
            ]},
            "disposal_id": disposal_id,
        },
    )


@router.post("/vehicles/{slug}/dispose/commit")
async def dispose_commit(
    slug: str, request: Request,
    settings: Settings = Depends(get_settings),
    conn=Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    vehicle = _load_vehicle_or_404(conn, slug)
    form = await request.form()
    disposal_id = (form.get("disposal_id") or "").strip()
    if not disposal_id:
        raise HTTPException(status_code=400, detail="disposal_id is required")
    # Idempotency: if this disposal_id already committed, bail out
    # rather than double-writing.
    existing = conn.execute(
        "SELECT disposal_id FROM vehicle_disposals WHERE disposal_id = ?",
        (disposal_id,),
    ).fetchone()
    if existing:
        return RedirectResponse(
            f"/vehicles/{slug}?saved=disposal_committed#disposals",
            status_code=303,
        )

    draft = _build_draft_from_form(
        vehicle=vehicle, form=form, disposal_id=disposal_id,
    )

    # Ensure the proceeds + gain/loss accounts are open on or before
    # the disposal date — otherwise bean-check rejects the posting.
    opener = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    existing_paths = {
        getattr(entry, "account", None)
        for entry in reader.load().entries
        if isinstance(getattr(entry, "account", None), str)
    }
    wanted = {draft.proceeds_account, draft.gain_loss_account}
    to_open = [p for p in wanted if p and p not in existing_paths]
    if to_open:
        try:
            opener.write_opens(
                to_open,
                comment=f"Vehicle disposal for {slug}",
                existing_paths=existing_paths,
            )
        except BeanCheckError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"bean-check failed opening disposal accounts: {exc}",
            )
        reader.invalidate()

    try:
        write_disposal(
            draft=draft,
            main_bean=settings.ledger_main,
            overrides_path=settings.connector_overrides_path,
        )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")

    # Cache row + mark the vehicle as sold + stamp the disposal_id
    # onto vehicles.disposal_txn_hash so the card can badge "Sold
    # (ledger transaction written)".
    conn.execute(
        """
        INSERT INTO vehicle_disposals
            (disposal_id, vehicle_slug, disposal_date, disposal_type,
             proceeds_amount, buyer_or_party, proceeds_account,
             gain_loss_account, adjusted_basis, accumulated_depreciation,
             notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            disposal_id, slug, draft.disposal_date.isoformat(),
            draft.disposal_type,
            str(draft.proceeds_amount),
            draft.buyer_or_party,
            draft.proceeds_account,
            draft.gain_loss_account,
            (form.get("adjusted_basis") or "").strip() or None,
            (form.get("accumulated_depreciation") or "").strip() or None,
            draft.notes,
        ),
    )
    conn.execute(
        "UPDATE vehicles SET sale_date = ?, sale_price = ?, "
        "disposal_txn_hash = ?, is_active = 0 "
        "WHERE slug = ?",
        (
            draft.disposal_date.isoformat(),
            str(draft.proceeds_amount),
            disposal_id, slug,
        ),
    )
    reader.invalidate()
    return RedirectResponse(
        f"/vehicles/{slug}?saved=disposal_committed#disposals",
        status_code=303,
    )


@router.post("/vehicles/{slug}/dispose/{disposal_id}/revoke")
async def dispose_revoke(
    slug: str, disposal_id: str,
    settings: Settings = Depends(get_settings),
    conn=Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Write an offsetting transaction for a previously committed
    disposal. The original row stays in the ledger (audit trail); the
    offset nets its money movement to zero. User can then file a
    replacement disposal with a fresh disposal_id."""
    vehicle = _load_vehicle_or_404(conn, slug)
    row = conn.execute(
        "SELECT * FROM vehicle_disposals WHERE disposal_id = ? AND vehicle_slug = ?",
        (disposal_id, slug),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="disposal not found")
    if row["revokes_disposal_id"] is not None:
        raise HTTPException(status_code=400, detail="cannot revoke a revoke row")
    if row["revoked_by_disposal_id"] is not None:
        raise HTTPException(status_code=400, detail="already revoked")

    # Rebuild the DisposalDraft from the stored row so the reversing
    # transaction mirrors the original exactly.
    try:
        odate = date_t.fromisoformat(str(row["disposal_date"])[:10])
    except ValueError:
        raise HTTPException(status_code=500, detail="invalid disposal_date in row")
    proceeds = Decimal(row["proceeds_amount"] or "0")
    basis = Decimal(row["adjusted_basis"] or "0") if row["adjusted_basis"] else Decimal("0")
    accum = Decimal(row["accumulated_depreciation"] or "0") if row["accumulated_depreciation"] else Decimal("0")
    asset_account = (
        vehicle.get("asset_account_path")
        or (
            f"Assets:{vehicle['entity_slug']}:Vehicles:{vehicle['slug']}"
            if vehicle.get("entity_slug")
            else f"Assets:Vehicles:{vehicle['slug']}"
        )
    )
    gl = compute_gain_loss(
        proceeds=proceeds, adjusted_basis=basis, accumulated_depreciation=accum,
    )
    if (row["gain_loss_account"] or "").startswith("Income:"):
        gl_post = -gl
    else:
        gl_post = gl
    original = DisposalDraft(
        disposal_id=disposal_id,
        vehicle_slug=slug,
        vehicle_display_name=vehicle.get("display_name"),
        disposal_date=odate,
        disposal_type=row["disposal_type"],
        proceeds_amount=proceeds,
        proceeds_account=row["proceeds_account"] or "",
        asset_account=asset_account,
        asset_amount_out=(basis - accum),
        gain_loss_account=row["gain_loss_account"] or "",
        gain_loss_amount=gl_post,
        buyer_or_party=row["buyer_or_party"],
        notes=row["notes"],
    )

    revoke_id = new_disposal_id()
    try:
        write_revoke(
            revoke_id=revoke_id,
            original=original,
            main_bean=settings.ledger_main,
            overrides_path=settings.connector_overrides_path,
        )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")

    conn.execute(
        """
        INSERT INTO vehicle_disposals
            (disposal_id, vehicle_slug, disposal_date, disposal_type,
             proceeds_amount, proceeds_account, gain_loss_account,
             adjusted_basis, accumulated_depreciation,
             revokes_disposal_id, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            revoke_id, slug, date_t.today().isoformat(),
            row["disposal_type"],
            str(proceeds),
            row["proceeds_account"],
            row["gain_loss_account"],
            row["adjusted_basis"],
            row["accumulated_depreciation"],
            disposal_id,
            "Revoke of " + disposal_id,
        ),
    )
    conn.execute(
        "UPDATE vehicle_disposals SET revoked_by_disposal_id = ? "
        "WHERE disposal_id = ?",
        (revoke_id, disposal_id),
    )
    # Vehicle is no longer "sold" for card-badge purposes — the
    # revoke offset cancels out the disposal transaction. The user
    # can either file a replacement (fresh disposal) or re-activate
    # the vehicle.
    conn.execute(
        "UPDATE vehicles SET sale_date = NULL, sale_price = NULL, "
        "disposal_txn_hash = NULL, is_active = 1 WHERE slug = ?",
        (slug,),
    )
    reader.invalidate()
    return RedirectResponse(
        f"/vehicles/{slug}?saved=disposal_revoked#disposals",
        status_code=303,
    )


@router.post("/vehicles/{slug}/promote-trips")
async def promote_trips_to_yearly(
    slug: str,
    request: Request,
    year: int | None = None,
    conn=Depends(get_db),
):
    """Copy the trip rollup for `year` into the vehicle_yearly_mileage
    row so the user doesn't retype the numbers they already logged.

    - Reads business / commuting / personal miles from mileage_trip_meta
      via the tolerant (vehicle_slug OR display-name) predicate used
      everywhere else on this page.
    - Pulls min(odometer_start) + max(odometer_end) for the year from
      the trip log so the Schedule C Part IV start / end columns come
      straight from the odometer history.
    - Idempotent: ON CONFLICT upserts over any existing row.
    """
    row = conn.execute(
        "SELECT slug, display_name FROM vehicles WHERE slug = ?",
        (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    vehicle = {"slug": row["slug"], "display_name": row["display_name"]}

    if year is None:
        form = await request.form()
        raw = form.get("year") or str(date_t.today().year)
        try:
            year = int(raw)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid year")

    breakdown = _year_mileage_breakdown(conn, vehicle, int(year))
    names = _mileage_filter_values(vehicle)
    placeholders = ",".join(["?"] * len(names)) if names else "''"

    # Odometer: pull start/end from the trip log's actual readings.
    odo_row = conn.execute(
        f"""
        SELECT MIN(odometer_start) AS odo_start,
               MAX(odometer_end)   AS odo_end
          FROM mileage_entries
         WHERE entry_date >= ? AND entry_date < ?
           AND (vehicle_slug = ? OR vehicle IN ({placeholders}))
        """,
        (
            f"{int(year):04d}-01-01", f"{int(year) + 1:04d}-01-01",
            slug, *names,
        ),
    ).fetchone()
    odo_start = int(odo_row["odo_start"]) if odo_row and odo_row["odo_start"] is not None else None
    odo_end = int(odo_row["odo_end"]) if odo_row and odo_row["odo_end"] is not None else None

    biz = int(round(breakdown.get("business") or 0)) or None
    com = int(round(breakdown.get("commuting") or 0)) or None
    per = int(round(breakdown.get("personal") or 0)) or None

    conn.execute(
        """
        INSERT INTO vehicle_yearly_mileage
            (vehicle_slug, year, start_mileage, end_mileage,
             business_miles, commuting_miles, personal_miles)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (vehicle_slug, year) DO UPDATE SET
            start_mileage   = excluded.start_mileage,
            end_mileage     = excluded.end_mileage,
            business_miles  = excluded.business_miles,
            commuting_miles = excluded.commuting_miles,
            personal_miles  = excluded.personal_miles
        """,
        (slug, int(year), odo_start, odo_end, biz, com, per),
    )
    return RedirectResponse(
        f"/vehicles/{slug}?year={int(year)}&saved=promoted",
        status_code=303,
    )


@router.post("/vehicles/{slug}/fuel")
async def add_vehicle_fuel_event(
    slug: str, request: Request,
    conn=Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Record a fuel / charging event. Quantity + unit are required;
    cost / odometer / location are all optional so EV home-charging
    (often no per-session cost available) still records as a
    provenance row."""
    row = conn.execute(
        "SELECT slug FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    form = await request.form()

    try:
        as_of = date_t.fromisoformat((form.get("as_of_date") or "").strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="as_of_date is required")

    fuel_type = (form.get("fuel_type") or "").strip().lower()
    unit = (form.get("unit") or "").strip().lower()
    qty_raw = (form.get("quantity") or "").strip()
    if not qty_raw:
        raise HTTPException(status_code=400, detail="quantity is required")
    try:
        quantity = float(qty_raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="quantity must be numeric")

    cost_raw = (form.get("cost_usd") or "").strip()
    cost_cents: int | None = None
    if cost_raw:
        try:
            # ADR-0022: parse user-entered USD as Decimal before
            # converting to integer cents — avoids binary-float drift
            # at the boundary.
            cost_cents = int(Decimal(cost_raw) * 100)
        except (ValueError, InvalidOperation):
            raise HTTPException(status_code=400, detail="cost must be numeric")
    odometer_raw = (form.get("odometer") or "").strip()
    odometer: int | None = int(odometer_raw) if odometer_raw else None

    try:
        add_fuel_event(
            conn,
            vehicle_slug=slug,
            as_of_date=as_of,
            fuel_type=fuel_type,
            quantity=quantity,
            unit=unit,
            cost_cents=cost_cents,
            odometer=odometer,
            location=(form.get("location") or "").strip() or None,
            notes=(form.get("notes") or "").strip() or None,
            connector_config_path=settings.connector_config_path,
            main_bean_path=settings.ledger_main,
        )
    except FuelValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return RedirectResponse(
        f"/vehicles/{slug}?saved=fuel#fuel", status_code=303,
    )


@router.post("/vehicles/{slug}/fuel/import")
def import_vehicle_fuel_events(
    slug: str,
    request: Request,
    target_year: int = Form(...),
    conn=Depends(get_db),
):
    row = conn.execute(
        "SELECT slug, entity_slug FROM vehicles WHERE slug = ? AND active = 1",
        (slug,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="vehicle not found")
    cands = _fuel_import_candidates(
        conn, vehicle_slug=slug, entity_slug=row["entity_slug"], year=int(target_year),
    )
    imported = 0
    for c in cands:
        try:
            add_fuel_event(
                conn,
                vehicle_slug=slug,
                as_of_date=date_t.fromisoformat(c["txn_date"]),
                fuel_type="gasoline",
                quantity=1.0,
                unit="gallon",
                cost_cents=int(round(float(c["amount"]) * 100)),
                notes=f"raw_row_id:{c['raw_row_id']}",
                location=c["payee"][:120] or None,
                source="schedule_c_import",
                connector_config_path=request.app.state.settings.connector_config_path,
                main_bean_path=request.app.state.settings.main_bean_path,
            )
            imported += 1
        except Exception:
            continue
    return RedirectResponse(
        f"/vehicles/{slug}?year={int(target_year)}&saved=fuel_import_{imported}#fuel",
        status_code=303,
    )


@router.post("/vehicles/{slug}/fuel/{event_id:int}/delete")
def delete_vehicle_fuel_event(
    slug: str, event_id: int, conn=Depends(get_db),
):
    delete_fuel_event(conn, event_id)
    return RedirectResponse(
        f"/vehicles/{slug}?saved=fuel_deleted#fuel", status_code=303,
    )


# ---------------------------------------------------------------------
# Phase 6 — credits, renewals, attribution override.
# ---------------------------------------------------------------------


@router.post("/vehicles/{slug}/credits")
async def add_vehicle_credit(
    slug: str, request: Request,
    conn=Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    row = conn.execute(
        "SELECT slug FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    form = await request.form()
    try:
        tax_year = int((form.get("tax_year") or "").strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="tax_year is required")
    credit_label = (form.get("credit_label") or "").strip()
    if not credit_label:
        raise HTTPException(status_code=400, detail="credit_label is required")
    add_credit(
        conn, vehicle_slug=slug, tax_year=tax_year,
        credit_label=credit_label,
        amount=(form.get("amount") or "").strip() or None,
        status=(form.get("status") or "").strip() or None,
        notes=(form.get("notes") or "").strip() or None,
        connector_config_path=settings.connector_config_path,
        main_bean_path=settings.ledger_main,
    )
    return RedirectResponse(
        f"/vehicles/{slug}?saved=credit#credits", status_code=303,
    )


@router.post("/vehicles/{slug}/credits/{credit_id:int}/delete")
def delete_vehicle_credit(
    slug: str, credit_id: int, conn=Depends(get_db),
):
    delete_credit(conn, credit_id)
    return RedirectResponse(
        f"/vehicles/{slug}?saved=credit_deleted#credits", status_code=303,
    )


@router.post("/vehicles/{slug}/renewals")
async def add_vehicle_renewal(
    slug: str, request: Request,
    conn=Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    row = conn.execute(
        "SELECT slug FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    form = await request.form()
    kind = (form.get("renewal_kind") or "").strip().lower()
    if kind not in VALID_RENEWAL_KINDS:
        raise HTTPException(status_code=400, detail="invalid renewal_kind")
    try:
        due = date_t.fromisoformat((form.get("due_date") or "").strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="due_date is required")
    cadence_raw = (form.get("cadence_months") or "").strip()
    cadence = int(cadence_raw) if cadence_raw else None
    add_renewal(
        conn, vehicle_slug=slug, renewal_kind=kind,
        due_date=due, cadence_months=cadence,
        notes=(form.get("notes") or "").strip() or None,
        connector_config_path=settings.connector_config_path,
        main_bean_path=settings.ledger_main,
    )
    return RedirectResponse(
        f"/vehicles/{slug}?saved=renewal#renewals", status_code=303,
    )


@router.post("/vehicles/{slug}/renewals/{renewal_id:int}/complete")
def complete_vehicle_renewal(
    slug: str, renewal_id: int, conn=Depends(get_db),
):
    complete_renewal(conn, renewal_id)
    return RedirectResponse(
        f"/vehicles/{slug}?saved=renewal_completed#renewals",
        status_code=303,
    )


@router.post("/vehicles/{slug}/renewals/{renewal_id:int}/delete")
def delete_vehicle_renewal(
    slug: str, renewal_id: int, conn=Depends(get_db),
):
    delete_renewal(conn, renewal_id)
    return RedirectResponse(
        f"/vehicles/{slug}?saved=renewal_deleted#renewals",
        status_code=303,
    )


# ---------------------------------------------------------------------
# Phase 7 — recurring trip templates.
# ---------------------------------------------------------------------


@router.get("/vehicle-templates", response_class=HTMLResponse)
def vehicle_templates_index(
    request: Request, conn=Depends(get_db),
):
    templates = list_templates(conn, include_inactive=True)
    vehicles = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name, entity_slug FROM vehicles "
            "WHERE is_active = 1 ORDER BY COALESCE(display_name, slug)"
        ).fetchall()
    ]
    return request.app.state.templates.TemplateResponse(
        request, "vehicle_trip_templates.html",
        {"templates": templates, "vehicles": vehicles},
    )


@router.post("/vehicle-templates")
async def save_vehicle_template(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn=Depends(get_db),
):
    form = await request.form()
    from lamella.core.registry.service import normalize_slug
    raw_slug = (form.get("slug") or "").strip()
    display = (form.get("display_name") or "").strip() or None
    slug = normalize_slug(raw_slug, fallback_display_name=display)
    if not slug or not display:
        raise HTTPException(
            status_code=400,
            detail="slug + display_name required (slug derives from display_name if blank)",
        )
    miles_raw = (form.get("default_miles") or "").strip()
    try:
        miles = float(miles_raw) if miles_raw else None
    except ValueError:
        raise HTTPException(status_code=400, detail="default_miles must be numeric")
    upsert_template(
        conn,
        slug=slug,
        display_name=display,
        vehicle_slug=(form.get("vehicle_slug") or "").strip() or None,
        entity=(form.get("entity") or "").strip() or None,
        default_from=(form.get("default_from") or "").strip() or None,
        default_to=(form.get("default_to") or "").strip() or None,
        default_purpose=(form.get("default_purpose") or "").strip() or None,
        default_miles=miles,
        default_category=(form.get("default_category") or "").strip() or None,
        is_round_trip=form.get("is_round_trip") == "1",
        is_active=form.get("is_active", "1") == "1",
    )
    # Stamp directive so the template survives DB wipe.
    try:
        from lamella.features.vehicles.writer import append_vehicle_trip_template
        append_vehicle_trip_template(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            slug=slug, display_name=display,
            vehicle_slug=(form.get("vehicle_slug") or "").strip() or None,
            entity=(form.get("entity") or "").strip() or None,
            default_from=(form.get("default_from") or "").strip() or None,
            default_to=(form.get("default_to") or "").strip() or None,
            default_purpose=(form.get("default_purpose") or "").strip() or None,
            default_miles=miles,
            default_category=(form.get("default_category") or "").strip() or None,
            is_round_trip=form.get("is_round_trip") == "1",
            is_active=form.get("is_active", "1") == "1",
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("trip-template directive write failed: %s", exc)
    return RedirectResponse("/vehicle-templates?saved=1", status_code=303)


@router.post("/vehicle-templates/{slug}/delete")
def delete_vehicle_template(
    slug: str, conn=Depends(get_db),
):
    delete_template(conn, slug)
    return RedirectResponse("/vehicle-templates?saved=deleted", status_code=303)


@router.post("/vehicles/{slug}/attribution")
async def set_vehicle_trip_attribution(
    slug: str, request: Request,
    settings: Settings = Depends(get_settings),
    conn=Depends(get_db),
):
    """Upsert the per-trip attribution override on mileage_trip_meta.
    Phase 6 persists it as SQLite state + stamps a
    `custom "mileage-attribution"` directive via the Phase 7 writer
    so the override survives a DB wipe."""
    row = conn.execute(
        "SELECT slug FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    form = await request.form()
    try:
        entry_date = date_t.fromisoformat(
            (form.get("entry_date") or "").strip()
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="entry_date required")
    vehicle = (form.get("vehicle") or "").strip()
    try:
        miles = float((form.get("miles") or "").strip() or "0")
    except ValueError:
        raise HTTPException(status_code=400, detail="miles must be numeric")
    if not vehicle or miles <= 0:
        raise HTTPException(
            status_code=400, detail="vehicle and positive miles required",
        )
    new_entity = (form.get("attributed_entity") or "").strip() or None
    set_trip_attribution(
        conn, entry_date=entry_date, vehicle=vehicle,
        miles=miles, attributed_entity=new_entity,
    )
    # Phase 7 writer stamps the directive; import lazily to avoid a
    # circular import when Phase 7 is not yet in scope during partial
    # deploys.
    try:
        from lamella.features.vehicles.writer import (
            append_mileage_attribution,
            append_mileage_attribution_revoked,
        )
        if new_entity:
            append_mileage_attribution(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                entry_date=entry_date, vehicle=vehicle, miles=miles,
                attributed_entity=new_entity,
            )
        else:
            append_mileage_attribution_revoked(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                entry_date=entry_date, vehicle=vehicle, miles=miles,
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("mileage-attribution directive write failed: %s", exc)
    return RedirectResponse(
        f"/vehicles/{slug}?saved=attribution#allocation",
        status_code=303,
    )


@router.post("/vehicles/{slug}/valuations")
async def add_vehicle_valuation(
    slug: str,
    request: Request,
    conn=Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    row = conn.execute(
        "SELECT slug FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="vehicle not found")
    form = await request.form()
    as_of_date = (form.get("as_of_date") or "").strip()
    value = (form.get("value") or "").strip()
    if not as_of_date or not value:
        raise HTTPException(
            status_code=400, detail="as_of_date and value required",
        )
    source = (form.get("source") or "").strip() or None
    notes = (form.get("notes") or "").strip() or None
    if notes and "data-confirm-button=" in notes:
        notes = notes.split('data-confirm-button="', 1)[0].strip() or None
    try:
        conn.execute(
            """
            INSERT INTO vehicle_valuations
                (vehicle_slug, as_of_date, value, source, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (vehicle_slug, as_of_date) DO UPDATE SET
                value = excluded.value,
                source = excluded.source,
                notes = excluded.notes
            """,
            (slug, as_of_date, value, source, notes),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    try:
        from datetime import date as _date_t
        from lamella.features.vehicles.writer import append_vehicle_valuation
        append_vehicle_valuation(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            slug=slug,
            as_of_date=_date_t.fromisoformat(as_of_date),
            value=value, source=source, notes=notes,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "vehicle-valuation directive write failed for %s %s: %s",
            slug, as_of_date, exc,
        )
    return RedirectResponse(
        f"/vehicles/{slug}?saved=valuation", status_code=303,
    )


@router.post("/vehicles/{slug}/valuations/{valuation_id}/delete")
def delete_vehicle_valuation(
    slug: str,
    valuation_id: int,
    conn=Depends(get_db),
):
    conn.execute(
        "DELETE FROM vehicle_valuations WHERE id = ? AND vehicle_slug = ?",
        (valuation_id, slug),
    )
    return RedirectResponse(
        f"/vehicles/{slug}?saved=valuation-removed", status_code=303,
    )


# ---------------------------------------------------------------------------
# Legacy /settings/vehicles* redirects — keep existing bookmarks working.
# ---------------------------------------------------------------------------


@router.get("/settings/vehicles")
def vehicles_settings_legacy_redirect():
    return RedirectResponse("/vehicles", status_code=307)


@router.post("/settings/vehicles")
async def vehicles_settings_save_legacy_redirect(request: Request):
    return RedirectResponse("/vehicles", status_code=307)


@router.get("/settings/vehicles/{slug}")
def vehicles_settings_detail_legacy_redirect(slug: str):
    return RedirectResponse(f"/vehicles/{slug}", status_code=307)


@router.post("/settings/vehicles/{slug}/mileage")
async def vehicles_settings_mileage_legacy_redirect(slug: str):
    return RedirectResponse(f"/vehicles/{slug}/mileage", status_code=307)


@router.post("/settings/vehicles/{slug}/valuations")
async def vehicles_settings_valuations_legacy_redirect(slug: str):
    return RedirectResponse(
        f"/vehicles/{slug}/valuations", status_code=307,
    )


@router.post("/settings/vehicles/{slug}/valuations/{valuation_id}/delete")
def _legacy_delete_valuation(slug: str, valuation_id: int):
    return RedirectResponse(
        f"/vehicles/{slug}/valuations/{valuation_id}/delete",
        status_code=307,
    )
