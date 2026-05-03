# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3
from datetime import date as date_t
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import (
    get_app_settings_store,
    get_db,
    get_ledger_reader,
    get_settings,
)
from lamella.features.mileage.beancount_writer import (
    MileageBeancountWriter,
    MileageSummaryError,
)
from lamella.features.mileage.import_parser import parse_input
from lamella.features.mileage.service import (
    ImportPreviewRow,
    MileageService,
    MileageValidationError,
)
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.settings.store import AppSettingsStore

log = logging.getLogger(__name__)

router = APIRouter()


def _known_entities(reader: LedgerReader) -> list[str]:
    """Derive the allowed entity list from the ledger's open directives
    (mirrors the /note flow's entity list). Falls back to an empty list so
    the UI shows a free-text input."""
    from beancount.core.data import Open

    seen: set[str] = set()
    for entry in reader.load().entries:
        if not isinstance(entry, Open):
            continue
        parts = entry.account.split(":")
        if len(parts) >= 2 and parts[0] in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
            seen.add(parts[1])
    return sorted(seen)


def _known_vehicles(
    conn: sqlite3.Connection,
) -> list[dict]:
    """Return a list of {name, slug, entity_slug} vehicle options,
    drawn exclusively from the `vehicles` registry table (/settings/
    vehicles). Inactive vehicles are excluded — retire a vehicle by
    toggling is_active off, not by deleting its mileage history."""
    out: list[dict] = []
    try:
        rows = conn.execute(
            "SELECT slug, display_name, entity_slug FROM vehicles "
            "WHERE is_active = 1 "
            "ORDER BY COALESCE(year, 9999) DESC, "
            "         COALESCE(display_name, slug)"
        ).fetchall()
    except Exception:  # noqa: BLE001
        return []
    from lamella.core.registry.alias import entity_label
    for r in rows:
        name = r["display_name"] or r["slug"]
        out.append({
            "name": name,
            "slug": r["slug"],
            "entity_slug": r["entity_slug"],
            "entity_display_name": entity_label(conn, r["entity_slug"]),
        })
    return out


def _service(settings: Settings, conn: sqlite3.Connection) -> MileageService:
    return MileageService(conn=conn, csv_path=settings.mileage_csv_resolved)


def _render_page(
    request: Request,
    *,
    settings: Settings,
    conn: sqlite3.Connection,
    reader: LedgerReader,
    store: AppSettingsStore,
    error: str | None = None,
    saved_row_index: int | None = None,
    summary_rows: list | None = None,
    summary_year: int | None = None,
    summary_result: dict | None = None,
    summary_error: str | None = None,
) -> HTMLResponse:
    service = _service(settings, conn)
    recent = service.list_entries(limit=25)
    vehicles = _known_vehicles(conn)
    entities = _known_entities(reader)
    today_iso = date_t.today().isoformat()
    ctx = {
        "recent": recent,
        "vehicles": vehicles,
        "entities": entities,
        "today": today_iso,
        "mileage_rate": settings.mileage_rate,
        "csv_path": str(settings.mileage_csv_resolved),
        "error": error,
        "saved_row_index": saved_row_index,
        "summary_rows": summary_rows or [],
        "summary_year": summary_year,
        "summary_result": summary_result,
        "summary_error": summary_error,
    }
    return request.app.state.templates.TemplateResponse(request, "mileage.html", ctx)


@router.get("/mileage", response_class=HTMLResponse)
def mileage_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    store: AppSettingsStore = Depends(get_app_settings_store),
):
    return _render_page(
        request, settings=settings, conn=conn, reader=reader, store=store,
    )


# ---------------------------------------------------------------------
# Full mileage list + per-trip edit (Phase 5A) — the "show me all
# the trips" + "let me fix one" surface that the /mileage page,
# capped at 25 recent rows, can't provide.
# ---------------------------------------------------------------------


_PAGE_SIZE = 100


@router.get("/mileage/all", response_class=HTMLResponse)
def mileage_all(
    request: Request,
    year: int | None = None,
    vehicle: str | None = None,
    fix: str | None = None,
    page: int = 1,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Paginated full list of mileage entries. ``fix`` filters to
    rows that match a specific data-health gap (splits / purpose /
    orphan) so the detail-page links land on the right subset."""
    service = _service(settings, conn)
    page = max(1, int(page))
    offset = (page - 1) * _PAGE_SIZE
    total = service.count_entries(year=year, vehicle=vehicle, fix=fix)
    rows = service.list_entries(
        year=year, vehicle=vehicle, fix=fix,
        limit=_PAGE_SIZE, offset=offset,
    )
    # Augment each row with its DB id so edit/delete links can
    # address it. MileageRow repurposes csv_row_index for the id.
    rows_with_id = []
    for r in rows:
        rows_with_id.append({
            "id": r.csv_row_index,
            "entry_date": r.entry_date,
            "vehicle": r.vehicle,
            "odometer_start": r.odometer_start,
            "odometer_end": r.odometer_end,
            "miles": r.miles,
            "purpose": r.purpose,
            "entity": r.entity,
            "from_loc": r.from_loc,
            "to_loc": r.to_loc,
            "notes": r.notes,
        })
    vehicles = _known_vehicles(conn)
    # Available years for the filter dropdown.
    year_rows = conn.execute(
        "SELECT DISTINCT CAST(strftime('%Y', entry_date) AS INTEGER) AS y "
        "FROM mileage_entries ORDER BY y DESC"
    ).fetchall()
    years = [int(r["y"]) for r in year_rows if r["y"] is not None]

    total_pages = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)
    return request.app.state.templates.TemplateResponse(
        request, "mileage_all.html",
        {
            "rows": rows_with_id,
            "vehicles": vehicles,
            "years": years,
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "page_size": _PAGE_SIZE,
            "year": year,
            "vehicle": vehicle,
            "fix": fix,
        },
    )


@router.get("/mileage/{entry_id:int}/edit", response_class=HTMLResponse)
def mileage_edit_form(
    entry_id: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    service = _service(settings, conn)
    row = service.entry_by_id(entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="mileage entry not found")
    entities = _known_entities(reader)
    vehicles = _known_vehicles(conn)
    return request.app.state.templates.TemplateResponse(
        request, "mileage_edit.html",
        {
            "row": row,
            "entities": entities,
            "vehicles": vehicles,
        },
    )


@router.post("/mileage/{entry_id:int}")
async def mileage_edit_submit(
    entry_id: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    service = _service(settings, conn)
    row = service.entry_by_id(entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="mileage entry not found")
    form = await request.form()

    try:
        parsed_date = date_t.fromisoformat(
            (form.get("entry_date") or "").strip()
        )
    except ValueError:
        return RedirectResponse(
            url=f"/mileage/{entry_id}/edit?error=bad_date",
            status_code=303,
        )

    miles_raw = (form.get("miles") or "").strip()
    try:
        miles_val = float(miles_raw) if miles_raw else 0.0
    except ValueError:
        return RedirectResponse(
            url=f"/mileage/{entry_id}/edit?error=bad_miles",
            status_code=303,
        )
    if miles_val < 0:
        return RedirectResponse(
            url=f"/mileage/{entry_id}/edit?error=negative_miles",
            status_code=303,
        )

    def _opt_int(name: str) -> int | None:
        v = (form.get(name) or "").strip()
        if not v:
            return None
        try:
            return int(float(v))
        except ValueError:
            return None

    # When both odometer readings are provided, the delta is ground
    # truth for total miles. A previous bug let the user save
    # start=14 + end=2014 + miles=2000 simultaneously — internally
    # inconsistent. Force miles to match (end - start); the user can
    # still allocate a subset to business_miles / personal_miles /
    # commuting_miles via the split fields if only part of the trip
    # was deductible.
    odo_start_v = _opt_int("odometer_start")
    odo_end_v = _opt_int("odometer_end")
    if odo_start_v is not None and odo_end_v is not None:
        if odo_end_v < odo_start_v:
            return RedirectResponse(
                url=f"/mileage/{entry_id}/edit?error=end_before_start",
                status_code=303,
            )
        miles_val = float(odo_end_v - odo_start_v)

    def _opt_float(name: str) -> float | None:
        v = (form.get(name) or "").strip()
        if not v:
            return None
        try:
            return float(v)
        except ValueError:
            return None

    category = (form.get("category") or "").strip().lower() or None
    if category is not None and category not in {
        "business", "commuting", "personal", "mixed",
    }:
        category = None

    vehicle = (form.get("vehicle") or "").strip() or row["vehicle"]
    entity = (form.get("entity") or "").strip() or row["entity"]
    known = _known_entities(reader) or [entity]

    try:
        service.update_entry(
            entry_id,
            entry_date=parsed_date,
            vehicle=vehicle,
            entity=entity,
            miles=miles_val,
            odometer_start=_opt_int("odometer_start"),
            odometer_end=_opt_int("odometer_end"),
            entry_time=(form.get("entry_time") or "").strip() or None,
            vehicle_slug=row.get("vehicle_slug"),
            purpose=(form.get("purpose") or "").strip() or None,
            from_loc=(form.get("from_loc") or "").strip() or None,
            to_loc=(form.get("to_loc") or "").strip() or None,
            notes=(form.get("notes") or "").strip() or None,
            business_miles=_opt_float("business_miles"),
            commuting_miles=_opt_float("commuting_miles"),
            personal_miles=_opt_float("personal_miles"),
            category=category,
            known_entities=known,
        )
    except MileageValidationError as exc:
        return RedirectResponse(
            url=f"/mileage/{entry_id}/edit?error={str(exc)[:120]}",
            status_code=303,
        )
    return RedirectResponse(
        url=f"/mileage/{entry_id}/edit?saved=1", status_code=303,
    )


@router.post("/mileage/{entry_id:int}/delete")
def mileage_delete(
    entry_id: int,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    service = _service(settings, conn)
    service.delete_entry(entry_id)
    return RedirectResponse(url="/mileage/all", status_code=303)


# ---------------------------------------------------------------------
# Mobile quick-entry — minimal form for one-tap trip logging.
# ---------------------------------------------------------------------


_MILEAGE_QUICK_SCOPE = "mileage-quick"
_MILEAGE_QUICK_LAST_VEHICLE_KEY = "last_vehicle_slug"


def _quick_get_last_vehicle_slug(conn: sqlite3.Connection) -> str | None:
    try:
        row = conn.execute(
            "SELECT value FROM user_ui_state WHERE scope = ? AND key = ?",
            (_MILEAGE_QUICK_SCOPE, _MILEAGE_QUICK_LAST_VEHICLE_KEY),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    return row["value"] if row else None


def _quick_set_last_vehicle_slug(
    conn: sqlite3.Connection, slug: str,
) -> None:
    try:
        conn.execute(
            """
            INSERT INTO user_ui_state (scope, key, value)
            VALUES (?, ?, ?)
            ON CONFLICT (scope, key) DO UPDATE SET
                value      = excluded.value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (_MILEAGE_QUICK_SCOPE, _MILEAGE_QUICK_LAST_VEHICLE_KEY, slug),
        )
    except sqlite3.OperationalError:
        # Migration 035 not applied — non-fatal; the form just
        # won't remember the last vehicle.
        log.warning("user_ui_state not present; quick-entry memory disabled")


def _most_recent_vehicle_slug(conn: sqlite3.Connection) -> str | None:
    """Fallback when user_ui_state is empty: the active vehicle with
    the most recent trip log entry."""
    try:
        row = conn.execute(
            """
            SELECT v.slug
              FROM vehicles v
              LEFT JOIN mileage_entries e
                     ON (e.vehicle_slug = v.slug
                         OR e.vehicle = v.slug
                         OR e.vehicle = v.display_name)
             WHERE v.is_active = 1
             GROUP BY v.slug
             ORDER BY COALESCE(MAX(e.entry_date), '0000-00-00') DESC,
                      v.slug
             LIMIT 1
            """
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    return row["slug"] if row else None


@router.get("/mileage/quick", response_class=HTMLResponse)
def mileage_quick_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    vehicles = _known_vehicles(conn)
    last_slug = _most_recent_vehicle_slug(conn)
    last_name = next(
        (v["name"] for v in vehicles if v["slug"] == last_slug),
        None,
    )
    return request.app.state.templates.TemplateResponse(
        request, "mileage_quick.html",
        {
            "vehicles": vehicles,
            "last_name": last_name,
            "today": date_t.today().isoformat(),
        },
    )


@router.post("/mileage/quick")
def mileage_quick_submit(
    request: Request,
    entry_date: str = Form(...),
    vehicle_slug: str = Form(...),
    odometer_end: str = Form(...),
    what: str = Form(""),
    category: str = Form("business"),
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Reduced-surface POST for mobile. Vehicle is a slug (from the
    quick-form dropdown); entity is inherited from vehicles.entity_slug
    (fallback "Personal"). `what` populates purpose and notes so the
    single field substantiates the trip."""
    try:
        parsed_date = date_t.fromisoformat(entry_date)
    except ValueError:
        return RedirectResponse(
            url="/mileage/quick?error=bad_date", status_code=303,
        )
    try:
        odo_val = int(float(odometer_end))
    except ValueError:
        return RedirectResponse(
            url="/mileage/quick?error=bad_odometer", status_code=303,
        )
    cat = (category or "").strip().lower() or "business"
    if cat not in {"business", "commuting", "personal"}:
        return RedirectResponse(
            url="/mileage/quick?error=bad_category", status_code=303,
        )

    vrow = conn.execute(
        "SELECT slug, display_name, entity_slug FROM vehicles "
        "WHERE slug = ? OR display_name = ?",
        (vehicle_slug, vehicle_slug),
    ).fetchone()
    if vrow is None:
        return RedirectResponse(
            url="/mileage/quick?error=unknown_vehicle", status_code=303,
        )
    vehicle_name = vrow["display_name"] or vrow["slug"]
    entity = vrow["entity_slug"] or "Personal"
    known = _known_entities(reader) or [entity]

    purpose = (what or "").strip() or None

    service = _service(settings, conn)
    try:
        service.add_entry(
            entry_date=parsed_date,
            vehicle=vehicle_name,
            vehicle_slug=vrow["slug"],
            entity=entity,
            odometer_end=odo_val,
            purpose=purpose,
            notes=purpose,
            category=cat,
            known_entities=known,
        )
    except MileageValidationError as exc:
        return RedirectResponse(
            url=f"/mileage/quick?error={str(exc)[:120]}",
            status_code=303,
        )

    _quick_set_last_vehicle_slug(conn, vrow["slug"])
    return RedirectResponse(url="/mileage/quick?saved=1", status_code=303)


@router.get("/settings/mileage-rates", response_class=HTMLResponse)
def mileage_rates_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Date-ranged IRS mileage rate editor."""
    service = _service(settings, conn)
    rates = service.list_rates()
    return request.app.state.templates.TemplateResponse(
        request, "settings_mileage_rates.html",
        {
            "rates": rates,
            "fallback_rate": settings.mileage_rate,
            "today": date_t.today().isoformat(),
        },
    )


@router.post("/settings/mileage-rates")
def mileage_rate_add(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    effective_from: str = Form(...),
    rate_per_mile: str = Form(...),
    notes: str = Form(""),
):
    try:
        parsed_date = date_t.fromisoformat(effective_from.strip())
        rate = float(rate_per_mile.strip())
    except ValueError:
        return RedirectResponse(
            url="/settings/mileage-rates?error=bad_input",
            status_code=303,
        )
    service = _service(settings, conn)
    service.upsert_rate(
        effective_from=parsed_date.isoformat(),
        rate_per_mile=rate,
        notes=(notes.strip() or None),
    )
    return RedirectResponse(url="/settings/mileage-rates", status_code=303)


@router.post("/settings/mileage-rates/{rate_id}/delete")
def mileage_rate_delete(
    rate_id: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    service = _service(settings, conn)
    service.delete_rate(rate_id)
    return RedirectResponse(url="/settings/mileage-rates", status_code=303)


@router.get("/mileage/last-odometer/{vehicle:path}", response_class=HTMLResponse)
def last_odometer(
    vehicle: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """HTMX partial: returns the last-recorded odometer for the
    selected vehicle and a small script that pre-fills the visible
    start_odometer input. The user can override if months have
    passed without logging (common foot-gun — a stale last-recorded
    reading produces a multi-thousand-mile 'trip' when the delta is
    actually accumulated across the whole gap)."""
    import html as _html
    service = _service(settings, conn)
    last = service.last_odometer_for(vehicle.strip())
    if not last:
        return HTMLResponse(
            '<span class="muted small">No prior odometer recorded. '
            'Enter both the start and end odometer for this trip.</span>'
            '<script>(function(){var el=document.getElementById("trip-odometer-start");'
            'if(el){el.value="";}})();</script>',
        )
    return HTMLResponse(
        f'<span class="small">'
        f'Last recorded: <strong>{last["odometer"]:,}</strong> '
        f'on {_html.escape(last["entry_date"])}. '
        f'Pre-filled as start odometer &mdash; override if there has '
        f'been a gap since the last log.'
        f'</span>'
        f'<script>(function(){{'
        f'var el=document.getElementById("trip-odometer-start");'
        f'if(el && !el.value){{el.value="{last["odometer"]}";}}'
        f'window.dispatchEvent(new Event("trip-odometer-changed"));'
        f'}})();</script>'
    )


@router.post("/mileage")
def create_mileage_entry(
    request: Request,
    entry_date: str = Form(...),
    vehicle: str = Form(...),
    entity: str = Form(...),
    odometer_start: str | None = Form(default=None),
    odometer_end: str | None = Form(default=None),
    miles: str | None = Form(default=None),
    purpose: str | None = Form(default=None),
    from_loc: str | None = Form(default=None),
    to_loc: str | None = Form(default=None),
    notes: str | None = Form(default=None),
    business_miles: str | None = Form(default=None),
    personal_miles: str | None = Form(default=None),
    commuting_miles: str | None = Form(default=None),
    category: str | None = Form(default=None),
    free_text: str | None = Form(default=None),
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    store: AppSettingsStore = Depends(get_app_settings_store),
):
    service = _service(settings, conn)
    try:
        parsed_date = date_t.fromisoformat(entry_date)
    except ValueError:
        return _error_response(
            request,
            settings=settings, conn=conn, reader=reader, store=store,
            message="invalid date — expected YYYY-MM-DD",
        )

    odo_val: int | None = None
    if odometer_end is not None and odometer_end.strip():
        try:
            odo_val = int(float(odometer_end.strip()))
        except ValueError:
            return _error_response(
                request,
                settings=settings, conn=conn, reader=reader, store=store,
                message=f"invalid odometer value {odometer_end!r}",
            )

    odo_start_val: int | None = None
    if odometer_start is not None and odometer_start.strip():
        try:
            odo_start_val = int(float(odometer_start.strip()))
        except ValueError:
            return _error_response(
                request,
                settings=settings, conn=conn, reader=reader, store=store,
                message=f"invalid start odometer value {odometer_start!r}",
            )

    miles_val: float | None = None
    if miles is not None and miles.strip():
        try:
            miles_val = float(miles.strip())
        except ValueError:
            return _error_response(
                request,
                settings=settings, conn=conn, reader=reader, store=store,
                message=f"invalid miles value {miles!r}",
            )

    known = _known_entities(reader)

    biz_val: float | None = None
    pers_val: float | None = None
    com_val: float | None = None
    for raw, name, target in (
        (business_miles, "business_miles", "biz_val"),
        (personal_miles, "personal_miles", "pers_val"),
        (commuting_miles, "commuting_miles", "com_val"),
    ):
        if raw and raw.strip():
            try:
                v = float(raw.strip())
            except ValueError:
                return _error_response(
                    request,
                    settings=settings, conn=conn, reader=reader, store=store,
                    message=f"invalid {name} value {raw!r}",
                )
            if target == "biz_val":
                biz_val = v
            elif target == "pers_val":
                pers_val = v
            else:
                com_val = v

    cat_val = (category or "").strip().lower() or None
    if cat_val is not None and cat_val not in {
        "business", "commuting", "personal", "mixed",
    }:
        return _error_response(
            request,
            settings=settings, conn=conn, reader=reader, store=store,
            message=f"invalid category {category!r}",
        )

    try:
        row = service.add_entry(
            entry_date=parsed_date,
            vehicle=vehicle,
            entity=entity,
            miles=miles_val,
            odometer_start=odo_start_val,
            odometer_end=odo_val,
            purpose=(purpose or "").strip() or None,
            from_loc=(from_loc or "").strip() or None,
            to_loc=(to_loc or "").strip() or None,
            notes=(notes or "").strip() or None,
            known_entities=known or None,
            business_miles=biz_val,
            personal_miles=pers_val,
            commuting_miles=com_val,
            category=cat_val,
            free_text=(free_text or "").strip() or None,
        )
    except MileageValidationError as exc:
        return _error_response(
            request,
            settings=settings, conn=conn, reader=reader, store=store,
            message=str(exc),
        )

    if _is_htmx(request):
        toast = (
            f'<div class="toast success" id="toast">'
            f'Saved {row.miles:.1f} mi for {row.vehicle} → {row.entity}.'
            f'</div>'
        )
        return HTMLResponse(toast, status_code=200, headers={"HX-Trigger": "mileage-saved"})
    return RedirectResponse(url="/mileage", status_code=303)


def _is_htmx(request: Request) -> bool:
    return "hx-request" in {k.lower() for k in request.headers.keys()}


def _error_response(
    request: Request,
    *,
    settings: Settings,
    conn: sqlite3.Connection,
    reader: LedgerReader,
    store: AppSettingsStore,
    message: str,
) -> HTMLResponse:
    if _is_htmx(request):
        toast = (
            f'<div class="toast error" id="toast">'
            f'{_escape(message)}'
            f'</div>'
        )
        return HTMLResponse(toast, status_code=400, headers={"HX-Trigger": "mileage-error"})
    return _render_page(
        request, settings=settings, conn=conn, reader=reader, store=store,
        error=message,
    )


def _escape(text: str) -> str:
    import html

    return html.escape(text or "")


@router.get("/mileage/summary", response_class=HTMLResponse)
def mileage_summary(
    request: Request,
    year: int | None = None,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    store: AppSettingsStore = Depends(get_app_settings_store),
):
    service = _service(settings, conn)
    target_year = year or date_t.today().year
    rows = service.yearly_summary(target_year, rate_per_mile=settings.mileage_rate)
    return _render_page(
        request, settings=settings, conn=conn, reader=reader, store=store,
        summary_rows=rows, summary_year=target_year,
    )


@router.post("/mileage/summary/generate", response_class=HTMLResponse)
def generate_mileage_summary(
    request: Request,
    year: int,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Write the year-end mileage summary + run bean-check. Runs as
    a job so the user sees the ledger write + validate pass instead
    of a page that freezes for 5–15s."""
    import html as _html

    def _work(ctx):
        service = _service(settings, conn)
        ctx.emit(
            f"Aggregating mileage entries for {year} …", outcome="info",
        )
        rows = service.yearly_summary(year, rate_per_mile=settings.mileage_rate)
        if not rows:
            ctx.emit(f"No mileage logged for {year}.", outcome="not_found")
            return {
                "terminal_html": (
                    f'<div class="muted" style="padding:0.5rem;">'
                    f'No mileage logged for {year}.</div>'
                ),
            }
        ctx.emit(
            f"Writing summary for {len(rows)} entity/vehicle row(s) "
            f"to mileage_summary.bean + running bean-check …",
            outcome="info",
        )
        writer = MileageBeancountWriter(
            main_bean=settings.ledger_main,
            summary_path=settings.mileage_summary_path,
        )
        try:
            result = writer.write_year(
                year=year, rows=rows, rate_per_mile=settings.mileage_rate,
            )
        except (MileageSummaryError, BeanCheckError) as exc:
            ctx.emit(f"Write failed: {exc}", outcome="error")
            raise
        reader.invalidate()
        ctx.emit(
            f"Wrote {result.rows_written} row(s) — deduction total "
            f"${result.deduction_total_usd:,.2f} at "
            f"${result.rate_per_mile:.3f}/mile.",
            outcome="success",
        )
        return {
            "terminal_html": (
                f'<div class="banner success" '
                f'style="padding:0.75rem;background:#d4edda;border-radius:4px;">'
                f'<strong>Wrote {year} mileage summary.</strong><br/>'
                f'Rows written: {result.rows_written}<br/>'
                f'Deduction total: <strong>'
                f'${result.deduction_total_usd:,.2f}</strong> at '
                f'${result.rate_per_mile:.3f}/mile<br/>'
                f'Path: <code>{_html.escape(str(settings.mileage_summary_path))}</code><br/>'
                f'{"<em>(replaced prior year summary)</em>" if result.replaced else ""}'
                f'</div>'
            ),
            "year": result.year,
            "rows_written": result.rows_written,
            "deduction_total_usd": result.deduction_total_usd,
        }

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="mileage-summary",
        title=f"Generating {year} mileage summary",
        fn=_work,
        return_url=f"/mileage?summary_year={year}",
    )
    return request.app.state.templates.TemplateResponse(
        request, "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": f"/mileage?summary_year={year}"},
    )


# ---- Mileage import ----------------------------------------------------


def _resolve_vehicle(conn: sqlite3.Connection, raw: str) -> dict | None:
    """Resolve a form-submitted vehicle identifier to a registry row.
    Accepts either slug or display_name. Inactive vehicles rejected.
    ``current_mileage`` pulled through so the import parser can
    disambiguate single-number lines (odometer vs trip distance)
    when there are no prior mileage_entries yet."""
    raw = (raw or "").strip()
    if not raw:
        return None
    row = conn.execute(
        "SELECT slug, display_name, entity_slug, current_mileage "
        "FROM vehicles "
        "WHERE is_active = 1 AND (slug = ? OR display_name = ?) LIMIT 1",
        (raw, raw),
    ).fetchone()
    if row is None:
        return None
    from lamella.core.registry.alias import entity_label
    return {
        "slug": row["slug"],
        "name": row["display_name"] or row["slug"],
        "entity_slug": row["entity_slug"],
        "entity_display_name": entity_label(conn, row["entity_slug"]),
        "current_mileage": row["current_mileage"],
    }


def _starting_anchor(
    service: MileageService, vehicle: dict,
) -> int | None:
    """Best-guess prior odometer reading for an import batch. Prefer
    the most recent trip in the log; fall back to ``current_mileage``
    from Settings → Vehicles so the first-ever import still gets an
    anchor to disambiguate single-number lines."""
    prior = service.last_odometer_for(vehicle["name"])
    if prior:
        return prior["odometer"]
    current = vehicle.get("current_mileage")
    try:
        return int(current) if current is not None else None
    except (TypeError, ValueError):
        return None


@router.get("/mileage/import", response_class=HTMLResponse)
def mileage_import_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Entry point for importing a mileage log. The form posts to
    /mileage/import/preview — the preview lets the user spot errors
    / conflicts before a second POST commits."""
    service = _service(settings, conn)
    vehicles = _known_vehicles(conn)
    entities = _known_entities(reader)
    batches = service.list_import_batches(limit=10)
    ctx = {
        "vehicles": vehicles,
        "entities": entities,
        "batches": batches,
        "preview_rows": None,
        "text": "",
        "selected_vehicle": None,
        "selected_entity": None,
        "source_format": None,
        "error": None,
    }
    return request.app.state.templates.TemplateResponse(
        request, "mileage_import.html", ctx,
    )


@router.post("/mileage/import/preview", response_class=HTMLResponse)
async def mileage_import_preview(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Parse the submitted file / text against the selected vehicle
    and render the preview — conflict flags, derived miles, line
    errors. Does NOT write anything."""
    form = await request.form()
    vehicle_raw = (form.get("vehicle") or "").strip()
    entity_raw = (form.get("entity") or "").strip()
    text = form.get("text") or ""
    upload = form.get("file")

    service = _service(settings, conn)
    vehicles = _known_vehicles(conn)
    entities = _known_entities(reader)
    vehicle = _resolve_vehicle(conn, vehicle_raw)

    error: str | None = None
    if vehicle is None:
        error = (
            f"vehicle {vehicle_raw!r} is not in the registry. "
            "Add it under Settings → Vehicles first."
        )
    if error is None and entity_raw and entities and entity_raw not in entities:
        error = f"entity {entity_raw!r} is not in the ledger's entity list"

    csv_bytes: bytes | None = None
    filename: str | None = None
    if upload is not None and hasattr(upload, "read"):
        filename = getattr(upload, "filename", None) or None
        data = await upload.read()
        if data:
            csv_bytes = data

    if error is None and not text.strip() and not csv_bytes:
        error = "provide either text or a CSV file to import"

    # If an upload was used but the textarea is empty, decode the
    # uploaded bytes into the textarea so the commit step — which
    # only carries `text`, never the raw file — can re-parse the
    # same input. Without this, a file upload previews 51 rows but
    # commits 0.
    if csv_bytes and not text.strip():
        try:
            text = csv_bytes.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = csv_bytes.decode("latin-1")

    preview_rows: list[ImportPreviewRow] = []
    source_format = "csv"
    if error is None and vehicle is not None:
        starting_odo = _starting_anchor(service, vehicle)
        preview_rows, source_format = parse_input(
            text=text if text.strip() else None,
            csv_bytes=None,
            starting_odometer=starting_odo,
        )
        preview_rows = service.mark_existing_duplicates(
            vehicle=vehicle["name"],
            rows=preview_rows,
        )

    ctx = {
        "vehicles": vehicles,
        "entities": entities,
        "batches": service.list_import_batches(limit=10),
        "preview_rows": preview_rows,
        "text": text,
        "filename": filename,
        "selected_vehicle": vehicle,
        "selected_entity": entity_raw or (vehicle["entity_slug"] if vehicle else None),
        "source_format": source_format,
        "error": error,
        # "ready" = error-free. Zero-mile "no trips today" markers
        # are valid entries — they record the vehicle sat still that
        # day, which has auditing value.
        "valid_count": sum(
            1 for r in preview_rows
            if r.error is None and r.miles is not None and r.miles >= 0
        ),
        "error_count": sum(1 for r in preview_rows if r.error),
        "conflict_count": sum(1 for r in preview_rows if r.conflict),
        "has_splits": any(
            r.business_miles is not None
            or r.personal_miles is not None
            or r.commuting_miles is not None
            or r.category is not None
            for r in preview_rows
        ),
    }
    return request.app.state.templates.TemplateResponse(
        request, "mileage_import.html", ctx,
    )


@router.post("/mileage/import/commit", response_class=HTMLResponse)
async def mileage_import_commit(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Persist the previewed rows. Re-parses the text exactly as the
    preview did so the preview and commit always agree on the
    candidate row set. Rows with errors are skipped; rows with
    conflicts are written (the preview has already warned the user)."""
    form = await request.form()
    vehicle_raw = (form.get("vehicle") or "").strip()
    entity_raw = (form.get("entity") or "").strip()
    text = form.get("text") or ""
    filename = form.get("filename") or None

    service = _service(settings, conn)
    vehicle = _resolve_vehicle(conn, vehicle_raw)
    if vehicle is None:
        raise HTTPException(
            status_code=400,
            detail=f"vehicle {vehicle_raw!r} is not in the registry",
        )

    starting_odo = _starting_anchor(service, vehicle)
    preview_rows, source_format = parse_input(
        text=text if text.strip() else None,
        csv_bytes=None,
        starting_odometer=starting_odo,
    )

    # Entity inherits from the vehicle when the user didn't pick
    # an override. Personal vehicles (entity_slug NULL) stamp
    # "Personal" so tax-time aggregation has something to group by.
    effective_entity = entity_raw or vehicle.get("entity_slug") or "Personal"

    batch_id = service.create_import_batch(
        vehicle_slug=vehicle["slug"],
        source_filename=filename,
        source_format=source_format,
    )
    result = service.write_import_rows(
        batch_id=batch_id,
        vehicle=vehicle["name"],
        vehicle_slug=vehicle["slug"],
        entity=effective_entity,
        rows=preview_rows,
    )
    vehicles = _known_vehicles(conn)
    entities = _known_entities(reader)
    ctx = {
        "vehicles": vehicles,
        "entities": entities,
        "batches": service.list_import_batches(limit=10),
        "preview_rows": None,
        "text": "",
        "selected_vehicle": None,
        "selected_entity": None,
        "source_format": None,
        "error": None,
        "commit_result": {
            "batch_id": result.batch_id,
            "rows_written": result.rows_written,
            "rows_skipped": result.rows_skipped,
            "conflicts": result.conflicts,
            "messages": result.messages,
            "vehicle": vehicle["name"],
        },
    }
    return request.app.state.templates.TemplateResponse(
        request, "mileage_import.html", ctx,
    )


@router.post("/mileage/import/batches/{batch_id}/delete")
def mileage_import_undo(
    batch_id: int,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    service = _service(settings, conn)
    service.delete_import_batch(batch_id)
    return RedirectResponse(url="/mileage/import", status_code=303)
