# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.core.fs import UnsafePathError, validate_safe_path
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.adapters.paperless.client import PaperlessClient, PaperlessError
from lamella.features.reports._pdf import PDFRenderingUnavailable
from lamella.features.reports.audit_portfolio import render_audit_pdf
from lamella.features.reports.estimated_tax import (
    render_estimated_tax_pdf,
    stream_estimated_tax_csv,
)
from lamella.features.reports.line_map import load_line_map
from lamella.features.reports.schedule_c import (
    build_schedule_c,
    stream_detail_csv,
    stream_summary_csv,
)
from lamella.features.reports.schedule_c_pdf import (
    build_context as build_c_context,
    render_schedule_c_html,
    render_schedule_c_pdf,
)
from lamella.features.reports.schedule_f import build_schedule_f
from lamella.features.reports.schedule_f_pdf import (
    build_context as build_f_context,
    render_schedule_f_html,
    render_schedule_f_pdf,
)
from lamella.features.reports.vehicles_pdf import (
    Form4562Context,
    Form4562Row,
    MileageLogContext,
    MileageLogRow,
    build_schedule_c_part_iv_context,
    render_form_4562_worksheet_pdf,
    render_mileage_log_pdf,
    render_schedule_c_part_iv_pdf,
)

log = logging.getLogger(__name__)

router = APIRouter()


def _entities(reader: LedgerReader) -> list[str]:
    from beancount.core.data import Open

    entities: set[str] = set()
    for entry in reader.load().entries:
        if isinstance(entry, Open):
            parts = entry.account.split(":")
            if len(parts) >= 2 and parts[0] in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
                entities.add(parts[1])
    return sorted(entities)


def _entities_with_display(reader: LedgerReader, conn) -> list[dict]:
    """Entity slugs paired with display names from the registry. Slugs
    with no registry entry fall back to the slug itself."""
    slugs = _entities(reader)
    display_by_slug: dict[str, str] = {}
    if conn is not None:
        try:
            rows = conn.execute(
                "SELECT slug, display_name FROM entities"
            ).fetchall()
            for r in rows:
                if r["display_name"]:
                    display_by_slug[r["slug"]] = r["display_name"]
        except Exception:  # noqa: BLE001
            pass
    return [
        {"slug": s, "display_name": display_by_slug.get(s, s)}
        for s in slugs
    ]


def _entities_with_schedule(reader: LedgerReader, conn) -> list[dict]:
    """Same as ``_entities_with_display`` but also pulls each entity's
    ``tax_schedule`` so the index matrix can show only the schedules
    that apply per entity (Schedule C for sole-prop / single-LLC,
    Schedule F for farm). Entities with no tax_schedule still appear,
    but their schedule columns render as a dash. The entity_type
    field flows through too so personal-only users can be hinted to
    skip the per-business sections."""
    slugs = _entities(reader)
    by_slug: dict[str, dict] = {s: {"slug": s, "display_name": s} for s in slugs}
    if conn is not None:
        try:
            for r in conn.execute(
                "SELECT slug, display_name, entity_type, tax_schedule "
                "FROM entities"
            ).fetchall():
                if r["slug"] not in by_slug:
                    continue
                if r["display_name"]:
                    by_slug[r["slug"]]["display_name"] = r["display_name"]
                by_slug[r["slug"]]["entity_type"] = r["entity_type"]
                by_slug[r["slug"]]["tax_schedule"] = r["tax_schedule"]
        except Exception:  # noqa: BLE001
            pass
    return [by_slug[s] for s in slugs]


def _tax_schedule_for(conn, entity_slug: str) -> str | None:
    """Look up an entity's ``tax_schedule`` (e.g. ``"C"``, ``"F"``,
    ``"A"``, ``"Personal"``) from the registry. Returns ``None`` for
    unknown entities or entities with no schedule set — the caller
    decides what to do with that.
    """
    if conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT tax_schedule FROM entities WHERE slug = ?",
            (entity_slug,),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return None
    if not row:
        return None
    return (row["tax_schedule"] or None) if "tax_schedule" in row.keys() else (row[0] or None)


# Forward-declared early routes for fragments that other handlers in this
# file own further down. Registered ahead of the catch-all
# ``/reports/{entity_slug}/{year}`` so paths like
# ``/reports/vehicles/mileage-log.pdf`` resolve to the specific handler
# instead of getting captured by the catch-all (which would 422 trying
# to int-parse "mileage-log.pdf" as a year). FastAPI matches routes in
# registration order, so these stubs must precede the catch-all.
@router.get("/reports/vehicles/mileage-log.pdf", include_in_schema=False)
def _vehicle_mileage_log_pdf_early(
    year: int,
    vehicle: str | None = None,
    request: Request = None,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    return vehicle_mileage_log_pdf(
        year=year, vehicle=vehicle, request=request,
        settings=settings, conn=conn,
    )


@router.get("/reports/vehicles/schedule-c-part-iv.pdf", include_in_schema=False)
def _vehicle_schedule_c_part_iv_pdf_early(
    entity: str,
    year: int,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    return vehicle_schedule_c_part_iv_pdf(
        entity=entity, year=year, settings=settings, conn=conn,
    )


@router.get("/reports/vehicles/form-4562-worksheet.pdf", include_in_schema=False)
def _vehicle_form_4562_worksheet_pdf_early(
    entity: str,
    year: int,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    return vehicle_form_4562_worksheet_pdf(
        entity=entity, year=year, settings=settings, conn=conn,
    )


@router.get("/reports/{entity_slug}/{year}", response_class=HTMLResponse)
def entity_report_dispatch(
    entity_slug: str,
    year: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    """Entity-first report URL: ``/reports/{entity_slug}/{year}``.

    Looks up the entity's ``tax_schedule`` from the registry and dispatches
    to the matching schedule view (C / F / personal). Replaces the older
    ``/reports/schedule-c?entity=...&year=...`` query-style URL — the
    schedule is a property of the entity, not of the URL. Old URLs still
    work for bookmark stability.
    """
    sched = _tax_schedule_for(conn, entity_slug)
    if sched is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No report available for entity '{entity_slug}': "
                "either the entity does not exist or it has no tax_schedule "
                "set in /entities. Set one (Schedule C / F / Personal) and "
                "this URL will render."
            ),
        )
    sched_upper = sched.strip().upper()
    if sched_upper == "C":
        return schedule_c_view(
            entity=entity_slug, year=year, request=request,
            settings=settings, reader=reader,
        )
    if sched_upper == "F":
        return schedule_f_view(
            entity=entity_slug, year=year, request=request,
            settings=settings, reader=reader,
        )
    raise HTTPException(
        status_code=501,
        detail=(
            f"Entity '{entity_slug}' has tax_schedule '{sched}' which does "
            "not have a site-chromed report yet. Available schedules: C, F."
        ),
    )


@router.get("/reports", response_class=HTMLResponse)
def reports_index(
    request: Request,
    entity: str | None = None,
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    """Reports matrix — every per-entity report is one tile per
    (entity, year) cell. Each cell links to the site-chromed View,
    the tax-form Preview, the PDF download, and the two CSVs.
    Replaces the old "fill out a form, click a button" UX where you
    had to pick entity + year + quarter before seeing any links.

    ``?entity=<slug>`` filters the matrix to a single entity so the
    "Reports" button on a business page lands on a focused view
    rather than the full multi-entity index.
    """
    current_year = date.today().year
    entities_with_schedule = _entities_with_schedule(reader, conn)
    entities_with_display = _entities_with_display(reader, conn)
    entities_all = _entities(reader)
    entity_filter = (entity or "").strip()
    if entity_filter:
        entities_with_schedule = [
            e for e in entities_with_schedule if e["slug"] == entity_filter
        ]
        entities_with_display = [
            e for e in entities_with_display if e["slug"] == entity_filter
        ]
        entities_all = [s for s in entities_all if s == entity_filter]
    ctx = {
        "entities_with_schedule": entities_with_schedule,
        "entities_with_display": entities_with_display,
        "entities": entities_all,
        "entity_filter": entity_filter or None,
        "current_year": current_year,
        "years": list(range(current_year, current_year - 5, -1)),
        "current_quarter": (date.today().month - 1) // 3 + 1,
    }
    return request.app.state.templates.TemplateResponse(request, "reports.html", ctx)


@router.get("/reports/schedule-c", response_class=HTMLResponse)
def schedule_c_view(
    entity: str,
    year: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Site-chromed Schedule C report. Same data as the tax-form
    preview at /reports/schedule-c.preview.html, but rendered inside
    base.html with the normal nav + sidebar so it reads like a
    page in the app rather than a printable form."""
    try:
        line_map = load_line_map(settings.schedule_c_lines_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    ctx_obj = build_c_context(
        entity=entity, year=year, entries=reader.load().entries,
        line_map=line_map, conn=request.app.state.db,
        mileage_csv_path=settings.mileage_csv_resolved,
        mileage_rate=settings.mileage_rate,
    )
    ctx = {
        "entity": entity, "year": year,
        "summary": ctx_obj.summary,
        "gross_receipts": ctx_obj.gross_receipts,
        "cogs": ctx_obj.cogs,
        "gross_income": ctx_obj.gross_income,
        "total_expenses": ctx_obj.total_expenses,
        "net": ctx_obj.net,
        "mileage_rows": ctx_obj.mileage_rows,
    }
    return request.app.state.templates.TemplateResponse(
        request, "report_schedule_c.html", ctx,
    )


@router.get("/reports/schedule-f", response_class=HTMLResponse)
def schedule_f_view(
    entity: str,
    year: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Site-chromed Schedule F report — sister to the Schedule C
    site-chrome view above, for farm entities."""
    try:
        line_map = load_line_map(settings.schedule_f_lines_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    ledger = reader.load()
    report = build_schedule_f(
        entity=entity, year=year, entries=ledger.entries, line_map=line_map,
    )
    total_expenses = sum((row.amount for row in report.summary), Decimal("0"))
    ctx = {
        "entity": entity, "year": year,
        "summary": report.summary,
        "total_expenses": total_expenses,
    }
    return request.app.state.templates.TemplateResponse(
        request, "report_schedule_f.html", ctx,
    )


def _csv_response(stream, filename: str) -> StreamingResponse:
    return StreamingResponse(
        stream,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _pdf_response(content: bytes, filename: str) -> Response:
    return Response(
        content,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _pdf_unavailable_response(detail: str) -> HTMLResponse:
    return HTMLResponse(
        f"<h1>PDF rendering unavailable</h1><p>{detail}</p>",
        status_code=503,
    )


@router.get("/reports/schedule-c.csv")
def schedule_c_summary(
    entity: str,
    year: int,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        line_map = load_line_map(settings.schedule_c_lines_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    ledger = reader.load()
    report = build_schedule_c(
        entity=entity, year=year, entries=ledger.entries, line_map=line_map
    )
    return _csv_response(
        stream_summary_csv(report), f"schedule-c-{entity}-{year}.csv"
    )


@router.get("/reports/schedule-c-detail.csv")
def schedule_c_detail(
    entity: str,
    year: int,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        line_map = load_line_map(settings.schedule_c_lines_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    ledger = reader.load()
    report = build_schedule_c(
        entity=entity, year=year, entries=ledger.entries, line_map=line_map
    )
    return _csv_response(
        stream_detail_csv(report), f"schedule-c-{entity}-{year}-detail.csv"
    )


@router.get("/reports/schedule-c.pdf")
def schedule_c_pdf(
    entity: str,
    year: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        line_map = load_line_map(settings.schedule_c_lines_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    ctx = build_c_context(
        entity=entity, year=year, entries=reader.load().entries, line_map=line_map,
        conn=request.app.state.db, mileage_csv_path=settings.mileage_csv_resolved,
        mileage_rate=settings.mileage_rate,
    )
    try:
        pdf = render_schedule_c_pdf(ctx)
    except PDFRenderingUnavailable as exc:
        return _pdf_unavailable_response(str(exc))
    _persist_report(settings, f"schedule-c-{entity}-{year}.pdf", pdf)
    return _pdf_response(pdf, f"schedule-c-{entity}-{year}.pdf")


@router.get("/reports/schedule-c.preview.html", response_class=HTMLResponse)
def schedule_c_preview(
    entity: str,
    year: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        line_map = load_line_map(settings.schedule_c_lines_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    ctx = build_c_context(
        entity=entity, year=year, entries=reader.load().entries, line_map=line_map,
        conn=request.app.state.db, mileage_csv_path=settings.mileage_csv_resolved,
        mileage_rate=settings.mileage_rate,
    )
    return HTMLResponse(render_schedule_c_html(ctx))


@router.get("/reports/schedule-f.csv")
def schedule_f_summary(
    entity: str,
    year: int,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        line_map = load_line_map(settings.schedule_f_lines_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    ledger = reader.load()
    report = build_schedule_f(
        entity=entity, year=year, entries=ledger.entries, line_map=line_map
    )
    return _csv_response(
        stream_summary_csv(report), f"schedule-f-{entity}-{year}.csv"
    )


@router.get("/reports/schedule-f-detail.csv")
def schedule_f_detail(
    entity: str,
    year: int,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        line_map = load_line_map(settings.schedule_f_lines_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    ledger = reader.load()
    report = build_schedule_f(
        entity=entity, year=year, entries=ledger.entries, line_map=line_map
    )
    return _csv_response(
        stream_detail_csv(report), f"schedule-f-{entity}-{year}-detail.csv"
    )


@router.get("/reports/schedule-f.pdf")
def schedule_f_pdf(
    entity: str,
    year: int,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        line_map = load_line_map(settings.schedule_f_lines_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    ctx = build_f_context(
        entity=entity, year=year, entries=reader.load().entries, line_map=line_map,
    )
    try:
        pdf = render_schedule_f_pdf(ctx)
    except PDFRenderingUnavailable as exc:
        return _pdf_unavailable_response(str(exc))
    _persist_report(settings, f"schedule-f-{entity}-{year}.pdf", pdf)
    return _pdf_response(pdf, f"schedule-f-{entity}-{year}.pdf")


@router.get("/reports/schedule-f.preview.html", response_class=HTMLResponse)
def schedule_f_preview(
    entity: str,
    year: int,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        line_map = load_line_map(settings.schedule_f_lines_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    ctx = build_f_context(
        entity=entity, year=year, entries=reader.load().entries, line_map=line_map,
    )
    return HTMLResponse(render_schedule_f_html(ctx))


@router.get("/reports/audit-portfolio.pdf")
async def audit_portfolio(
    entity: str,
    year: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        line_map = load_line_map(settings.schedule_c_lines_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    paperless = None
    if settings.paperless_configured:
        paperless = PaperlessClient(
            base_url=settings.paperless_url,  # type: ignore[arg-type]
            api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
            extra_headers=settings.paperless_extra_headers(),
        )
    try:
        pdf = await render_audit_pdf(
            entity=entity,
            year=year,
            entries=reader.load().entries,
            line_map=line_map,
            conn=request.app.state.db,
            paperless_client=paperless,
            max_receipt_bytes=settings.audit_max_receipt_bytes,
        )
    except PDFRenderingUnavailable as exc:
        return _pdf_unavailable_response(str(exc))
    finally:
        if paperless is not None:
            await paperless.aclose()
    _persist_report(settings, f"audit-portfolio-{entity}-{year}.pdf", pdf)
    return _pdf_response(pdf, f"audit-portfolio-{entity}-{year}.pdf")


@router.get("/reports/estimated-tax.pdf")
def estimated_tax_pdf(
    year: int,
    quarter: int,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        pdf = render_estimated_tax_pdf(
            year=year, quarter=quarter, rate=settings.estimated_tax_flat_rate,
            entries=reader.load().entries,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except PDFRenderingUnavailable as exc:
        return _pdf_unavailable_response(str(exc))
    _persist_report(settings, f"estimated-tax-{year}-q{quarter}.pdf", pdf)
    return _pdf_response(pdf, f"estimated-tax-{year}-q{quarter}.pdf")


@router.get("/reports/estimated-tax.csv")
def estimated_tax_csv(
    year: int,
    quarter: int,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        stream = stream_estimated_tax_csv(
            year=year, quarter=quarter, rate=settings.estimated_tax_flat_rate,
            entries=reader.load().entries,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return _csv_response(stream, f"estimated-tax-{year}-q{quarter}.csv")


def _persist_report(settings: Settings, filename: str, content: bytes) -> None:
    """Best-effort write of generated PDFs to ${REPORTS_OUTPUT_DIR}. Disk
    failures are logged but do not break the HTTP response.

    The ``filename`` argument is built from request query params (entity
    slug, year, vehicle slug, etc.). ADR-0030 requires that the resolved
    write path lands inside ``reports_output_resolved`` even though the
    pieces are typed as slugs/integers — defense in depth against a
    future endpoint passing a less-constrained value.
    """
    try:
        out_dir = settings.reports_output_resolved
        out_dir.mkdir(parents=True, exist_ok=True)
        try:
            target = validate_safe_path(filename, allowed_roots=[out_dir])
        except UnsafePathError as exc:
            log.warning("refusing to persist %s: %s", filename, exc)
            return
        target.write_bytes(content)
    except OSError as exc:
        log.warning("could not persist %s: %s", filename, exc)


# ---------------------------------------------------------------------
# Vehicle worksheets — Phase 5D
# ---------------------------------------------------------------------


def _load_mileage_log_rows(
    conn, *, year: int, vehicle_slug: str | None,
) -> tuple[list[MileageLogRow], dict[str, float]]:
    """Pull trips for the year (all vehicles OR one slug) + its
    split totals from trip-meta. Returns (rows, totals-dict) where
    totals carries business / commuting / personal miles."""
    start = f"{year:04d}-01-01"
    end = f"{year + 1:04d}-01-01"
    if vehicle_slug:
        vrow = conn.execute(
            "SELECT slug, display_name FROM vehicles WHERE slug = ?",
            (vehicle_slug,),
        ).fetchone()
        if vrow is None:
            raise HTTPException(status_code=404, detail="vehicle not found")
        names = [vrow["slug"]] + (
            [vrow["display_name"]] if vrow["display_name"] else []
        )
        placeholders = ",".join(["?"] * len(names))
        trips = conn.execute(
            f"""
            SELECT entry_date, vehicle, miles, odometer_end,
                   purpose, from_loc, to_loc, notes
              FROM mileage_entries
             WHERE entry_date >= ? AND entry_date < ?
               AND (vehicle_slug = ? OR vehicle IN ({placeholders}))
          ORDER BY entry_date, id
            """,
            (start, end, vehicle_slug, *names),
        ).fetchall()
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
    else:
        trips = conn.execute(
            """
            SELECT entry_date, vehicle, miles, odometer_end,
                   purpose, from_loc, to_loc, notes
              FROM mileage_entries
             WHERE entry_date >= ? AND entry_date < ?
          ORDER BY entry_date, id
            """,
            (start, end),
        ).fetchall()
        split_row = conn.execute(
            """
            SELECT COALESCE(SUM(business_miles), 0) AS biz,
                   COALESCE(SUM(commuting_miles), 0) AS com,
                   COALESCE(SUM(personal_miles), 0) AS per
              FROM mileage_trip_meta
             WHERE entry_date >= ? AND entry_date < ?
            """,
            (start, end),
        ).fetchone()

    rows: list[MileageLogRow] = []
    for t in trips:
        try:
            d = date.fromisoformat(str(t["entry_date"])[:10])
        except ValueError:
            continue
        # Compose the "business purpose" cell from purpose + notes so
        # 0-mile maintenance days (oil change in notes) substantiate.
        purpose_bits = []
        if t["purpose"]:
            purpose_bits.append(t["purpose"])
        if t["notes"]:
            purpose_bits.append(t["notes"])
        rows.append(MileageLogRow(
            entry_date=d,
            vehicle=t["vehicle"] or "",
            business_purpose=" — ".join(purpose_bits),
            from_loc=t["from_loc"],
            to_loc=t["to_loc"],
            miles=float(t["miles"] or 0),
            odometer=int(t["odometer_end"]) if t["odometer_end"] is not None else None,
        ))
    totals = {
        "business": float(split_row["biz"] or 0) if split_row else 0.0,
        "commuting": float(split_row["com"] or 0) if split_row else 0.0,
        "personal": float(split_row["per"] or 0) if split_row else 0.0,
    }
    return rows, totals


@router.get("/reports/vehicles/mileage-log.pdf")
def vehicle_mileage_log_pdf(
    year: int,
    vehicle: str | None = None,
    request: Request = None,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    rows, totals = _load_mileage_log_rows(
        conn, year=year, vehicle_slug=vehicle,
    )
    display_name = None
    if vehicle:
        vrow = conn.execute(
            "SELECT display_name FROM vehicles WHERE slug = ?", (vehicle,),
        ).fetchone()
        display_name = vrow["display_name"] if vrow else None
    ctx = MileageLogContext(
        year=year,
        vehicle_slug=vehicle,
        vehicle_display_name=display_name,
        rows=rows,
        total_business_miles=totals["business"],
        total_commuting_miles=totals["commuting"],
        total_personal_miles=totals["personal"],
    )
    try:
        pdf = render_mileage_log_pdf(ctx)
    except PDFRenderingUnavailable as exc:
        return _pdf_unavailable_response(str(exc))
    name = f"mileage-log-{vehicle or 'fleet'}-{year}.pdf"
    _persist_report(settings, name, pdf)
    return _pdf_response(pdf, name)


def _vehicles_for_entity(conn, entity: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM vehicles WHERE COALESCE(entity_slug, '') = ?",
        (entity,),
    ).fetchall()
    return [dict(r) for r in rows]


@router.get("/reports/vehicles/schedule-c-part-iv.pdf")
def vehicle_schedule_c_part_iv_pdf(
    entity: str,
    year: int,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    vehicles = _vehicles_for_entity(conn, entity)
    vehicles_by_slug = {v["slug"]: v for v in vehicles}
    if not vehicles_by_slug:
        raise HTTPException(
            status_code=404,
            detail=f"no vehicles found for entity {entity!r}",
        )
    slugs = list(vehicles_by_slug.keys())
    placeholders = ",".join(["?"] * len(slugs))
    yearly_rows = [
        dict(r) for r in conn.execute(
            f"SELECT * FROM vehicle_yearly_mileage "
            f"WHERE vehicle_slug IN ({placeholders}) AND year = ?",
            (*slugs, year),
        ).fetchall()
    ]
    ctx = build_schedule_c_part_iv_context(
        entity=entity, year=year,
        yearly_rows=yearly_rows,
        vehicles_by_slug=vehicles_by_slug,
    )
    try:
        pdf = render_schedule_c_part_iv_pdf(ctx)
    except PDFRenderingUnavailable as exc:
        return _pdf_unavailable_response(str(exc))
    name = f"vehicles-schedule-c-part-iv-{entity}-{year}.pdf"
    _persist_report(settings, name, pdf)
    return _pdf_response(pdf, name)


@router.get("/reports/vehicles/form-4562-worksheet.pdf")
def vehicle_form_4562_worksheet_pdf(
    entity: str,
    year: int,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    vehicles = _vehicles_for_entity(conn, entity)
    if not vehicles:
        raise HTTPException(
            status_code=404,
            detail=f"no vehicles found for entity {entity!r}",
        )
    # Collect elections for the target year only.
    rows: list[Form4562Row] = []
    for v in vehicles:
        el = conn.execute(
            "SELECT * FROM vehicle_elections "
            "WHERE vehicle_slug = ? AND tax_year = ?",
            (v["slug"], year),
        ).fetchone()
        rows.append(Form4562Row(
            vehicle_slug=v["slug"],
            vehicle_display_name=v.get("display_name") or v["slug"],
            placed_in_service_date=v.get("placed_in_service_date"),
            gvwr_lbs=v.get("gvwr_lbs"),
            fuel_type=v.get("fuel_type"),
            purchase_price=v.get("purchase_price"),
            purchase_fees=v.get("purchase_fees"),
            tax_year=year,
            depreciation_method=el["depreciation_method"] if el else None,
            section_179_amount=el["section_179_amount"] if el else None,
            bonus_depreciation_amount=el["bonus_depreciation_amount"] if el else None,
            basis_at_placed_in_service=el["basis_at_placed_in_service"] if el else None,
            business_use_pct_override=el["business_use_pct_override"] if el else None,
            listed_property_qualified=el["listed_property_qualified"] if el else None,
            notes=el["notes"] if el else None,
        ))
    ctx = Form4562Context(entity=entity, year=year, rows=rows)
    try:
        pdf = render_form_4562_worksheet_pdf(ctx)
    except PDFRenderingUnavailable as exc:
        return _pdf_unavailable_response(str(exc))
    name = f"vehicles-form-4562-{entity}-{year}.pdf"
    _persist_report(settings, name, pdf)
    return _pdf_response(pdf, name)


# Declared LAST so explicit /reports/{filename}.{ext} routes (schedule-c.csv,
# schedule-f.pdf, audit-portfolio.pdf, estimated-tax.csv, …) are matched
# first. The path validator rejects values that look like report filenames
# (contain a dot) so a stray /reports/audit-portfolio.pdf doesn't get
# treated as an entity slug; the registry lookup catches everything else.
@router.get("/reports/{entity_slug}", response_class=HTMLResponse)
def entity_report_index(
    entity_slug: str,
    conn = Depends(get_db),
):
    """Single-entity reports view. Redirects to ``/reports?entity={slug}``
    so the matrix template renders the per-year tile grid for one entity.
    Mirrors the ``/reports?entity=`` query form so both URL shapes work.
    """
    if "." in entity_slug or "/" in entity_slug:
        raise HTTPException(status_code=404, detail="Not a valid entity slug")
    # Reserved fragment names that other routers own. If they ever land
    # AFTER reports.router in the include order, FastAPI would route
    # the request here first and the entity-registry lookup would 404
    # with a misleading error. Hard-fail with status 404 so the wrong
    # ordering surfaces during dev rather than silently shadowing the
    # real route. The actual fix is to include the owning router
    # BEFORE reports.router (see main.py).
    _RESERVED = {
        "intercompany", "balance-audit", "vehicles",
        "schedule-c", "schedule-f",
        "audit-portfolio.pdf", "estimated-tax.pdf", "estimated-tax.csv",
    }
    if entity_slug in _RESERVED:
        raise HTTPException(
            status_code=404,
            detail=(
                f"'{entity_slug}' is a reserved report path that should be "
                "served by another router. This 404 means router include "
                "order shadowed the explicit route. Move the owning "
                "router before reports.router in main.py."
            ),
        )
    if conn is not None:
        try:
            row = conn.execute(
                "SELECT 1 FROM entities WHERE slug = ?", (entity_slug,),
            ).fetchone()
        except Exception:  # noqa: BLE001
            row = None
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Entity '{entity_slug}' not found in the registry. "
                    "Add it on /entities first, then reload this page."
                ),
            )
    return RedirectResponse(url=f"/reports?entity={entity_slug}", status_code=303)
