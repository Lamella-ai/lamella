# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""/import routes.

Module filename ends in `_` because `import` is a reserved keyword in
Python. The route prefix is `/import` regardless.
"""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_ai_service, get_db, get_ledger_reader, get_settings
from lamella.features.import_ import preview as preview_mod
from lamella.features.import_.classify import KNOWN_SOURCE_CLASSES, is_generic
from lamella.features.import_.mapping import (
    MappingResult,
    deserialize_mapping,
    heuristic_map,
    propose_mapping,
    serialize_mapping,
)
from lamella.features.import_.service import (
    STATUS_CATEGORIZED,
    STATUS_CLASSIFIED,
    STATUS_COMMITTED,
    STATUS_ERROR,
    STATUS_INGESTED,
    STATUS_MAPPED,
    STATUS_PREVIEWED,
    ImportError_,
    ImportService,
)
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.service import RuleService

log = logging.getLogger(__name__)


router = APIRouter(prefix="/import", tags=["import"])


def get_import_service(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn=Depends(get_db),
    ai: AIService = Depends(get_ai_service),
    reader: LedgerReader = Depends(get_ledger_reader),
) -> ImportService:
    return ImportService(
        conn=conn,
        settings=settings,
        ai=ai,
        reader=reader,
        reviews=ReviewService(conn),
        rules=RuleService(conn),
    )


ENTITY_CHOICES = (
    "Acme", "WidgetCo", "Personal", "Rentals", "FarmCo",
    "Consulting", "ThetaCo",
)


def _redirect_for(record) -> RedirectResponse:
    """Return a redirect for the current status of an import record."""
    status = record.status
    if status in (STATUS_MAPPED, STATUS_CLASSIFIED):
        if status == STATUS_MAPPED:
            return RedirectResponse(f"/import/{record.id}/ingest", status_code=303)
        return RedirectResponse(f"/import/{record.id}/classify", status_code=303)
    if status in (STATUS_INGESTED, STATUS_CATEGORIZED, STATUS_PREVIEWED):
        return RedirectResponse(f"/import/{record.id}/preview", status_code=303)
    return RedirectResponse(f"/import/{record.id}", status_code=303)


# ----------------------------------------------------------------------
# Upload list + upload form
# ----------------------------------------------------------------------


@router.get("", response_class=HTMLResponse)
def import_index(
    request: Request,
    service: ImportService = Depends(get_import_service),
):
    recent = service.list_recent(limit=50)
    ctx = {
        "recent": recent,
        "max_upload_bytes": service.settings.import_max_upload_bytes,
    }
    return request.app.state.templates.TemplateResponse(request, "import.html", ctx)


@router.post("")
async def upload_file(
    request: Request,
    file: UploadFile,
    service: ImportService = Depends(get_import_service),
):
    body = await file.read()
    if not body:
        raise HTTPException(status_code=400, detail="Empty upload")
    max_bytes = service.settings.import_max_upload_bytes
    if len(body) > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"Upload exceeds {max_bytes} bytes",
        )
    filename = file.filename or "upload"
    outcome = service.register_upload(filename=filename, body=body)
    record = outcome.record
    if outcome.was_new:
        try:
            service.classify(record.id)
        except Exception as exc:  # noqa: BLE001
            log.warning("classify after upload failed: %s", exc)
            service.conn.execute(
                "UPDATE imports SET status = ?, error = ? WHERE id = ?",
                (STATUS_ERROR, str(exc)[:2000], record.id),
            )
        record = service.get(record.id) or record
    return _redirect_for(record)


# ----------------------------------------------------------------------
# Detail + JSON
# ----------------------------------------------------------------------


@router.get("/{import_id}", response_class=HTMLResponse)
def import_detail(
    import_id: int,
    request: Request,
    service: ImportService = Depends(get_import_service),
):
    record = service.get(import_id)
    if record is None:
        raise HTTPException(status_code=404)
    sources = service.list_sources(import_id)
    ctx = {
        "record": record,
        "sources": sources,
    }
    return request.app.state.templates.TemplateResponse(
        request, "import_detail.html", ctx
    )


@router.get("/{import_id}.json")
def import_detail_json(
    import_id: int,
    service: ImportService = Depends(get_import_service),
):
    record = service.get(import_id)
    if record is None:
        raise HTTPException(status_code=404)
    return {
        "id": record.id,
        "status": record.status,
        "filename": record.filename,
        "rows_imported": record.rows_imported,
        "rows_committed": record.rows_committed,
        "source_class": record.source_class,
        "entity": record.entity,
        "error": record.error,
        "committed_at": record.committed_at,
    }


# ----------------------------------------------------------------------
# Classify
# ----------------------------------------------------------------------


@router.get("/{import_id}/classify", response_class=HTMLResponse)
def classify_page(
    import_id: int,
    request: Request,
    service: ImportService = Depends(get_import_service),
):
    record = service.get(import_id)
    if record is None:
        raise HTTPException(status_code=404)
    sources = service.list_sources(import_id)
    ctx = {
        "record": record,
        "sources": sources,
        "known_classes": KNOWN_SOURCE_CLASSES,
        "entities": ENTITY_CHOICES,
    }
    return request.app.state.templates.TemplateResponse(
        request, "import_classify.html", ctx
    )


@router.post("/{import_id}/classify")
async def classify_apply(
    import_id: int,
    request: Request,
    service: ImportService = Depends(get_import_service),
):
    form = await request.form()
    for src in service.list_sources(import_id):
        sc = form.get(f"source_class_{src['id']}")
        ent = form.get(f"entity_{src['id']}")
        stype = form.get(f"sheet_type_{src['id']}")
        service.update_source_overrides(
            source_id=int(src["id"]),
            source_class=sc or None,
            entity=(ent or None) if ent != "" else None,
            sheet_type=stype or None,
        )
    advanced = service.mark_classify_complete(import_id)
    if advanced:
        return RedirectResponse(f"/import/{import_id}/ingest", status_code=303)
    return RedirectResponse(f"/import/{import_id}/map", status_code=303)


# ----------------------------------------------------------------------
# Column mapping (generic sources only)
# ----------------------------------------------------------------------


@router.get("/{import_id}/map", response_class=HTMLResponse)
async def map_page(
    import_id: int,
    request: Request,
    service: ImportService = Depends(get_import_service),
    ai: AIService = Depends(get_ai_service),
):
    record = service.get(import_id)
    if record is None:
        raise HTTPException(status_code=404)
    sources = [
        s for s in service.list_sources(import_id)
        if is_generic(s["source_class"]) and s["sheet_type"] == "primary"
    ]
    if not sources:
        return RedirectResponse(f"/import/{import_id}/ingest", status_code=303)

    # For each source, compute or load the mapping.
    view_sources: list[dict] = []
    for src in sources:
        preview = preview_mod.preview_sheet(
            Path(record.stored_path),
            src["sheet_name"] if src["sheet_name"] != "(csv)" else None,
        )
        existing = deserialize_mapping(src["notes"])
        if existing is None:
            try:
                proposal = await propose_mapping(
                    ai,
                    preview=preview,
                    input_ref=f"import:{import_id}:sheet:{src['sheet_name']}",
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("propose_mapping failed: %s", exc)
                proposal = MappingResult(
                    column_map=heuristic_map(preview.columns),
                    header_row_index=preview.header_row_index,
                    confidence=0.4,
                    notes=f"AI unavailable ({exc}); heuristic mapping used.",
                    source="heuristic",
                )
            service.save_mapping(source_id=int(src["id"]), mapping=proposal)
            existing = proposal
        view_sources.append(
            {
                "source": src,
                "preview": preview,
                "mapping": existing,
            }
        )
    ctx = {
        "record": record,
        "sources": view_sources,
        "canonical_options": [
            "", "date", "amount", "currency", "payee", "description", "memo",
            "location", "payment_method", "transaction_id",
            "ann_master_category", "ann_subcategory", "ann_business_expense",
            "ann_business", "ann_expense_category", "ann_expense_memo",
            "ann_amount2",
        ],
        "edit_threshold": service.settings.import_ai_confidence_threshold,
    }
    return request.app.state.templates.TemplateResponse(
        request, "import_map.html", ctx
    )


@router.post("/{import_id}/map")
async def map_apply(
    import_id: int,
    request: Request,
    service: ImportService = Depends(get_import_service),
):
    form = await request.form()
    for src in service.list_sources(import_id):
        if not is_generic(src["source_class"]) or src["sheet_type"] != "primary":
            continue
        existing = deserialize_mapping(src["notes"]) or MappingResult(
            column_map={},
            header_row_index=0,
            confidence=0.5,
            notes="",
            source="user",
        )
        new_map: dict[str, str | None] = {}
        for key in existing.column_map.keys():
            field_name = f"col_{src['id']}_{key}"
            value = form.get(field_name)
            if value is None or value == "":
                new_map[key] = None
            else:
                new_map[key] = str(value)
        updated = MappingResult(
            column_map=new_map,
            header_row_index=existing.header_row_index,
            confidence=existing.confidence,
            notes=existing.notes,
            source="user",
            decision_id=existing.decision_id,
        )
        service.save_mapping(source_id=int(src["id"]), mapping=updated)
    return RedirectResponse(f"/import/{import_id}/ingest", status_code=303)


# ----------------------------------------------------------------------
# Ingest
# ----------------------------------------------------------------------


@router.get("/{import_id}/ingest", response_class=HTMLResponse)
def ingest_page(
    import_id: int,
    request: Request,
    service: ImportService = Depends(get_import_service),
):
    record = service.get(import_id)
    if record is None:
        raise HTTPException(status_code=404)
    if record.status in (STATUS_INGESTED, STATUS_CATEGORIZED, STATUS_PREVIEWED):
        return RedirectResponse(f"/import/{import_id}/preview", status_code=303)
    sources = service.list_sources(import_id)
    ctx = {"record": record, "sources": sources}
    return request.app.state.templates.TemplateResponse(
        request, "import_ingest.html", ctx
    )


@router.post("/{import_id}/ingest", response_class=HTMLResponse)
async def ingest_apply(
    import_id: int,
    request: Request,
    service: ImportService = Depends(get_import_service),
):
    """Ingest the parsed spreadsheet into staging then categorize via
    AI. The categorize step issues one LLM call per row; on a 500-row
    import this can run for minutes. Runs as a background job so the
    user sees live progress."""
    import asyncio as _asyncio

    def _work(ctx):
        ctx.emit(f"Ingesting import #{import_id} …", outcome="info")
        summary = service.ingest(import_id)
        ctx.emit(
            f"Staged {summary.total_rows} row(s) · "
            f"{summary.duplicate_rows} duplicate(s)",
            outcome="success" if summary.total_rows else "info",
        )
        if not summary.total_rows:
            return {"rows": 0}
        ctx.set_total(summary.total_rows)
        ctx.emit(
            f"Classifying {summary.total_rows} row(s) with AI …",
            outcome="info",
        )
        loop = _asyncio.new_event_loop()
        try:
            loop.run_until_complete(service.categorize(import_id))
        finally:
            loop.close()
        ctx.emit("Categorize complete", outcome="success")
        return {"rows": summary.total_rows}

    runner = request.app.state.job_runner
    redirect_url = f"/import/{import_id}/preview"
    job_id = runner.submit(
        kind="import-ingest",
        title=f"Ingesting import #{import_id}",
        fn=_work,
        return_url=redirect_url,
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": redirect_url},
    )


# ----------------------------------------------------------------------
# Preview
# ----------------------------------------------------------------------


@router.get("/{import_id}/preview", response_class=HTMLResponse)
def preview_page(
    import_id: int,
    request: Request,
    service: ImportService = Depends(get_import_service),
):
    record = service.get(import_id)
    if record is None:
        raise HTTPException(status_code=404)
    rows = service.preview_rows(import_id)
    # Summary stats for the banner.
    total_rows = len(rows)
    review_rows = sum(1 for r in rows if r["needs_review"])
    per_entity: dict[str, Decimal] = {}
    per_account: dict[str, Decimal] = {}
    per_year: dict[str, int] = {}
    for r in rows:
        # raw_rows.amount is TEXT (post-migration 057); read as Decimal
        # to preserve precision in the per-entity / per-account totals.
        amt = Decimal(str(r["amount"])) if r["amount"] is not None else Decimal("0")
        ent = r["entity"] or "—"
        per_entity[ent] = per_entity.get(ent, Decimal("0")) + amt
        acct = r["account"] or "Expenses:Uncategorized"
        per_account[acct] = per_account.get(acct, Decimal("0")) + amt
        if r["date"]:
            year = str(r["date"])[:4]
            per_year[year] = per_year.get(year, 0) + 1
    ctx = {
        "record": record,
        "rows": rows,
        "total_rows": total_rows,
        "review_rows": review_rows,
        "per_entity": sorted(per_entity.items()),
        "per_account": sorted(per_account.items()),
        "per_year": sorted(per_year.items()),
        "entities": ENTITY_CHOICES,
    }
    return request.app.state.templates.TemplateResponse(
        request, "import_preview.html", ctx
    )


@router.post("/{import_id}/preview/recategorize")
def preview_recategorize(
    import_id: int,
    service: ImportService = Depends(get_import_service),
    raw_row_id: int = Form(...),
    account: str = Form(...),
    entity: str | None = Form(default=None),
    schedule_c_category: str | None = Form(default=None),
):
    service.recategorize(
        raw_row_id=raw_row_id,
        account=account,
        entity=entity or None,
        schedule_c_category=schedule_c_category or None,
    )
    return RedirectResponse(f"/import/{import_id}/preview", status_code=303)


# ----------------------------------------------------------------------
# Commit / cancel / delete
# ----------------------------------------------------------------------


@router.post("/{import_id}/commit")
def commit(
    import_id: int,
    service: ImportService = Depends(get_import_service),
):
    try:
        service.commit(import_id)
    except BeanCheckError as exc:
        raise HTTPException(status_code=422, detail=f"bean-check failed: {exc}")
    except ImportError_ as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return RedirectResponse(f"/import/{import_id}", status_code=303)


@router.post("/{import_id}/cancel")
def cancel(
    import_id: int,
    service: ImportService = Depends(get_import_service),
):
    service.cancel(import_id)
    return RedirectResponse(f"/import/{import_id}", status_code=303)


@router.delete("/{import_id}")
def hard_delete(
    import_id: int,
    service: ImportService = Depends(get_import_service),
):
    try:
        service.hard_delete(import_id)
    except ImportError_ as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return Response(status_code=204)
