# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Dangling-document-link report.

Shows links whose Paperless document has been deleted (>=3 consecutive
404 sweeps + 7-day cooldown). User clicks "Sweep now" to probe; user
clicks "Unlink" on a row to clean it up. NEVER auto-unlinks. Transport
errors during the sweep are not counted as evidence — see the safety
guards in :mod:`lamella.features.receipts.dangling`.
"""
from __future__ import annotations

import logging
import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from lamella.adapters.paperless.client import PaperlessClient
from lamella.core.config import Settings
from lamella.features.receipts.dangling import (
    list_dangling_links,
    link_health_status,
    purge_confirmed_dead,
    sweep_paperless_link_health,
)
from lamella.web.deps import get_db, get_settings

log = logging.getLogger(__name__)
router = APIRouter()


def _render_dangling_documents_page(
    request: Request,
    *,
    conn: sqlite3.Connection,
    receipts_only: bool = False,
):
    """Shared rendering core for the dangling-link report. ``receipts_only``
    narrows the rows to links whose Paperless document is a *receipt*
    (per the local index's document_type role mapping). Today every
    dangling link is at least nominally a receipt-link, so the filter
    is mostly cosmetic — but giving the user a stable
    ``/documents/receipts/dangling`` URL preserves the old bookmark
    target when /receipts/dangling 308-redirects here, and leaves room
    for the future (when invoices/statements may also be linkable and
    we want a separate "dangling invoices" report)."""
    dangling = list_dangling_links(conn)
    if receipts_only:
        # Filter to paperless_ids whose role in paperless_doc_index is
        # receipt or invoice (matching the documents-page convention),
        # OR rows with no document_type at all. Anything explicitly
        # tagged as a non-receipt type is excluded.
        if dangling:
            ids = [d.paperless_id for d in dangling]
            placeholders = ",".join("?" for _ in ids)
            type_rows = conn.execute(
                f"SELECT paperless_id, document_type_name "
                f"FROM paperless_doc_index "
                f"WHERE paperless_id IN ({placeholders})",
                ids,
            ).fetchall()
            type_by_id: dict[int, str | None] = {
                int(r["paperless_id"]): r["document_type_name"]
                for r in type_rows
            }

            def _is_receipt_like(pid: int) -> bool:
                # No row in the index → unknown → keep (matches the
                # "receipts_only" scope on /documents which keeps
                # NULL document_type rows).
                if pid not in type_by_id:
                    return True
                tn = (type_by_id.get(pid) or "").strip().lower()
                if not tn:
                    return True
                return any(k in tn for k in ("receipt", "invoice"))

            dangling = [d for d in dangling if _is_receipt_like(d.paperless_id)]
    status = link_health_status(conn)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "dangling_documents.html",
        {
            "dangling": dangling,
            "status": status,
            "type_filter_locked": receipts_only,
            "type_filter_label": "Receipt" if receipts_only else None,
        },
    )


@router.get("/documents/dangling", response_class=HTMLResponse)
def dangling_documents_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Report-page surface. Lists dangling links + a "Sweep now" button."""
    return _render_dangling_documents_page(request, conn=conn)


@router.get("/documents/receipts/dangling", response_class=HTMLResponse)
def dangling_receipts_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Receipt-filtered sibling of /documents/dangling.

    Old bookmarks at /receipts/dangling 308-redirect here. The page is
    the same dangling-link report, scoped to paperless_ids that are
    receipt-typed (or have no document_type assigned, matching the
    /documents 'receipts_only' scope semantics).
    """
    return _render_dangling_documents_page(
        request, conn=conn, receipts_only=True,
    )


@router.post("/documents/dangling/sweep")
async def dangling_documents_sweep(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Run one Paperless link-health sweep. Probes every distinct
    paperless_id in document_links and updates counters. Returns a
    summary the page can render.

    A single sweep is NOT enough to mark a link as dangling — the
    counter has to reach 3, AND the first-404 timestamp has to be
    >= 7 days old. This is exactly the safety the user asked for:
    a network blip that returns 404 once during a sweep does not
    mass-delete receipts.
    """
    if not settings.paperless_url or not settings.paperless_api_token:
        raise HTTPException(
            status_code=503,
            detail="Paperless is not configured — nothing to sweep.",
        )
    try:
        async with PaperlessClient(
            base_url=settings.paperless_url,
            api_token=settings.paperless_api_token.get_secret_value(),
        ) as client:
            result = await sweep_paperless_link_health(conn, client)
    except Exception as exc:  # noqa: BLE001
        log.warning("dangling-documents sweep failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail=f"Paperless sweep failed: {exc}",
        )

    # After each sweep, purge any paperless_ids that have now crossed
    # the confirmed-dead gate (3x404 + 7-day cooldown). The purge
    # deletes the paperless_doc_index row and writes a tombstone
    # directive to connector_links.bean so the ID is never re-ingested.
    # Purge failures are logged but never propagate to the caller —
    # the sweep result is still surfaced to the user.
    purge_result = None
    try:
        purge_result = purge_confirmed_dead(
            conn,
            connector_links=settings.connector_links_path,
            main_bean=settings.ledger_main,
        )
        if purge_result.purged:
            log.info(
                "dangling-purge: purged %d doc(s) from paperless_doc_index "
                "after sweep; %d tombstone(s) written.",
                purge_result.purged,
                purge_result.tombstoned,
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("dangling-purge: purge step failed: %s", exc)

    content = {
        "checked": result.checked,
        "seen": result.seen,
        "not_found": result.not_found,
        "transport_errors": result.transport_errors,
        "crossed_threshold": result.crossed_threshold,
        "purged": purge_result.purged if purge_result else 0,
    }
    if request.headers.get("hx-request", "").lower() == "true":
        return JSONResponse(
            content=content,
            headers={"HX-Refresh": "true"},
        )
    return content
