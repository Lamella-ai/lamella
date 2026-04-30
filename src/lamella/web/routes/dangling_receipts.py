# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Dangling-receipt-link report.

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
    sweep_paperless_link_health,
)
from lamella.web.deps import get_db, get_settings

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/receipts/dangling", response_class=HTMLResponse)
def dangling_receipts_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Report-page surface. Lists dangling links + a "Sweep now" button."""
    dangling = list_dangling_links(conn)
    status = link_health_status(conn)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "dangling_receipts.html",
        {
            "dangling": dangling,
            "status": status,
        },
    )


@router.post("/receipts/dangling/sweep")
async def dangling_receipts_sweep(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Run one Paperless link-health sweep. Probes every distinct
    paperless_id in receipt_links and updates counters. Returns a
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
        log.warning("dangling-receipts sweep failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail=f"Paperless sweep failed: {exc}",
        )

    if request.headers.get("hx-request", "").lower() == "true":
        return JSONResponse(
            content={
                "checked": result.checked,
                "seen": result.seen,
                "not_found": result.not_found,
                "transport_errors": result.transport_errors,
                "crossed_threshold": result.crossed_threshold,
            },
            headers={"HX-Refresh": "true"},
        )
    return {
        "checked": result.checked,
        "seen": result.seen,
        "not_found": result.not_found,
        "transport_errors": result.transport_errors,
        "crossed_threshold": result.crossed_threshold,
    }
