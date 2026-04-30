# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""/inbox — the staged-queue workflow.

Mental model: "/inbox" = stuff waiting for you, the way an email inbox
holds threads waiting for a reply. The actual queue is the staged-
transaction list (post-import, pre-classification). This module USED
to be a count-tiles dashboard; that responsibility moved to the main
dashboard at `/` (each tile remains a deep-link to the focused page
that owns the workflow). The staged-queue itself moved here from
/review; /review is kept as a 301 alias for bookmark stability.

Per ADR-0047 the dashboard owns "what should I do next"; per
ADR-0048 the workflow URL matches the user's mental label.
"""
from __future__ import annotations

import logging
import sqlite3

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.web.deps import get_db, get_ledger_reader, get_review_service
from lamella.features.review_queue.service import ReviewService
from lamella.features.import_.staging import count_pending_items

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/inbox", response_class=HTMLResponse)
def inbox_page(
    request: Request,
    source: str | None = None,
    hide_transfers: bool = False,
    sort: str = "groups",
    page: int = 1,
    service: ReviewService = Depends(get_review_service),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Canonical staged-queue surface. Same content as the legacy
    /review URL, served at /inbox to match the mental label. Loads
    the unified staged-list context exactly the way /review used to;
    legacy review_items render is the empty-staged fallback."""
    from lamella.web.routes.staging_review import _staged_list_context

    legacy_items = service.list_open()
    staging_pending = count_pending_items(conn)
    # ADR-0058 — surface the multi-source-observation queue count in the
    # inbox header so the user has a one-click jump to /inbox/duplicates
    # whenever an import lands rows the dedup oracle flagged.
    try:
        duplicates_count = int(conn.execute(
            "SELECT COUNT(*) FROM staged_transactions "
            "WHERE status = 'likely_duplicate'"
        ).fetchone()[0] or 0)
    except Exception:  # noqa: BLE001 — schema may not exist on a brand-new DB
        duplicates_count = 0
    if staging_pending > 0 or not legacy_items:
        try:
            ledger_entries = reader.load().entries if staging_pending > 0 else []
        except Exception:  # noqa: BLE001
            ledger_entries = []
        ctx = _staged_list_context(
            conn,
            source=source,
            hide_transfers=hide_transfers,
            sort=sort,
            page=page,
            entries=ledger_entries,
        )
        ctx["duplicates_count"] = duplicates_count
        templates = request.app.state.templates
        if request.headers.get("HX-Request"):
            return templates.TemplateResponse(
                request, "partials/_staged_list.html", ctx,
            )
        return templates.TemplateResponse(
            request, "staging_review.html", ctx,
        )
    # Empty-staged fallback: hand off to the legacy /review handler so
    # the rare "ledger has FIXMEs but no staging rows" path still
    # renders. Avoids duplicating ~50 lines of FIXME-grouping code.
    from lamella.web.routes.review import review_page
    return review_page(
        request, source=source, hide_transfers=hide_transfers,
        sort=sort, page=page, service=service, reader=reader, conn=conn,
    )


@router.get("/review", response_class=HTMLResponse)
def review_redirect(request: Request):
    """Legacy URL — /review is now /inbox. 301 so existing bookmarks
    and external links keep working but any sidebar / cross-reference
    drift gets caught by the redirect."""
    target = "/inbox"
    if request.url.query:
        target = f"/inbox?{request.url.query}"
    return RedirectResponse(target, status_code=301)
