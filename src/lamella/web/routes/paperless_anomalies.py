# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Anomaly review queue (ADR-0062 §8).

GET  /documents/anomalies
    Lists documents currently flagged with `Lamella_DateAnomaly` or
    `Lamella_NeedsReview`, joined to the most recent matching
    `paperless_writeback_log` row so the user sees why-flagged.

POST /documents/anomalies/{paperless_id}/confirm-date
    Removes the `Lamella_DateAnomaly` tag from the document, writes
    a `workflow_action` audit row noting manual resolution.

POST /documents/anomalies/{paperless_id}/re-extract
    Adds the `Lamella_AwaitingExtraction` tag and removes
    `Lamella_NeedsReview` so the next workflow tick re-processes it.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from lamella.adapters.paperless.client import PaperlessClient, PaperlessError
from lamella.core.config import Settings
from lamella.features.paperless_bridge.tag_workflow import (
    KIND_WORKFLOW_ACTION,
    KIND_WORKFLOW_ANOMALY,
    TAG_AWAITING_EXTRACTION,
    TAG_DATE_ANOMALY,
    TAG_NEEDS_REVIEW,
)
from lamella.web.deps import get_db, get_paperless, get_settings

log = logging.getLogger(__name__)

router = APIRouter()


def _is_htmx(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"


def _load_anomaly_rows(conn: sqlite3.Connection, *, limit: int) -> list[dict[str, Any]]:
    """Pull the most recent workflow_anomaly row per paperless_id,
    joined to the local doc index for vendor/title context."""
    rows = conn.execute(
        f"""
        SELECT wl.id, wl.paperless_id, wl.kind, wl.payload_json,
               wl.applied_at, pdi.vendor, pdi.title,
               pdi.document_date, pdi.tags_json
          FROM paperless_writeback_log wl
          LEFT JOIN paperless_doc_index pdi
                 ON pdi.paperless_id = wl.paperless_id
         WHERE wl.kind = '{KIND_WORKFLOW_ANOMALY}'
         ORDER BY wl.applied_at DESC, wl.id DESC
         LIMIT ?
        """,
        (limit,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    seen_doc_ids: set[int] = set()
    for row in rows:
        if row["paperless_id"] in seen_doc_ids:
            # Keep the most recent anomaly row per doc; older
            # markers for the same doc are historical noise.
            continue
        seen_doc_ids.add(row["paperless_id"])
        try:
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        except ValueError:
            payload = {"_parse_error": True}
        details = payload.get("details") or {}
        reason = details.get("reason") or payload.get("action_summary") or "(unknown)"
        out.append({
            "id": row["id"],
            "paperless_id": row["paperless_id"],
            "vendor": row["vendor"] or "(unknown)",
            "title": row["title"] or "",
            "document_date": row["document_date"],
            "rule": payload.get("rule") or "?",
            "reason": reason,
            "summary": payload.get("action_summary") or "",
            "applied_at": row["applied_at"],
        })
    return out


@router.get("/documents/anomalies", response_class=HTMLResponse)
def anomalies_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    limit: int = 200,
):
    rows = _load_anomaly_rows(conn, limit=min(max(limit, 1), 1000))
    return request.app.state.templates.TemplateResponse(
        request, "paperless_anomalies.html",
        {
            "rows": rows,
            "total": len(rows),
            "paperless_base_url": (settings.paperless_url or "").rstrip("/"),
        },
    )


def _write_resolution_audit(
    conn: sqlite3.Connection,
    *,
    paperless_id: int,
    action: str,
    note: str,
) -> None:
    payload = {
        "rule": "manual_resolution",
        "status": "success",
        "action_summary": note,
        "manual_action": action,
    }
    dedup_key = (
        f"manual:{action}:{paperless_id}:"
        f"{datetime.now(timezone.utc).isoformat()}"
    )
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO paperless_writeback_log
                (paperless_id, kind, dedup_key, payload_json, ai_decision_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                paperless_id, KIND_WORKFLOW_ACTION, dedup_key,
                json.dumps(payload), None,
            ),
        )
        conn.commit()
    except sqlite3.Error as exc:
        log.warning("manual-resolution audit insert failed: %s", exc)


@router.post("/documents/anomalies/{paperless_id}/confirm-date")
async def confirm_date(
    paperless_id: int,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    client: PaperlessClient = Depends(get_paperless),
):
    """Mark the document's date as confirmed-by-user. Removes the
    DateAnomaly tag (and NeedsReview if present from the same flow)
    so the doc drops off the queue. Writes a workflow_action audit
    row for forensics."""
    tags = await client.list_tags()
    removed: list[str] = []
    for name in (TAG_DATE_ANOMALY, TAG_NEEDS_REVIEW):
        tid = tags.get(name)
        if tid is None:
            continue
        try:
            await client.remove_tag(paperless_id, tid)
            removed.append(name)
        except PaperlessError as exc:
            log.warning(
                "confirm-date: failed to remove %s on doc %d: %s",
                name, paperless_id, exc,
            )
    _write_resolution_audit(
        conn,
        paperless_id=paperless_id,
        action="confirm-date",
        note=f"user confirmed date — removed tags: {', '.join(removed) or '(none)'}",
    )
    if _is_htmx(request):
        return HTMLResponse("")  # row vanishes from the queue
    return HTMLResponse(
        f"<p>Date confirmed for #{paperless_id}. "
        f"<a href='/documents/anomalies'>back to queue</a></p>"
    )


@router.post("/documents/anomalies/{paperless_id}/re-extract")
async def re_extract(
    paperless_id: int,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    client: PaperlessClient = Depends(get_paperless),
):
    """Queue the document for re-extraction by the workflow scheduler.
    Adds AwaitingExtraction; removes NeedsReview so the
    extract_missing_fields rule picks it up on the next tick."""
    aw_tid = await client.ensure_tag(TAG_AWAITING_EXTRACTION)
    try:
        await client.add_tag(paperless_id, aw_tid)
    except PaperlessError as exc:
        log.warning(
            "re-extract: add AwaitingExtraction failed for doc %d: %s",
            paperless_id, exc,
        )
    tags = await client.list_tags()
    nr_tid = tags.get(TAG_NEEDS_REVIEW)
    if nr_tid is not None:
        try:
            await client.remove_tag(paperless_id, nr_tid)
        except PaperlessError as exc:
            log.warning(
                "re-extract: remove NeedsReview failed for doc %d: %s",
                paperless_id, exc,
            )
    _write_resolution_audit(
        conn,
        paperless_id=paperless_id,
        action="re-extract",
        note="user requested re-extraction; queued for next workflow tick",
    )
    if _is_htmx(request):
        return HTMLResponse("")
    return HTMLResponse(
        f"<p>Re-extraction queued for #{paperless_id}. "
        f"<a href='/documents/anomalies'>back to queue</a></p>"
    )
