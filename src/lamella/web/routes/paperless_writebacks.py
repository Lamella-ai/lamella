# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Audit view: every Paperless doc Lamella has touched.

Surfaces `paperless_writeback_log` as a browsable page so a human
can answer "which docs did the AI edit, and what did it change?"
without combing through Paperless or the ai_decisions raw table.

One row per writeback. Columns: when, doc id + link, kind
(verify_correction / enrichment_note), diff summary, link back to
the ai_decisions row that produced it.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from lamella.core.config import Settings
from lamella.web.deps import get_db, get_settings

log = logging.getLogger(__name__)

router = APIRouter()


def _parse_ts(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    try:
        return datetime.fromisoformat(str(raw).replace(" ", "T"))
    except ValueError:
        return None


def _summarize(kind: str, payload: dict[str, Any]) -> str:
    """One-line summary for the audit table."""
    if kind == "verify_correction":
        diffs = payload.get("diffs") or []
        if not diffs:
            return "no field-level changes"
        return ", ".join(
            f"{d.get('field')}: {d.get('before') or '∅'} → "
            f"{d.get('after') or '∅'}"
            for d in diffs[:3]
        ) + (f" (+{len(diffs) - 3} more)" if len(diffs) > 3 else "")
    if kind == "enrichment_note":
        note = payload.get("note") or ""
        return note.splitlines()[0][:140] if note else "(no body)"
    return json.dumps(payload)[:140]


def _load_rows(conn: sqlite3.Connection, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT wl.id, wl.paperless_id, wl.kind, wl.dedup_key,
               wl.payload_json, wl.ai_decision_id, wl.applied_at,
               pdi.vendor, pdi.title
          FROM paperless_writeback_log wl
          LEFT JOIN paperless_doc_index pdi
                 ON pdi.paperless_id = wl.paperless_id
         ORDER BY wl.applied_at DESC, wl.id DESC
         LIMIT ?
        """,
        (limit,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        except ValueError:
            payload = {"_parse_error": True, "raw": row["payload_json"]}
        out.append({
            "id": row["id"],
            "paperless_id": row["paperless_id"],
            "kind": row["kind"],
            "dedup_key": row["dedup_key"],
            "payload": payload,
            "summary": _summarize(row["kind"], payload),
            "ai_decision_id": row["ai_decision_id"],
            "applied_at": _parse_ts(row["applied_at"]) or datetime.now(timezone.utc),
            "vendor": row["vendor"],
            "title": row["title"],
        })
    return out


@router.get("/documents/writebacks", response_class=HTMLResponse)
def writebacks_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    limit: int = 200,
):
    """Audit page for Paperless writebacks. `limit` caps the page
    size at 200 by default — high enough to eyeball a week of
    activity, low enough to render fast."""
    rows = _load_rows(conn, min(max(limit, 1), 1000))
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM paperless_writeback_log"
    ).fetchone()["n"]
    return request.app.state.templates.TemplateResponse(
        request, "paperless_writebacks.html",
        {
            "rows": rows,
            "total": int(total or 0),
            "paperless_base_url": (settings.paperless_url or "").rstrip("/"),
        },
    )
