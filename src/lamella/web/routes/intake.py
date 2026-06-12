# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Generic / pasted-text intake route — NEXTGEN.md Phase D.

``GET /intake`` shows a paste textbox. ``POST /intake/preview``
parses the pasted text, runs heuristic column detection + fuzzy
duplicate detection against staging history, and re-renders with
the proposed mapping and any duplicate warnings.
``POST /intake/stage`` commits the rows to the unified staging
surface with ``source='paste'``. High-severity duplicate batches
are refused unless the user explicitly confirms via
``confirm_duplicate=1``.

The duplicate detector is source-agnostic: a pasted row whose
``(date, abs(amount), normalized_description)`` fingerprint matches
a SimpleFIN row from the same window gets flagged the same as a
re-paste of the same statement. Both scenarios are real — the
user mentioned both in the Phase D design review.
"""
from __future__ import annotations

import logging
import secrets
import sqlite3
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse

from lamella.core.config import Settings
from lamella.features.import_.archive import archive_file
from lamella.web.deps import get_db, get_settings
from lamella.features.import_.staging import (
    IntakeError,
    IntakeService,
    StagingService,
    detect_columns_by_content,
    detect_paste_duplicates,
    heuristic_column_map,
    parse_pasted_text,
)

log = logging.getLogger(__name__)

router = APIRouter()


def _build_column_map(parsed):
    """Pick the strongest available mapping: header-based first, then
    fall back to content-based when headers are synthetic or none of
    them matched the heuristic patterns."""
    column_map = heuristic_column_map(parsed.columns)
    if all(v is None for v in column_map.values()) or parsed.header_row_index < 0:
        column_map = detect_columns_by_content(parsed)
    return column_map


@router.get("/intake", response_class=HTMLResponse)
def intake_page(request: Request):
    """Empty paste form."""
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "intake.html",
        {
            "text": "",
            "parsed": None,
            "column_map": None,
            "duplicate_report": None,
            "error": None,
            "staged_count": None,
        },
    )


@router.post("/intake/preview", response_class=HTMLResponse)
def intake_preview(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    text: str = Form(...),
    has_header: str | None = Form(default="1"),
):
    """Parse the paste, run duplicate detection, show the result."""
    templates = request.app.state.templates
    try:
        parsed = parse_pasted_text(text, has_header=bool(has_header))
    except IntakeError as exc:
        return templates.TemplateResponse(
            request,
            "intake.html",
            {
                "text": text,
                "parsed": None,
                "column_map": None,
                "duplicate_report": None,
                "error": str(exc),
                "staged_count": None,
            },
            status_code=400,
        )

    column_map = _build_column_map(parsed)
    duplicate_report = detect_paste_duplicates(conn, parsed, column_map)

    return templates.TemplateResponse(
        request,
        "intake.html",
        {
            "text": text,
            "parsed": parsed,
            "column_map": column_map,
            "duplicate_report": duplicate_report,
            "error": None,
            "staged_count": None,
        },
    )


@router.post("/intake/stage", response_class=HTMLResponse)
def intake_stage(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
    text: str = Form(...),
    has_header: str | None = Form(default="1"),
    confirm_duplicate: str | None = Form(default=None),
):
    """Commit the pasted rows to the staging surface.

    Re-derives the column map + duplicate report from the paste text
    so the contract is "submit the same paste, get the same result."
    If the duplicate severity is ``high`` (>= 80% of rows match
    existing staging history), the endpoint refuses unless
    ``confirm_duplicate`` is set — the user has to acknowledge
    they're intentionally re-staging. Per-row matches at any
    severity are flagged on the staged row's decision as
    ``needs_review=True`` so the review UI surfaces them.
    """
    templates = request.app.state.templates
    try:
        parsed = parse_pasted_text(text, has_header=bool(has_header))
    except IntakeError as exc:
        return templates.TemplateResponse(
            request,
            "intake.html",
            {
                "text": text,
                "parsed": None,
                "column_map": None,
                "duplicate_report": None,
                "error": str(exc),
                "staged_count": None,
            },
            status_code=400,
        )

    column_map = _build_column_map(parsed)
    duplicate_report = detect_paste_duplicates(conn, parsed, column_map)

    if duplicate_report.severity == "high" and not confirm_duplicate:
        # Refuse: this paste almost entirely overlaps existing staging
        # history. User has to opt in explicitly.
        return templates.TemplateResponse(
            request,
            "intake.html",
            {
                "text": text,
                "parsed": parsed,
                "column_map": column_map,
                "duplicate_report": duplicate_report,
                "error": (
                    f"{int(duplicate_report.overlap_ratio * 100)}% of "
                    f"these rows match existing staging history. "
                    "Check the duplicate report and re-submit with "
                    "'I've reviewed' if you really want to stage them."
                ),
                "staged_count": None,
            },
            status_code=409,
        )

    session_id = "paste-" + secrets.token_hex(6)

    # ADR-0060 — archive the raw paste text under <ledger_dir>/imports/
    # before staging, then key every row's source_ref off the resulting
    # file_id. Re-pasting the same content reuses the existing file_id
    # via content-sha256 dedup, so staged rows upsert in place.
    paste_filename = (
        f"paste-{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H%M%S')}.csv"
    )
    archived = archive_file(
        conn,
        ledger_dir=settings.ledger_dir,
        content=text.encode("utf-8"),
        original_filename=paste_filename,
        source_format="paste",
    )

    service = IntakeService(conn)
    result = service.stage_paste(
        session_id=session_id,
        parsed=parsed,
        column_map=column_map,
        archived_file_id=archived.file_id,
    )

    # Flag per-row duplicates on the staging surface so the review UI
    # (and anything reading v_staged_pending) picks them up as needing
    # human attention.
    staging = StagingService(conn)
    for match in duplicate_report.matches:
        row = staging.get_by_ref(
            source="paste",
            source_ref={"file_id": archived.file_id, "row": match.row_index},
        )
        if row is None:
            # The row was skipped by stage_paste (bad date/amount) —
            # no staged_id to flag.
            continue
        existing = staging.get_decision(row.id)
        hint_bits: list[str] = []
        hint_bits.append(
            "Likely duplicate of staged #"
            + ", #".join(str(i) for i in match.matched_staged_ids[:3])
        )
        src_list = ", ".join(match.matched_sources)
        if src_list:
            hint_bits.append(f"matched source(s): {src_list}")
        rationale = " | ".join(hint_bits)
        if existing and existing.rationale:
            rationale = existing.rationale + " | " + rationale
        staging.record_decision(
            staged_id=row.id,
            account=existing.account if existing else None,
            confidence="unresolved",
            decided_by="auto",
            rationale=rationale,
            needs_review=True,
        )
        result.duplicates_flagged += 1

    conn.commit()

    log.info(
        "intake: pasted %d rows → %d staged, %d skipped, %d duplicate-flagged "
        "(session=%s, overlap=%.0f%%, severity=%s)",
        result.total_rows, result.staged, result.skipped,
        result.duplicates_flagged, session_id,
        duplicate_report.overlap_ratio * 100,
        duplicate_report.severity,
    )
    return templates.TemplateResponse(
        request,
        "intake.html",
        {
            "text": "",
            "parsed": None,
            "column_map": None,
            "duplicate_report": duplicate_report,
            "error": None,
            "staged_count": result.staged,
            "skipped_count": result.skipped,
            "duplicates_flagged": result.duplicates_flagged,
            "errors_list": result.errors,
            "session_id": session_id,
            "confirmed": bool(confirm_duplicate),
        },
    )
