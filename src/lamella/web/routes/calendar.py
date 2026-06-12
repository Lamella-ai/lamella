# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Calendar routes — month grid + day view.

- GET /calendar               → redirect to /calendar/<current-month>
- GET /calendar/<YYYY-MM>     → month grid
- GET /calendar/<YYYY-MM-DD>  → day view
- POST /calendar/<YYYY-MM-DD>/review  → toggle mark-reviewed
- POST /calendar/<YYYY-MM-DD>/note    → create single-day unscoped note
- POST /calendar/<YYYY-MM-DD>/note/<note_id>/delete → delete a day note

All writes that change review state also append a `custom "day-review"`
directive so reconstruct can rebuild the SQLite row. Marks-undone
writes a `custom "day-review-deleted"` tombstone.
"""
from __future__ import annotations

import asyncio
import calendar as pycal
import json
import logging
import re
from datetime import date, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.features.calendar.ai import (
    audit_day as run_audit_day,
    audit_entries_to_json,
    summarize_day as run_summarize_day,
)
from lamella.features.calendar.flags import compute_day_flags
from lamella.features.calendar.queries import (
    activity_in_range,
    day_activity,
)
from lamella.features.calendar.tz import app_tz, today_local
from lamella.features.calendar.writer import (
    append_day_review,
    append_day_review_deleted,
)
from lamella.core.config import Settings
from lamella.web.deps import (
    get_ai_service,
    get_db,
    get_ledger_reader,
    get_settings,
)
from lamella.features.notes.service import NoteService
from lamella.core.ledger_writer import BeanCheckError

log = logging.getLogger(__name__)

router = APIRouter()

_MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")
_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")


def _parse_month(s: str) -> tuple[int, int]:
    m = _MONTH_RE.match(s)
    if not m:
        raise HTTPException(status_code=404, detail="bad month (expected YYYY-MM)")
    year, month = int(m.group(1)), int(m.group(2))
    if month < 1 or month > 12 or year < 1900 or year > 9999:
        raise HTTPException(status_code=404, detail="bad month")
    return year, month


def _parse_date(s: str) -> date:
    m = _DATE_RE.match(s)
    if not m:
        raise HTTPException(status_code=404, detail="bad date (expected YYYY-MM-DD)")
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        raise HTTPException(status_code=404, detail="bad date") from None


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    first = date(year, month, 1)
    last_day = pycal.monthrange(year, month)[1]
    last = date(year, month, last_day)
    return first, last


def _grid_cells(year: int, month: int) -> list[date | None]:
    """Return a list of dates (or None for leading blanks) for a
    standard Sun-starting calendar month grid."""
    first, last = _month_bounds(year, month)
    # weekday(): Monday=0, Sunday=6. We want Sunday as column 0 to match
    # the common US convention in the template (easy to swap).
    # Shift so Sunday=0.
    lead = (first.weekday() + 1) % 7
    cells: list[date | None] = [None] * lead
    d = first
    while d <= last:
        cells.append(d)
        d += timedelta(days=1)
    # Pad tail so the grid has full rows (7 cols).
    while len(cells) % 7 != 0:
        cells.append(None)
    return cells


def _prev_next_month(year: int, month: int) -> tuple[str, str]:
    if month == 1:
        prev = (year - 1, 12)
    else:
        prev = (year, month - 1)
    if month == 12:
        nxt = (year + 1, 1)
    else:
        nxt = (year, month + 1)
    return f"{prev[0]:04d}-{prev[1]:02d}", f"{nxt[0]:04d}-{nxt[1]:02d}"


@router.get("/calendar", response_class=HTMLResponse)
def calendar_root(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    today = today_local(settings)
    return RedirectResponse(
        url=f"/calendar/{today.year:04d}-{today.month:02d}",
        status_code=303,
    )


@router.get("/calendar/{token}")
def calendar_dispatch(
    token: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    """Single URL scheme for month + day:
      * /calendar/YYYY-MM     → month grid
      * /calendar/YYYY-MM-DD  → day view
    """
    if _MONTH_RE.match(token):
        return _render_month(token, request, settings, reader, conn)
    if _DATE_RE.match(token):
        return _render_day(token, request, settings, reader, conn)
    raise HTTPException(status_code=404, detail="not found")


def _render_month(
    token: str,
    request: Request,
    settings: Settings,
    reader: LedgerReader,
    conn,
) -> HTMLResponse:
    year, month = _parse_month(token)
    first, last = _month_bounds(year, month)
    entries = list(reader.load().entries)
    aggregates = activity_in_range(conn, entries, first, last, settings=settings)
    cells = _grid_cells(year, month)

    today = today_local(settings)
    prev_m, next_m = _prev_next_month(year, month)
    month_label = first.strftime("%B %Y")
    return request.app.state.templates.TemplateResponse(
        request,
        "calendar_month.html",
        {
            "year": year,
            "month": month,
            "month_label": month_label,
            "cells": cells,
            "aggregates": aggregates,
            "today": today,
            "prev_month": prev_m,
            "next_month": next_m,
        },
    )


def _render_day(
    token: str,
    request: Request,
    settings: Settings,
    reader: LedgerReader,
    conn,
) -> HTMLResponse:
    d = _parse_date(token)
    entries = list(reader.load().entries)
    view = day_activity(conn, entries, d, settings=settings)
    view.flags = [_flag_dict(f) for f in compute_day_flags(conn, entries, d)]

    # Prefer the full-aggregate status so the day view matches what
    # the month grid shows. The DayView.status property is the tighter
    # fallback when the aggregate lookup returns empty.
    aggs = activity_in_range(conn, entries, d, d, settings=settings)
    agg_status = aggs[d].status if d in aggs else view.status

    today = today_local(settings)
    prev_d = (d - timedelta(days=1)).isoformat()
    next_d = (d + timedelta(days=1)).isoformat()
    month_link = f"{d.year:04d}-{d.month:02d}"
    # Build day label in Python — strftime's no-leading-zero day
    # specifier differs by platform (%-d on POSIX, %#d on Windows),
    # and getting the wrong one raises ValueError and 500s the page.
    day_label = f"{d.strftime('%A')}, {d.strftime('%B')} {d.day}, {d.year}"

    # Decode the audit-result JSON blob here so the template stays
    # free of json-parsing logic. Missing or malformed blob → None.
    audit: dict[str, Any] | None = None
    if view.day_review_row and view.day_review_row.get("ai_audit_result"):
        try:
            audit = json.loads(view.day_review_row["ai_audit_result"])
        except (ValueError, TypeError):
            audit = None

    return request.app.state.templates.TemplateResponse(
        request,
        "calendar_day.html",
        {
            "day": d,
            "day_iso": d.isoformat(),
            "day_label": day_label,
            "view": view,
            "audit": audit,
            "today": today,
            "prev_day": prev_d,
            "next_day": next_d,
            "month_link": month_link,
            "status": agg_status,
        },
    )


def _flag_dict(flag) -> dict[str, Any]:
    return {
        "code": flag.code,
        "severity": flag.severity,
        "title": flag.title,
        "detail": flag.detail,
    }


@router.post("/calendar/{date_str}/review")
def toggle_review(
    date_str: str,
    request: Request,
    unmark: str | None = Form(default=None),
    conn = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    d = _parse_date(date_str)
    now = datetime.now(tz=app_tz(settings))

    row = conn.execute(
        "SELECT review_date FROM day_reviews WHERE review_date = ?",
        (d.isoformat(),),
    ).fetchone()

    if unmark:
        if row is not None:
            conn.execute(
                "DELETE FROM day_reviews WHERE review_date = ?",
                (d.isoformat(),),
            )
        try:
            append_day_review_deleted(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                review_date=d,
            )
        except BeanCheckError as exc:
            log.error("day-review-deleted rejected by bean-check: %s", exc)
            raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    else:
        if row is None:
            conn.execute(
                "INSERT INTO day_reviews (review_date, last_reviewed_at, updated_at) "
                "VALUES (?, ?, CURRENT_TIMESTAMP)",
                (d.isoformat(), now.isoformat(timespec="seconds")),
            )
        else:
            conn.execute(
                "UPDATE day_reviews SET last_reviewed_at = ?, "
                "updated_at = CURRENT_TIMESTAMP WHERE review_date = ?",
                (now.isoformat(timespec="seconds"), d.isoformat()),
            )
        try:
            append_day_review(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                review_date=d,
                last_reviewed_at=now,
            )
        except BeanCheckError as exc:
            log.error("day-review rejected by bean-check: %s", exc)
            raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    return RedirectResponse(url=f"/calendar/{d.isoformat()}", status_code=303)


@router.post("/calendar/{date_str}/note")
def add_day_note(
    date_str: str,
    request: Request,
    body: str = Form(...),
    conn = Depends(get_db),
):
    """Create a single-day, unscoped note anchored on ``date_str``.

    Writes a row into the `notes` table with
    ``active_from == active_to == date_str`` and no entity/card
    scope. The existing classify_txn pipeline picks it up
    automatically via `NoteService.notes_active_on`.
    """
    d = _parse_date(date_str)
    text = (body or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="note body required")
    service = NoteService(conn)
    service.create(
        body=text,
        entity_hint=None,
        merchant_hint=None,
        active_from=d,
        active_to=d,
        entity_scope=None,
        card_scope=None,
        card_override=False,
        keywords=None,
    )
    return RedirectResponse(url=f"/calendar/{d.isoformat()}", status_code=303)


@router.post("/calendar/{date_str}/note/{note_id}/delete")
def delete_day_note(
    date_str: str,
    note_id: int,
    request: Request,
    conn = Depends(get_db),
):
    d = _parse_date(date_str)
    conn.execute("DELETE FROM notes WHERE id = ?", (int(note_id),))
    return RedirectResponse(url=f"/calendar/{d.isoformat()}", status_code=303)


# ---- phase 2: AI summary + audit + next-unreviewed -----------------------

def _day_note_body(conn, d: date) -> str | None:
    """Return the concatenated body of every single-day, unscoped
    note on ``d``. Used to feed summarize_day + audit_day context.

    Multiple day notes → joined with a blank line. None when
    nothing's there. We do this in the route rather than require
    callers to know the schema."""
    rows = conn.execute(
        """
        SELECT body FROM notes
         WHERE active_from = ? AND active_to = ?
           AND entity_scope IS NULL AND card_scope IS NULL
         ORDER BY captured_at
        """,
        (d.isoformat(), d.isoformat()),
    ).fetchall()
    if not rows:
        return None
    bodies = [r["body"] for r in rows if r["body"]]
    return "\n\n".join(bodies) if bodies else None


def _inline_error(message: str) -> HTMLResponse:
    """Render an overlay-like error card so hx-swap=beforeend lands
    something visible at the body end instead of a bare 'Not Found'.

    The card clicks away on the scrim.
    """
    html = (
        '<div id="calendar-inline-error" class="job-modal-overlay" '
        'style="cursor:pointer;" onclick="this.remove()">'
        '<div class="job-modal" onclick="event.stopPropagation()" '
        'style="padding:1.5rem;max-width:520px;">'
        '<h2 style="margin:0 0 .5rem 0;font-size:1.1rem;">Can\'t run this</h2>'
        f'<p style="margin:0;">{message}</p>'
        '<p style="margin:.75rem 0 0 0;" class="muted-small">Click outside to dismiss.</p>'
        '</div></div>'
    )
    return HTMLResponse(html, status_code=200)


@router.post("/calendar/{date_str}/ai-summary")
def trigger_ai_summary(
    date_str: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    ai: AIService = Depends(get_ai_service),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    """Kick a background job that summarizes the day. The job
    re-builds the view inside its own connection (JobContext
    opens short-lived SQLite connections), calls the AI, and
    writes the result to ``day_reviews.ai_summary`` + mirrors to
    a ``custom "day-review"`` directive for ledger round-trip."""
    d = _parse_date(date_str)
    if not ai.enabled:
        return _inline_error(
            "AI isn't configured. Set OPENROUTER_API_KEY in /settings and try again.",
        )
    if ai.spend_cap_reached():
        return _inline_error(
            "Monthly AI spend cap reached. Raise it in /settings/ai or wait for next month.",
        )

    settings_path = settings.connector_config_path
    main_bean_path = settings.ledger_main

    def _work(ctx):
        ctx.set_total(1)
        ctx.emit(f"Summarizing {d.isoformat()}…", outcome="info")
        # Rebuild view with a fresh reader.load() so we don't fight the
        # request thread's ledger cache.
        entries = list(reader.load().entries)
        view = day_activity(conn, entries, d, settings=settings)
        flags = compute_day_flags(conn, entries, d)
        flag_notes = [f"{f.title}: {f.detail}" for f in flags]

        client = ai.new_client()
        if client is None:
            ctx.emit("AI client unavailable (disabled or cap reached)", outcome="error")
            return {"ok": False, "reason": "client unavailable"}

        loop = asyncio.new_event_loop()
        try:
            summary = loop.run_until_complete(
                run_summarize_day(
                    client,
                    day=d,
                    day_note=_day_note_body(conn, d),
                    transactions=view.transactions,
                    mileage=view.mileage,
                    paperless=view.paperless,
                    flag_notes=flag_notes,
                    model=ai.model_for("summarize_day"),
                )
            )
            loop.run_until_complete(client.aclose())
        finally:
            loop.close()

        if not summary:
            ctx.emit("AI returned no summary", outcome="error")
            return {"ok": False, "reason": "no summary"}

        now_iso = datetime.now(tz=app_tz(settings)).isoformat(timespec="seconds")
        # Upsert the cache row.
        conn.execute(
            """
            INSERT INTO day_reviews
                (review_date, ai_summary, ai_summary_at, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(review_date) DO UPDATE SET
                ai_summary = excluded.ai_summary,
                ai_summary_at = excluded.ai_summary_at,
                updated_at = CURRENT_TIMESTAMP
            """,
            (d.isoformat(), summary, now_iso),
        )
        # Mirror to ledger.
        try:
            append_day_review(
                connector_config=settings_path,
                main_bean=main_bean_path,
                review_date=d,
                last_reviewed_at=None,
                ai_summary=summary,
                ai_summary_at=now_iso,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("day-review ledger mirror failed: %s", exc)

        ctx.advance()
        ctx.emit("Summary written", outcome="success")
        return {"ok": True}

    job_id = request.app.state.job_runner.submit(
        kind="calendar-ai-summary",
        title=f"AI summary · {d.isoformat()}",
        fn=_work,
        total=1,
        meta={"day": d.isoformat()},
        return_url=f"/calendar/{d.isoformat()}",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": f"/calendar/{d.isoformat()}"},
    )


@router.post("/calendar/{date_str}/ai-audit")
def trigger_ai_audit(
    date_str: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    ai: AIService = Depends(get_ai_service),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    """Kick a background job that re-classifies every txn on the
    day with full day context. Writes the result to
    ``day_reviews.ai_audit_result`` as a JSON blob. Does NOT
    mutate any ledger classification — the user acts on findings
    through the existing /card and /review surfaces."""
    d = _parse_date(date_str)
    if not ai.enabled:
        return _inline_error(
            "AI isn't configured. Set OPENROUTER_API_KEY in /settings and try again.",
        )
    if ai.spend_cap_reached():
        return _inline_error(
            "Monthly AI spend cap reached. Raise it in /settings/ai or wait for next month.",
        )

    settings_path = settings.connector_config_path
    main_bean_path = settings.ledger_main

    def _work(ctx):
        entries = list(reader.load().entries)
        view = day_activity(conn, entries, d, settings=settings)
        txn_count = len(view.transactions)
        ctx.set_total(max(1, txn_count))
        ctx.emit(
            f"Auditing {txn_count} transaction(s) on {d.isoformat()}…",
            outcome="info",
        )
        if txn_count == 0:
            conn.execute(
                """
                INSERT INTO day_reviews
                    (review_date, ai_audit_result, ai_audit_result_at, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(review_date) DO UPDATE SET
                    ai_audit_result = excluded.ai_audit_result,
                    ai_audit_result_at = excluded.ai_audit_result_at,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    d.isoformat(),
                    json.dumps({"entries": [], "agreed": 0, "disagreed": 0}),
                    datetime.now(tz=app_tz(settings)).isoformat(timespec="seconds"),
                ),
            )
            ctx.emit("No transactions to audit", outcome="info")
            return {"ok": True, "entries": 0}

        client = ai.new_client()
        if client is None:
            ctx.emit("AI client unavailable", outcome="error")
            return {"ok": False, "reason": "client unavailable"}

        # Active notes for this day — feeds into propose_account via
        # the same NoteService path classify_txn uses.
        notes = NoteService(conn).notes_active_on(d)

        # Build the entity accounts map. Keep it simple: every
        # Expenses account currently in the ledger, grouped by
        # entity slug (path segment 1). That's what classify_txn
        # uses when the router has no vector index warmed.
        accounts_by_entity: dict[str, list[str]] = {}
        from beancount.core.data import Open
        for e in entries:
            if isinstance(e, Open) and e.account.startswith("Expenses:"):
                parts = e.account.split(":")
                if len(parts) >= 2:
                    accounts_by_entity.setdefault(parts[1], []).append(e.account)

        def _resolve_entity(card_acct: str) -> str | None:
            # Entity is the 2nd segment of the card account path.
            parts = (card_acct or "").split(":")
            return parts[1] if len(parts) >= 2 else None

        idx_box = {"i": 0}

        def _on_progress(idx, total, t):
            idx_box["i"] = idx
            label = (t.narration or "").strip()[:60]
            ctx.raise_if_cancelled()
            ctx.emit(f"Re-classifying: {label or t.txn_hash[:12]}", outcome="info")

        # WP6 Site 5 — collect every active loan's tracked account
        # paths so audit_day skips the AI call for transactions
        # touching them. See calendar/ai.py audit_day docstring for
        # rationale.
        from lamella.features.loans.claim import load_loans_snapshot as _load_loans
        _loan_paths: set[str] = set()
        for _l in _load_loans(conn):
            for _p in (
                _l.get("liability_account_path"),
                _l.get("interest_account_path"),
                _l.get("escrow_account_path"),
            ):
                if _p:
                    _loan_paths.add(_p)

        loop = asyncio.new_event_loop()
        try:
            audit_entries = loop.run_until_complete(
                run_audit_day(
                    client,
                    day=d,
                    transactions=view.transactions,
                    active_notes=notes,
                    mileage_entries=view.mileage,
                    receipt_by_hash=None,
                    entity_accounts_by_entity=accounts_by_entity,
                    resolve_entity=_resolve_entity,
                    model=ai.model_for("audit_day"),
                    on_progress=_on_progress,
                    loan_tracked_paths=_loan_paths,
                )
            )
            loop.run_until_complete(client.aclose())
        finally:
            loop.close()

        agreed = sum(1 for e in audit_entries if e.agreed)
        disagreed = sum(
            1 for e in audit_entries if not e.agreed and e.skipped_reason is None
        )
        skipped = sum(1 for e in audit_entries if e.skipped_reason is not None)

        result_payload = {
            "entries": audit_entries_to_json(audit_entries),
            "agreed": agreed,
            "disagreed": disagreed,
            "skipped": skipped,
        }
        result_json = json.dumps(result_payload)
        now_iso = datetime.now(tz=app_tz(settings)).isoformat(timespec="seconds")
        conn.execute(
            """
            INSERT INTO day_reviews
                (review_date, ai_audit_result, ai_audit_result_at, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(review_date) DO UPDATE SET
                ai_audit_result = excluded.ai_audit_result,
                ai_audit_result_at = excluded.ai_audit_result_at,
                updated_at = CURRENT_TIMESTAMP
            """,
            (d.isoformat(), result_json, now_iso),
        )
        try:
            append_day_review(
                connector_config=settings_path,
                main_bean=main_bean_path,
                review_date=d,
                last_reviewed_at=None,
                ai_audit_result=result_json,
                ai_audit_result_at=now_iso,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("day-review audit ledger mirror failed: %s", exc)

        # Advance progress counter to the full total to match the spinner.
        while idx_box["i"] < txn_count - 1:
            idx_box["i"] += 1
            ctx.advance()
        ctx.advance()
        ctx.emit(
            f"Audit done · agreed={agreed} disagreed={disagreed} skipped={skipped}",
            outcome="success" if disagreed == 0 else "info",
        )
        return {"ok": True, "agreed": agreed, "disagreed": disagreed, "skipped": skipped}

    job_id = request.app.state.job_runner.submit(
        kind="calendar-ai-audit",
        title=f"AI audit · {d.isoformat()}",
        fn=_work,
        meta={"day": d.isoformat()},
        return_url=f"/calendar/{d.isoformat()}",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": f"/calendar/{d.isoformat()}"},
    )


@router.get("/calendar/{date_str}/next")
def next_unreviewed_with_activity(
    date_str: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
    conn = Depends(get_db),
):
    """Redirect to the next day (after ``date_str``) that has
    activity AND is unreviewed OR dirty. Skips empty and
    already-clean-reviewed days. 404 when nothing ahead.

    The search horizon is bounded: look up to 370 days ahead so
    a year-plus hop doesn't scan forever. A well-managed ledger
    shouldn't ever need to skip that far."""
    d = _parse_date(date_str)
    search_start = d + timedelta(days=1)
    search_end = search_start + timedelta(days=370)

    entries = list(reader.load().entries)
    aggs = activity_in_range(conn, entries, search_start, search_end, settings=settings)

    # Iterate ascending; first unreviewed-or-dirty day with activity wins.
    cur = search_start
    while cur <= search_end:
        agg = aggs.get(cur)
        if agg and agg.has_activity and agg.status in ("unreviewed", "dirty"):
            return RedirectResponse(
                url=f"/calendar/{cur.isoformat()}", status_code=303,
            )
        cur += timedelta(days=1)

    raise HTTPException(status_code=404, detail="no unreviewed day with activity ahead")
