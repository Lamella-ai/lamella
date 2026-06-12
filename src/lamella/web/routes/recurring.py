# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.core.config import Settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.recurring.detector import run_detection
from lamella.features.recurring.service import (
    RecurringService,
    RecurringStatus,
    RecurringValidationError,
)
from lamella.features.recurring.writer import (
    append_recurring_confirmed,
    append_recurring_ignored,
)

log = logging.getLogger(__name__)

router = APIRouter()


def _open_accounts(reader: LedgerReader) -> set[str]:
    from beancount.core.data import Open

    return {e.account for e in reader.load().entries if isinstance(e, Open)}


def _render(
    request: Request,
    conn: sqlite3.Connection,
    *,
    saved: bool = False,
    error: str | None = None,
    message: str | None = None,
):
    service = RecurringService(conn)
    proposed = service.list(status=RecurringStatus.PROPOSED.value)
    confirmed = service.list(status=RecurringStatus.CONFIRMED.value)
    ignored = service.list(status=RecurringStatus.IGNORED.value)
    stopped = service.list(status=RecurringStatus.STOPPED.value)
    last_run = conn.execute(
        "SELECT * FROM recurring_detections ORDER BY id DESC LIMIT 1"
    ).fetchone()
    ctx = {
        "proposed": proposed,
        "confirmed": confirmed,
        "ignored": ignored,
        "stopped": stopped,
        "last_run": dict(last_run) if last_run else None,
        "saved": saved,
        "error": error,
        "message": message,
    }
    return request.app.state.templates.TemplateResponse(request, "recurring.html", ctx)


@router.get("/recurring", response_class=HTMLResponse)
def recurring_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    return _render(request, conn)


@router.post("/recurring/scan", response_class=HTMLResponse)
def recurring_scan_now(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Run recurring-payment detection as a background job — surfaces
    progress for a scan that iterates every ledger transaction."""
    def _work(ctx):
        ctx.emit("Loading ledger for recurring-scan …", outcome="info")
        entries = reader.load().entries
        ctx.emit(
            f"Scanning {len(entries)} entries · "
            f"window={settings.recurring_scan_window_days}d · "
            f"min_occurrences={settings.recurring_min_occurrences}",
            outcome="info",
        )
        result = run_detection(
            conn=conn,
            entries=entries,
            scan_window_days=settings.recurring_scan_window_days,
            min_occurrences=settings.recurring_min_occurrences,
        )
        ctx.emit(
            f"Scan complete: {result.candidates_found} candidates, "
            f"{result.new_proposals} new, {result.updates} updates",
            outcome="success",
        )
        return {
            "candidates_found": result.candidates_found,
            "new_proposals": result.new_proposals,
            "updates": result.updates,
        }

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="recurring-scan",
        title="Recurring-payment detection scan",
        fn=_work,
        return_url="/recurring",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/recurring"},
    )


@router.post("/recurring/{recurring_id}/confirm")
def recurring_confirm(
    request: Request,
    recurring_id: int,
    label: str | None = Form(default=None),
    expected_day: str | None = Form(default=None),
    source_account: str | None = Form(default=None),
    target_account: str | None = Form(default=None),
    save_rule: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        day = int(expected_day) if expected_day and expected_day.strip() else None
    except ValueError:
        return _render(request, conn, error="expected_day must be an integer")
    try:
        RecurringService(conn).confirm(
            recurring_id,
            label=label,
            expected_day=day,
            source_account=source_account,
            open_accounts=_open_accounts(reader),
        )
    except RecurringValidationError as exc:
        return _render(request, conn, error=str(exc))

    # Stamp the confirmation into the ledger so it survives a DB delete.
    settings_obj = request.app.state.settings
    from decimal import Decimal
    row = conn.execute(
        "SELECT label, entity, source_account, merchant_pattern, cadence, "
        "       expected_amount, expected_day "
        "FROM recurring_expenses WHERE id = ?",
        (recurring_id,),
    ).fetchone()
    if row is not None:
        try:
            append_recurring_confirmed(
                connector_rules=settings_obj.connector_rules_path,
                main_bean=settings_obj.ledger_main,
                label=row["label"],
                entity=row["entity"] or "",
                source_account=row["source_account"],
                target_account=(target_account or "").strip() or None,
                merchant_pattern=row["merchant_pattern"],
                cadence=row["cadence"] or "monthly",
                expected_amount=Decimal(str(row["expected_amount"])),
                expected_day=(
                    int(row["expected_day"])
                    if row["expected_day"] is not None
                    else None
                ),
            )
            reader.invalidate()
        except BeanCheckError as exc:
            log.error("recurring confirm ledger write rejected: %s", exc)
            return _render(request, conn, error=f"bean-check: {exc}")

    # Optionally save a classification rule so every future occurrence
    # of this merchant routes to target_account automatically. Uses the
    # merchant_pattern stored on the proposal.
    target_account = (target_account or "").strip() or None
    if target_account and save_rule == "1":
        row = conn.execute(
            "SELECT merchant_pattern, source_account FROM recurring_expenses WHERE id = ?",
            (recurring_id,),
        ).fetchone()
        if row is not None:
            pattern = row["merchant_pattern"]
            card = row["source_account"]
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO classification_rules
                        (pattern_type, pattern_value, target_account,
                         card_account, confidence, hit_count, created_by)
                    VALUES ('merchant_contains', ?, ?, ?, 1.0, 0, 'recurring-confirm')
                    """,
                    (pattern, target_account, card),
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("rule save during recurring-confirm failed: %s", exc)
    return RedirectResponse(url="/recurring", status_code=303)


@router.post("/recurring/{recurring_id}/stop")
def recurring_stop(
    request: Request,
    recurring_id: int,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Mark a detection as stopped — the subscription ended, the free
    trial expired, the service was cancelled. Distinct from ignore
    (which means 'not actually recurring') and from confirm (which
    means 'still active'). Keeps the row visible in the archive so
    budget variance doesn't silently drop it."""
    try:
        conn.execute(
            "UPDATE recurring_expenses SET status = 'stopped', ignored_at = CURRENT_TIMESTAMP "
            "WHERE id = ?",
            (recurring_id,),
        )
    except Exception as exc:  # noqa: BLE001
        return _render(request, conn, error=str(exc))
    return RedirectResponse(url="/recurring", status_code=303)


@router.post("/recurring/{recurring_id}/ignore")
def recurring_ignore(
    request: Request,
    recurring_id: int,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    row = conn.execute(
        "SELECT label, source_account, merchant_pattern "
        "FROM recurring_expenses WHERE id = ?",
        (recurring_id,),
    ).fetchone()
    try:
        RecurringService(conn).ignore(recurring_id)
    except RecurringValidationError as exc:
        return _render(request, conn, error=str(exc))
    if row is not None:
        try:
            append_recurring_ignored(
                connector_rules=request.app.state.settings.connector_rules_path,
                main_bean=request.app.state.settings.ledger_main,
                label=row["label"],
                source_account=row["source_account"],
                merchant_pattern=row["merchant_pattern"],
            )
            reader.invalidate()
        except BeanCheckError as exc:
            log.error("recurring ignore ledger write rejected: %s", exc)
            return _render(request, conn, error=f"bean-check: {exc}")
    return RedirectResponse(url="/recurring", status_code=303)


@router.post("/recurring/{recurring_id}/edit")
def recurring_edit(
    request: Request,
    recurring_id: int,
    label: str | None = Form(default=None),
    expected_day: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_db),
):
    try:
        day = int(expected_day) if expected_day and expected_day.strip() else None
    except ValueError:
        return _render(request, conn, error="expected_day must be an integer")
    try:
        RecurringService(conn).update_proposal(
            recurring_id, label=label, expected_day=day,
        )
    except RecurringValidationError as exc:
        return _render(request, conn, error=str(exc))
    return RedirectResponse(url="/recurring", status_code=303)
