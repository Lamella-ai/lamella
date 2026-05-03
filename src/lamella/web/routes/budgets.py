# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.features.budgets.models import BudgetValidationError
from lamella.features.budgets.progress import progress_for_budget
from lamella.features.budgets.service import BudgetService
from lamella.features.budgets.writer import append_budget, append_budget_revoke
from lamella.web.deps import get_db, get_ledger_reader
from lamella.core.ledger_writer import BeanCheckError


def _settings(request: Request):
    return request.app.state.settings

log = logging.getLogger(__name__)

router = APIRouter()


def _entities(reader: LedgerReader) -> list[str]:
    from beancount.core.data import Open

    out: set[str] = set()
    for entry in reader.load().entries:
        if isinstance(entry, Open):
            parts = entry.account.split(":")
            if len(parts) >= 2 and parts[0] in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
                out.add(parts[1])
    return sorted(out)


def _render(
    request: Request,
    *,
    conn: sqlite3.Connection,
    reader: LedgerReader,
    error: str | None = None,
    saved: bool = False,
):
    service = BudgetService(conn)
    budgets = service.list()
    entries = reader.load().entries
    progresses = [progress_for_budget(b, entries) for b in budgets]
    ctx = {
        "budgets": progresses,
        "entities": _entities(reader),
        "error": error,
        "saved": saved,
    }
    return request.app.state.templates.TemplateResponse(request, "budgets.html", ctx)


@router.get("/budgets", response_class=HTMLResponse)
def budgets_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    return _render(request, conn=conn, reader=reader)


@router.post("/budgets")
def create_budget(
    request: Request,
    label: str = Form(...),
    entity: str = Form(...),
    account_pattern: str = Form(...),
    period: str = Form(...),
    amount: str = Form(...),
    alert_threshold: str = Form(default="0.8"),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        amt = Decimal(amount)
        thr = float(alert_threshold)  # ratio (0..1) — ADR-0022 exempt
    except (ValueError, InvalidOperation):
        return _render(request, conn=conn, reader=reader, error="amount/threshold must be numeric")
    open_accounts = BudgetService.open_accounts(reader.load().entries)
    service = BudgetService(conn)
    try:
        # Validate first (dataclass construction) so we don't stamp a
        # bad directive that bean-check would reject anyway.
        service.validate_pattern(account_pattern, open_accounts=open_accounts)
        settings_obj = _settings(request)
        append_budget(
            connector_budgets=settings_obj.connector_budgets_path,
            main_bean=settings_obj.ledger_main,
            label=label, entity=entity, account_pattern=account_pattern,
            period=period, amount=amt, alert_threshold=thr,
        )
        service.create(
            label=label, entity=entity, account_pattern=account_pattern,
            period=period, amount=amt, alert_threshold=thr,
            open_accounts=open_accounts,
        )
        reader.invalidate()
    except BudgetValidationError as exc:
        return _render(request, conn=conn, reader=reader, error=str(exc))
    except BeanCheckError as exc:
        log.error("budget ledger write rejected: %s", exc)
        return _render(request, conn=conn, reader=reader, error=f"bean-check: {exc}")
    return RedirectResponse(url="/budgets", status_code=303)


@router.post("/budgets/{budget_id}")
def update_budget(
    request: Request,
    budget_id: int,
    label: str | None = Form(default=None),
    account_pattern: str | None = Form(default=None),
    period: str | None = Form(default=None),
    amount: str | None = Form(default=None),
    alert_threshold: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    open_accounts = BudgetService.open_accounts(reader.load().entries)
    service = BudgetService(conn)
    try:
        service.update(
            budget_id,
            label=label,
            account_pattern=account_pattern,
            period=period,
            amount=Decimal(amount) if amount else None,
            alert_threshold=float(alert_threshold) if alert_threshold else None,
            open_accounts=open_accounts if account_pattern else None,
        )
    except BudgetValidationError as exc:
        return _render(request, conn=conn, reader=reader, error=str(exc))
    return RedirectResponse(url="/budgets", status_code=303)


@router.post("/budgets/{budget_id}/delete")
def delete_budget(
    request: Request,
    budget_id: int,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    service = BudgetService(conn)
    existing = service.get(budget_id)
    if existing is None:
        return RedirectResponse(url="/budgets", status_code=303)
    try:
        settings_obj = _settings(request)
        append_budget_revoke(
            connector_budgets=settings_obj.connector_budgets_path,
            main_bean=settings_obj.ledger_main,
            label=existing.label,
            entity=existing.entity,
            account_pattern=existing.account_pattern,
            period=existing.period.value,
        )
    except BeanCheckError as exc:
        log.error("budget revoke ledger write rejected: %s", exc)
        return _render(request, conn=conn, reader=reader, error=f"bean-check: {exc}")
    reader.invalidate()
    service.delete(budget_id)
    return RedirectResponse(url="/budgets", status_code=303)
