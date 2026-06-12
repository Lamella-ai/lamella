# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Balance-anchor audit surface.

Routes:
    GET  /settings/accounts/{path}/balances     — per-account drill-down
    POST /settings/accounts/{path}/balances     — add an anchor
    POST /settings/accounts/{path}/balances/{id}/delete
    GET  /reports/balance-audit                  — portfolio summary
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date as date_t

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.features.dashboard.balances import service as balance_service
from lamella.features.dashboard.balances.writer import (
    append_balance_anchor,
    append_balance_anchor_revoked,
)
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings

log = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/settings/accounts/{account_path:path}/balances",
    response_class=HTMLResponse,
)
def account_balances_page(
    account_path: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Per-account balance-anchor admin. Shows all anchors, the drift
    between consecutive segments, and a form to add a new anchor."""
    # accounts_meta is the source of truth for which accounts exist;
    # check the path is real so we don't let a typo through.
    row = conn.execute(
        "SELECT account_path, display_name, kind, entity_slug, "
        "       institution, last_four "
        "FROM accounts_meta WHERE account_path = ?",
        (account_path,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"account {account_path!r} not found in accounts_meta",
        )
    entries = reader.load().entries
    audit = balance_service.compute_account_audit(conn, entries, account_path)
    from lamella.core.registry.alias import entity_label
    account = dict(row)
    account["entity_display_name"] = entity_label(
        conn, account.get("entity_slug"),
    )
    return request.app.state.templates.TemplateResponse(
        request, "balances_account.html",
        {
            "account": account,
            "audit": audit,
        },
    )


@router.post("/settings/accounts/{account_path:path}/balances")
async def add_account_balance_anchor(
    account_path: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Record a known balance for this account on a given date."""
    row = conn.execute(
        "SELECT 1 FROM accounts_meta WHERE account_path = ?",
        (account_path,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"account {account_path!r} not found in accounts_meta",
        )
    form = await request.form()
    as_of = (form.get("as_of_date") or "").strip()
    balance = (form.get("balance") or "").strip()
    if not as_of or not balance:
        raise HTTPException(
            status_code=400, detail="as_of_date and balance are required",
        )
    # Scrub the balance input — users paste "$1,234.56" all the time.
    balance_clean = balance.replace(",", "").replace("$", "").strip()
    currency = (form.get("currency") or "USD").strip() or "USD"
    source = (form.get("source") or "").strip() or None
    notes = (form.get("notes") or "").strip() or None
    balance_service.upsert_anchor(
        conn,
        account_path=account_path,
        as_of_date=as_of,
        balance=balance_clean,
        currency=currency,
        source=source,
        notes=notes,
    )
    try:
        append_balance_anchor(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            account_path=account_path,
            as_of_date=as_of,
            balance=balance_clean,
            currency=currency,
            source=source,
            notes=notes,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "balance-anchor directive write failed for %s %s: %s",
            account_path, as_of, exc,
        )
    return RedirectResponse(
        f"/settings/accounts/{account_path}/balances?saved=1",
        status_code=303,
    )


@router.post(
    "/settings/accounts/{account_path:path}/balances/{anchor_id:int}/delete",
)
def delete_account_balance_anchor(
    account_path: str,
    anchor_id: int,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    result = balance_service.delete_anchor(conn, anchor_id)
    if result is None:
        raise HTTPException(status_code=404, detail="anchor not found")
    _, as_of = result
    try:
        append_balance_anchor_revoked(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            account_path=account_path,
            as_of_date=as_of,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "balance-anchor-revoked directive write failed for %s %s: %s",
            account_path, as_of, exc,
        )
    return RedirectResponse(
        f"/settings/accounts/{account_path}/balances?removed=1",
        status_code=303,
    )


@router.get("/reports/balance-audit", response_class=HTMLResponse)
def balance_audit_report(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Portfolio-level balance audit: every account with at least one
    anchor, with total drift and segment breakdown. Joined with
    accounts_meta so the table can show friendly names + institution
    + entity instead of raw colon-separated paths."""
    entries = reader.load().entries
    audits = balance_service.compute_portfolio_audit(conn, entries)
    # Attach account metadata per audit so the template renders
    # "Personal · Bank One · Prime Checking" alongside the raw path.
    from lamella.core.registry.alias import entity_label
    meta_by_path = {}
    for r in conn.execute(
        "SELECT account_path, display_name, kind, entity_slug, "
        "       institution, last_four FROM accounts_meta"
    ).fetchall():
        meta = dict(r)
        meta["entity_display_name"] = entity_label(
            conn, meta.get("entity_slug"),
        )
        meta_by_path[meta["account_path"]] = meta
    return request.app.state.templates.TemplateResponse(
        request, "balance_audit_report.html",
        {"audits": audits, "meta_by_path": meta_by_path},
    )
