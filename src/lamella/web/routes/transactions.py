# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Flat transactions list — every posting in the ledger plus a banner
linking to staged-but-not-yet-promoted rows. The point: a single place
to answer "did my import land?" without forcing the user to drill
through /accounts/<path> or guess a /search query."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date as date_t, timedelta
from decimal import Decimal

from beancount.core.data import Transaction
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.identity import get_txn_id
from lamella.web.deps import get_db, get_ledger_reader
from lamella.features.import_.staging import count_pending_items

router = APIRouter()

PAGE_SIZE = 50


@dataclass(frozen=True)
class TxnRow:
    date: date_t
    payee: str
    narration: str
    amount: Decimal              # always positive; sign lives in `flow`
    currency: str
    expense_account: str
    source_account: str
    is_fixme: bool
    # 'out' = money left an asset (expense, transfer to liability)
    # 'in'  = money entered an asset (income, refund)
    # 'flat' = pure transfer / unclassifiable
    flow: str
    txn_hash: str
    lamella_txn_id: str | None
    # Set to the list of linked Paperless doc ids (sorted asc) when
    # the txn is in `document_links`; empty tuple otherwise. Lets the
    # template render a "📄 #16962" pill so the user can verify what
    # got matched and flip to the receipt with one click.
    receipt_paperless_ids: tuple[int, ...] = ()


def _is_pad_generated(entry: Transaction) -> bool:
    """Beancount's Pad plugin materializes balance-assertion fillers as
    Transaction entries with flag 'P' and a narration like
    '(Padding inserted for Balance of ...)'. They aren't user
    transactions and shouldn't appear in the human-facing list."""
    if getattr(entry, "flag", None) == "P":
        return True
    narration = (entry.narration or "")
    return narration.startswith("(Padding inserted ")


def _row_for(entry: Transaction) -> TxnRow:
    """Pick a primary direction (in / out / flat) and amount for the
    flat list view.

    Beancount sign convention used here:
      - Expense leg sign IS the user-perceived direction. A positive
        expense posting (you spent money) renders as an outflow;
        a negative expense posting (refund / credit) renders as an
        inflow.
      - Income legs are typically negative (income credits the
        Income account). Negative income = inflow (money received);
        positive income = outflow (correction back out).
      - When neither an Expense nor an Income leg exists (pure
        Asset↔Asset / Asset↔Liability transfer, e.g. CC autopay),
        the row is shown as 'flat' with no sign — direction is
        ambiguous from the account topology alone.
    """
    dest_account = ""
    source = ""
    is_fixme = False

    expense_signed: Decimal | None = None
    expense_currency = "USD"
    income_signed: Decimal | None = None
    income_currency = "USD"
    transfer_amount: Decimal | None = None
    transfer_currency = "USD"

    for p in entry.postings:
        acct = p.account or ""
        n: Decimal | None = (
            Decimal(p.units.number)
            if p.units and p.units.number is not None
            else None
        )
        currency = (p.units.currency if p.units and p.units.currency else "USD")

        if acct.startswith("Expenses:"):
            if not dest_account:
                dest_account = acct
            if acct.split(":")[-1].upper() == "FIXME":
                is_fixme = True
            if n is not None and (
                expense_signed is None or abs(n) > abs(expense_signed)
            ):
                expense_signed = n
                expense_currency = currency
        elif acct.startswith("Income:"):
            if not dest_account:
                dest_account = acct
            if n is not None and (
                income_signed is None or abs(n) > abs(income_signed)
            ):
                income_signed = n
                income_currency = currency
        elif acct.startswith(("Assets:", "Liabilities:")):
            if not source:
                source = acct
            if n is not None and (
                transfer_amount is None or abs(n) > transfer_amount
            ):
                transfer_amount = abs(n)
                transfer_currency = currency

    if expense_signed is not None:
        primary_amount = abs(expense_signed)
        primary_currency = expense_currency
        flow = "out" if expense_signed > 0 else "in"
    elif income_signed is not None:
        primary_amount = abs(income_signed)
        primary_currency = income_currency
        flow = "in" if income_signed < 0 else "out"
    elif transfer_amount is not None:
        primary_amount = transfer_amount
        primary_currency = transfer_currency
        flow = "flat"
    else:
        primary_amount = Decimal("0")
        primary_currency = "USD"
        flow = "flat"

    return TxnRow(
        date=entry.date,
        payee=getattr(entry, "payee", None) or "",
        narration=entry.narration or "",
        amount=primary_amount,
        currency=primary_currency,
        expense_account=dest_account,
        source_account=source,
        is_fixme=is_fixme,
        flow=flow,
        txn_hash=txn_hash(entry),
        lamella_txn_id=get_txn_id(entry),
    )


def _matches(needle: str, row: TxnRow) -> bool:
    n = needle.lower()
    return (
        n in row.payee.lower()
        or n in row.narration.lower()
        or n in row.expense_account.lower()
        or n in row.source_account.lower()
    )


@router.get("/transactions", response_class=HTMLResponse)
def transactions_page(
    request: Request,
    page: int = 1,
    q: str = "",
    account: str = "",
    days: int = 365,
    fixme_only: int = 0,
    has_receipt: int = 0,
    reader: LedgerReader = Depends(get_ledger_reader),
    conn: sqlite3.Connection = Depends(get_db),
):
    days = max(1, min(int(days or 365), 3650))
    since = date_t.today() - timedelta(days=days)
    needle = q.strip()
    acct_filter = account.strip()

    # Build a hash → [paperless_id, ...] map once so the per-row
    # hydration is O(1) instead of N queries. Sorted by paperless_id
    # asc so multi-link rows render predictably.
    receipt_link_map: dict[str, list[int]] = {}
    try:
        for r in conn.execute(
            "SELECT txn_hash, paperless_id FROM document_links "
            "ORDER BY txn_hash, paperless_id"
        ).fetchall():
            receipt_link_map.setdefault(r["txn_hash"], []).append(
                int(r["paperless_id"])
            )
    except Exception:  # noqa: BLE001 — document_links is optional
        pass

    rows: list[TxnRow] = []
    fixme_total = 0
    receipt_total = 0
    for entry in reader.load().entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date < since:
            continue
        # Skip override-correction blocks — same reasoning as /search.
        tags = getattr(entry, "tags", None) or set()
        if "lamella-override" in tags:
            continue
        # Skip Beancount's auto-generated padding entries — they exist
        # to satisfy balance assertions, not as user activity.
        if _is_pad_generated(entry):
            continue
        row = _row_for(entry)
        link_ids = receipt_link_map.get(row.txn_hash, [])
        if link_ids:
            row = TxnRow(
                date=row.date, payee=row.payee, narration=row.narration,
                amount=row.amount, currency=row.currency,
                expense_account=row.expense_account,
                source_account=row.source_account,
                is_fixme=row.is_fixme, flow=row.flow,
                txn_hash=row.txn_hash, lamella_txn_id=row.lamella_txn_id,
                receipt_paperless_ids=tuple(link_ids),
            )
            receipt_total += 1
        if acct_filter and acct_filter not in (
            row.expense_account, row.source_account,
        ):
            # Loose match: also accept prefix on either leg.
            if not (
                row.expense_account.startswith(acct_filter)
                or row.source_account.startswith(acct_filter)
            ):
                continue
        if needle and not _matches(needle, row):
            continue
        if row.is_fixme:
            fixme_total += 1
        if fixme_only and not row.is_fixme:
            continue
        if has_receipt and not row.receipt_paperless_ids:
            continue
        rows.append(row)

    rows.sort(key=lambda r: (r.date, r.txn_hash), reverse=True)
    total = len(rows)

    page = max(1, int(page or 1))
    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE
    page_rows = rows[start:end]
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

    staged_pending = count_pending_items(conn)

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "transactions.html",
        {
            "rows": page_rows,
            "total": total,
            "fixme_total": fixme_total,
            "page": page,
            "total_pages": total_pages,
            "page_size": PAGE_SIZE,
            "q": q,
            "account": account,
            "days": days,
            "fixme_only": int(fixme_only),
            "has_receipt": int(has_receipt),
            "receipt_total": receipt_total,
            "staged_pending": staged_pending,
        },
    )
