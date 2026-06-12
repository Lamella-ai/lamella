# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Escrow lifecycle helpers (WP7).

Escrow is the loan's own checkbook. This module extracts inflows
(payment-time deposits) and outflows (servicer disbursing to tax /
insurance), produces a running balance and year-to-date summary,
and offers a reconciliation helper that compares ledger-computed
against a statement-observed balance.

Four pure entry points:

- `escrow_flows(loan, entries)` → chronological list of `EscrowFlow`
  records with sign-based inflow/outflow classification.
- `running_balance(flows)` → list of `(date, balance_after_flow)`
  tuples the UI uses to draw a time-series chart.
- `ytd_summary(flows, year)` → aggregate totals for a calendar year.
- `reconcile(flows, statement_balance, statement_date)` →
  `ReconciliationResult` with the delta and a recommended action.

The one ledger-writing helper is `build_reconciliation_block(...)`
which produces the transaction text for a one-click adjustment.
The actual write happens at the route layer so this module stays
pure.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Literal, Sequence

from beancount.core.data import Transaction


Kind = Literal["inflow", "outflow"]


@dataclass(frozen=True)
class EscrowFlow:
    date: date
    amount: Decimal            # unsigned magnitude
    counterpart: str           # the non-escrow account on the other side
    narration: str
    txn_hash: str
    kind: Kind


@dataclass(frozen=True)
class EscrowYTD:
    year: int
    total_in: Decimal
    total_out: Decimal
    net: Decimal
    biggest_outflow: EscrowFlow | None


@dataclass(frozen=True)
class ReconciliationResult:
    ledger_balance: Decimal
    statement_balance: Decimal
    delta: Decimal             # statement - ledger; positive = statement higher
    needs_adjustment: bool
    suggested_offset_path: str | None


# -------------------------------------------------------------- small utils


def _as_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _as_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _today(as_of: date | None = None) -> date:
    return as_of or datetime.now(timezone.utc).date()


# --------------------------------------------------------------- escrow_flows


def escrow_flows(
    loan: dict, entries: Iterable[Any],
) -> list[EscrowFlow]:
    """Every transaction that touches the escrow account.

    Inflow classification: escrow posting is positive (money deposited
    into escrow). Outflow: escrow posting is negative (servicer paid
    tax/insurance out). `counterpart` is the first non-escrow posting
    with an opposite-sign amount — usually the liability account for
    inflows or the tax/insurance expense account for outflows.
    """
    from lamella.core.beancount_io.txn_hash import txn_hash

    escrow_path = loan.get("escrow_account_path")
    if not escrow_path:
        return []

    flows: list[EscrowFlow] = []
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        escrow_amt: Decimal | None = None
        counterpart: str | None = None
        for p in entry.postings or []:
            if getattr(p, "account", None) != escrow_path:
                continue
            units = getattr(p, "units", None)
            if units is None or getattr(units, "number", None) is None:
                continue
            escrow_amt = Decimal(units.number)
            break
        if escrow_amt is None or escrow_amt == 0:
            continue
        # Counterpart = first non-escrow, non-liability posting.
        # Outflows: prefer expense/tax/insurance account.
        # Inflows: prefer the liability account (the payment that fed escrow).
        kind: Kind = "inflow" if escrow_amt > 0 else "outflow"
        for p in entry.postings or []:
            acct = getattr(p, "account", None)
            if acct == escrow_path or acct is None:
                continue
            # Heuristic: for outflows, skip asset counterparts and
            # prefer expense accounts; for inflows, skip expense
            # accounts and prefer liability/asset.
            if kind == "outflow" and acct.startswith("Expenses:"):
                counterpart = acct
                break
            if kind == "inflow" and acct.startswith(("Liabilities:", "Assets:")):
                counterpart = acct
                break
        if counterpart is None:
            # Fallback: first non-escrow account.
            for p in entry.postings or []:
                acct = getattr(p, "account", None)
                if acct and acct != escrow_path:
                    counterpart = acct
                    break

        d = _as_date(entry.date) or _today()
        flows.append(EscrowFlow(
            date=d,
            amount=abs(escrow_amt),
            counterpart=counterpart or "",
            narration=entry.narration or "",
            txn_hash=txn_hash(entry),
            kind=kind,
        ))
    flows.sort(key=lambda f: f.date)
    return flows


# ------------------------------------------------------------ running_balance


def running_balance(
    flows: Sequence[EscrowFlow],
) -> list[tuple[date, Decimal]]:
    """Running balance after each flow, chronologically.

    Inflows add, outflows subtract. Result is fed straight to an
    SVG chart — caller decides sampling (one point per month is
    fine for the 12-month view; per-transaction is fine too).
    """
    balance = Decimal("0")
    out: list[tuple[date, Decimal]] = []
    for f in sorted(flows, key=lambda f: f.date):
        if f.kind == "inflow":
            balance += f.amount
        else:
            balance -= f.amount
        out.append((f.date, balance))
    return out


# --------------------------------------------------------------- ytd_summary


def ytd_summary(
    flows: Sequence[EscrowFlow], year: int,
) -> EscrowYTD:
    in_flows = [f for f in flows if f.date.year == year and f.kind == "inflow"]
    out_flows = [f for f in flows if f.date.year == year and f.kind == "outflow"]
    total_in = sum((f.amount for f in in_flows), Decimal("0"))
    total_out = sum((f.amount for f in out_flows), Decimal("0"))
    biggest = max(out_flows, key=lambda f: f.amount, default=None)
    return EscrowYTD(
        year=year,
        total_in=total_in,
        total_out=total_out,
        net=total_in - total_out,
        biggest_outflow=biggest,
    )


# ---------------------------------------------------------------- reconcile


def reconcile(
    flows: Sequence[EscrowFlow],
    statement_balance: Decimal,
    statement_date: date,
    *,
    tolerance: Decimal = Decimal("1.00"),
    default_offset_path: str | None = None,
) -> ReconciliationResult:
    """Compare ledger-computed balance at `statement_date` against
    the user-entered statement balance. Flag any delta > tolerance.
    """
    # Ledger-computed balance at statement_date: sum flows up to and
    # including that date.
    ledger = Decimal("0")
    for f in flows:
        if f.date <= statement_date:
            if f.kind == "inflow":
                ledger += f.amount
            else:
                ledger -= f.amount
    delta = statement_balance - ledger
    return ReconciliationResult(
        ledger_balance=ledger,
        statement_balance=statement_balance,
        delta=delta,
        needs_adjustment=bool(abs(delta) > tolerance),
        suggested_offset_path=default_offset_path,
    )


# ---------------------------------------------------- reconciliation writer


def build_reconciliation_block(
    loan: dict,
    *,
    statement_date: date,
    delta: Decimal,
    offset_account: str,
    narration: str | None = None,
) -> str:
    """Produce the transaction text for an escrow reconciliation.

    Positive delta (statement > ledger): servicer shows more than
    we have booked → inflow to escrow, offset as
    Income:Escrow-Adjustment (or user-pick).
    Negative delta: outflow.
    """
    slug = loan.get("slug") or ""
    escrow_path = loan.get("escrow_account_path") or ""
    text = narration or (
        f"Escrow reconciliation — {loan.get('display_name') or slug}"
    )
    amt_escrow = delta           # signed: positive = inflow
    amt_offset = -delta
    block = (
        f'\n{statement_date} * "{text}" #lamella-loan-escrow-reconcile\n'
        f'  lamella-loan-slug: "{slug}"\n'
        f'  lamella-loan-escrow-statement-date: {statement_date}\n'
        f'  {escrow_path}  {amt_escrow:.2f} USD\n'
        f'  {offset_account}  {amt_offset:.2f} USD\n'
    )
    return block
