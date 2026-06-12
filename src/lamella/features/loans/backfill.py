# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Historical payment backfill (WP11).

Lets the user import a year (or several) of loan-payment history from
a CSV — typically exported from the servicer's portal. Each row gets
its principal/interest/escrow split derived from the loan's
amortization model + configured monthlies; rows whose total doesn't
match the model within tolerance are surfaced as invalid before the
job runs, so a typo or a column-mismatch never produces a chain of
bad transactions.

This module is pure with respect to its inputs: no I/O, no DB
writes, no ledger access. The route layer + job worker
(`routes/loans_backfill.py`) handle uploads and writes; this module
just transforms text → ``BackfillRow`` lists with annotated splits +
validation states.

Three public entry points:

- ``parse_csv(text) -> list[BackfillRow]`` — text → rows, with
  per-row error annotations for malformed inputs.
- ``compute_splits(rows, loan) -> list[BackfillRow]`` — fills in
  per-row principal/interest/escrow/tax/insurance via the
  amortization model.
- ``validate(rows, tolerance) -> tuple[list, list]`` — partitions
  into ``(valid, invalid)``; invalid rows carry an ``error`` string.
"""
from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

from lamella.features.loans.amortization import (
    payment_number_on,
    split_for_payment_number,
)

log = logging.getLogger(__name__)


# Default tolerance: $0.02 absolute. Servicer statements occasionally
# round per-leg in ways that produce 1¢ or 2¢ deltas vs. our model.
DEFAULT_TOLERANCE = Decimal("0.02")


# CSV column aliases — the user's export will use one of these
# spellings depending on servicer / locale. Lower-cased for matching.
_DATE_KEYS = ("date", "payment_date", "posted_date", "transaction_date")
_AMOUNT_KEYS = ("amount", "total", "payment_amount", "total_amount")
_OFFSET_KEYS = (
    "offset_account", "from_account", "source_account", "paid_from",
)
_NARRATION_KEYS = ("narration", "description", "memo", "note", "notes")


@dataclass
class BackfillRow:
    """One row from the user's CSV, with computed splits.

    ``error`` is None for valid rows, or a human-readable message
    explaining why this row can't be written. The job worker only
    writes rows where ``error is None``.
    """
    line_no: int                                # 1-based, including header
    raw: dict[str, str] = field(default_factory=dict)
    txn_date: date | None = None
    total_amount: Decimal | None = None
    offset_account: str | None = None
    narration: str | None = None
    expected_n: int | None = None               # filled by compute_splits
    principal: Decimal = Decimal("0")
    interest: Decimal = Decimal("0")
    escrow: Decimal = Decimal("0")
    tax: Decimal = Decimal("0")
    insurance: Decimal = Decimal("0")
    error: str | None = None

    @property
    def split_total(self) -> Decimal:
        return (
            self.principal + self.interest + self.escrow
            + self.tax + self.insurance
        )


# --------------------------------------------------------------------- helpers


def _to_decimal(s: str | None) -> Decimal | None:
    if s is None or str(s).strip() == "":
        return None
    raw = str(s).strip()
    raw = raw.replace(",", "").replace("$", "").replace(" ", "")
    if raw.startswith("(") and raw.endswith(")"):
        raw = "-" + raw[1:-1]
    if not raw:
        return None
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return None


def _to_date(s: str | None) -> date | None:
    if s is None or str(s).strip() == "":
        return None
    raw = str(s).strip()
    # Try ISO first, then a few common fallbacks.
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _normalize_header(header: str) -> str:
    return (header or "").strip().lower().replace(" ", "_").replace("-", "_")


def _pick(row: dict[str, str], keys: Sequence[str]) -> str | None:
    """Return the first non-empty value among `keys` (case-insensitive)."""
    for key in keys:
        v = row.get(key)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


# ----------------------------------------------------------- public API


def parse_csv(text: str) -> list[BackfillRow]:
    """Parse CSV text into BackfillRow objects.

    Header is required; supported aliases per ``_DATE_KEYS`` etc.
    Per-row errors are populated for missing/malformed date or amount;
    other parsing problems set ``error`` and leave the row in the
    list so the preview UI can show "row 7: bad date" before the user
    confirms a job that would otherwise silently skip it.

    The amount is treated as the total payment for that date; the
    ``compute_splits`` step derives principal/interest/escrow from the
    amortization schedule.
    """
    if not text or not text.strip():
        return []

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        return []

    # Re-key headers to the lowered/normalized form so callers don't
    # have to worry about case or hyphen-vs-underscore.
    norm_headers = [_normalize_header(h) for h in reader.fieldnames]

    rows: list[BackfillRow] = []
    for line_no, raw in enumerate(reader, start=2):  # header is line 1
        norm: dict[str, str] = {}
        for src_h, norm_h in zip(reader.fieldnames, norm_headers):
            norm[norm_h] = raw.get(src_h, "") or ""

        bf = BackfillRow(line_no=line_no, raw=norm)

        date_str = _pick(norm, _DATE_KEYS)
        amount_str = _pick(norm, _AMOUNT_KEYS)

        bf.txn_date = _to_date(date_str)
        bf.total_amount = _to_decimal(amount_str)
        bf.offset_account = _pick(norm, _OFFSET_KEYS) or None
        bf.narration = _pick(norm, _NARRATION_KEYS) or None

        if not date_str:
            bf.error = "missing date column (date / payment_date / posted_date)"
        elif bf.txn_date is None:
            bf.error = f"unparseable date: {date_str!r}"
        elif not amount_str:
            bf.error = "missing amount column (amount / total / payment_amount)"
        elif bf.total_amount is None:
            bf.error = f"unparseable amount: {amount_str!r}"
        elif bf.total_amount <= 0:
            bf.error = (
                f"non-positive amount: {bf.total_amount}; backfill is for "
                f"posted payments, not refunds or credits"
            )

        rows.append(bf)

    return rows


def compute_splits(
    rows: Sequence[BackfillRow], loan: dict,
) -> list[BackfillRow]:
    """Annotate each valid row with its expected_n + per-leg split.

    Errored rows pass through unchanged (the caller can still display
    them in the preview). For valid rows, the principal/interest split
    comes from the amortization schedule indexed by the row's
    payment-number-on-date; escrow/tax/insurance default to the loan's
    configured monthlies when those are present (zero otherwise).
    """
    principal = _D(loan.get("original_principal"))
    apr = _D(loan.get("interest_rate_apr"))
    term = int(loan.get("term_months") or 0)
    first_payment = _date(loan.get("first_payment_date"))
    escrow_monthly = _D(loan.get("escrow_monthly"))
    tax_monthly = _D(loan.get("property_tax_monthly"))
    insurance_monthly = _D(loan.get("insurance_monthly"))

    out: list[BackfillRow] = []
    for bf in rows:
        if bf.error is not None:
            out.append(bf)
            continue
        if bf.txn_date is None or bf.total_amount is None:
            out.append(replace(
                bf, error="row missing date or amount after parse"
            ))
            continue
        if not first_payment or term <= 0 or principal <= 0:
            out.append(replace(
                bf, error=(
                    "loan terms incomplete (need first_payment_date, "
                    "term_months, and original_principal) — finish loan "
                    "configuration before backfilling"
                ),
            ))
            continue

        n = payment_number_on(first_payment, bf.txn_date, term)
        if n <= 0:
            out.append(replace(
                bf, error=(
                    f"payment date {bf.txn_date} is before first payment "
                    f"date {first_payment}"
                ),
            ))
            continue

        split = split_for_payment_number(
            principal, apr, term, n,
            escrow_monthly=escrow_monthly or None,
        )
        out.append(replace(
            bf,
            expected_n=n,
            principal=split["principal"],
            interest=split["interest"],
            escrow=escrow_monthly,
            tax=tax_monthly,
            insurance=insurance_monthly,
        ))
    return out


def validate(
    rows: Sequence[BackfillRow], tolerance: Decimal = DEFAULT_TOLERANCE,
) -> tuple[list[BackfillRow], list[BackfillRow]]:
    """Partition rows into (valid, invalid).

    A row is invalid if (a) it already carries an error from parsing
    or split computation, or (b) its computed split total disagrees
    with the user-supplied amount by more than ``tolerance``.
    """
    valid: list[BackfillRow] = []
    invalid: list[BackfillRow] = []
    for bf in rows:
        if bf.error is not None:
            invalid.append(bf)
            continue
        if bf.total_amount is None or bf.expected_n is None:
            invalid.append(replace(
                bf, error="row not fully populated (missing amount or split)"
            ))
            continue
        delta = (bf.split_total - bf.total_amount).copy_abs()
        if delta > tolerance:
            invalid.append(replace(
                bf,
                error=(
                    f"split total {bf.split_total} does not match "
                    f"row amount {bf.total_amount} (Δ={delta}); the row's "
                    f"amount may include a late fee or escrow shortage "
                    f"this module doesn't model — use the per-payment "
                    f"record-missing-payment form for one-offs"
                ),
            ))
            continue
        valid.append(bf)
    return valid, invalid


# ---------------------------------------------------------- internal coercion


def _D(v: Any) -> Decimal:
    if v is None or v == "":
        return Decimal("0")
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _date(v: Any) -> date | None:
    if v is None or v == "":
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    try:
        return datetime.fromisoformat(str(v)[:10]).date()
    except (TypeError, ValueError):
        return None


# ------------------------------------------------------------- sample CSV


SAMPLE_CSV = (
    "date,amount,offset_account,narration\n"
    "2023-01-01,3500.00,Assets:Personal:Checking:1234,January 2023 mortgage payment\n"
    "2023-02-01,3500.00,Assets:Personal:Checking:1234,February 2023\n"
    "2023-03-01,3500.00,Assets:Personal:Checking:1234,March 2023\n"
    "2023-04-01,3500.00,Assets:Personal:Checking:1234,April 2023\n"
)
