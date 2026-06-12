# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Payment coverage engine.

The system knows the first payment date, the term, and today's date,
so it can compute exactly how many payments *should* have posted by
now and match those against what the ledger actually carries. This
module produces a `CoverageReport`: ordered `CoverageRow` objects
(expected × actual), plus aggregate counts and a graceful-degradation
signal the health model uses to suppress noise on long gaps.

Three public entry points:

- `build_schedule(loan, *, pauses=(), as_of=None)` → expected rows.
- `extract_actuals(loan, entries)` → actual payments from the ledger.
- `match(expected, actuals, *, window_days=15)` → CoverageReport.

Plus a convenience `coverage_for(loan, entries, ...)` that chains
the three. Everything is pure with respect to its inputs.

Graceful degradation (until WP12 — pauses/forbearance — lands):
when any run of consecutive missing payments reaches 3 or more, the
report's `long_gap_detected` flag fires. The health model consumes
that flag and emits a single `long-payment-gap` attention item
instead of N individual missing-payment blockers — because the most
likely cause of a 6-month gap is a forbearance period the system
doesn't yet model, not six individually-actionable missed payments.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Literal, Sequence

from beancount.core.data import Transaction

from lamella.features.loans.amortization import (
    amortization_schedule,
    monthly_payment,
    payment_number_on,
    split_for_payment_number,
)


Status = Literal["on_time", "late", "missing"]


@dataclass(frozen=True)
class ExpectedPayment:
    n: int
    expected_date: date
    principal: Decimal
    interest: Decimal
    escrow: Decimal
    tax: Decimal
    insurance: Decimal
    total: Decimal


@dataclass(frozen=True)
class ActualPayment:
    txn_hash: str
    date: date
    principal_leg: Decimal
    interest_leg: Decimal
    escrow_leg: Decimal
    total: Decimal
    auto_classified: bool
    # The specific WP6 tier stamped on the override block, when the
    # transaction is an auto-classified split. None for manual
    # classifications or transactions without a WP6 override.
    autoclass_tier: str | None = None


@dataclass(frozen=True)
class CoverageRow:
    expected: ExpectedPayment
    actual: ActualPayment | None
    status: Status
    extras: list[ActualPayment] = field(default_factory=list)


@dataclass(frozen=True)
class CoverageReport:
    rows: list[CoverageRow]
    expected_count: int
    matched_count: int
    missing_count: int
    late_count: int
    extras: list[ActualPayment]
    long_gap_detected: bool
    long_gap_start: date | None
    long_gap_end: date | None
    long_gap_count: int


# --------------------------------------------------------------------- helpers


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


def _as_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _today(as_of: date | None) -> date:
    return as_of or datetime.now(timezone.utc).date()


def _add_months(d: date, months: int) -> date:
    """Add N months to a date, clamping to month-end when the target
    month has fewer days. Jan 31 + 1 month = Feb 28/29, not error."""
    month_index = d.month - 1 + months
    year = d.year + month_index // 12
    month = month_index % 12 + 1
    # Clamp day to last day of target month.
    if month == 12:
        next_month_first = date(year + 1, 1, 1)
    else:
        next_month_first = date(year, month + 1, 1)
    last_day = (next_month_first - timedelta(days=1)).day
    day = min(d.day, last_day)
    return date(year, month, day)


def _snap_to_due_day(d: date, due_day: int | None) -> date:
    """Snap a date's day-of-month to `due_day` when set.

    Useful when first_payment_date was (say) the 3rd but the servicer
    configured due day is the 1st — subsequent expected dates should
    land on the 1st, not keep the 3rd offset indefinitely.
    """
    if not due_day:
        return d
    # Clamp due_day to the month's actual length (day 31 in Feb → 28/29).
    if d.month == 12:
        next_month_first = date(d.year + 1, 1, 1)
    else:
        next_month_first = date(d.year, d.month + 1, 1)
    last_day = (next_month_first - timedelta(days=1)).day
    return date(d.year, d.month, min(int(due_day), last_day))


def _date_covered_by_pause(d: date, pauses: Sequence[Any]) -> bool:
    """A date is paused if it falls within any `(pause.start_date,
    pause.end_date or today]` range. WP12 plugs real pauses in;
    WP3 always receives an empty list."""
    today = datetime.now(timezone.utc).date()
    for p in pauses:
        start = _as_date(getattr(p, "start_date", None))
        end = _as_date(getattr(p, "end_date", None)) or today
        if start and start <= d <= end:
            return True
    return False


# ---------------------------------------------------------- expected schedule


def build_schedule(
    loan: dict,
    *,
    pauses: Sequence[Any] = (),
    as_of: date | None = None,
) -> list[ExpectedPayment]:
    """Generate expected-payment rows from the loan's terms.

    One row per payment number from 1 to the current month. Dates
    follow `first_payment_date + (n-1) months`, snapped to
    `payment_due_day` when set. Months covered by any pause are
    skipped (empty tuple until WP12 plugs real pauses in).

    Principal/interest split uses the amortization model; escrow /
    tax / insurance come from the configured monthlies.
    """
    today = _today(as_of)
    first = _as_date(loan.get("first_payment_date"))
    term = int(loan.get("term_months") or 0)
    if not first or term <= 0:
        return []

    principal = _as_decimal(loan.get("original_principal")) or Decimal("0")
    apr = _as_decimal(loan.get("interest_rate_apr")) or Decimal("0")
    escrow_monthly = _as_decimal(loan.get("escrow_monthly")) or Decimal("0")
    tax_monthly = _as_decimal(loan.get("property_tax_monthly")) or Decimal("0")
    insurance_monthly = _as_decimal(loan.get("insurance_monthly")) or Decimal("0")

    if principal <= 0 or apr < 0:
        return []

    due_day = loan.get("payment_due_day")

    # DEFERRED-WP13-PHASE2: variable APR. ARMs and most HELOCs reset the
    # rate periodically (1/3/5/7/10-year fixed → annual reset; HELOC
    # tracks prime monthly). Today the schedule uses a single ``apr``
    # value for the entire term, which silently mis-models any loan
    # that's reset since funding. The fix shape:
    #   - new state table ``loan_rate_history (loan_slug, effective_date, apr)``
    #     reconstructable from ``custom "loan-rate-change"`` directives
    #   - build_schedule slices the schedule into segments by the
    #     applicable APR and re-amortizes the remaining balance at
    #     each rate-change boundary
    # Until that lands, the "Today's status (model)" panel on a loan
    # whose APR has changed should be read as "what the model would
    # say at the funded-day rate", not gospel.
    schedule = amortization_schedule(principal, apr, term)
    n_today = payment_number_on(first, today, term)

    rows: list[ExpectedPayment] = []
    for n in range(1, n_today + 1):
        d = _add_months(first, n - 1)
        d = _snap_to_due_day(d, due_day)
        if _date_covered_by_pause(d, pauses):
            continue
        if n - 1 >= len(schedule):
            # Beyond the end of the schedule — can happen when the
            # loan is closed / payoff is pending. Stop generating.
            break
        pmt = schedule[n - 1]
        total = pmt.total + escrow_monthly + tax_monthly + insurance_monthly
        rows.append(ExpectedPayment(
            n=n,
            expected_date=d,
            principal=pmt.principal,
            interest=pmt.interest,
            escrow=escrow_monthly,
            tax=tax_monthly,
            insurance=insurance_monthly,
            total=total,
        ))
    return rows


# ------------------------------------------------------------ actual extraction


def extract_actuals(
    loan: dict, entries: Sequence[Any],
) -> list[ActualPayment]:
    """Every transaction touching the liability account, with leg
    breakdown computed from the override-of chain.

    The per-leg classification mirrors the existing logic in
    routes/loans.py::loan_detail: liability postings sum into
    `principal_leg`, interest-path postings into `interest_leg`,
    escrow-path postings into `escrow_leg`. `auto_classified` is
    derived from the `lamella-loan-autoclass-tier` meta key that WP6
    will start writing; absence means manual classification.
    """
    from lamella.core.beancount_io.txn_hash import txn_hash

    liability = loan.get("liability_account_path")
    interest = loan.get("interest_account_path")
    escrow = loan.get("escrow_account_path")
    if not liability:
        return []

    actuals: list[ActualPayment] = []
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        postings = entry.postings or []
        touches_liability = any(
            getattr(p, "account", None) == liability for p in postings
        )
        if not touches_liability:
            continue

        principal_leg = Decimal("0")
        interest_leg = Decimal("0")
        escrow_leg = Decimal("0")
        for p in postings:
            acct = getattr(p, "account", None)
            units = getattr(p, "units", None)
            if units is None or getattr(units, "number", None) is None:
                continue
            amt = Decimal(units.number)
            if acct == liability:
                # Paydown posts a positive number on the liability
                # (balance moves from negative toward zero). A draw
                # posts negative. Coverage only cares about payments,
                # so we sum positive amounts as "principal paid."
                if amt > 0:
                    principal_leg += amt
            elif acct == interest and interest:
                interest_leg += amt
            elif acct == escrow and escrow:
                escrow_leg += amt

        meta = getattr(entry, "meta", None) or {}
        tier_meta = meta.get("lamella-loan-autoclass-tier")
        auto = bool(tier_meta)
        tier_str = str(tier_meta) if tier_meta else None
        total = principal_leg + interest_leg + escrow_leg
        # Guard: a draw-only entry (liability went more negative,
        # no positive liability leg) has principal_leg = 0 and
        # interest/escrow 0 too. Skip those — they're not payments.
        if total <= 0:
            continue

        d = _as_date(entry.date) or date.today()
        actuals.append(ActualPayment(
            txn_hash=txn_hash(entry),
            date=d,
            principal_leg=principal_leg,
            interest_leg=interest_leg,
            escrow_leg=escrow_leg,
            total=total,
            auto_classified=auto,
            autoclass_tier=tier_str,
        ))
    actuals.sort(key=lambda a: a.date)
    return actuals


# --------------------------------------------------------------------- match


# ≥3 consecutive missing triggers graceful degradation per the plan.
# Shipping until WP12 plugs real pauses into build_schedule().
_LONG_GAP_THRESHOLD = 3


def match(
    expected: Sequence[ExpectedPayment],
    actuals: Sequence[ActualPayment],
    *,
    window_days: int = 15,
    late_grace_days: int = 15,
) -> CoverageReport:
    """Match each expected row to the unused actual inside its
    window. Unmatched expected rows become `missing`; unmatched
    actuals become `extras`.

    Each expected's window spans from `expected_date - window_days`
    to the NEXT expected's `expected_date - window_days` (exclusive).
    The final expected row's window extends to infinity so a
    late-posted final payment still matches. Within a window, the
    actual closest to the expected date wins.

    Late badging: an actual more than `late_grace_days` after its
    matched expected date is `late`, not `on_time`.
    """
    exp_list = sorted(expected, key=lambda e: (e.expected_date, e.n))
    consumed: set[int] = set()  # indices into actuals

    # Pre-compute each expected's window bounds.
    boundaries: list[tuple[date, date]] = []
    window_delta = timedelta(days=window_days)
    for i, e in enumerate(exp_list):
        start = e.expected_date - window_delta
        if i + 1 < len(exp_list):
            end = exp_list[i + 1].expected_date - window_delta
        else:
            end = date.max
        boundaries.append((start, end))

    rows: list[CoverageRow] = []
    late_count = 0
    for (window_start, window_end), e in zip(boundaries, exp_list):
        # Actuals inside this window, not yet consumed.
        best_idx = -1
        best_delta = timedelta.max
        for i, a in enumerate(actuals):
            if i in consumed:
                continue
            if window_start <= a.date < window_end:
                delta = abs(a.date - e.expected_date)
                if delta < best_delta:
                    best_idx = i
                    best_delta = delta

        if best_idx == -1:
            rows.append(CoverageRow(expected=e, actual=None, status="missing"))
            continue

        actual = actuals[best_idx]
        consumed.add(best_idx)
        # Possible double-post: another actual within ±3 days of the
        # matched one that's also inside this window. Surface as extras
        # so WP8 anomaly can flag them.
        extras: list[ActualPayment] = []
        for j, other in enumerate(actuals):
            if j in consumed:
                continue
            if (
                window_start <= other.date < window_end
                and abs(other.date - actual.date) <= timedelta(days=3)
            ):
                extras.append(other)
                consumed.add(j)

        status: Status = "on_time"
        if actual.date - e.expected_date > timedelta(days=late_grace_days):
            status = "late"
            late_count += 1
        rows.append(CoverageRow(
            expected=e, actual=actual, status=status, extras=extras,
        ))

    # Unmatched actuals become report-level extras.
    unmatched_extras = [
        actuals[i] for i in range(len(actuals)) if i not in consumed
    ]

    # Graceful-degradation signal: find the longest run of consecutive
    # `missing` rows. When ≥ threshold, health.py collapses individual
    # missing-payment next-actions into one `long-payment-gap` item.
    missing_count = sum(1 for r in rows if r.status == "missing")
    matched_count = sum(1 for r in rows if r.status != "missing")
    long_run = 0
    current_run = 0
    long_run_start: date | None = None
    long_run_end: date | None = None
    run_start: date | None = None
    for r in rows:
        if r.status == "missing":
            if current_run == 0:
                run_start = r.expected.expected_date
            current_run += 1
            if current_run > long_run:
                long_run = current_run
                long_run_start = run_start
                long_run_end = r.expected.expected_date
        else:
            current_run = 0
            run_start = None

    long_gap_detected = long_run >= _LONG_GAP_THRESHOLD

    return CoverageReport(
        rows=rows,
        expected_count=len(rows),
        matched_count=matched_count,
        missing_count=missing_count,
        late_count=late_count,
        extras=unmatched_extras,
        long_gap_detected=long_gap_detected,
        long_gap_start=long_run_start if long_gap_detected else None,
        long_gap_end=long_run_end if long_gap_detected else None,
        long_gap_count=long_run if long_gap_detected else 0,
    )


# --------------------------------------------------------------- entry point


def coverage_for(
    loan: dict,
    entries: Sequence[Any],
    *,
    pauses: Sequence[Any] = (),
    as_of: date | None = None,
    window_days: int = 15,
) -> CoverageReport | None:
    """Convenience wrapper: build schedule, extract actuals, match.

    Returns `None` when the loan lacks enough terms to compute an
    expected schedule (no first_payment_date or no term_months). The
    health model treats `None` as "coverage panel not applicable"
    rather than "empty coverage."
    """
    schedule = build_schedule(loan, pauses=pauses, as_of=as_of)
    if not schedule:
        return None
    actuals = extract_actuals(loan, entries)
    return match(schedule, actuals, window_days=window_days)
