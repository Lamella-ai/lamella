# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Loan anomaly detection (WP8).

Pure-arithmetic detectors that notice unusual patterns without
ML/AI. Flag them; never auto-fix. Each anomaly carries a
`recommended_action` that the health model folds into its
next-actions list.

Detectors in this module:

- `_detect_payment_drift` — rolling median of recent payment totals;
  flag deviations > 10% that aren't already explained by a known
  config change. Baseline resets on config change (per the plan's
  feedback — option A: read `custom "loan"` directive history to
  find the most recent change of the monthly-components tuple,
  restrict the rolling median to payments after that date).
- `_detect_interest_high_vs_model` — actual interest > model
  interest with anchored balance also higher than model →
  suggests a missed payment that was never recorded.
- `_detect_principal_low_vs_model` — inverse. Usually a mis-split.
- `_detect_apr_mismatch` — derive effective APR from consecutive
  payments; when it drifts > 0.5% from configured, the configured
  APR is likely wrong.
- `_detect_anchor_disagreement` — statement anchor vs ledger-walked
  balance at the same date. > 1% delta → missing or mis-split
  payments between anchors.
- `_detect_double_post` — consumes coverage report extras.
- `_detect_sustained_overflow` — DEFERRED until WP6 stamps the
  `lamella-loan-autoclass-tier` meta on overrides. Shell kept below
  so the integration point is visible; it currently returns [].

DEFERRED-ANOMALY-DISMISS: a user-dismissible flag stored in a ledger
directive would let the user silence a real-but-acknowledged anomaly
without having to "fix" it. Out of scope for WP8 — the config-reset
logic in `_detect_payment_drift` handles the common case where an
anomaly is already addressed by a loan edit.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Sequence


Severity = Literal["attention", "info"]


@dataclass(frozen=True)
class Anomaly:
    kind: str
    severity: Severity
    detail: str
    numbers: dict[str, str] = field(default_factory=dict)
    # The recommended_action is a NextAction (health.NextAction). We
    # type it loosely to avoid the cross-import — health.py imports
    # from anomalies and folds these into its next_actions list.
    recommended_action: Any = None


# --------------------------------------------------------------------- helpers


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


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _median(values: Sequence[Decimal]) -> Decimal:
    vs = sorted(values)
    n = len(vs)
    if n == 0:
        return Decimal("0")
    if n % 2 == 1:
        return vs[n // 2]
    return (vs[n // 2 - 1] + vs[n // 2]) / Decimal("2")


def _monthly_components(
    directive_row: dict,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    """The tuple whose change triggers a drift-baseline reset."""
    return (
        _as_decimal(directive_row.get("escrow_monthly")) or Decimal("0"),
        _as_decimal(directive_row.get("property_tax_monthly")) or Decimal("0"),
        _as_decimal(directive_row.get("insurance_monthly")) or Decimal("0"),
        _as_decimal(directive_row.get("monthly_payment_estimate")) or Decimal("0"),
    )


def _most_recent_config_reset_date(history: Sequence[dict]) -> date | None:
    """Scan `custom "loan"` directive history; return the date of the
    most recent directive whose monthly-components tuple differs from
    the directive immediately before it. None when the history has
    one or zero entries, or when nothing has ever changed.

    This is the correct drift-baseline reset point per §3.8 of the
    implementation doc — a rolling median that drags stale values
    forward will keep firing after a user has already addressed the
    config by editing the loan.
    """
    if len(history) <= 1:
        return None
    prior_tuple = _monthly_components(history[0])
    reset_date: date | None = None
    for entry in history[1:]:
        current = _monthly_components(entry)
        if current != prior_tuple:
            reset_date = _as_date(entry.get("directive_date"))
        prior_tuple = current
    return reset_date


# ---------------------------------------------- per-detector implementations


def _detect_payment_drift(
    actuals: Sequence[Any],
    history: Sequence[dict],
    *,
    deviation_pct: Decimal = Decimal("0.10"),
    window: int = 6,
) -> list[Anomaly]:
    """Rolling median of the last `window` actuals (post-reset). Flag
    when the most recent payment deviates by > `deviation_pct`."""
    reset_date = _most_recent_config_reset_date(history)

    relevant: list[Any] = []
    for a in actuals:
        a_date = _as_date(getattr(a, "date", None))
        if reset_date is None or (a_date and a_date >= reset_date):
            relevant.append(a)

    # Minimum signal: fewer than 3 relevant payments and we can't
    # trust the median. Sustained-overflow (WP6) catches the
    # "new config, a couple outlier payments" case separately.
    if len(relevant) < 3:
        return []

    sample = relevant[-window:]
    totals = [
        _as_decimal(getattr(a, "total", None)) or Decimal("0")
        for a in sample
    ]
    median = _median(totals)
    if median <= 0:
        return []

    latest = sample[-1]
    latest_total = _as_decimal(getattr(latest, "total", None)) or Decimal("0")
    delta = latest_total - median
    pct = abs(delta) / median if median else Decimal("0")
    if pct <= deviation_pct:
        return []

    direction = "higher" if delta > 0 else "lower"
    return [Anomaly(
        kind="payment-drift",
        severity="attention",
        detail=(
            f"Most recent payment ({latest_total}) is {pct:.0%} "
            f"{direction} than the rolling median ({median}) of the "
            f"last {len(sample)} payments. The usual cause is a "
            f"servicer escrow recalc — update the configured monthly "
            f"on the edit page and the anomaly will stop firing."
        ),
        numbers={
            "latest_total": str(latest_total),
            "rolling_median": str(median),
            "deviation_pct": f"{pct:.2%}",
            "sample_size": str(len(sample)),
            "reset_date": reset_date.isoformat() if reset_date else "",
        },
    )]


def _detect_interest_high_vs_model(
    loan: dict,
    actuals: Sequence[Any],
    anchored_balance: Decimal | None,
    model_remaining: Decimal | None,
) -> list[Anomaly]:
    """When total actual interest paid significantly exceeds what the
    amortization model projected AND the anchored balance is also
    higher than model, the most likely cause is a missed payment that
    never got recorded in the ledger."""
    if anchored_balance is None or model_remaining is None:
        return []
    if anchored_balance <= model_remaining:
        return []

    # Heuristic threshold: anchored balance > model by > 2% of principal.
    original = _as_decimal(loan.get("original_principal")) or Decimal("0")
    if original <= 0:
        return []
    gap = anchored_balance - model_remaining
    if gap / original < Decimal("0.02"):
        return []

    return [Anomaly(
        kind="interest-high-vs-model",
        severity="attention",
        detail=(
            f"Anchored balance ({anchored_balance}) is higher than the "
            f"amortization model projects ({model_remaining}) by "
            f"{gap}. Common cause: one or more missed payments that "
            f"never got recorded. Use the payment-coverage panel to "
            f"locate missing months."
        ),
        numbers={
            "anchored_balance": str(anchored_balance),
            "model_remaining": str(model_remaining),
            "gap": str(gap),
        },
    )]


def _detect_principal_low_vs_model(
    loan: dict,
    anchored_balance: Decimal | None,
    model_remaining: Decimal | None,
) -> list[Anomaly]:
    """The inverse. Anchored balance noticeably LOWER than model
    without corresponding extra-principal history = mis-split
    payments (interest leg getting principal's share, say)."""
    if anchored_balance is None or model_remaining is None:
        return []
    if anchored_balance >= model_remaining:
        return []

    original = _as_decimal(loan.get("original_principal")) or Decimal("0")
    if original <= 0:
        return []
    gap = model_remaining - anchored_balance
    if gap / original < Decimal("0.02"):
        return []

    return [Anomaly(
        kind="principal-low-vs-model",
        severity="attention",
        detail=(
            f"Anchored balance ({anchored_balance}) is lower than the "
            f"amortization model projects ({model_remaining}) by "
            f"{gap}. When extra-principal history doesn't explain the "
            f"gap, this usually means a payment was split wrong (too "
            f"much routed to principal, too little to interest). "
            f"Review recent payments in the payments panel."
        ),
        numbers={
            "anchored_balance": str(anchored_balance),
            "model_remaining": str(model_remaining),
            "gap": str(gap),
        },
    )]


def _detect_apr_mismatch(
    loan: dict,
    actuals: Sequence[Any],
) -> list[Anomaly]:
    """Derive effective APR from the last 3+ payments' interest legs
    relative to the running balance. A > 0.5% drift over multiple
    consecutive payments on a fixed-rate loan means the configured
    APR is wrong (or the loan is actually variable-rate)."""
    configured_apr = _as_decimal(loan.get("interest_rate_apr"))
    if configured_apr is None or configured_apr <= 0:
        return []
    if len(actuals) < 3:
        return []

    original = _as_decimal(loan.get("original_principal")) or Decimal("0")
    if original <= 0:
        return []

    # Walk the LAST 3 actuals; use running-balance-at-month-start
    # approximation from the amortization model. This is a heuristic
    # — it assumes the model's schedule is roughly right; if APR is
    # wildly wrong, even the anchor won't perfectly align but the
    # direction will. Good enough for "flag for review."
    monthly_rate_configured = (
        configured_apr / Decimal("100") / Decimal("12")
    )

    sample = list(actuals)[-3:]
    derived_rates: list[Decimal] = []
    # For each payment: effective_rate ≈ interest_leg / balance_before_payment.
    # We don't have balance_before per-payment here; approximate as
    # principal - sum(principal_legs_before). That's rough; detection
    # threshold is tuned to be forgiving.
    principal_before = original
    for a in sample:
        interest_leg = _as_decimal(getattr(a, "interest_leg", None)) or Decimal("0")
        principal_leg = _as_decimal(getattr(a, "principal_leg", None)) or Decimal("0")
        if principal_before <= 0 or interest_leg <= 0:
            principal_before -= principal_leg
            continue
        # Monthly rate implied = interest / balance_before.
        rate = interest_leg / principal_before
        derived_rates.append(rate)
        principal_before -= principal_leg

    if len(derived_rates) < 3:
        return []

    median_derived = _median(derived_rates)
    # Convert monthly rate → annual APR %.
    derived_apr = median_derived * Decimal("12") * Decimal("100")
    delta = abs(derived_apr - configured_apr)
    if delta < Decimal("0.5"):
        return []

    return [Anomaly(
        kind="apr-mismatch",
        severity="attention",
        detail=(
            f"Effective APR derived from the last 3 payments "
            f"({derived_apr:.3f}%) drifts from the configured APR "
            f"({configured_apr}%) by {delta:.3f}%. On a fixed-rate "
            f"loan this means the configured APR is wrong or the "
            f"loan is actually variable-rate. Update on the edit page."
        ),
        numbers={
            "configured_apr": str(configured_apr),
            "derived_apr": f"{derived_apr:.3f}",
            "delta": f"{delta:.3f}",
        },
    )]


def _detect_anchor_disagreement(
    anchors: Sequence[Any],
    actuals: Sequence[Any],
    original_principal: Decimal,
) -> list[Anomaly]:
    """Two anchors, between them N payments. The ledger-walked balance
    at the later anchor's date should match the later anchor's balance
    within a small percent. When it doesn't, payments are missing or
    mis-split in the gap."""
    past_anchors: list[dict] = []
    for a in anchors:
        a_date = _as_date(
            a.get("as_of_date") if isinstance(a, dict)
            else getattr(a, "as_of_date", None)
        )
        a_bal = _as_decimal(
            a.get("balance") if isinstance(a, dict)
            else getattr(a, "balance", None)
        )
        if a_date and a_bal is not None:
            past_anchors.append({"date": a_date, "balance": a_bal})
    if len(past_anchors) < 2:
        return []
    if original_principal <= 0:
        return []

    past_anchors.sort(key=lambda x: x["date"])
    earlier, later = past_anchors[-2], past_anchors[-1]

    principal_between = Decimal("0")
    for a in actuals:
        a_date = _as_date(getattr(a, "date", None))
        if a_date and earlier["date"] < a_date <= later["date"]:
            principal_between += _as_decimal(
                getattr(a, "principal_leg", 0)
            ) or Decimal("0")
    expected_balance = earlier["balance"] - principal_between
    gap = abs(expected_balance - later["balance"])
    if gap / original_principal < Decimal("0.01"):
        return []

    return [Anomaly(
        kind="anchor-disagreement",
        severity="attention",
        detail=(
            f"Anchor on {later['date']} says balance is "
            f"{later['balance']}, but walking forward from the anchor "
            f"on {earlier['date']} ({earlier['balance']}) through "
            f"ledger payments gives {expected_balance} — a {gap} "
            f"discrepancy. Payments between these anchor dates are "
            f"probably missing or mis-split."
        ),
        numbers={
            "earlier_date": earlier["date"].isoformat(),
            "earlier_balance": str(earlier["balance"]),
            "later_date": later["date"].isoformat(),
            "later_balance": str(later["balance"]),
            "walk_forward_balance": str(expected_balance),
            "gap": str(gap),
        },
    )]


def _detect_double_post(coverage: Any) -> list[Anomaly]:
    """Coverage's per-row `extras` carry actuals within ±3 days of a
    matched payment that looked like duplicates. Surface each as an
    info-tier anomaly the user can confirm or dismiss."""
    out: list[Anomaly] = []
    if coverage is None:
        return out
    for row in getattr(coverage, "rows", []):
        extras = getattr(row, "extras", None) or []
        if not extras:
            continue
        expected = getattr(row, "expected", None)
        actual = getattr(row, "actual", None)
        for e in extras:
            e_date = _as_date(getattr(e, "date", None))
            e_total = getattr(e, "total", None)
            out.append(Anomaly(
                kind="double-post",
                severity="info",
                detail=(
                    f"Payment on {e_date} (${e_total}) is within 3 days "
                    f"of the payment matched to expected "
                    f"#{getattr(expected, 'n', '?')}. Possible "
                    f"duplicate posting — if so, remove one; if both "
                    f"are legit (e.g., extra-principal), ignore."
                ),
                numbers={
                    "extra_date": e_date.isoformat() if e_date else "",
                    "extra_total": str(e_total),
                    "matched_date": (
                        getattr(actual, "date", "").isoformat()
                        if actual and getattr(actual, "date", None)
                        else ""
                    ),
                },
            ))
    return out


def _detect_sustained_overflow(
    actuals: Sequence[Any],
    *,
    run_length: int = 3,
) -> list[Anomaly]:
    """Flag the servicer-change window described in §2.5.3 of the
    implementation doc. When N consecutive auto-classified payments
    carry tier="over", the user's real monthly is higher than
    configured and they should update the loan settings — otherwise
    the next N payments will also be routed to bonus_principal (or
    bonus_escrow) and the mis-configuration compounds.

    Reads the `autoclass_tier` field populated by
    `coverage.extract_actuals` from the lamella-loan-autoclass-tier meta
    key on auto-classified override blocks. Manual classifications
    have `autoclass_tier=None` and break the run.
    """
    if not actuals:
        return []
    # Look at the most recent `run_length` auto-classified payments.
    # A manual classification (tier None) in the middle breaks the
    # run — once the user has stepped in, we shouldn't alarm about a
    # stale over-sequence.
    recent = list(actuals)[-run_length:]
    if len(recent) < run_length:
        return []
    tiers = [getattr(a, "autoclass_tier", None) for a in recent]
    if not all(t == "over" for t in tiers):
        return []
    first, last = recent[0], recent[-1]
    return [Anomaly(
        kind="sustained-overflow",
        severity="attention",
        detail=(
            f"The last {run_length} payments have auto-classified at "
            f"tier=\"over\" — each one routed the excess to the loan's "
            f"overflow destination. The most likely cause is a servicer "
            f"escrow recalc that raised the configured monthly; update "
            f"the loan's escrow/tax/insurance amounts on the edit page "
            f"and the next payment should auto-classify at tier=\"exact\"."
        ),
        numbers={
            "run_length": str(run_length),
            "earliest_date": getattr(first, "date", ""),
            "latest_date": getattr(last, "date", ""),
        },
    )]


# ---------------------------------------------------------------------- entry


def detect(
    loan: dict,
    coverage: Any,
    anchors: Sequence[Any],
    actuals: Sequence[Any],
    history: Sequence[dict] = (),
    *,
    anchored_balance: Decimal | None = None,
    model_remaining: Decimal | None = None,
) -> list[Anomaly]:
    """Run every detector; return the combined list.

    `history` is the result of
    `reader.read_loan_directive_history(entries, slug)`. An empty
    history is valid — detectors that need the reset-date signal
    degrade gracefully (no reset point found means treat the whole
    actuals series as a single baseline).
    """
    original_principal = _as_decimal(loan.get("original_principal")) or Decimal("0")

    out: list[Anomaly] = []
    out.extend(_detect_payment_drift(actuals, history))
    out.extend(_detect_interest_high_vs_model(
        loan, actuals, anchored_balance, model_remaining,
    ))
    out.extend(_detect_principal_low_vs_model(
        loan, anchored_balance, model_remaining,
    ))
    out.extend(_detect_apr_mismatch(loan, actuals))
    out.extend(_detect_anchor_disagreement(
        anchors, actuals, original_principal,
    ))
    out.extend(_detect_double_post(coverage))
    out.extend(_detect_sustained_overflow(actuals))
    return out
