# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Auto-classification of loan payments (WP6).

When a FIXME payment arrives on a configured loan account, the loan
module has more information than any generic classifier could bring
to bear — the amortization model's split plus the configured escrow
/ tax / insurance monthlies. This module is the single write path
for that knowledge.

Five call sites preempt AI classification (see
FEATURE_LOANS_IMPLEMENTATION.md §2.5.2). Three of them (bulk_classify,
enricher, and a post-ingest sweep for SimpleFIN) route the claimed
transaction through `process(claim, txn, loan, ...)`. The other two
(audit, calendar/ai) are read-only surfaces that just skip the AI
path on a claim without calling `process()`.

`process()` owns the entire write decision:

- PAYMENT + tier="exact" or "over": writes the override via
  OverrideWriter.append_split with `extra_meta={lamella-loan-autoclass-*}`,
  inserts one row into loan_autoclass_log.
- PAYMENT + tier="under" or "far": no write; caller surfaces in its
  own UI channel (review queue / anomaly panel / partial-payment
  flow — see WP12).
- ESCROW_DISBURSEMENT / DRAW / REVOLVING_SKIP: no write. Caller
  continues its own flow without calling AI.

Tier thresholds live as module constants so anomaly tests
(_detect_sustained_overflow) and UI copy can import them and stay
in sync.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Sequence

from beancount.core.data import Transaction

from lamella.features.loans.amortization import (
    payment_number_on,
    split_for_payment_number,
)
from lamella.features.loans.claim import Claim, ClaimKind

log = logging.getLogger(__name__)


# --------------------------------------------------------- tier thresholds


# | actual - expected | ≤ this → tier "exact".
TIER_EXACT_TOLERANCE = Decimal("0.02")

# (actual - expected) ≤ this fraction of expected → tier "over"
# (assuming above exact threshold). 50% covers large-extra-principal
# months without drifting into "clearly a different kind of txn."
TIER_OVER_MAX_FRACTION = Decimal("0.5")

# (expected - actual) > this fraction of expected → "far" (not "under").
# Mirror of TIER_OVER_MAX_FRACTION on the downside.
TIER_UNDER_MAX_FRACTION = Decimal("0.5")

# TODO(wp12-late-fee-autodetect): the auto-classify path currently
# routes any (actual - expected) excess to bonus_principal /
# bonus_escrow. A late fee — typically $25-$75 over expected,
# fixed-amount, recurring on the same merchant — should ideally
# split off as its own Expenses:{Entity}:{Slug}:LateFees leg
# instead of inflating principal. Heuristic: if overflow is in
# [$15, $100] AND there's a "late" / "fee" hit in narration AND
# the loan has had ≥1 prior late_fee leg in the last 12 months,
# auto-route to LateFees. Until that lands, users record late
# fees explicitly via the record-payment form's late_fee field
# (see routes/loans.py::record_mortgage_payment).


Tier = Literal["exact", "over", "under", "far"]


# --------------------------------------------------------------- dataclasses


@dataclass(frozen=True)
class ClassifyPlan:
    """Everything needed to write the override — computed by plan(),
    consumed by apply(). Kept separate so tests can exercise the
    arithmetic without needing a real ledger."""

    tier: Tier
    # (account, amount) pairs that sum to |actual_total|.
    splits: list[tuple[str, Decimal]]
    from_account: str | None            # the source-of-funds leg (may be None)
    actual_total: Decimal
    expected_total: Decimal
    overflow_amount: Decimal             # >0 only for tier="over"
    overflow_dest: str | None            # resolved account path; None for non-over
    overflow_dest_source: str            # "default" | "user"
    decision_id: str                     # uuid4 — stays stable across apply
    narration_hint_escrow: bool          # "escrow" whole-word in narration
    skip_reason: str | None              # human text for tier="under"/"far"


@dataclass(frozen=True)
class ProcessOutcome:
    """Return value of `process()`. Callers dispatch post-write
    bookkeeping off these fields."""

    claim_kind: ClaimKind
    tier: Tier | None                    # None for non-PAYMENT claims
    wrote_override: bool                 # True ↔ tier in (exact, over)
    decision_id: str | None              # uuid when wrote_override else None
    skip_reason: str | None              # caller-facing text for under/far
    txn_hash: str


# --------------------------------------------------------------------- helpers


def _as_decimal(v: Any) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _as_date(v: Any) -> date | None:
    if v is None or v == "":
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    try:
        return date.fromisoformat(str(v)[:10])
    except (TypeError, ValueError):
        return None


def _txn_fixme_amount(txn: Transaction) -> tuple[str | None, Decimal | None, str]:
    """Return (fixme_account, abs_amount, currency) for the FIXME
    leg. Return (None, None, 'USD') when no FIXME leg."""
    for p in txn.postings or []:
        acct = getattr(p, "account", None) or ""
        if acct.endswith(":FIXME") or acct.split(":")[-1].upper() == "FIXME":
            units = getattr(p, "units", None)
            if units is None or getattr(units, "number", None) is None:
                continue
            return acct, abs(Decimal(units.number)), (units.currency or "USD")
    return None, None, "USD"


def _source_leg(txn: Transaction, skip_account: str | None = None) -> str | None:
    """The real source-of-funds leg: first Assets:/Liabilities:Credit
    posting that isn't the FIXME and isn't `skip_account`."""
    for p in txn.postings or []:
        acct = getattr(p, "account", None) or ""
        if not acct or acct == skip_account:
            continue
        if acct.split(":")[-1].upper() == "FIXME":
            continue
        if acct.startswith("Assets:") or acct.startswith("Liabilities:Credit"):
            return acct
    return None


def _compute_tier(actual: Decimal, expected: Decimal) -> Tier:
    if expected <= 0:
        return "far"
    delta = actual - expected
    if abs(delta) <= TIER_EXACT_TOLERANCE:
        return "exact"
    if delta > 0:
        return "over" if delta / expected <= TIER_OVER_MAX_FRACTION else "far"
    # delta < 0
    return "under" if (-delta) / expected <= TIER_UNDER_MAX_FRACTION else "far"


def _resolve_overflow_dest(
    loan: dict,
) -> tuple[str | None, str, str]:
    """Return (dest_account, dest_source, dest_kind).

    dest_source: "default" (from loans.overflow_default config) or
    "user" (explicit per-payment reassign — not used inside plan();
    a user-reassign endpoint rewrites the override block with
    dest_source="user").
    dest_kind: one of the enum values from the loan config so the
    caller can disambiguate 'bonus_principal' vs 'bonus_escrow'
    when routing the overflow leg into the splits list.
    """
    default = (loan.get("overflow_default") or "bonus_principal").strip().lower()
    if default == "bonus_principal":
        return loan.get("liability_account_path"), "default", "bonus_principal"
    if default == "bonus_escrow":
        return loan.get("escrow_account_path"), "default", "bonus_escrow"
    # "ask" or any other value: leave dest None so plan() can downgrade.
    return None, "default", default


def _narration_mentions_escrow(txn: Transaction) -> bool:
    import re
    narration = (getattr(txn, "narration", None) or "")
    payee = (getattr(txn, "payee", None) or "")
    hay = f"{narration} {payee}".lower()
    return bool(re.search(r"\bescrow\b", hay))


# ------------------------------------------------------------ plan()


def _build_plan(
    *,
    actual_total: Decimal,
    txn_date: date,
    loan: dict,
    from_account: str | None,
    narration_hint: bool,
) -> ClassifyPlan:
    """Shared tier-decision + split-construction for plan() and
    plan_from_facts(). Pure.

    Callers provide the already-extracted `actual_total`, `txn_date`,
    `from_account`, and `narration_hint`. This function handles the
    amortization lookup, tier determination, overflow routing, and
    assembles the final ClassifyPlan.
    """
    decision_id = str(uuid.uuid4())

    if actual_total <= 0:
        return ClassifyPlan(
            tier="far", splits=[], from_account=from_account,
            actual_total=Decimal("0"), expected_total=Decimal("0"),
            overflow_amount=Decimal("0"), overflow_dest=None,
            overflow_dest_source="default",
            decision_id=decision_id,
            narration_hint_escrow=narration_hint,
            skip_reason="no positive amount on transaction",
        )

    principal = _as_decimal(loan.get("original_principal")) or Decimal("0")
    apr = _as_decimal(loan.get("interest_rate_apr")) or Decimal("0")
    term = int(loan.get("term_months") or 0)
    first = _as_date(loan.get("first_payment_date"))
    if not (principal > 0 and term > 0 and first):
        return ClassifyPlan(
            tier="far", splits=[], from_account=from_account,
            actual_total=actual_total, expected_total=Decimal("0"),
            overflow_amount=Decimal("0"), overflow_dest=None,
            overflow_dest_source="default",
            decision_id=decision_id,
            narration_hint_escrow=narration_hint,
            skip_reason="loan terms incomplete (no principal/APR/term/first-payment)",
        )

    n = payment_number_on(first, txn_date, term)
    escrow_monthly = _as_decimal(loan.get("escrow_monthly")) or Decimal("0")
    model_split = split_for_payment_number(
        principal, apr, term, n, escrow_monthly=escrow_monthly,
    )
    tax_monthly = _as_decimal(loan.get("property_tax_monthly")) or Decimal("0")
    insurance_monthly = _as_decimal(loan.get("insurance_monthly")) or Decimal("0")

    expected_total = (
        model_split["principal"] + model_split["interest"]
        + escrow_monthly + tax_monthly + insurance_monthly
    )
    tier = _compute_tier(actual_total, expected_total)

    if tier in ("under", "far"):
        reason = (
            "Payment amount differs materially from the model split; "
            "surfacing for user review."
        )
        return ClassifyPlan(
            tier=tier, splits=[], from_account=from_account,
            actual_total=actual_total, expected_total=expected_total,
            overflow_amount=Decimal("0"), overflow_dest=None,
            overflow_dest_source="default",
            decision_id=decision_id,
            narration_hint_escrow=narration_hint,
            skip_reason=reason,
        )

    # Compute the splits for exact/over tiers.
    liability_path = loan.get("liability_account_path")
    interest_path = loan.get("interest_account_path")
    escrow_path = loan.get("escrow_account_path")
    tax_path = (
        f"Expenses:{loan.get('entity_slug')}:{loan.get('slug')}:PropertyTax"
        if (tax_monthly > 0 and loan.get("entity_slug")) else None
    )
    insurance_path = (
        f"Expenses:{loan.get('entity_slug')}:{loan.get('slug')}:Insurance"
        if (insurance_monthly > 0 and loan.get("entity_slug")) else None
    )

    overflow_amount = (
        actual_total - expected_total if tier == "over" else Decimal("0")
    )
    overflow_dest, overflow_source, overflow_kind = _resolve_overflow_dest(loan)

    # If overflow_default is "ask" (no auto route), downgrade to
    # under/far so the caller surfaces rather than writes. This is
    # how the user opts out of automatic overflow handling.
    #
    # This matters especially at ingest-time (plan_from_facts): there
    # is no interactive user to ask, so "ask" must not silently route
    # to any default. The Tier 2 post-commit writer sees
    # wrote_override=False and leaves the row in staging for the user
    # to handle via /review/staged when they next visit.
    if tier == "over" and overflow_kind not in ("bonus_principal", "bonus_escrow"):
        return ClassifyPlan(
            tier="under" if overflow_amount < expected_total else "far",
            splits=[], from_account=from_account,
            actual_total=actual_total, expected_total=expected_total,
            overflow_amount=overflow_amount, overflow_dest=None,
            overflow_dest_source=overflow_source,
            decision_id=decision_id,
            narration_hint_escrow=narration_hint,
            skip_reason=(
                "Loan's overflow_default='ask' — the user is prompted "
                "for every over-expected payment; not auto-writing."
            ),
        )

    splits: list[tuple[str, Decimal]] = []
    # Principal goes to the liability; bonus_principal overflow folds
    # into the same leg.
    principal_amt = model_split["principal"]
    if overflow_kind == "bonus_principal":
        principal_amt = principal_amt + overflow_amount
    if liability_path and principal_amt > 0:
        splits.append((liability_path, principal_amt))
    if interest_path and model_split["interest"] > 0:
        splits.append((interest_path, model_split["interest"]))
    escrow_amt = escrow_monthly
    if overflow_kind == "bonus_escrow":
        escrow_amt = escrow_amt + overflow_amount
    if escrow_path and escrow_amt > 0:
        splits.append((escrow_path, escrow_amt))
    if tax_path and tax_monthly > 0:
        splits.append((tax_path, tax_monthly))
    if insurance_path and insurance_monthly > 0:
        splits.append((insurance_path, insurance_monthly))

    return ClassifyPlan(
        tier=tier,
        splits=splits,
        from_account=from_account,
        actual_total=actual_total,
        expected_total=expected_total,
        overflow_amount=overflow_amount,
        overflow_dest=overflow_dest if tier == "over" else None,
        overflow_dest_source=overflow_source,
        decision_id=decision_id,
        narration_hint_escrow=narration_hint,
        skip_reason=None,
    )


def plan(
    txn: Transaction, loan: dict, *, as_of: date | None = None,
) -> ClassifyPlan:
    """Compute the expected-vs-actual split, determine tier, and
    assemble the override plan.

    Pure with respect to inputs — no DB writes, no ledger writes.
    `as_of` defaults to the transaction's own date so the payment
    number is stable across the same FIXME being processed multiple
    times.
    """
    fixme_account, actual_total, _currency = _txn_fixme_amount(txn)
    if actual_total is None:
        # No FIXME leg at all — guard defensively. (Shouldn't happen
        # if is_claimed_by_loan returned PAYMENT, but tests exercise
        # the edge case.)
        return ClassifyPlan(
            tier="far", splits=[], from_account=fixme_account,
            actual_total=Decimal("0"), expected_total=Decimal("0"),
            overflow_amount=Decimal("0"), overflow_dest=None,
            overflow_dest_source="default",
            decision_id=str(uuid.uuid4()),
            narration_hint_escrow=False,
            skip_reason="no FIXME leg on transaction",
        )

    return _build_plan(
        actual_total=actual_total,
        txn_date=as_of or _as_date(txn.date) or date.today(),
        loan=loan,
        from_account=_source_leg(txn, skip_account=fixme_account),
        narration_hint=_narration_mentions_escrow(txn),
    )


def plan_from_facts(
    *,
    actual_total: Decimal,
    txn_date: date,
    loan: dict,
    source_account: str | None = None,
    narration: str | None = None,
    payee: str | None = None,
) -> ClassifyPlan:
    """Facts-level variant of `plan()` for the SimpleFIN ingest path.

    SimpleFIN `_classify()` runs before the bean Transaction exists,
    so there's no FIXME leg to inspect and no postings to walk for
    the source-of-funds. The caller provides the equivalent facts
    directly: the transaction's absolute amount, posted date, the
    source account SimpleFIN is mapped to, and narration/payee text
    for the escrow-hint heuristic.

    Returns the same `ClassifyPlan` shape as `plan()` so downstream
    callers (`apply`, `apply_ingest_split`, `process`) don't need to
    branch.
    """
    import re
    hay = f"{(narration or '').lower()} {(payee or '').lower()}"
    narration_hint = bool(re.search(r"\bescrow\b", hay))
    return _build_plan(
        actual_total=actual_total,
        txn_date=txn_date,
        loan=loan,
        from_account=source_account,
        narration_hint=narration_hint,
    )


# ------------------------------------------------------------------ apply()


def _extra_meta_for(plan: ClassifyPlan) -> dict:
    """Construct the lamella-loan-autoclass-* meta dict for the override."""
    meta: dict[str, Any] = {
        "lamella-loan-autoclass-tier": plan.tier,
        "lamella-loan-autoclass-decision-id": plan.decision_id,
    }
    if plan.tier == "over":
        meta["lamella-loan-autoclass-overflow"] = f"{plan.overflow_amount:.2f}"
        if plan.overflow_dest:
            meta["lamella-loan-autoclass-overflow-dest"] = plan.overflow_dest
        meta["lamella-loan-autoclass-overflow-dest-source"] = plan.overflow_dest_source
    return meta


def apply(
    plan: ClassifyPlan,
    txn: Transaction,
    loan: dict,
    *,
    settings: Any,
    reader: Any,
    conn: Any,
) -> None:
    """Write the override, stamp the autoclass meta, log the decision.

    No-op for tier in ("under", "far"): those don't auto-write.
    The caller inspects the returned ProcessOutcome.wrote_override
    to know.
    """
    from lamella.core.ledger_writer import BeanCheckError
    from lamella.features.rules.overrides import OverrideWriter

    if plan.tier not in ("exact", "over"):
        return
    if not plan.splits:
        return

    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    from lamella.core.beancount_io.txn_hash import txn_hash as _hash
    target_hash = _hash(txn)
    from_account = plan.from_account or ""

    # Determine currency from the FIXME leg.
    _, _, currency = _txn_fixme_amount(txn)

    # In-place first: rewrite the FIXME txn's whole posting block to the
    # categorized split + stamp the lamella-loan-autoclass-* meta on the
    # txn header. The override-overlay path is the fallback when
    # in-place can't proceed safely (missing source-leg, the source
    # file isn't in ledger_dir, an existing override on this hash
    # can't be cleanly stripped, bean-check rejects the rewrite, …).
    #
    # Why this is safe to migrate now: the rewriter supports
    # transaction-level extra_meta as of WP-Setup5's txn_inplace
    # M→N rewriter; previously the audit signal
    # (lamella-loan-autoclass-tier read by _detect_sustained_overflow +
    # the audit panel) would have been silently dropped.
    in_place_done = False
    txn_meta = getattr(txn, "meta", None) or {}
    src_file = txn_meta.get("filename")
    src_lineno = txn_meta.get("lineno")

    # Source-of-funds leg: the non-FIXME posting on the original txn.
    # _source_leg already prefers Assets:/Liabilities:Credit which
    # is what FIXME txns have on the other side. If we can't
    # determine it the rewrite can't proceed and we fall back.
    source_leg_acct = _source_leg(txn, skip_account=from_account)

    if src_file and src_lineno is not None and source_leg_acct:
        try:
            from pathlib import Path as _P
            from lamella.core.rewrite.txn_inplace import (
                InPlaceRewriteError,
                rewrite_txn_postings,
            )
            try:
                writer.rewrite_without_hash(target_hash)
            except BeanCheckError:
                raise InPlaceRewriteError("override-strip blocked")

            # Build the FULL new posting block: source leg
            # absorbs the negative of actual_total, splits land
            # the positive amounts. Sums to zero by construction
            # (plan.splits sum = plan.actual_total).
            new_postings: list[tuple[str, Decimal, str]] = [
                (source_leg_acct, -plan.actual_total, currency),
            ]
            for acct, amt in plan.splits:
                new_postings.append((acct, amt, currency))

            extra_meta_list = list(_extra_meta_for(plan).items())

            rewrite_txn_postings(
                source_file=_P(src_file),
                txn_start_line=int(src_lineno),
                new_postings=new_postings,
                extra_meta=extra_meta_list,
                ledger_dir=settings.ledger_dir,
                main_bean=settings.ledger_main,
            )
            in_place_done = True
        except InPlaceRewriteError as exc:
            log.info(
                "loan auto-classify: in-place refused for %s: %s "
                "— falling back to override",
                target_hash[:12], exc,
            )

    if not in_place_done:
        writer.append_split(
            txn_date=_as_date(txn.date) or date.today(),
            txn_hash=target_hash,
            from_account=from_account,
            splits=plan.splits,
            currency=currency,
            narration=(
                getattr(txn, "narration", None)
                or f"Loan payment ({loan.get('slug')})"
            ),
            extra_meta=_extra_meta_for(plan),
        )

    # Log the decision for WP8 sustained-overflow detection and the
    # audit panel. Best-effort — the ledger is source of truth.
    try:
        conn.execute(
            "INSERT INTO loan_autoclass_log "
            "(decision_id, loan_slug, txn_hash, tier, expected_total, "
            " actual_total, overflow_amount, overflow_dest, overflow_dest_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                plan.decision_id, loan["slug"], target_hash, plan.tier,
                f"{plan.expected_total:.2f}", f"{plan.actual_total:.2f}",
                f"{plan.overflow_amount:.2f}" if plan.tier == "over" else None,
                plan.overflow_dest,
                plan.overflow_dest_source,
            ),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "loan_autoclass_log insert failed for decision=%s slug=%s: %s",
            plan.decision_id, loan["slug"], exc,
        )


def apply_ingest_split(
    plan: ClassifyPlan,
    sf_txn: Any,                  # simplefin.schemas.SimpleFINTransaction
    source_account: str,
    loan: dict,
    *,
    writer: Any,                  # simplefin.writer.SimpleFINWriter
    conn: Any,
    target_path: Any = None,      # Path | None — preview vs. active
    lamella_txn_id: str | None = None,  # carried from staged row, when available
) -> None:
    """Write a WP6 Tier 2 classified-at-ingest split.

    Called from simplefin/ingest's post-commit pass for each claimed
    entry. Writes a multi-leg SimpleFIN transaction directly (no
    FIXME override — there's no bean txn to override; DEFER-FIXME
    means staging held the row instead). Marks staging promoted on
    success and inserts the loan_autoclass_log row.

    No-op for tier in (under, far): the plan already refused to
    produce splits, so nothing to write. Caller's
    `_auto_classify_claimed_ingest_entries` checks
    `outcome.wrote_override` and only promotes staging when True.
    """
    if plan.tier not in ("exact", "over") or not plan.splits:
        return

    # Source amount comes from the SimpleFIN transaction's reported
    # amount (signed from the account's POV). Writer flips the split
    # sign to balance. For a mortgage payment on a checking account,
    # amount is negative (money leaving) and splits land positive
    # on the loan accounts.
    source_amount = Decimal(str(sf_txn.amount))
    narration = getattr(sf_txn, "description", None) or getattr(sf_txn, "memo", None)
    payee = getattr(sf_txn, "payee", None) or getattr(sf_txn, "merchant", None)
    txn_date = getattr(sf_txn, "posted_date", None) or date.today()

    writer.append_split_entry(
        txn_date=txn_date,
        simplefin_id=getattr(sf_txn, "id", ""),
        source_account=source_account,
        source_amount=source_amount,
        splits=plan.splits,
        narration=narration,
        payee=payee,
        currency="USD",
        extra_meta=_extra_meta_for(plan),
        target_path=target_path,
        lamella_txn_id=lamella_txn_id,
    )

    # Log the decision for WP8 sustained-overflow detection.
    try:
        conn.execute(
            "INSERT INTO loan_autoclass_log "
            "(decision_id, loan_slug, txn_hash, tier, expected_total, "
            " actual_total, overflow_amount, overflow_dest, overflow_dest_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                plan.decision_id, loan["slug"],
                # txn_hash column doubles as "identifier of the thing
                # we resolved." For ingest-time writes there's no bean
                # txn_hash yet, so we store the simplefin-id — the
                # reader can disambiguate via presence of the
                # lamella-simplefin-id meta on the written transaction.
                f"simplefin:{getattr(sf_txn, 'id', '')}",
                plan.tier,
                f"{plan.expected_total:.2f}",
                f"{plan.actual_total:.2f}",
                f"{plan.overflow_amount:.2f}" if plan.tier == "over" else None,
                plan.overflow_dest,
                plan.overflow_dest_source,
            ),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "loan_autoclass_log insert failed for decision=%s slug=%s: %s",
            plan.decision_id, loan["slug"], exc,
        )


# ----------------------------------------------------------------- process()


def process(
    claim: Claim,
    txn: Transaction,
    loan: dict,
    *,
    settings: Any,
    reader: Any,
    conn: Any,
) -> ProcessOutcome:
    """Preempt-and-act for a claimed loan transaction.

    See module docstring for the per-ClaimKind dispatch table.
    Centralized here so the five preemption sites each call one
    function and let this module decide whether to write.
    """
    from lamella.core.beancount_io.txn_hash import txn_hash as _hash
    target_hash = _hash(txn)

    # Master-switch: a loan with auto_classify_enabled = 0 preempts
    # AI (principle 3) but never auto-writes. User hand-categorizes.
    if claim.kind == ClaimKind.PAYMENT and not loan.get(
        "auto_classify_enabled", 1,
    ):
        return ProcessOutcome(
            claim_kind=ClaimKind.PAYMENT,
            tier=None,
            wrote_override=False,
            decision_id=None,
            skip_reason="Loan has auto_classify_enabled=0; preempted without auto-write.",
            txn_hash=target_hash,
        )

    if claim.kind != ClaimKind.PAYMENT:
        # Non-payment claims: preempt but don't split.
        return ProcessOutcome(
            claim_kind=claim.kind,
            tier=None,
            wrote_override=False,
            decision_id=None,
            skip_reason=f"Claim kind {claim.kind.value} doesn't auto-split.",
            txn_hash=target_hash,
        )

    # PAYMENT: plan + apply.
    p = plan(txn, loan)
    if p.tier in ("exact", "over"):
        try:
            apply(p, txn, loan, settings=settings, reader=reader, conn=conn)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "auto_classify.apply failed for txn=%s slug=%s: %s",
                target_hash[:12], loan.get("slug"), exc,
            )
            return ProcessOutcome(
                claim_kind=ClaimKind.PAYMENT,
                tier=p.tier,
                wrote_override=False,
                decision_id=p.decision_id,
                skip_reason=f"apply failed: {exc}",
                txn_hash=target_hash,
            )
        return ProcessOutcome(
            claim_kind=ClaimKind.PAYMENT,
            tier=p.tier,
            wrote_override=True,
            decision_id=p.decision_id,
            skip_reason=None,
            txn_hash=target_hash,
        )

    return ProcessOutcome(
        claim_kind=ClaimKind.PAYMENT,
        tier=p.tier,
        wrote_override=False,
        decision_id=None,
        skip_reason=p.skip_reason,
        txn_hash=target_hash,
    )
