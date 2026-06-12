# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Loan claim detection (WP6 — principle-3 enforcement).

A transaction is "claimed" by a loan when the loan module has more
structured information about it than any general-purpose AI
classifier could bring to bear. The claim-check runs at five
preemption sites (see FEATURE_LOANS_IMPLEMENTATION.md §2.5.2);
when a claim fires, the AI path is skipped entirely and (at
write-path sites) the loan module's `auto_classify.process()`
dispatches based on the claim's kind.

Two entry points:

- `is_claimed_by_loan(txn, conn, reader=None) -> Claim | None`:
  for sites that already have a bean `Transaction` in hand
  (bulk_classify, enricher, audit, calendar).
- `claim_from_simplefin_facts(simplefin_txn, source_account, conn)
  -> Claim | None`: pre-bean-write variant used by simplefin/ingest
  when the bean Transaction doesn't exist yet.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

from beancount.core.data import Transaction


class ClaimKind(Enum):
    """What the loan module should do with a claimed transaction.

    PAYMENT: normal payment against the liability. auto_classify
        splits it (when tier=exact/over) or defers to the user
        (tier=under/far).
    ESCROW_DISBURSEMENT: transaction touches escrow but not the
        liability — e.g., servicer paid property tax out of escrow.
        No auto-split; caller just skips the AI path.
    DRAW: revolving loan where the liability balance increased.
        WP13 territory; WP6 will claim these (preempt AI) but
        auto_classify.process short-circuits.
    REVOLVING_SKIP: revolving-loan payment. Preempted but not
        auto-split (amortization model doesn't apply).
    """

    PAYMENT              = "payment"
    ESCROW_DISBURSEMENT  = "escrow_disbursement"
    DRAW                 = "draw"
    REVOLVING_SKIP       = "revolving_skip"


@dataclass(frozen=True)
class Claim:
    kind: ClaimKind
    loan_slug: str


# --------------------------------------------------------------------- helpers


def _as_decimal(v: Any) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None


_BASE_COLS = (
    "slug", "liability_account_path", "interest_account_path",
    "escrow_account_path", "simplefin_account_id",
    "monthly_payment_estimate", "is_active",
    "escrow_monthly", "property_tax_monthly", "insurance_monthly",
    # institution — added for WP11's claim_from_csv_row substring
    # match. Not used by the WP6 path-based / simplefin-id claim
    # checks; harmless extra column for those.
    "institution",
)

# `is_revolving` ships with WP13's migration 050. We conditionally
# SELECT it when the column exists so WP6 can land before WP13
# without the preemption path erroring on a missing column.
_OPTIONAL_COLS = ("is_revolving", "auto_classify_enabled")


def _existing_columns(conn: Any) -> set[str]:
    try:
        rows = conn.execute("PRAGMA table_info(loans)").fetchall()
    except Exception:  # noqa: BLE001
        return set()
    # PRAGMA table_info returns (cid, name, type, notnull, dflt_value, pk)
    return {row[1] for row in rows}


def _loans_snapshot(conn: Any) -> list[dict]:
    """Snapshot every loan with its tracked paths.

    Called at most once per is_claimed_by_loan invocation; the list
    is small (typically ≤ 10 loans) so we don't cache across calls —
    the ledger could have been edited between calls and caching
    would stale. For hot loops (bulk_classify iterates every
    FIXME), the caller should memoize externally.

    Selects the optional columns (is_revolving, auto_classify_enabled)
    when the current schema has them; otherwise falls back to the
    base 10-column set so this works across WP6-only and WP6+WP13
    deployments.
    """
    # Fake conns in tests may not implement PRAGMA; when that returns
    # an empty set, we still build a full SELECT and let the fake
    # decide what to include. Real SQLite with a WP6-only schema
    # returns the 10 base columns; WP6+WP13 adds is_revolving; WP6
    # itself (via migration 047) adds auto_classify_enabled so that
    # shows up once 047 has applied.
    existing = _existing_columns(conn)
    cols = list(_BASE_COLS)
    for opt in _OPTIONAL_COLS:
        if (not existing) or (opt in existing):
            cols.append(opt)
    col_sql = ", ".join(cols)
    try:
        rows = conn.execute(
            f"SELECT {col_sql} FROM loans WHERE is_active = 1"
        ).fetchall()
    except Exception:  # noqa: BLE001
        return []

    out: list[dict] = []
    for r in rows:
        if hasattr(r, "keys"):
            out.append(dict(r))
            continue
        out.append({col: r[idx] for idx, col in enumerate(cols)})
    return out


def _expected_monthly(loan: dict) -> Decimal:
    """Approximate expected monthly payment for a loan. Used as the
    tie-breaker when multiple loans could claim a single transaction
    (rare — mostly when two loans share a checking account)."""
    p = _as_decimal(loan.get("monthly_payment_estimate")) or Decimal("0")
    escrow = _as_decimal(loan.get("escrow_monthly")) or Decimal("0")
    tax = _as_decimal(loan.get("property_tax_monthly")) or Decimal("0")
    insurance = _as_decimal(loan.get("insurance_monthly")) or Decimal("0")
    return p + escrow + tax + insurance


def _is_revolving(loan: dict) -> bool:
    """True when loan.is_revolving = 1. The column ships with WP13's
    migration 050; earlier DBs return False via .get() default."""
    v = loan.get("is_revolving")
    if v is None:
        return False
    return bool(v)


def _txn_amount_hint(txn: Transaction) -> Decimal | None:
    """Pull the largest non-FIXME posting's absolute amount as a
    reasonable stand-in for the transaction's total. Used for the
    closest-expected-monthly tie-breaker."""
    best = Decimal("0")
    for p in txn.postings or []:
        units = getattr(p, "units", None)
        if units is None or getattr(units, "number", None) is None:
            continue
        acct = getattr(p, "account", None) or ""
        if acct.split(":")[-1].upper() == "FIXME":
            continue
        amt = abs(Decimal(units.number))
        if amt > best:
            best = amt
    return best if best > 0 else None


def _classify_kind(
    txn: Transaction, loan: dict,
) -> ClaimKind:
    """Given a transaction already matched to a loan by account
    touching, pick which ClaimKind it is."""
    liability = loan.get("liability_account_path")
    escrow = loan.get("escrow_account_path")
    revolving = _is_revolving(loan)

    # Scan the transaction's postings against the loan's paths.
    touches_liability = False
    liability_sign_positive = False  # paydown posts positive on the liability
    touches_escrow = False
    for p in txn.postings or []:
        acct = getattr(p, "account", None)
        units = getattr(p, "units", None)
        if units is None or getattr(units, "number", None) is None:
            continue
        amt = Decimal(units.number)
        if acct == liability:
            touches_liability = True
            if amt > 0:
                liability_sign_positive = True
        elif escrow and acct == escrow:
            touches_escrow = True

    if revolving:
        if touches_liability and not liability_sign_positive:
            return ClaimKind.DRAW
        if touches_liability:
            return ClaimKind.REVOLVING_SKIP
        # Revolving loans don't typically have escrow, but defensively:
        if touches_escrow:
            return ClaimKind.ESCROW_DISBURSEMENT
        return ClaimKind.REVOLVING_SKIP

    # Non-revolving path.
    if touches_escrow and not touches_liability:
        return ClaimKind.ESCROW_DISBURSEMENT
    return ClaimKind.PAYMENT


# -------------------------------------------------------------- is_claimed_by


def load_loans_snapshot(conn: Any) -> list[dict]:
    """Public wrapper around the internal loans snapshot.

    Hot-loop callers (bulk_classify, enricher) fetch once at loop
    entry and pass the result to ``is_claimed_by_loan(loans=...)``
    so each claim check is an O(1) dict scan rather than a SQL
    round-trip per transaction.
    """
    return _loans_snapshot(conn)


def is_claimed_by_loan(
    txn: Transaction, conn: Any, reader: Any = None,
    *, loans: list[dict] | None = None,
) -> Claim | None:
    """Return a Claim when `txn` belongs to a loan, else None.

    Resolution order:
    1. Posting touches any configured loan path (liability, interest,
       escrow). Tie-break across multiple matching loans: the loan
       whose expected-monthly is closest to the transaction's largest
       non-FIXME leg.
    2. Transaction meta carries lamella-simplefin-account-id matching a
       loan's simplefin_account_id.
    3. Otherwise None.

    Pass ``loans=load_loans_snapshot(conn)`` once at loop entry to
    skip the per-call SQL fetch.

    TODO(wp5-group-member-claim): when WP5's group panel ships and
    users start producing confirmed-group data, add a ClaimKind
    variant (e.g. GROUP_MEMBER) and check loan_payment_groups here
    before the path-based resolution. A FIXME whose txn_hash appears
    in any confirmed group's member_hashes is already classified —
    Sites 1/2/3 must skip it. Today the gap is masked by accidental
    safety: individual member amounts are smaller than the loan's
    expected monthly so auto_classify lands at tier='far' and won't
    write. That's not designed safety; close it when confirmed-group
    data starts existing.
    """
    if loans is None:
        loans = _loans_snapshot(conn)
    if not loans:
        return None

    # Gather accounts the transaction touches for fast membership tests.
    accounts_in_txn: set[str] = set()
    for p in txn.postings or []:
        acct = getattr(p, "account", None)
        if acct:
            accounts_in_txn.add(acct)

    # Step 1: account-path match.
    candidates: list[dict] = []
    for loan in loans:
        paths = {
            loan.get("liability_account_path"),
            loan.get("interest_account_path"),
            loan.get("escrow_account_path"),
        } - {None, ""}
        if paths & accounts_in_txn:
            candidates.append(loan)

    if len(candidates) == 1:
        winner = candidates[0]
        return Claim(
            kind=_classify_kind(txn, winner),
            loan_slug=winner["slug"],
        )
    if len(candidates) > 1:
        # Tie-breaker: closest expected-monthly to the txn's amount.
        amount = _txn_amount_hint(txn) or Decimal("0")
        winner = min(
            candidates,
            key=lambda l: abs((_expected_monthly(l) - amount)),
        )
        return Claim(
            kind=_classify_kind(txn, winner),
            loan_slug=winner["slug"],
        )

    # Step 2: SimpleFIN-id match from txn meta.
    meta = getattr(txn, "meta", None) or {}
    sf_id = meta.get("lamella-simplefin-account-id") or meta.get("simplefin-account-id")
    if sf_id:
        for loan in loans:
            if loan.get("simplefin_account_id") == sf_id:
                return Claim(
                    kind=(ClaimKind.REVOLVING_SKIP if _is_revolving(loan)
                          else ClaimKind.PAYMENT),
                    loan_slug=loan["slug"],
                )

    return None


# ----------------------------------------------------- simplefin facts variant


def claim_from_csv_row(
    row: Any, conn: Any, *, loans: list[dict] | None = None,
) -> Claim | None:
    """Pre-bean-write claim check for spreadsheet imports (Site 6).

    The importer's categorize cascade (importer/categorize.py) walks
    raw_rows from imported spreadsheets and routes them through:
    annotation → payee_rules → classification_rules → AI. Loan
    payments slip through that cascade today and the AI tries to
    single-classify them — wrong, because the AI doesn't know about
    amortization or escrow splits. This function detects loan-related
    rows BEFORE the AI step so they can be marked needs_review with a
    clear "use the backfill flow" message instead.

    Two detection signals (cheap → expensive, first match wins):

    1. Exact: ``row['payment_method']`` equals any loan's
       ``simplefin_account_id``. Rare for spreadsheets (which usually
       carry institution names, not opaque ids), but covers the case
       of a SimpleFIN export re-imported via the spreadsheet path.
    2. Substring: any loan's ``institution`` (lower-cased, trimmed,
       length >= 3) appears in the row's lowered payment_method,
       payee, or description. Length floor on the institution prevents
       hits like "WF" matching "WFM" or "BB" matching "BBQ".

    On match: returns a ``Claim(PAYMENT, loan_slug)``. The caller
    (categorize_import) skips the AI step and marks the row
    needs_review with ``reason`` pointing the user at the
    /settings/loans/{slug}/backfill flow for proper amortization-
    driven splits.

    ``row`` is duck-typed: any object exposing ``payment_method``,
    ``payee``, ``description`` via dict-like or attribute access.
    """
    if loans is None:
        loans = _loans_snapshot(conn)
    if not loans:
        return None

    def _g(key: str) -> str:
        if hasattr(row, "keys"):
            try:
                return str(row[key] or "")
            except (KeyError, IndexError, TypeError):
                return ""
        return str(getattr(row, key, "") or "")

    payment_method = _g("payment_method").strip()
    payee = _g("payee").strip()
    description = _g("description").strip()

    # Signal 1: simplefin_account_id exact match against payment_method.
    if payment_method:
        for loan in loans:
            sf_id = (loan.get("simplefin_account_id") or "").strip()
            if sf_id and sf_id == payment_method:
                return Claim(
                    kind=(ClaimKind.REVOLVING_SKIP if _is_revolving(loan)
                          else ClaimKind.PAYMENT),
                    loan_slug=loan["slug"],
                )

    # Signal 2: institution substring in any text field.
    haystack = " ".join((payment_method, payee, description)).lower()
    if not haystack.strip():
        return None

    for loan in loans:
        inst = (loan.get("institution") or "").strip().lower()
        # Length floor prevents two-letter institutions matching common
        # words ("WF" → "WFM" / "Costco WFM"). The user can always add
        # an explicit payee_rule for short-named institutions.
        if len(inst) < 3:
            continue
        if inst in haystack:
            return Claim(
                kind=(ClaimKind.REVOLVING_SKIP if _is_revolving(loan)
                      else ClaimKind.PAYMENT),
                loan_slug=loan["slug"],
            )

    return None


def claim_from_simplefin_facts(
    sf_txn: Any, source_account: str, conn: Any,
) -> Claim | None:
    """Pre-bean-write claim check for simplefin/ingest.

    At classify time in the ingest flow, there's no bean Transaction
    yet — just a SimpleFINTransaction + a source_account derived
    from the SimpleFIN account id. Match on:

    1. source_account equals a loan's liability_account_path.
    2. The SimpleFIN account id equals a loan's simplefin_account_id
       (usually an alternate mapping when the user hasn't scaffolded
       a liability account with the exact name SimpleFIN uses).
    """
    if not source_account and not getattr(sf_txn, "account_id", None):
        return None
    loans = _loans_snapshot(conn)
    if not loans:
        return None

    for loan in loans:
        if source_account and loan.get("liability_account_path") == source_account:
            return Claim(
                kind=(ClaimKind.REVOLVING_SKIP if _is_revolving(loan)
                      else ClaimKind.PAYMENT),
                loan_slug=loan["slug"],
            )

    sf_id = getattr(sf_txn, "account_id", None)
    if sf_id:
        for loan in loans:
            if loan.get("simplefin_account_id") == sf_id:
                return Claim(
                    kind=(ClaimKind.REVOLVING_SKIP if _is_revolving(loan)
                          else ClaimKind.PAYMENT),
                    loan_slug=loan["slug"],
                )

    return None
