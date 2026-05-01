# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Payout-source detector.

A "payout source" is a marketplace or payment processor that holds
funds on the user's behalf between sales / charges and disburses
them to a checking account on a cadence: eBay, PayPal, Stripe,
Shopify, Square, Etsy, Venmo, Cash App, Amazon Seller, etc. The
SimpleFIN feed sees only the *disbursement* leg — the marketplace's
internal ledger (sales, fees, shipping, refunds, holdbacks) lives
outside the bank's view and has to be imported separately.

Modeling the payout source as ``Assets:{Entity}:{Brand}`` (or
``Assets:{Entity}:{Brand}:Seller`` for brands that legitimately
span inflow + outflow roles like Amazon) lets the disbursement
land as a transfer between two of the user's accounts instead of
being misclassified as income or, worse, as ``Expenses:FIXME``.
The marketplace-internal data can then be imported on its own
schedule and reconcile against the same account.

This module's job is the *suggestion* loop: scan the ledger and
staging surface, identify recurring inflows whose merchant text
matches one of the known patterns, count direction, and emit
``PayoutCandidate`` records the UI surfaces as a one-click
"scaffold this" nudge.

Out of scope:
  * Writing the ``Open`` directive / kind / rule. The caller does
    that via the existing registry + rules writers once the user
    accepts the suggestion.
  * Importing marketplace-internal data (sales / fees / shipping).
    That's a separate connector.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

log = logging.getLogger(__name__)

__all__ = [
    "PayoutPattern",
    "PayoutCandidate",
    "PAYOUT_PATTERNS",
    "match_payout_pattern",
    "suggested_account_path",
    "detect_payout_sources",
    "read_payout_dismissals",
]


# --- pattern catalogue ----------------------------------------------------


@dataclass(frozen=True)
class PayoutPattern:
    """One recognized payout source.

    ``id`` is a stable slug used for grouping and as the rule's
    pattern_value when the user accepts a suggestion. ``display``
    is shown to the user. ``keywords`` are lowercase substrings
    matched against the txn's payee + narration text — any one
    matching is enough. ``leaf`` is the suggested final segment of
    the account path (so ``Assets:{Entity}:{leaf}``); colon-
    containing leaves like ``Amazon:Seller`` produce nested paths
    when the user has the brand on both sides of the books.
    """
    id: str
    display: str
    keywords: tuple[str, ...]
    leaf: str


# Order matters: earlier patterns win when multiple match (rare, but
# Amazon-Seller's specific keywords need to win against any future
# generic Amazon entry).
PAYOUT_PATTERNS: tuple[PayoutPattern, ...] = (
    # Amazon Seller — only the explicit seller-payout text. Generic
    # "amazon" deliberately not in this list because the same brand
    # is also a retail outflow on most users' books; we surface a
    # candidate only when the bank narration shows seller-disbursement
    # markers.
    PayoutPattern(
        id="amazon_seller",
        display="Amazon Seller",
        keywords=(
            "amazon mktpl", "amazon mktpla", "amzn mktp",
            "amazon pmts", "amazon.com services", "amzn pmts",
            "amazon payments",
        ),
        leaf="Amazon:Seller",
    ),
    PayoutPattern(
        id="paypal",
        display="PayPal",
        keywords=("paypal",),
        leaf="PayPal",
    ),
    PayoutPattern(
        id="ebay",
        display="eBay",
        keywords=("ebay",),
        # Beancount account segments MUST start with [A-Z] (per ADR-0045);
        # "eBay" is rejected as a syntax error. The display name keeps
        # the canonical brand casing; only the on-ledger segment is
        # normalized.
        leaf="Ebay",
    ),
    PayoutPattern(
        id="stripe",
        display="Stripe",
        keywords=("stripe",),
        leaf="Stripe",
    ),
    PayoutPattern(
        id="shopify",
        display="Shopify",
        keywords=("shopify",),
        leaf="Shopify",
    ),
    # "Square" is a common English word; require the brand-specific
    # tokens that appear in real bank descriptions.
    PayoutPattern(
        id="square",
        display="Square",
        keywords=("squareup", "square inc", "sq *", "square cash"),
        leaf="Square",
    ),
    PayoutPattern(
        id="etsy",
        display="Etsy",
        keywords=("etsy",),
        leaf="Etsy",
    ),
    PayoutPattern(
        id="venmo",
        display="Venmo",
        keywords=("venmo",),
        leaf="Venmo",
    ),
    PayoutPattern(
        id="cashapp",
        display="Cash App",
        keywords=("cash app", "cashapp", "cash-app"),
        leaf="CashApp",
    ),
)


# Module-level invariant per ADR-0045: every PayoutPattern.leaf is a
# Beancount-legal segment chain. A constant whose first character is
# lowercase ("eBay") would scaffold to "Assets:Entity:eBay" and
# bean-check would reject the write. Catch drift at import time, not
# at the moment a user clicks Scaffold and watches it silently fail.
def _assert_pattern_leaves_are_legal() -> None:
    from lamella.core.registry.service import (
        InvalidAccountSegmentError,
        validate_beancount_account,
    )
    for pat in PAYOUT_PATTERNS:
        # The leaf becomes one or more segments under Assets:<Entity>:
        # so we validate the synthesized full path with a placeholder
        # entity that we know is segment-legal.
        candidate = f"Assets:Placeholder:{pat.leaf}"
        try:
            validate_beancount_account(candidate)
        except InvalidAccountSegmentError as exc:
            raise AssertionError(
                f"PayoutPattern id={pat.id!r} has illegal leaf "
                f"{pat.leaf!r}: {exc.reason}"
            ) from exc


_assert_pattern_leaves_are_legal()


def match_payout_pattern(text: str | None) -> PayoutPattern | None:
    """Return the first matching pattern for ``text``, or None.

    Matches the same way the rule engine does — lowercase substring
    on the combined payee + narration. Only the first match wins;
    the catalogue is ordered so specific patterns (Amazon Seller)
    precede generic ones.
    """
    if not text:
        return None
    hay = text.lower()
    for pat in PAYOUT_PATTERNS:
        if any(kw in hay for kw in pat.keywords):
            return pat
    return None


def suggested_account_path(entity: str, leaf: str) -> str:
    """Build ``Assets:{entity}:{leaf}``. ``leaf`` may contain colons
    (e.g. ``Amazon:Seller``) for the nested-path disambiguation."""
    return f"Assets:{entity}:{leaf}"


# --- detection --------------------------------------------------------------


@dataclass(frozen=True)
class PayoutCandidate:
    """One detected, undismissed payout source the user might want
    to scaffold. Multiple receiving accounts for the same pattern
    produce multiple candidates so a user with the same merchant
    paying both `Personal` and `Acme` checking sees both.

    ``inbound_share`` is the fraction of matching txns where money
    flowed INTO the receiving account (positive amount). A genuine
    payout source clusters near 1.0; a payment processor the user
    BOTH receives from and pays to (rare, but possible) drifts
    lower.
    """
    pattern_id: str
    display_name: str
    suggested_leaf: str
    receiving_account: str
    entity: str
    suggested_path: str
    hits: int
    inbound_count: int
    outbound_count: int
    inbound_share: float
    sample_dates: tuple[str, ...]
    sample_amounts: tuple[Decimal, ...]
    already_scaffolded: bool


@dataclass
class _Bucket:
    """Mutable accumulator used during the scan."""
    pattern: PayoutPattern
    receiving_account: str
    inbound: int = 0
    outbound: int = 0
    sample_dates: list[str] = field(default_factory=list)
    sample_amounts: list[Decimal] = field(default_factory=list)

    def add(self, *, posting_date: str, amount: Decimal) -> None:
        if amount > 0:
            self.inbound += 1
        elif amount < 0:
            self.outbound += 1
        # Keep the 3 most recent (we'll sort + trim at the end).
        self.sample_dates.append(posting_date)
        self.sample_amounts.append(amount)


def _entity_from_path(path: str) -> str | None:
    """Beancount entity-first hierarchy: Assets:{Entity}:Rest. Returns
    None for paths that don't fit (e.g. plain ``Assets:Checking``)."""
    parts = path.split(":")
    if len(parts) < 3:
        return None
    if parts[0] != "Assets":
        return None
    return parts[1]


def _scan_ledger(
    entries: Iterable,
    buckets: dict[tuple[str, str], _Bucket],
) -> None:
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        text = " ".join(filter(None, [entry.payee or "", entry.narration or ""]))
        pat = match_payout_pattern(text)
        if pat is None:
            continue
        for posting in (entry.postings or []):
            account = posting.account or ""
            if not account.startswith("Assets:"):
                continue
            if posting.units is None or posting.units.number is None:
                continue
            amount = Decimal(str(posting.units.number))
            if amount == 0:
                continue
            key = (pat.id, account)
            bucket = buckets.get(key)
            if bucket is None:
                bucket = _Bucket(pattern=pat, receiving_account=account)
                buckets[key] = bucket
            bucket.add(
                posting_date=str(entry.date),
                amount=amount,
            )
            # One bucket-add per transaction even if multiple Asset
            # postings — first wins. Splits onto two checking accounts
            # would otherwise double-count the inflow.
            break


def _scan_staged(
    conn: sqlite3.Connection,
    buckets: dict[tuple[str, str], _Bucket],
) -> None:
    """Scan still-pending staged rows. The FIXME-proposed eBay rows
    the user is staring at on /review are exactly the population
    that should drive a payout-source suggestion."""
    try:
        rows = conn.execute(
            "SELECT id, source, source_ref, posting_date, amount, "
            "       payee, description "
            "  FROM staged_transactions "
            " WHERE status IN ('new', 'classified', 'matched') "
        ).fetchall()
    except sqlite3.OperationalError:
        # Staging tables missing on a legacy DB — skip silently.
        return
    if not rows:
        return

    # Build the SimpleFIN account_id → account_path map once.
    try:
        sf_map_rows = conn.execute(
            "SELECT simplefin_account_id, account_path "
            "  FROM accounts_meta "
            " WHERE simplefin_account_id IS NOT NULL "
            "   AND simplefin_account_id <> '' "
            "   AND account_path IS NOT NULL"
        ).fetchall()
        sf_map = {
            str(r["simplefin_account_id"]): str(r["account_path"])
            for r in sf_map_rows if r["account_path"]
        }
    except sqlite3.OperationalError:
        sf_map = {}

    import json as _json
    for r in rows:
        text = " ".join(filter(None, [r["payee"] or "", r["description"] or ""]))
        pat = match_payout_pattern(text)
        if pat is None:
            continue
        # Resolve the receiving account from source_ref.
        try:
            ref = _json.loads(r["source_ref"]) if isinstance(r["source_ref"], str) else (r["source_ref"] or {})
        except Exception:  # noqa: BLE001 — bad JSON shouldn't crash detection
            ref = {}
        account: str | None = None
        if isinstance(ref, dict):
            if isinstance(ref.get("account_path"), str):
                account = ref["account_path"]
            elif isinstance(ref.get("account_id"), str):
                account = sf_map.get(ref["account_id"])
        if not account or not account.startswith("Assets:"):
            continue
        try:
            amount = Decimal(str(r["amount"]))
        except Exception:  # noqa: BLE001
            continue
        if amount == 0:
            continue
        key = (pat.id, account)
        bucket = buckets.get(key)
        if bucket is None:
            bucket = _Bucket(pattern=pat, receiving_account=account)
            buckets[key] = bucket
        bucket.add(
            posting_date=str(r["posting_date"]),
            amount=amount,
        )


def reclassify_pending_rows_for_pattern(
    conn: sqlite3.Connection,
    *,
    pattern_id: str,
    entity: str,
    target_account: str,
    decided_by: str = "payout-detector",
) -> int:
    """Update the proposed account for every still-pending staged row
    matching ``pattern_id`` and routed to ``entity``'s receiving
    account, so the user sees the new payout target on /review
    instead of the stale ``Expenses:FIXME`` proposal that lived
    there before the suggestion was accepted.

    Does NOT promote rows or write to the ledger — that stays the
    user's call. We only flip the proposal so the existing Accept
    flow works without the user typing the path back in.

    Returns the number of staged_decisions rows touched (UPSERTs
    counted as 1 each). A 0 return is fine: it just means no rows
    in the queue happen to match the new pattern right now.
    """
    pat = next((p for p in PAYOUT_PATTERNS if p.id == pattern_id), None)
    if pat is None:
        return 0

    # Pull all currently-pending rows. Realistic queues are tens to
    # low hundreds of rows; matching keywords in Python is simpler
    # and more correct than a SQL LIKE chain (which would need to
    # OR every keyword and case-fold).
    try:
        rows = conn.execute(
            "SELECT id, source, source_ref, payee, description "
            "  FROM staged_transactions "
            " WHERE status IN ('new', 'classified', 'matched')"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    if not rows:
        return 0

    # SimpleFIN account_id → account_path map (so we can check the
    # row's receiving account belongs to the target entity).
    try:
        sf_map_rows = conn.execute(
            "SELECT simplefin_account_id, account_path "
            "  FROM accounts_meta "
            " WHERE simplefin_account_id IS NOT NULL "
            "   AND simplefin_account_id <> '' "
            "   AND account_path IS NOT NULL"
        ).fetchall()
        sf_map = {
            str(r["simplefin_account_id"]): str(r["account_path"])
            for r in sf_map_rows if r["account_path"]
        }
    except sqlite3.OperationalError:
        sf_map = {}

    import json as _json
    touched = 0
    for r in rows:
        text = (r["payee"] or "") + " " + (r["description"] or "")
        if not match_payout_pattern(text) or match_payout_pattern(text).id != pattern_id:
            continue
        # Resolve the row's receiving account. The pattern is
        # matched, but we only want to retag rows that hit the
        # target entity's account — the same eBay merchant text
        # against a different entity's checking shouldn't get
        # reclassified by a one-entity-scoped scaffold action.
        try:
            ref = _json.loads(r["source_ref"]) if isinstance(r["source_ref"], str) else (r["source_ref"] or {})
        except Exception:  # noqa: BLE001
            ref = {}
        account: str | None = None
        if isinstance(ref, dict):
            if isinstance(ref.get("account_path"), str):
                account = ref["account_path"]
            elif isinstance(ref.get("account_id"), str):
                account = sf_map.get(ref["account_id"])
        if not account:
            continue
        parts = account.split(":")
        if len(parts) < 3 or parts[0] != "Assets" or parts[1] != entity:
            continue

        # UPSERT staged_decisions. INSERT OR REPLACE is fine here:
        # if a prior decision (rule / AI) already exists, the user's
        # explicit scaffold action supersedes it.
        conn.execute(
            "INSERT OR REPLACE INTO staged_decisions "
            "    (staged_id, account, confidence, confidence_score, "
            "     decided_by, rationale, needs_review, decided_at) "
            " VALUES (?, ?, 'high', 0.95, ?, ?, 0, datetime('now'))",
            (
                r["id"], target_account, decided_by,
                f"Auto-routed by payout-source scaffold: {pattern_id} → {target_account}",
            ),
        )
        # Mark the staged row as 'classified' so the review surface
        # shows the proposal prominently. (Status flips back to
        # 'promoted' only when the actual ledger write happens.)
        conn.execute(
            "UPDATE staged_transactions "
            "   SET status = 'classified', updated_at = datetime('now') "
            " WHERE id = ? AND status IN ('new', 'matched')",
            (r["id"],),
        )
        touched += 1
    if touched:
        conn.commit()
    return touched


def read_payout_dismissals(entries: Iterable) -> set[tuple[str, str]]:
    """Read ``custom "payout-source-dismissed"`` directives out of the
    ledger and return the set of dismissed ``(pattern_id, entity)``
    pairs. The detector consults this so a user's "not a payout
    source" click on /review or /card stops the suggestion from
    re-firing on every page load.

    The dismiss writer (``routes/payout_sources.py::dismiss_payout_source``)
    encodes the key as a single positional arg ``"<pattern_id>:<entity>"``
    plus structured ``lamella-pattern-id`` / ``lamella-entity``
    metadata. We accept either form so on-disk evolution is forgiving:
    the structured metadata wins when present.
    """
    from lamella.core.transform.custom_directive import (
        custom_arg, custom_meta, read_custom_directives,
    )

    out: set[tuple[str, str]] = set()
    for entry in read_custom_directives(entries, "payout-source-dismissed"):
        # Prefer the structured metadata; fall back to the
        # positional arg's "pattern_id:entity" string. Do NOT split
        # the positional arg on every colon — pattern_ids are stable
        # and slug-shaped, but entity slugs are user-defined and
        # could in principle contain a colon. Splitting once on the
        # FIRST colon mirrors how the writer joins the two halves.
        pattern_id = custom_meta(entry, "lamella-pattern-id")
        entity = custom_meta(entry, "lamella-entity")
        if not pattern_id or not entity:
            key = custom_arg(entry, 0)
            if isinstance(key, str) and ":" in key:
                pattern_id, entity = key.split(":", 1)
        if isinstance(pattern_id, str) and isinstance(entity, str) and pattern_id and entity:
            out.add((pattern_id, entity))
    return out


def _existing_paths(conn: sqlite3.Connection) -> set[str]:
    """Account paths already opened in the ledger (per accounts_meta).
    Used to mark a candidate as ``already_scaffolded`` so the UI can
    suppress or relabel the suggestion."""
    try:
        rows = conn.execute(
            "SELECT account_path FROM accounts_meta "
            " WHERE account_path IS NOT NULL"
        ).fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(r["account_path"]) for r in rows if r["account_path"]}


def detect_payout_sources(
    conn: sqlite3.Connection,
    entries: Iterable,
    *,
    min_hits: int = 3,
    min_inbound_share: float = 0.8,
) -> list[PayoutCandidate]:
    """Find recurring inflows from known marketplaces / processors
    that aren't yet routed to a payout-source account.

    Args:
        conn: SQLite connection (for staged_transactions + accounts_meta).
        entries: Beancount entry list (for ledger history).
        min_hits: minimum total transactions across (ledger + staging)
            for a (pattern, receiving_account) bucket to qualify.
        min_inbound_share: fraction of matching txns that must be
            INBOUND on the receiving account. A real payout source
            sits near 1.0; a processor the user pays AND receives
            from drifts lower.

    Returns: candidates sorted by total hits descending. Empty list
    when no merchant clears both thresholds.
    """
    # Materialize entries once so we can both scan for direction
    # data AND scan for dismissals without paying the iterator cost
    # twice. (LedgerReader returns a list anyway; this is defensive
    # for callers handing in a generator.)
    entries_list = list(entries) if entries is not None else []

    buckets: dict[tuple[str, str], _Bucket] = {}
    _scan_ledger(entries_list, buckets)
    _scan_staged(conn, buckets)
    if not buckets:
        return []

    dismissed = read_payout_dismissals(entries_list)
    existing = _existing_paths(conn)
    out: list[PayoutCandidate] = []
    for (_, _), bucket in buckets.items():
        total = bucket.inbound + bucket.outbound
        if total < min_hits:
            continue
        share = bucket.inbound / total if total else 0.0
        if share < min_inbound_share:
            continue
        entity = _entity_from_path(bucket.receiving_account)
        if not entity:
            # Receiving account isn't entity-first (rare: a legacy
            # ``Assets:Checking`` shape). Skip — we wouldn't know
            # which ``Assets:{Entity}:{Brand}`` to suggest.
            continue
        # User has explicitly told us this isn't a payout source —
        # respect the dismissal across the (pattern, entity) pair
        # so a different entity's eBay dismissal doesn't suppress
        # a legitimate Acme dismissal candidate.
        if (bucket.pattern.id, entity) in dismissed:
            continue
        suggested = suggested_account_path(entity, bucket.pattern.leaf)
        # Sort + trim samples: most recent 3.
        sample_pairs = sorted(
            zip(bucket.sample_dates, bucket.sample_amounts),
            key=lambda p: p[0],
            reverse=True,
        )[:3]
        out.append(
            PayoutCandidate(
                pattern_id=bucket.pattern.id,
                display_name=bucket.pattern.display,
                suggested_leaf=bucket.pattern.leaf,
                receiving_account=bucket.receiving_account,
                entity=entity,
                suggested_path=suggested,
                hits=total,
                inbound_count=bucket.inbound,
                outbound_count=bucket.outbound,
                inbound_share=share,
                sample_dates=tuple(d for d, _ in sample_pairs),
                sample_amounts=tuple(a for _, a in sample_pairs),
                already_scaffolded=(suggested in existing),
            )
        )
    out.sort(key=lambda c: (-c.hits, c.pattern_id, c.receiving_account))
    return out
