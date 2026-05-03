# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Cross-source intake-time deduplication — ADR-0058.

Single oracle every staging path consults *before* inserting a new
row. Returns ``None`` if the incoming `(date, signed amount,
description)` triple looks like a brand-new event, or a
:class:`DedupHit` describing the existing source record that already
observes this leg of this event.

**Source-agnostic by construction.** The oracle never names
specific sources. Every staged row carries a ``source`` tag (the
allowlist in ``service.SOURCES`` is currently
``simplefin / csv / ods / xlsx / paste / reboot``, but new sources
land via the same allowlist — there's no privileged path). All the
matching logic operates on ``(date, signed amount, description)``,
so any source can dedup against any other source, and a SaaS
deployment that never enables a bank bridge at all uses the same
oracle to dedup CSV vs paste vs reboot.

**Multi-source semantics.** One real-world transaction can be
observed by N source records — that's the point of the paired
``lamella-source-N`` / ``lamella-source-reference-id-N`` keys
(ADR-0019). Concrete example: a transfer from Checking to PayPal
gets observed by an original ledger entry (both legs), then by a
bank-feed source (Checking leg), then by a payment-processor CSV
import (PayPal leg). Four source records, two real-world legs, one
transaction. The oracle's job is to recognize when an incoming row
is a same-leg re-observation (so its lamella-txn-id can inherit
the existing event's lineage) — NOT to flag transfer counterparts
as duplicates (those are the matcher's job; opposite-sign legs of
the same event are deliberately separate staged rows because they
will collapse on promotion).

**Sign-aware matching is load-bearing.** A ``+50`` import row is a
duplicate of an existing ``+50`` row, but it is NOT a duplicate of
an existing ``-50`` row — that's a transfer counterpart, a
different relationship. The oracle compares signed amounts (after
quantizing to cents) so the two cases stay separated.

**Multi-leg ledger walk.** A ledger transfer entry has multiple
postings (e.g. ``Checking -50`` and ``PayPal +50``). When checking
an incoming row against the ledger, the oracle walks EVERY posting
on every entry in the window, not just the first concrete one — so
an import row representing the credit side correctly matches the
credit-side leg of an existing 2-leg ledger transfer.

Algorithm:
    1. Walk ``staged_transactions`` within ±N days. A candidate
       matches when (signed amount, normalized description) match
       AND the candidate is not in a terminal state. First match
       wins. Skip ``dismissed`` / ``failed``.
    2. If no staged hit and a ``LedgerReader`` was supplied, walk
       every ``Transaction`` in the window. For each, walk every
       posting. A posting matches when (signed amount, normalized
       description) match.
    3. Return :class:`DedupHit` carrying the matched record's
       lamella-txn-id (so the new staged row can inherit it,
       keeping multi-source lineage on a single event). ``None``
       when nothing matches — the row is a brand-new observation.

The function is read-only: it never mutates the database or the
ledger. Mutating-on-hit is the staging service's job (lands the row
in ``status='likely_duplicate'`` with the matched id inherited).
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from lamella.features.import_.staging.intake import (
    _normalize_desc,
    content_fingerprint,
)

log = logging.getLogger(__name__)

__all__ = [
    "DedupHit",
    "find_match",
]


@dataclass(frozen=True)
class DedupHit:
    """One collision the oracle found between an incoming row and
    an existing same-leg observation.

    ``kind='staged'`` — collided with a staged_transactions row.
    ``staged_id`` is set; ``txn_hash`` is None.

    ``kind='ledger'`` — collided with a posting on a ledger
    Transaction. ``txn_hash`` is set; ``staged_id`` is None.
    ``filename`` / ``lineno`` may be set if available.

    ``confidence`` is the strength of the match:
      * ``'high'`` — signed amount + normalized description match
        exactly within the date window. Cheap, near-zero false
        positives.
      * ``'medium'`` — signed amount matches and either (a) the
        payee tokens overlap or (b) the description tokens overlap
        beyond a Jaccard threshold. Catches cross-source
        observations of the same event when each source phrases
        the description differently (a bank feed says "Transfer
        from PayPal" while the payment-processor CSV says
        "Transfer to Bank"). Higher false-positive risk than
        ``'high'`` — the UI surfaces these with a "verify
        carefully" treatment.

    ``matched_lamella_txn_id`` is the existing record's immutable
    event identity. The staging service uses it to make the new
    row inherit the same lamella-txn-id, keeping the multi-source
    observation lineage on a single event (per ADR-0019).

    ``matched_account`` is set for ledger hits — it's the account
    of the specific posting that matched.

    ``fingerprint`` is the content fingerprint that matched.

    ``matched_date`` / ``matched_description`` are the existing
    record's values, surfaced verbatim so the UI can render
    "matches: 2026-04-15 — Coffee Shop" without a second lookup.

    ``why`` is a human-readable hint about which signal triggered
    the medium-tier match (e.g. "shared payee 'PayPal'" or
    "description tokens overlap 60%"). None for high-tier matches.
    """
    kind: str  # 'staged' | 'ledger'
    fingerprint: str
    matched_date: str
    matched_description: str | None
    confidence: str = "high"  # 'high' | 'medium'
    matched_lamella_txn_id: str | None = None
    matched_account: str | None = None
    staged_id: int | None = None
    staged_source: str | None = None
    txn_hash: str | None = None
    filename: str | None = None
    lineno: int | None = None
    why: str | None = None


def find_match(
    conn: sqlite3.Connection,
    *,
    posting_date: str,
    amount: Decimal | str | int | float,
    description: str | None,
    payee: str | None = None,
    reader=None,  # LedgerReader | None — late-imported to avoid cycle
    window_days: int = 3,
    exclude_id: int | None = None,
) -> DedupHit | None:
    """Look up the incoming triple against staged history then the
    ledger; return the first hit or ``None``.

    Two-tier match. The high tier requires signed-amount equality
    AND normalized-description equality within the date window —
    cheap, near-zero false positives. When the high tier misses,
    the medium tier requires signed-amount equality AND either
    payee equality OR token-overlap on description above
    ``MEDIUM_OVERLAP_THRESHOLD`` (default Jaccard ≥ 0.5 over tokens
    of length ≥ 3). Medium tier catches the case where each source
    phrases the same event differently (a bank feed says "Transfer
    from PayPal" while a payment-processor CSV says
    "Transfer to Bank").

    Sign-equality is required at every tier — opposite signs at the
    same magnitude are transfer counterparts (the matcher's job),
    not duplicates.

    ``window_days`` defaults to 3 to absorb posted-date drift.
    ``exclude_id`` lets re-stages skip themselves when the upsert
    path re-checks an already-staged row's content.
    """
    incoming_amount = _signed_decimal(amount)
    incoming_desc = _normalize_desc(description)
    incoming_payee = _normalize_desc(payee)

    # 1. Staged side — bounded by the date window so this is cheap
    #    even on a multi-thousand-row staging table.
    staged_hit = _find_staged_match(
        conn,
        target_amount=incoming_amount,
        target_desc=incoming_desc,
        target_payee=incoming_payee,
        posting_date=posting_date,
        window_days=window_days,
        exclude_id=exclude_id,
    )
    if staged_hit is not None:
        return staged_hit

    # 2. Ledger side — only if a reader was supplied. Caller may
    #    intentionally pass None (e.g. a unit test of staged-only
    #    dedup) and that's fine; we degrade gracefully.
    if reader is None:
        return None
    return _find_ledger_match(
        reader=reader,
        target_amount=incoming_amount,
        target_desc=incoming_desc,
        target_payee=incoming_payee,
        posting_date=posting_date,
        window_days=window_days,
    )


# Token-overlap defaults for the medium tier. Tokens ≥ 3 chars to
# avoid stop-word noise. Jaccard ≥ 0.5 means at least half the
# union of tokens is shared — empirically the threshold below
# which false positives dominate (recurring same-amount charges,
# "Payment Thank You" → "Payment").
_MEDIUM_TOKEN_MIN_LEN = 3
_MEDIUM_OVERLAP_THRESHOLD = 0.5

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


def _tokens(text: str) -> set[str]:
    """Normalize and tokenize free text; tokens shorter than the
    threshold are dropped to suppress stop-word noise."""
    if not text:
        return set()
    return {
        t.lower()
        for t in _TOKEN_RE.findall(text)
        if len(t) >= _MEDIUM_TOKEN_MIN_LEN
    }


def _token_jaccard(a: str, b: str) -> float:
    """Jaccard similarity on tokens. 0 when either side is empty."""
    ta = _tokens(a)
    tb = _tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _medium_tier_signal(
    *,
    incoming_desc: str,
    incoming_payee: str,
    candidate_desc: str | None,
    candidate_payee: str | None,
) -> str | None:
    """Decide whether two records describe the same event despite
    their normalized descriptions differing. Returns a human-readable
    "why" string when yes; None when not enough signal.

    Order:
      1. Payee equality on non-empty payees — the strongest medium
         signal. Most bank feeds expose a stable merchant name.
      2. Description token overlap ≥ threshold.
      3. Description ↔ payee token overlap (cross-mismatched: a
         source whose payee is "PayPal" matches a source whose
         description contains "PayPal").
    """
    # 1. Direct payee equality.
    if incoming_payee and candidate_payee:
        cand_payee_norm = _normalize_desc(candidate_payee)
        if incoming_payee == cand_payee_norm:
            return f"shared payee {incoming_payee!r}"
    # 2. Description token overlap.
    cand_desc_norm = _normalize_desc(candidate_desc)
    if incoming_desc and cand_desc_norm:
        sim = _token_jaccard(incoming_desc, cand_desc_norm)
        if sim >= _MEDIUM_OVERLAP_THRESHOLD:
            return f"description tokens overlap {int(sim * 100)}%"
    # 3. Cross-field token overlap — handles "bank-feed payee
    #    matches CSV description" and vice versa.
    for left, right, label in (
        (incoming_payee, cand_desc_norm, "incoming payee in matched description"),
        (incoming_desc, _normalize_desc(candidate_payee or ""),
         "matched payee in incoming description"),
    ):
        if left and right:
            sim = _token_jaccard(left, right)
            if sim >= _MEDIUM_OVERLAP_THRESHOLD:
                return f"{label} ({int(sim * 100)}%)"
    return None


def _signed_decimal(amount) -> Decimal:
    """Coerce the incoming amount to a signed Decimal quantized to
    cents — preserves direction (debit / credit) so duplicates and
    transfer counterparts stay distinguishable."""
    try:
        amt = (
            amount if isinstance(amount, Decimal)
            else Decimal(str(amount))
        )
    except (InvalidOperation, ValueError):
        amt = Decimal(0)
    return amt.quantize(Decimal("0.01"))


def _find_staged_match(
    conn: sqlite3.Connection,
    *,
    target_amount: Decimal,
    target_desc: str,
    target_payee: str,
    posting_date: str,
    window_days: int,
    exclude_id: int | None,
) -> DedupHit | None:
    """Bounded scan of ``staged_transactions`` for a signed-amount
    match within the date window. Tries the high-confidence tier
    (description equality) first; falls back to the medium tier
    (payee equality / token overlap) when the high tier misses but
    the amount + window match. Skips terminal-state rows."""
    rows = conn.execute(
        """
        SELECT id, source, posting_date, amount, description, payee,
               status, lamella_txn_id
          FROM staged_transactions
         WHERE posting_date BETWEEN date(?, ?) AND date(?, ?)
           AND status NOT IN ('dismissed', 'failed')
        """,
        (
            posting_date, f"-{window_days} days",
            posting_date, f"+{window_days} days",
        ),
    ).fetchall()
    medium_hit: DedupHit | None = None
    for r in rows:
        if exclude_id is not None and int(r["id"]) == exclude_id:
            continue
        try:
            amt = Decimal(r["amount"])
        except (InvalidOperation, ValueError):
            continue
        # Sign-aware: a +50 incoming row matches a +50 existing row,
        # but does NOT match a -50 existing row (that's a transfer
        # counterpart, the matcher's job).
        if amt.quantize(Decimal("0.01")) != target_amount:
            continue
        cand_desc_norm = _normalize_desc(r["description"])
        if cand_desc_norm == target_desc and target_desc:
            return DedupHit(
                kind="staged",
                fingerprint=content_fingerprint(
                    posting_date=r["posting_date"],
                    amount=amt,
                    description=r["description"],
                ),
                matched_date=r["posting_date"],
                matched_description=r["description"],
                confidence="high",
                matched_lamella_txn_id=r["lamella_txn_id"],
                staged_id=int(r["id"]),
                staged_source=r["source"],
            )
        # Medium-tier candidate — keep the first one found, but
        # don't return early so a later high-tier match still wins
        # if one exists in the same window.
        if medium_hit is None:
            why = _medium_tier_signal(
                incoming_desc=target_desc,
                incoming_payee=target_payee,
                candidate_desc=r["description"],
                candidate_payee=r["payee"],
            )
            if why is not None:
                medium_hit = DedupHit(
                    kind="staged",
                    fingerprint=content_fingerprint(
                        posting_date=r["posting_date"],
                        amount=amt,
                        description=r["description"],
                    ),
                    matched_date=r["posting_date"],
                    matched_description=r["description"],
                    confidence="medium",
                    matched_lamella_txn_id=r["lamella_txn_id"],
                    staged_id=int(r["id"]),
                    staged_source=r["source"],
                    why=why,
                )
    return medium_hit


def _find_ledger_match(
    *,
    reader,
    target_amount: Decimal,
    target_desc: str,
    target_payee: str,
    posting_date: str,
    window_days: int,
) -> DedupHit | None:
    """Walk ``LedgerReader.load().entries`` for a Transaction with
    at least one posting whose signed amount matches the incoming
    row. Tries the high-confidence tier (narration matches) first;
    falls back to medium (payee / token-overlap match) when high
    misses.

    Multi-leg-aware: every posting on each candidate Transaction is
    a separate match candidate. A credit-side import legitimately
    matches the credit-side leg of a multi-leg transfer entry."""
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash
    try:
        target_date = date.fromisoformat(posting_date[:10])
    except (TypeError, ValueError):
        return None
    lo = target_date - timedelta(days=window_days)
    hi = target_date + timedelta(days=window_days)
    try:
        entries = reader.load().entries
    except Exception as exc:  # noqa: BLE001
        log.warning("dedup_oracle: ledger load failed: %s", exc)
        return None
    medium_hit: DedupHit | None = None
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        entry_date = entry.date
        if not (lo <= entry_date <= hi):
            continue
        narration_norm = _normalize_desc(entry.narration)
        narration_match = (
            target_desc and narration_norm == target_desc
        )
        # First posting whose signed amount matches.
        matched_posting = None
        for p in entry.postings or ():
            if not (p.units and p.units.number is not None):
                continue
            posting_amt = Decimal(str(p.units.number))
            if posting_amt.quantize(Decimal("0.01")) != target_amount:
                continue
            matched_posting = (p, posting_amt)
            break
        if matched_posting is None:
            continue
        p, posting_amt = matched_posting
        meta = getattr(entry, "meta", None) or {}
        entry_meta = meta or {}
        if narration_match:
            return DedupHit(
                kind="ledger",
                fingerprint=content_fingerprint(
                    posting_date=entry.date.isoformat(),
                    amount=posting_amt,
                    description=entry.narration,
                ),
                matched_date=entry.date.isoformat(),
                matched_description=entry.narration,
                confidence="high",
                matched_lamella_txn_id=(
                    str(entry_meta.get("lamella-txn-id"))
                    if entry_meta.get("lamella-txn-id") else None
                ),
                matched_account=p.account,
                txn_hash=txn_hash(entry),
                filename=entry_meta.get("filename"),
                lineno=(
                    int(entry_meta["lineno"])
                    if entry_meta.get("lineno") is not None else None
                ),
            )
        # Medium-tier candidate — keep first, don't return.
        if medium_hit is None:
            why = _medium_tier_signal(
                incoming_desc=target_desc,
                incoming_payee=target_payee,
                candidate_desc=entry.narration,
                candidate_payee=entry.payee,
            )
            if why is not None:
                medium_hit = DedupHit(
                    kind="ledger",
                    fingerprint=content_fingerprint(
                        posting_date=entry.date.isoformat(),
                        amount=posting_amt,
                        description=entry.narration,
                    ),
                    matched_date=entry.date.isoformat(),
                    matched_description=entry.narration,
                    confidence="medium",
                    matched_lamella_txn_id=(
                        str(entry_meta.get("lamella-txn-id"))
                        if entry_meta.get("lamella-txn-id") else None
                    ),
                    matched_account=p.account,
                    txn_hash=txn_hash(entry),
                    filename=entry_meta.get("filename"),
                    lineno=(
                        int(entry_meta["lineno"])
                        if entry_meta.get("lineno") is not None else None
                    ),
                    why=why,
                )
    return medium_hit
