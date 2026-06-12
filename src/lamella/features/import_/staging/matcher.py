# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Unified cross-source transfer matcher — NEXTGEN.md Phase C.

Finds pairs across the unified staging surface. One staged
transaction is a pair candidate for another when:

  * Their amounts have equal absolute values.
  * Their posting dates are within ``window_days`` of each other.
  * Their signs are **opposite** (transfer) or **equal**
    (duplicate from different sources).

This replaces the source-scoped pair detectors (the importer's
``transfers.detect`` which only sees one CSV upload, and the
SimpleFIN ``pair_detector`` which only sees the FIXME file). With
both sources feeding ``staged_transactions``, the matcher sees the
PayPal CSV row and the Bank One SimpleFIN row as peers and can
pair them.

Scoring produces a confidence band (``high | medium | low``) per
proposal. ``apply_pairs`` writes only proposals above the caller's
floor, skipping rows that are already paired.

Non-goals for this module:

  * Collapsing paired rows into a single balanced Beancount
    transaction on write. That's the writer's job in Phase B2 /
    Phase C2 once review UI moves onto staging.

Account-type compatibility rules ARE applied — the canonical
credit-card / loan / line-of-credit / mortgage payment is "cash
leaves an Asset and reduces a Liability balance" and the matcher
treats that pattern as a strong transfer signal. It also relaxes
``require_cross_source`` for that pattern: both legs of a CC
payment commonly arrive on the same SimpleFIN feed, so blanket
excluding same-source pairs would silently miss the dominant
transfer case.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from lamella.features.import_.staging.service import StagedRow, StagingService

log = logging.getLogger(__name__)

__all__ = [
    "PairProposal",
    "find_pairs",
    "apply_pairs",
    "sweep",
]


# --- scoring knobs --------------------------------------------------------

# Amount magnitude match is a prerequisite — scored at 0.5. Every other
# signal stacks on top. Max realistic score: 0.5 + 0.25 + 0.10 + 0.10 = 0.95.
_BASE_SCORE_AMOUNT = 0.5
_BAND_THRESHOLDS = [
    # (confidence, min_score). Order matters: descending.
    ("high",   0.75),
    ("medium", 0.55),
    ("low",    0.35),
]


def _score_date_proximity(days_apart: int) -> float:
    if days_apart <= 0:
        return 0.25
    if days_apart <= 1:
        return 0.20
    if days_apart <= 3:
        return 0.10
    if days_apart <= 7:
        return 0.05
    return 0.0


def _band_for_score(score: float) -> str | None:
    for band, threshold in _BAND_THRESHOLDS:
        if score >= threshold:
            return band
    return None


def _confidence_rank(band: str) -> int:
    return {"low": 0, "medium": 1, "high": 2}.get(band, -1)


_WORD_RE = re.compile(r"[A-Za-z0-9]+")


def _narration_similarity(a: StagedRow, b: StagedRow) -> float:
    """Lightweight token overlap for payee + description. Returns a
    value in [0, 0.10] that's added to the pair score."""
    a_text = " ".join(filter(None, [a.payee, a.description])).lower()
    b_text = " ".join(filter(None, [b.payee, b.description])).lower()
    if not a_text or not b_text:
        return 0.0
    a_tokens = {t for t in _WORD_RE.findall(a_text) if len(t) > 2}
    b_tokens = {t for t in _WORD_RE.findall(b_text) if len(t) > 2}
    if not a_tokens or not b_tokens:
        return 0.0
    # Bidirectional substring signal too — "PAYPAL TRANSFER" ↔ "Paypal"
    # should score even when tokenization trims one side.
    if any(t in b_text for t in a_tokens) or any(t in a_text for t in b_tokens):
        shared = a_tokens & b_tokens
        if shared:
            return 0.10
        return 0.05
    return 0.0


def _account_root_for(row: StagedRow, account_map: dict[str, str]) -> str | None:
    """Resolve the Beancount root (Assets / Liabilities / Expenses /
    Income / Equity) of a staged row's backing account.

    Looks at source-specific keys in ``source_ref`` against the
    caller-supplied ``account_map`` (simplefin_account_id → path and
    explicit account_path → path). Returns None when we can't
    determine the backing account — scoring then contributes 0.
    """
    ref = row.source_ref or {}
    path: str | None = None
    if isinstance(ref, dict):
        if isinstance(ref.get("account_path"), str):
            path = ref["account_path"]
        elif isinstance(ref.get("account_id"), str):
            path = account_map.get(ref["account_id"])
    if not path:
        return None
    root = path.split(":", 1)[0]
    if root in ("Assets", "Liabilities", "Expenses", "Income", "Equity"):
        return root
    return None


# Account-type pair scoring. Transfers cross an account-type
# boundary in well-defined ways — money moves FROM Assets or
# Liabilities TO Assets or Liabilities. Anything that would
# require going through Income or Expenses is almost certainly
# not a transfer (it's earned income / paid expense).
#
# Asset↔Liability is the canonical case (credit-card / loan /
# line-of-credit / mortgage / tax-liability payment). A liability
# account by definition cannot hold cash — it only carries a
# balance — so any "Payment" / "Payment Received" landing on one
# is the receiving leg of a transfer from cash. We weight that
# heavily, with a direction guard below to keep refund-into-
# checking + same-day CC-purchase coincidences from getting the
# boost (those are opposite-sign and same-amount but neither leg
# is a transfer).
#
# Dict key is the sorted pair of roots so (Assets, Liabilities)
# and (Liabilities, Assets) get the same score.
_TRANSFER_ROOT_SCORE = {
    ("Assets", "Liabilities"): +0.15,   # CC / loan / LoC payment: canonical
    ("Assets", "Assets"):      +0.05,   # intra-bank / brokerage sweep
    ("Liabilities", "Liabilities"): 0.0,  # unusual; neutral
    ("Assets", "Equity"): 0.0,            # owner draw / contribution — legitimate
    ("Liabilities", "Equity"): 0.0,
    # Any pairing touching Income or Expenses = not a transfer.
    ("Assets", "Income"):    -0.15,
    ("Assets", "Expenses"):  -0.15,
    ("Income", "Liabilities"):   -0.15,
    ("Expenses", "Liabilities"): -0.15,
    ("Income", "Income"):    -0.25,
    ("Expenses", "Expenses"): -0.25,
    ("Expenses", "Income"):  -0.25,
}


def _is_directional_liability_payment(
    a_root: str | None, b_root: str | None,
    a_amount: Decimal, b_amount: Decimal,
) -> bool:
    """True when the pair is the canonical 'cash leaves an Asset
    and reduces a Liability balance' shape — a real payment on a
    credit card / loan / line of credit / mortgage / tax liability.

    Distinguishes a real liability payment (Asset side negative,
    Liability side positive) from a coincidental refund-into-
    checking + same-day CC-purchase that happens to share an
    amount (also opposite-sign, also Asset↔Liability, but neither
    leg is a transfer). The guard is what lets us crank the
    Asset↔Liability boost up without pairing those false positives.
    """
    if not a_root or not b_root:
        return False
    if {a_root, b_root} != {"Assets", "Liabilities"}:
        return False
    asset_amount = a_amount if a_root == "Assets" else b_amount
    liability_amount = b_amount if a_root == "Assets" else a_amount
    # Asset cash decreased; Liability balance reduced.
    return asset_amount < 0 and liability_amount > 0


def _score_account_type_pair(
    a_root: str | None, b_root: str | None,
    a_amount: Decimal, b_amount: Decimal,
    *, kind: str,
) -> float:
    """Adjust the pair score by the account-root pair. Only applies
    to transfer proposals; duplicates are cross-source by
    construction and the roots usually match their source shape.

    For the Asset↔Liability case the boost is gated on the
    direction guard above so a non-payment coincidence doesn't get
    promoted into the high band.
    """
    if kind != "transfer" or not a_root or not b_root:
        return 0.0
    key = tuple(sorted([a_root, b_root]))
    base = _TRANSFER_ROOT_SCORE.get(key, 0.0)
    if key == ("Assets", "Liabilities") and base > 0:
        if not _is_directional_liability_payment(
            a_root, b_root, a_amount, b_amount,
        ):
            return 0.0
    return base


# --- data types -----------------------------------------------------------


@dataclass(frozen=True)
class PairProposal:
    """One candidate pair the matcher found.

    ``apply_pairs`` turns high-enough-confidence proposals into
    ``staged_pairs`` rows.
    """
    kind: str                # 'transfer' | 'duplicate'
    confidence: str          # 'high' | 'medium' | 'low'
    score: float             # 0.0 … ~0.95
    a_staged_id: int
    b_staged_id: int
    reason: str


# --- core API -------------------------------------------------------------


def find_pairs(
    conn: sqlite3.Connection,
    *,
    window_days: int = 7,
    min_amount: Decimal | str | float = Decimal("1.00"),
    min_confidence: str = "medium",
    require_cross_source: bool = True,
) -> list[PairProposal]:
    """Scan ``staged_transactions`` for candidate pairs.

    Args:
        window_days: date tolerance (|date(a) − date(b)| ≤ window_days).
        min_amount: skip tiny transactions to reduce noise.
        min_confidence: band floor for returned proposals.
        require_cross_source: when True, only propose pairs between
            different ``source`` tags. Cross-source pairs are what the
            existing per-source detectors cannot catch, so they're the
            unique value of this matcher. When False, this function also
            pairs intra-source rows (useful for the reboot re-ingest
            pass in Phase E).

    Returns a list of proposals sorted by score (highest first). Each
    staged row appears in at most one proposal (greedy assignment).
    Already-paired rows in ``staged_pairs`` are skipped.
    """
    min_amount_d = (
        min_amount if isinstance(min_amount, Decimal)
        else Decimal(str(min_amount))
    )
    candidates = _load_candidates(conn, min_amount=min_amount_d)
    if len(candidates) < 2:
        return []

    paired_ids = _already_paired_ids(conn)
    candidates = [c for c in candidates if c.id not in paired_ids]

    account_map = _load_account_map(conn)
    proposals = _pair_within_window(
        candidates,
        window_days=window_days,
        require_cross_source=require_cross_source,
        account_map=account_map,
    )

    # Greedy assignment: sort by score desc, take pairs one at a time,
    # skip any pair whose rows are already used.
    proposals.sort(key=lambda p: p.score, reverse=True)
    used: set[int] = set()
    out: list[PairProposal] = []
    min_rank = _confidence_rank(min_confidence)
    for p in proposals:
        if p.a_staged_id in used or p.b_staged_id in used:
            continue
        if _confidence_rank(p.confidence) < min_rank:
            continue
        used.add(p.a_staged_id)
        used.add(p.b_staged_id)
        out.append(p)
    return out


def sweep(
    conn: sqlite3.Connection,
    *,
    window_days: int = 7,
    min_amount: Decimal | str | float = Decimal("1.00"),
    find_floor: str = "medium",
    apply_floor: str = "high",
    require_cross_source: bool = True,
) -> dict[str, int]:
    """One-call convenience: find candidate pairs and persist the
    confident ones.

    Called at the end of every ingest (SimpleFIN fetch, importer
    categorize) so pairs surface as soon as both sides are on the
    staging surface. ``find_floor`` is the minimum confidence for
    a proposal to appear; ``apply_floor`` is the stricter minimum
    for it to be written. Returns counts (``{'found': N, 'applied': M}``)
    so the caller can log outcomes.
    """
    proposals = find_pairs(
        conn,
        window_days=window_days,
        min_amount=min_amount,
        min_confidence=find_floor,
        require_cross_source=require_cross_source,
    )
    written = apply_pairs(conn, proposals, min_confidence=apply_floor)
    return {"found": len(proposals), "applied": written}


def apply_pairs(
    conn: sqlite3.Connection,
    proposals: Iterable[PairProposal],
    *,
    min_confidence: str = "high",
) -> int:
    """Persist proposals ≥ ``min_confidence`` into ``staged_pairs``.

    Skips rows already participating in a pair (defensive — ``find_pairs``
    already avoids them, but the check is cheap and callers may supply
    stale proposals). Returns the count actually written.
    """
    svc = StagingService(conn)
    written = 0
    min_rank = _confidence_rank(min_confidence)
    for p in proposals:
        if _confidence_rank(p.confidence) < min_rank:
            continue
        # Double-check before writing: another pass may have paired one
        # side of this proposal since find_pairs ran.
        if svc.pairs_for(p.a_staged_id) or svc.pairs_for(p.b_staged_id):
            continue
        svc.record_pair(
            kind=p.kind,
            confidence=p.confidence,
            a_staged_id=p.a_staged_id,
            b_staged_id=p.b_staged_id,
            reason=p.reason,
        )
        written += 1
    return written


# --- helpers --------------------------------------------------------------


def _load_candidates(
    conn: sqlite3.Connection, *, min_amount: Decimal,
) -> list[StagedRow]:
    """Pull all pending (new|classified|matched) staged rows with an
    absolute amount at or above ``min_amount``. Done in one query to
    keep the matcher O(n²) in memory rather than per-candidate
    round-trips."""
    from lamella.features.import_.staging.service import _row_to_staged

    rows = conn.execute(
        "SELECT * FROM staged_transactions "
        "WHERE status IN ('new', 'classified', 'matched') "
        "ORDER BY posting_date ASC, id ASC"
    ).fetchall()
    out: list[StagedRow] = []
    for r in rows:
        try:
            amt_abs = abs(Decimal(r["amount"]))
        except Exception:  # noqa: BLE001
            continue
        if amt_abs < min_amount:
            continue
        out.append(_row_to_staged(r))
    return out


def _already_paired_ids(conn: sqlite3.Connection) -> set[int]:
    ids: set[int] = set()
    for row in conn.execute(
        "SELECT a_staged_id, b_staged_id FROM staged_pairs"
    ).fetchall():
        ids.add(int(row["a_staged_id"]))
        if row["b_staged_id"] is not None:
            ids.add(int(row["b_staged_id"]))
    return ids


def _load_account_map(conn: sqlite3.Connection) -> dict[str, str]:
    """Pull the simplefin_account_id → account_path map from
    accounts_meta. Used to derive the Beancount root of the staged
    row's backing account for account-type-aware scoring."""
    try:
        rows = conn.execute(
            "SELECT simplefin_account_id, account_path "
            "FROM accounts_meta "
            "WHERE simplefin_account_id IS NOT NULL "
            "  AND simplefin_account_id <> '' "
            "  AND account_path IS NOT NULL"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {
        str(r["simplefin_account_id"]): r["account_path"]
        for r in rows
        if r["account_path"]
    }


def _pair_within_window(
    candidates: list[StagedRow],
    *,
    window_days: int,
    require_cross_source: bool,
    account_map: dict[str, str] | None = None,
) -> list[PairProposal]:
    """Compare each candidate to every other candidate within the date
    window. O(n²) on the candidate set — fine for realistic pending
    queue sizes (tens of rows per day, hundreds per week)."""
    from datetime import date

    def _parse_date(s: str) -> date | None:
        try:
            return date.fromisoformat(s)
        except ValueError:
            return None

    # Pre-compute parsed dates / abs amounts once.
    parsed: list[tuple[StagedRow, date, Decimal]] = []
    for row in candidates:
        d = _parse_date(row.posting_date)
        if d is None:
            continue
        try:
            parsed.append((row, d, abs(Decimal(row.amount))))
        except Exception:  # noqa: BLE001
            continue

    proposals: list[PairProposal] = []
    for i in range(len(parsed)):
        a, a_date, a_abs = parsed[i]
        a_amount = Decimal(a.amount)
        for j in range(i + 1, len(parsed)):
            b, b_date, b_abs = parsed[j]
            if a_abs != b_abs:
                continue
            days = abs((a_date - b_date).days)
            if days > window_days:
                continue
            b_amount = Decimal(b.amount)
            same_sign = (a_amount.is_signed() == b_amount.is_signed())
            kind = "duplicate" if same_sign else "transfer"

            a_root = _account_root_for(a, account_map or {})
            b_root = _account_root_for(b, account_map or {})

            # Carve-out around require_cross_source: an Asset↔Liability
            # pair whose direction is "cash out of Asset reduces
            # Liability balance" is the canonical CC / loan / LoC /
            # mortgage / tax payment. Both legs frequently land on the
            # same SimpleFIN feed, so the cross-source default would
            # silently exclude the dominant transfer pattern. Allow it
            # through when the roots + direction match; fall through
            # to the original same-source skip otherwise.
            if require_cross_source and a.source == b.source:
                if kind != "transfer" or not _is_directional_liability_payment(
                    a_root, b_root, a_amount, b_amount,
                ):
                    continue

            score = _BASE_SCORE_AMOUNT
            score += _score_date_proximity(days)
            if a.source != b.source:
                score += 0.10
            score += _narration_similarity(a, b)

            root_delta = _score_account_type_pair(
                a_root, b_root, a_amount, b_amount, kind=kind,
            )
            score += root_delta

            band = _band_for_score(score)
            if band is None:
                continue

            root_suffix = ""
            if a_root and b_root:
                root_suffix = f", {a_root}↔{b_root}"
            reason = (
                f"{kind}: amount {a_abs} match, "
                f"{days}d apart, {a.source}↔{b.source}{root_suffix}"
            )
            proposals.append(
                PairProposal(
                    kind=kind,
                    confidence=band,
                    score=round(score, 3),
                    a_staged_id=a.id,
                    b_staged_id=b.id,
                    reason=reason,
                )
            )
    return proposals
