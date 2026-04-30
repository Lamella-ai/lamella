# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Cascading transaction → Paperless-document candidate finder.

Given a ledger transaction (date, amount, narration/payee), return Paperless
documents that plausibly correspond, scored by how reliable the match is.
Runs entirely against the local paperless_doc_index so it's fast and
deterministic — the matcher doesn't touch the Paperless API.

The cascade exists because Paperless metadata is noisy:
  * `created` dates are sometimes wrong (OCR year = "2064", etc.)
  * Monetary custom fields are inconsistent across docs
  * Correspondent / title are usually trustworthy but only probabilistic
  * Dollar amounts often appear literally in OCR content, even when no
    custom field is populated

So we run each signal as its own query and combine the results.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScoredCandidate:
    paperless_id: int
    title: str | None
    correspondent_name: str | None
    created_date: date | None
    receipt_date: date | None
    total_amount: Decimal | None
    score: float                       # 0..1
    reasons: tuple[str, ...]           # e.g. ("amount exact", "corr matches", "3d off")
    # denormalized for rendering
    document_type_name: str | None = None
    subtotal_amount: Decimal | None = None
    tax_amount: Decimal | None = None
    vendor: str | None = None
    payment_last_four: str | None = None
    content_excerpt: str | None = None
    tags_json: str | None = None

    @property
    def effective_date(self) -> date | None:
        return self.receipt_date or self.created_date


# Tokens that carry no signal for merchant matching. Aggressively trimmed
# because Paperless titles and OCR content use a lot of common words (form,
# receipt, payment, company-suffix noise) that will otherwise false-match.
_STOPWORDS = frozenset(
    {
        "the", "inc", "llc", "co", "corp", "ltd", "company", "store",
        "payment", "payments", "purchase", "invoice", "receipt", "a", "an",
        "and", "of", "for", "from", "to", "usa", "us", "com", "net", "www",
        "ach", "ref", "ebill", "ebilling", "online", "transfer", "card",
        "acct", "auth", "authorized", "fee", "form", "subscription", "pay",
        "new", "west", "east", "north", "south", "ave", "blvd", "rd", "st",
        "amount", "paid", "thank", "you", "pmts", "pmt", "wf", "sa", "ca",
        "ny", "tx", "il", "co", "az", "nv", "wa", "or", "fl", "ga",
    }
)

# Require ≥4 characters so short noise ("for", "inc", three-letter state
# abbreviations already in stopwords) doesn't slip through.
_TOKEN_RE = re.compile(r"[A-Za-z0-9]{4,}")


def _merchant_tokens(text: str | None) -> set[str]:
    if not text:
        return set()
    words = _TOKEN_RE.findall(text.lower())
    return {w for w in words if w not in _STOPWORDS}


def _parse_decimal_col(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _parse_date_col(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


@dataclass
class _StageHit:
    row: dict[str, Any]
    score_base: float
    reasons: list[str]


# Document types that look like receipts on amount + date but
# AREN'T receipts. A bank statement contains exact-amount line items
# from many receipts; auto-linking it as the receipt for one of those
# txns is a false positive. Same for tax statements (1099, W-2),
# investment statements, and any other periodic summary. The
# user-uploaded `document_type` in Paperless is the most semantic
# signal we have for this. Names are lower-cased for case-insensitive
# substring match against `paperless_doc_index.document_type_name`.
RECEIPT_EXCLUDED_DOCTYPE_PATTERNS = (
    "statement",        # bank / cc / brokerage / loan statements
    "1099",             # tax forms
    "w-2", "w2",
    "tax",              # generic tax-form catch-all
    "consolidated",     # "Consolidated Form 1099" / similar wrappers
    "summary",          # "Year-end summary"
    "letter",           # bank letters / disclosures
    "disclosure",
    "policy",           # insurance policy doc, not a receipt
    "contract",
)


def _doctype_excluded(document_type_name: str | None) -> bool:
    """Return True when this document's user-set Paperless document
    type marks it as a non-receipt summary doc (statement / tax form /
    etc.). Substring match so 'Bank Statement' / 'Brokerage Statement' /
    '2025 Form 1099 Consolidated' all trip the gate.
    """
    if not document_type_name:
        return False
    name = document_type_name.lower()
    return any(p in name for p in RECEIPT_EXCLUDED_DOCTYPE_PATTERNS)


def find_paperless_candidates(
    conn: sqlite3.Connection,
    *,
    txn_amount: Decimal,
    txn_date: date,
    narration: str | None = None,
    payee: str | None = None,
    last_four: str | None = None,
    tight_window_days: int = 3,
    wide_window_days: int = 30,
    fuzzy_cents: int = 50,
    limit: int = 3,
    min_score: float = 0.60,
    exclude_already_linked: bool = True,
) -> list[ScoredCandidate]:
    """Return up to `limit` scored candidate Paperless docs for this
    transaction. Caller presents them in the UI with reasons; click-to-link
    writes the receipt_links row via the existing linker.

    Docs whose Paperless ``document_type`` matches a non-receipt
    pattern (bank statement, 1099, W-2, etc. — see
    ``RECEIPT_EXCLUDED_DOCTYPE_PATTERNS``) are excluded entirely.
    A bank statement reliably contains exact-amount + merchant +
    date matches for every receipt summarized within it; treating
    one as the receipt for a single txn is a false positive.
    """
    if txn_amount is None or txn_date is None:
        return []
    amt = abs(Decimal(txn_amount))
    # ADR-0022: post-migration 057 the money columns (total_amount,
    # subtotal_amount) are TEXT. Compare numerically by pulling rows
    # within a date window (or by amount-IS-NOT-NULL for any-date)
    # and using Decimal equality in Python — that side-steps both
    # CAST-to-REAL precision loss AND Paperless's "358" vs "358.00"
    # formatting variance.
    tight_lo = txn_date - timedelta(days=tight_window_days)
    tight_hi = txn_date + timedelta(days=tight_window_days)
    wide_lo = txn_date - timedelta(days=wide_window_days)
    wide_hi = txn_date + timedelta(days=wide_window_days)
    cents = Decimal(fuzzy_cents) / Decimal(100)

    merchant_signal = _merchant_tokens(narration) | _merchant_tokens(payee)

    # Gather raw stage hits keyed by paperless_id. Later stages add reasons /
    # bump score_base instead of re-inserting.
    hits: dict[int, _StageHit] = {}

    def _add_hit(row: Any, stage_base: float, stage_reason: str) -> None:
        pid = int(row["paperless_id"])
        hit = hits.get(pid)
        if hit is None:
            hits[pid] = _StageHit(dict(row), stage_base, [stage_reason])
        else:
            # Already matched via an earlier stage; the stronger stage wins
            # on score_base, and we accumulate reasons.
            if stage_base > hit.score_base:
                hit.score_base = stage_base
            if stage_reason not in hit.reasons:
                hit.reasons.append(stage_reason)

    link_filter = (
        " AND paperless_id NOT IN (SELECT paperless_id FROM receipt_links)"
        if exclude_already_linked else ""
    )

    def _date_pred(date_lo: date, date_hi: date) -> tuple[str, list[str]]:
        # Receipt date in window OR (receipt date NULL AND created date in
        # window). Returns SQL fragment + bind params.
        sql = (
            "(receipt_date BETWEEN ? AND ? "
            "OR (receipt_date IS NULL AND created_date BETWEEN ? AND ?))"
        )
        binds = [
            date_lo.isoformat(), date_hi.isoformat(),
            date_lo.isoformat(), date_hi.isoformat(),
        ]
        return sql, binds

    def _decimal_eq(text_val: Any, target: Decimal) -> bool:
        d = _parse_decimal_col(text_val)
        return d is not None and d == target

    # Stage 1: amount exact + date tight. Strongest signal.
    date_sql, date_binds = _date_pred(tight_lo, tight_hi)
    rows = conn.execute(
        "SELECT * FROM paperless_doc_index "
        "WHERE total_amount IS NOT NULL AND " + date_sql + link_filter
        + " LIMIT 500",
        tuple(date_binds),
    ).fetchall()
    for row in rows:
        if _decimal_eq(row["total_amount"], amt):
            _add_hit(row, stage_base=0.90, stage_reason="amount + date")

    # Stage 2: amount exact + wide date window. Date may be OCR-wrong.
    date_sql, date_binds = _date_pred(wide_lo, wide_hi)
    rows = conn.execute(
        "SELECT * FROM paperless_doc_index "
        "WHERE total_amount IS NOT NULL AND " + date_sql + link_filter
        + " LIMIT 500",
        tuple(date_binds),
    ).fetchall()
    for row in rows:
        if _decimal_eq(row["total_amount"], amt):
            _add_hit(row, stage_base=0.70, stage_reason="amount, wide date")

    # Stage 3: subtotal exact + tight date. Picks up receipts where only
    # subtotal is populated.
    date_sql, date_binds = _date_pred(tight_lo, tight_hi)
    rows = conn.execute(
        "SELECT * FROM paperless_doc_index "
        "WHERE subtotal_amount IS NOT NULL AND " + date_sql + link_filter
        + " LIMIT 500",
        tuple(date_binds),
    ).fetchall()
    for row in rows:
        if _decimal_eq(row["subtotal_amount"], amt):
            _add_hit(row, stage_base=0.55, stage_reason="subtotal + date")

    # Stage 4: amount exact with any date. Weak on its own, strong with
    # merchant corroboration later.
    rows = conn.execute(
        "SELECT * FROM paperless_doc_index "
        "WHERE total_amount IS NOT NULL" + link_filter
        + " LIMIT 500",
        (),
    ).fetchall()
    for row in rows:
        if _decimal_eq(row["total_amount"], amt):
            _add_hit(row, stage_base=0.45, stage_reason="amount, any date")

    # Stage 5: amount within ±fuzzy_cents + tight date (tax rounding / tip).
    # ADR-0022: total_amount is TEXT, so filter by date in SQL and do the
    # Decimal delta-compare in Python.
    rows = conn.execute(
        "SELECT * FROM paperless_doc_index "
        "WHERE total_amount IS NOT NULL "
        "AND receipt_date BETWEEN ? AND ?"
        + link_filter
        + " LIMIT 500",
        (tight_lo.isoformat(), tight_hi.isoformat()),
    ).fetchall()
    for row in rows:
        t = _parse_decimal_col(row["total_amount"])
        if t is None:
            continue
        if abs(t - amt) <= cents and t != amt:
            pid = int(row["paperless_id"])
            hit = hits.get(pid)
            reason = f"amount ±${cents:.2f}"
            if hit is None:
                hits[pid] = _StageHit(dict(row), 0.40, [reason])
            elif reason not in hit.reasons:
                hit.reasons.append(reason)

    # Stage 6: correspondent EXACT match. Unlike previous iterations, this
    # stage never adds new candidates from a title-only merchant substring
    # match (that produced noise — an Amazon plan fee "matching" an Amazon
    # 1099 form). Instead we accept a candidate on correspondent equality
    # with a wide date window, because a correspondent match in Paperless
    # is a reliable signal where free-text title matching isn't.
    if merchant_signal:
        # Use SQLite's INSTR to find correspondent matches; each token is a
        # case-folded contains-check. Only fires when correspondent_name is
        # populated (i.e., Paperless attributes this doc to a vendor).
        clauses: list[str] = []
        params: list[Any] = []
        for token in list(merchant_signal)[:4]:
            clauses.append("INSTR(LOWER(correspondent_name), ?) > 0")
            params.append(token)
        if clauses:
            merchant_sql = " OR ".join(clauses)
            date_sql, date_binds = _date_pred(wide_lo, wide_hi)
            rows = conn.execute(
                "SELECT * FROM paperless_doc_index "
                f"WHERE correspondent_name IS NOT NULL AND ({merchant_sql}) "
                f"AND {date_sql}"
                + link_filter
                + " LIMIT 500",
                tuple(params + date_binds),
            ).fetchall()
            for row in rows:
                _add_hit(row, stage_base=0.55, stage_reason="correspondent + wide date")

    # Stage 7: last-four hint (if provided). Boost any hit whose content or
    # payment_last_four contains the last four.
    if last_four:
        for pid, hit in hits.items():
            row = hit.row
            hit_last_four = (row.get("payment_last_four") or "").strip()
            content = (row.get("content_excerpt") or "").lower()
            if hit_last_four == last_four or (last_four and last_four in content):
                hit.score_base = min(1.0, hit.score_base + 0.10)
                hit.reasons.append("last-four match")

    # Drop hits whose document_type marks them as non-receipt
    # summaries (bank statements, 1099s, etc.). A bank statement
    # contains exact-amount + merchant + date matches for many real
    # receipts; auto-linking one as the receipt for a single txn is
    # a false positive the user has to clean up by hand.
    excluded_pids = [
        pid for pid, hit in hits.items()
        if _doctype_excluded(hit.row.get("document_type_name"))
    ]
    for pid in excluded_pids:
        log.debug(
            "find_paperless_candidates: excluded paperless_id=%d "
            "(document_type=%r — non-receipt summary)",
            pid, hits[pid].row.get("document_type_name"),
        )
        del hits[pid]

    # Final scoring: boost for amount appearing in content, merchant tokens
    # appearing in title or content, date proximity.
    scored: list[ScoredCandidate] = []
    for pid, hit in hits.items():
        row = hit.row
        base = hit.score_base
        reasons = list(hit.reasons)

        amt_in_content = str(amt) in (row.get("content_excerpt") or "")
        if amt_in_content:
            base = min(1.0, base + 0.08)
            reasons.append("amount in content")

        corr_name = (row.get("correspondent_name") or "").lower()
        title = (row.get("title") or "").lower()
        content = (row.get("content_excerpt") or "").lower()
        merchant_hits = sum(
            1
            for t in merchant_signal
            if t in corr_name or t in title or t in content
        )
        if merchant_hits >= 2:
            base = min(1.0, base + 0.10)
            reasons.append("merchant tokens")
        elif merchant_hits == 1:
            base = min(1.0, base + 0.05)
            reasons.append("merchant token")

        # Date delta
        rd = _parse_date_col(row.get("receipt_date")) or _parse_date_col(row.get("created_date"))
        if rd is not None:
            delta = abs((rd - txn_date).days)
            if delta == 0:
                reasons.append("same day")
            elif delta <= 3:
                reasons.append(f"{delta}d off")
            elif delta <= 30:
                reasons.append(f"{delta}d off")
                base = max(0.0, base - 0.03)
            else:
                reasons.append(f"{delta}d off")
                base = max(0.0, base - 0.10)

        excerpt = row.get("content_excerpt")
        if isinstance(excerpt, str) and len(excerpt) > 280:
            excerpt = excerpt[:280].rstrip() + "…"
        scored.append(
            ScoredCandidate(
                paperless_id=pid,
                title=row.get("title"),
                correspondent_name=row.get("correspondent_name"),
                created_date=_parse_date_col(row.get("created_date")),
                receipt_date=_parse_date_col(row.get("receipt_date")),
                total_amount=_parse_decimal_col(row.get("total_amount")),
                score=round(base, 3),
                reasons=tuple(dict.fromkeys(reasons)),
                document_type_name=row.get("document_type_name"),
                subtotal_amount=_parse_decimal_col(row.get("subtotal_amount")),
                tax_amount=_parse_decimal_col(row.get("tax_amount")),
                vendor=row.get("vendor"),
                payment_last_four=row.get("payment_last_four"),
                content_excerpt=excerpt,
                tags_json=row.get("tags_json"),
            )
        )

    scored.sort(key=lambda c: c.score, reverse=True)
    # Drop weak matches so the UI doesn't drown in 0.52 "one shared token"
    # candidates. Keep in mind callers can lower min_score to 0.0 if they
    # want to debug.
    filtered = [c for c in scored if c.score >= min_score]
    return filtered[:limit]
