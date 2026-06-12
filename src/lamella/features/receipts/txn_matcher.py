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

import json
import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from beancount.core.data import Transaction

from lamella.features.receipts.scorer import (
    AUTO_LINK_THRESHOLD,
    REVIEW_THRESHOLD,
    ScoredLedgerCandidate,
    Scorer,
    ScoringSettings,
    merchant_tokens as _scorer_merchant_tokens,
)

log = logging.getLogger(__name__)


# ADR-0063: cascade stage bases + per-adjustment bumps live on
# ``ScoringSettings`` so the forward-direction SQL cascade and the
# direction-free :class:`Scorer` (used by the reverse direction)
# share a single source of truth. Tuning either threshold updates
# both directions atomically. The names are aliased to module-level
# locals to keep the existing SQL stage code readable.
_DEFAULT_SETTINGS = ScoringSettings()
_BASE_TIGHT = _DEFAULT_SETTINGS.base_amount_tight_date
_BASE_WIDE = _DEFAULT_SETTINGS.base_amount_wide_date
_BASE_SUBTOTAL_TIGHT = _DEFAULT_SETTINGS.base_subtotal_tight_date
_BASE_ANY_DATE = _DEFAULT_SETTINGS.base_amount_any_date
_BASE_FUZZY = _DEFAULT_SETTINGS.base_amount_fuzzy_tight_date
_BASE_CORR_WIDE = _DEFAULT_SETTINGS.base_correspondent_wide_date
_BUMP_LAST_FOUR = _DEFAULT_SETTINGS.bump_last_four_match
_BUMP_AMT_IN_CONTENT = _DEFAULT_SETTINGS.bump_amount_in_content
_BUMP_TOKENS_2PLUS = _DEFAULT_SETTINGS.bump_merchant_tokens_two_plus
_BUMP_TOKEN_1 = _DEFAULT_SETTINGS.bump_merchant_token_one
_PEN_DATE_30D = _DEFAULT_SETTINGS.penalty_date_30d
_PEN_DATE_OVER_30D = _DEFAULT_SETTINGS.penalty_date_over_30d


@dataclass(frozen=True)
class ScoredCandidate:
    paperless_id: int
    title: str | None
    correspondent_name: str | None
    created_date: date | None
    document_date: date | None
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
        return self.document_date or self.created_date


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
    """Backward-compat shim — delegates to ``scorer.merchant_tokens``
    so the forward and reverse scoring paths share token rules
    (ADR-0063 §2). Kept under the underscore-prefixed name because
    callers in this module already import it locally.
    """
    return _scorer_merchant_tokens(text)


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


# TODO(adr-0061-phase-2): document_type column added by Worker A's
# migration 067. Until that migration runs locally, this check falls
# back to the legacy regex behavior (RECEIPT_EXCLUDED_DOCTYPE_PATTERNS
# below). Once Phase 2 lands and rows are populated, the discriminator
# path takes over for any row with a non-null document_type. Rows whose
# document_type IS NULL still fall through to the legacy regex so the
# matcher behaves correctly during the rolling migration window.

# Document types that look like receipts on amount + date but
# AREN'T receipts. A bank statement contains exact-amount line items
# from many receipts; auto-linking it as the receipt for one of those
# txns is a false positive. Same for tax statements (1099, W-2),
# investment statements, and any other periodic summary. The
# user-uploaded `document_type` in Paperless is the most semantic
# signal we have for this. Names are lower-cased for case-insensitive
# substring match against `paperless_doc_index.document_type_name`.
#
# RETAINED post-ADR-0061 §4 as a fallback when the discriminator
# column is NULL (pre-Phase-2 deploys, freshly-synced rows that
# Paperless hasn't classified yet).
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


# Canonical document_type values that are excluded from the
# auto-link / candidate path (ADR-0061 §4). Bank statements and tax
# forms contain exact-amount line items for many real receipts;
# auto-linking one as the receipt for a single txn is a false
# positive. The match is case-insensitive on the canonical value.
EXCLUDED_DOCUMENT_TYPES: frozenset[str] = frozenset({"statement", "tax"})


def _load_doc_type_roles(conn: sqlite3.Connection) -> dict[int, str]:
    row = conn.execute(
        "SELECT value FROM app_settings WHERE key = 'paperless_doc_type_roles'"
    ).fetchone()
    if not row or not row[0]:
        return {}
    try:
        parsed = json.loads(row[0])
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: dict[int, str] = {}
    for k, v in parsed.items():
        if str(k).strip().isdigit() and str(v) in {"receipt", "invoice", "ignore"}:
            out[int(k)] = str(v)
    return out


def _has_document_type_column(conn: sqlite3.Connection) -> bool:
    """Return True when paperless_doc_index has the Phase-2
    ``document_type`` column. Phase 3 ships before Phase 2 in some
    rollouts so the matcher must work both ways: with column present
    and column absent.
    """
    try:
        rows = conn.execute(
            "PRAGMA table_info(paperless_doc_index)"
        ).fetchall()
    except sqlite3.Error:
        return False
    for row in rows:
        # PRAGMA table_info returns rows of (cid, name, type, notnull,
        # dflt_value, pk). Column name is index 1.
        try:
            name = row[1] if not isinstance(row, sqlite3.Row) else row["name"]
        except (IndexError, KeyError):
            continue
        if name == "document_type":
            return True
    return False


def _doctype_excluded(
    conn: sqlite3.Connection,
    *,
    document_type_id: int | None,
    document_type_name: str | None,
    document_type: str | None = None,
) -> bool:
    """Return True when this document is a non-receipt summary doc
    (statement / tax form / etc.) that should NOT participate in the
    auto-link candidate path.

    Resolution order (ADR-0061 §4):

    1. If the row carries a canonical ``document_type`` value
       (Phase-2 column populated), exclude when it's in
       ``EXCLUDED_DOCUMENT_TYPES`` (``statement`` / ``tax``).
    2. Otherwise consult the user-controlled
       ``paperless_doc_type_roles`` setting indexed by Paperless
       document-type id.
    3. Finally fall back to the legacy
       ``RECEIPT_EXCLUDED_DOCTYPE_PATTERNS`` substring check on the
       free-text ``document_type_name`` so pre-Phase-2 deploys keep
       working without manual classification.
    """
    # Phase-2 discriminator: canonical ``document_type`` wins when
    # the column is populated. NULL falls through to the legacy
    # paths so the matcher stays correct during the rolling
    # migration window.
    if document_type:
        canonical = document_type.strip().lower()
        if canonical in EXCLUDED_DOCUMENT_TYPES:
            return True
        # Any other canonical value (receipt / invoice / order /
        # other) is INCLUDED — the user has explicitly classified
        # it and the role table is the authoritative signal.
        return False
    roles = _load_doc_type_roles(conn)
    if document_type_id is not None and document_type_id in roles:
        return roles[document_type_id] == "ignore"
    if not document_type_name:
        return False
    name = document_type_name.lower()
    return any(p in name for p in RECEIPT_EXCLUDED_DOCTYPE_PATTERNS)


def find_document_candidates(
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
    writes the document_links row via the existing linker.

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
        " AND paperless_id NOT IN (SELECT paperless_id FROM document_links)"
        if exclude_already_linked else ""
    )
    # Always exclude docs tombstoned by the dangling-link purge
    # (migration 066). These have been confirmed deleted from Paperless
    # after 3 consecutive 404s + 7-day cooldown; they must never be
    # proposed as candidates for auto-linking.
    deleted_filter = (
        " AND paperless_id NOT IN (SELECT paperless_id FROM paperless_deleted_docs)"
    )

    def _date_pred(date_lo: date, date_hi: date) -> tuple[str, list[str]]:
        # Document date in window OR (document date NULL AND created date in
        # window). Returns SQL fragment + bind params.
        # ADR-0061 Phase 2: column renamed from receipt_date to document_date.
        sql = (
            "(document_date BETWEEN ? AND ? "
            "OR (document_date IS NULL AND created_date BETWEEN ? AND ?))"
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
        "WHERE total_amount IS NOT NULL AND " + date_sql + link_filter + deleted_filter
        + " LIMIT 500",
        tuple(date_binds),
    ).fetchall()
    for row in rows:
        if _decimal_eq(row["total_amount"], amt):
            _add_hit(row, stage_base=_BASE_TIGHT, stage_reason="amount + date")

    # Stage 2: amount exact + wide date window. Date may be OCR-wrong.
    date_sql, date_binds = _date_pred(wide_lo, wide_hi)
    rows = conn.execute(
        "SELECT * FROM paperless_doc_index "
        "WHERE total_amount IS NOT NULL AND " + date_sql + link_filter + deleted_filter
        + " LIMIT 500",
        tuple(date_binds),
    ).fetchall()
    for row in rows:
        if _decimal_eq(row["total_amount"], amt):
            _add_hit(row, stage_base=_BASE_WIDE, stage_reason="amount, wide date")

    # Stage 3: subtotal exact + tight date. Picks up receipts where only
    # subtotal is populated.
    date_sql, date_binds = _date_pred(tight_lo, tight_hi)
    rows = conn.execute(
        "SELECT * FROM paperless_doc_index "
        "WHERE subtotal_amount IS NOT NULL AND " + date_sql + link_filter + deleted_filter
        + " LIMIT 500",
        tuple(date_binds),
    ).fetchall()
    for row in rows:
        if _decimal_eq(row["subtotal_amount"], amt):
            _add_hit(row, stage_base=_BASE_SUBTOTAL_TIGHT, stage_reason="subtotal + date")

    # Stage 4: amount exact with any date. Weak on its own, strong with
    # merchant corroboration later.
    rows = conn.execute(
        "SELECT * FROM paperless_doc_index "
        "WHERE total_amount IS NOT NULL" + link_filter + deleted_filter
        + " LIMIT 500",
        (),
    ).fetchall()
    for row in rows:
        if _decimal_eq(row["total_amount"], amt):
            _add_hit(row, stage_base=_BASE_ANY_DATE, stage_reason="amount, any date")

    # Stage 5: amount within ±fuzzy_cents + tight date (tax rounding / tip).
    # ADR-0022: total_amount is TEXT, so filter by date in SQL and do the
    # Decimal delta-compare in Python.
    rows = conn.execute(
        "SELECT * FROM paperless_doc_index "
        "WHERE total_amount IS NOT NULL "
        "AND document_date BETWEEN ? AND ?"
        + link_filter + deleted_filter
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
                hits[pid] = _StageHit(dict(row), _BASE_FUZZY, [reason])
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
                + link_filter + deleted_filter
                + " LIMIT 500",
                tuple(params + date_binds),
            ).fetchall()
            for row in rows:
                _add_hit(row, stage_base=_BASE_CORR_WIDE, stage_reason="correspondent + wide date")

    # Stage 7: last-four hint (if provided). Boost any hit whose content or
    # payment_last_four contains the last four.
    if last_four:
        for pid, hit in hits.items():
            row = hit.row
            hit_last_four = (row.get("payment_last_four") or "").strip()
            content = (row.get("content_excerpt") or "").lower()
            if hit_last_four == last_four or (last_four and last_four in content):
                hit.score_base = min(1.0, hit.score_base + _BUMP_LAST_FOUR)
                hit.reasons.append("last-four match")

    # Drop hits whose document_type marks them as non-receipt
    # summaries (bank statements, 1099s, etc.). A bank statement
    # contains exact-amount + merchant + date matches for many real
    # receipts; auto-linking one as the receipt for a single txn is
    # a false positive the user has to clean up by hand.
    #
    # ADR-0061 §4: prefer the canonical ``document_type`` column
    # when populated; rows still on the legacy schema (column
    # absent or value NULL) fall through to the regex-based check
    # in ``_doctype_excluded``.
    excluded_pids = [
        pid for pid, hit in hits.items()
        if _doctype_excluded(
            conn,
            document_type_id=hit.row.get("document_type_id"),
            document_type_name=hit.row.get("document_type_name"),
            document_type=hit.row.get("document_type"),
        )
    ]
    for pid in excluded_pids:
        log.debug(
            "find_document_candidates: excluded paperless_id=%d "
            "(document_type=%r, document_type_name=%r — non-receipt summary)",
            pid,
            hits[pid].row.get("document_type"),
            hits[pid].row.get("document_type_name"),
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
            base = min(1.0, base + _BUMP_AMT_IN_CONTENT)
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
            base = min(1.0, base + _BUMP_TOKENS_2PLUS)
            reasons.append("merchant tokens")
        elif merchant_hits == 1:
            base = min(1.0, base + _BUMP_TOKEN_1)
            reasons.append("merchant token")

        # Date delta. Row carries `document_date` post-ADR-0061 Phase 2;
        # the ScoredCandidate Python field is also `document_date` post-Phase 4.
        rd = _parse_date_col(row.get("document_date")) or _parse_date_col(row.get("created_date"))
        if rd is not None:
            delta = abs((rd - txn_date).days)
            if delta == 0:
                reasons.append("same day")
            elif delta <= 3:
                reasons.append(f"{delta}d off")
            elif delta <= 30:
                reasons.append(f"{delta}d off")
                base = max(0.0, base - _PEN_DATE_30D)
            else:
                reasons.append(f"{delta}d off")
                base = max(0.0, base - _PEN_DATE_OVER_30D)

        excerpt = row.get("content_excerpt")
        if isinstance(excerpt, str) and len(excerpt) > 280:
            excerpt = excerpt[:280].rstrip() + "…"
        scored.append(
            ScoredCandidate(
                paperless_id=pid,
                title=row.get("title"),
                correspondent_name=row.get("correspondent_name"),
                created_date=_parse_date_col(row.get("created_date")),
                document_date=_parse_date_col(row.get("document_date")),
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


# ─── Reverse direction (ADR-0063 §2): doc -> ledger candidates ───────


def _txn_amount_cents(entry: Transaction) -> tuple[int, str] | None:
    """Pick the dominant Expenses/Income/Liabilities/Equity posting
    on the txn and return (abs_amount_cents, currency). Mirrors the
    forward-direction selection in
    ``auto_match._best_expense_amount`` so the two directions agree
    on which posting represents the txn's headline amount.
    """
    target_roots = ("Expenses", "Income", "Liabilities", "Equity")
    best: tuple[Decimal, str] | None = None
    for p in entry.postings or ():
        acct = p.account or ""
        if not acct:
            continue
        root = acct.split(":", 1)[0]
        if root not in target_roots:
            continue
        if p.units and p.units.number is not None:
            amt = abs(Decimal(p.units.number))
            ccy = p.units.currency or "USD"
            if best is None or amt > best[0]:
                best = (amt, ccy)
    if best is None:
        return None
    cents = int((best[0] * Decimal(100)).quantize(Decimal("1")))
    return cents, best[1]


def find_ledger_candidates(
    conn: sqlite3.Connection,
    *,
    doc_date: date,
    doc_total: Decimal | None,
    doc_currency: str | None,
    doc_vendor: str | None,
    doc_doctype: str | None,
    doc_id: int,
    ledger_entries: Any,
    doc_subtotal: Decimal | None = None,
    doc_correspondent: str | None = None,
    doc_content_excerpt: str | None = None,
    doc_last_four: str | None = None,
    max_results: int = 10,
    window_days: int = 30,
    min_score: float = REVIEW_THRESHOLD,
    settings: ScoringSettings | None = None,
) -> list[ScoredLedgerCandidate]:
    """Inverse of :func:`find_document_candidates`: for one document,
    walk transactions in a ``±window_days`` window and score each.

    Returned list is sorted by score descending. Excludes txns whose
    hash already appears in ``document_links`` for this doc, and
    excludes pairs in ``document_link_blocks``. Excludes txns with no
    expense-side posting (transfers / equity-only) — they have no
    receipt to attach.

    ``ledger_entries`` is the list of Beancount entries to scan;
    callers pass ``LedgerReader.load().entries``. Walking the ledger
    is per-call rather than maintained as a separate index so the
    reverse path stays simple — the document feed is typically small
    (<10/day) and the window is bounded.
    """
    if doc_date is None or doc_total is None:
        return []
    # Statement / tax docs are excluded at the source; mirror the
    # SQL-level exclusion the forward direction applies in
    # find_document_candidates.
    if doc_doctype and doc_doctype.strip().lower() in {"statement", "tax"}:
        return []

    scorer = Scorer(settings or _DEFAULT_SETTINGS)
    doc_total_cents = int((abs(Decimal(doc_total)) * Decimal(100)).quantize(Decimal("1")))
    doc_subtotal_cents = (
        int((abs(Decimal(doc_subtotal)) * Decimal(100)).quantize(Decimal("1")))
        if doc_subtotal is not None
        else None
    )

    # Pull blocked txn_hashes for this paperless_id once so we don't
    # propose them as candidates.
    blocked_hashes: set[str] = set()
    try:
        blocked_rows = conn.execute(
            "SELECT txn_hash FROM document_link_blocks WHERE paperless_id = ?",
            (int(doc_id),),
        ).fetchall()
        for r in blocked_rows:
            try:
                blocked_hashes.add(r["txn_hash"])
            except (IndexError, KeyError, TypeError):
                blocked_hashes.add(r[0])
    except sqlite3.Error:
        pass

    # Already-linked txn hashes: a txn that already has a doc attached
    # is not a reverse-direction candidate (we only auto-link unlinked
    # txns). Existing ADR-0008 dedup also covers this on the write path
    # but excluding here saves cycles.
    linked_hashes: set[str] = set()
    try:
        rows = conn.execute("SELECT DISTINCT txn_hash FROM document_links").fetchall()
        for r in rows:
            try:
                linked_hashes.add(r["txn_hash"])
            except (IndexError, KeyError, TypeError):
                linked_hashes.add(r[0])
    except sqlite3.Error:
        pass

    # Lazy import to avoid a circular at module load (scorer doesn't
    # need txn_hash; txn_matcher would normally pull from
    # core.beancount_io which itself imports rewrite/transform).
    from lamella.core.beancount_io.txn_hash import txn_hash as _hash_fn

    out: list[ScoredLedgerCandidate] = []
    seen: set[str] = set()
    lo = doc_date - timedelta(days=window_days)
    hi = doc_date + timedelta(days=window_days)

    for entry in ledger_entries or ():
        if not isinstance(entry, Transaction):
            continue
        if not isinstance(entry.date, date):
            continue
        if entry.date < lo or entry.date > hi:
            continue

        amt_ccy = _txn_amount_cents(entry)
        if amt_ccy is None:
            continue
        cents, ccy = amt_ccy

        try:
            h = _hash_fn(entry)
        except Exception:  # noqa: BLE001 — defensive; bad txn shouldn't kill the sweep
            continue
        if h in linked_hashes or h in blocked_hashes or h in seen:
            continue
        seen.add(h)

        result = scorer.score(
            doc_date=doc_date,
            doc_total_cents=doc_total_cents,
            doc_currency=doc_currency,
            doc_vendor=doc_vendor,
            doc_doctype=doc_doctype,
            txn_date=entry.date,
            txn_amount_cents=cents,
            txn_currency=ccy,
            txn_payee=entry.payee,
            txn_description=entry.narration,
            doc_subtotal_cents=doc_subtotal_cents,
            doc_content_excerpt=doc_content_excerpt,
            doc_correspondent=doc_correspondent,
            doc_last_four=doc_last_four,
        )
        if result.total < min_score:
            continue
        amount_decimal = Decimal(cents) / Decimal(100)
        out.append(
            ScoredLedgerCandidate(
                txn_hash=h,
                txn_date=entry.date,
                txn_amount=amount_decimal,
                payee=entry.payee,
                narration=entry.narration,
                score=result.total,
                reasons=result.reasons,
            )
        )

    out.sort(key=lambda c: c.score, reverse=True)
    return out[:max_results]


__all__ = [
    "EXCLUDED_DOCUMENT_TYPES",
    "RECEIPT_EXCLUDED_DOCTYPE_PATTERNS",
    "ScoredCandidate",
    "ScoredLedgerCandidate",
    "find_document_candidates",
    "find_ledger_candidates",
]
