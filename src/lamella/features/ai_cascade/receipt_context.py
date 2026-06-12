# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Paperless receipt context for classification.

Feeds OCR'd receipt content into the classify prompt. Two
lookup paths:

1. **Linked receipt.** When the txn is already in the ledger
   and a ``document_links`` row exists for it, we know which
   Paperless document goes with this charge — pull its vendor,
   total, and OCR excerpt straight from ``paperless_doc_index``.

2. **Candidate receipt by amount + date.** For new SimpleFIN
   txns (no txn_hash yet, no link yet), scan the local
   Paperless index for an unambiguous match on ``total_amount``
   within ±``tolerance_days`` of the posting date. A single
   match → "likely receipt" context passed to the AI. Multiple
   matches → ambiguous, no context (don't bias on the wrong
   receipt). No matches → no context.

Both paths are read-only against ``paperless_doc_index`` — they
never hit the Paperless HTTP API at classify time. The index is
kept fresh by the separate ``paperless/sync.py`` background job,
so the content_excerpt there is at most a few minutes stale
regardless of how many classifies run.

Hardware Store is the motivating case. Same merchant, 5+ possible
categories, 3+ possible entities. Without the receipt line items
the AI is guessing. With them it can tell "lumber + concrete
mix" from "printer paper + ink cartridges" and classify
accordingly — especially combined with active notes
("working on the Main Residence deck this week").
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

log = logging.getLogger(__name__)

__all__ = [
    "DocumentContext",
    "fetch_document_context",
    "DEFAULT_EXCERPT_CHARS",
]


# Keep the OCR excerpt passed into the prompt bounded so prompts
# don't explode. Paperless's content_excerpt is ~4KB; we slice to
# ~1500 chars which covers most receipts (a Hardware Store receipt
# with 20 line items fits comfortably).
DEFAULT_EXCERPT_CHARS = 1500


@dataclass(frozen=True)
class DocumentContext:
    """The slice of receipt data that feeds into the classify prompt."""
    paperless_id: int
    vendor: str | None
    total: Decimal | None
    document_date: date | None
    content_excerpt: str                 # trimmed OCR text
    source: str                          # 'linked' | 'candidate'
    confidence_note: str                 # human-readable why-this-receipt
    # Non-empty when the receipt's OCR date looks implausible
    # relative to the txn date — e.g., a Warehouse Club gas receipt OCR'd
    # as "2064-01-08" when the txn posted on "2026-04-15." The
    # prompt surfaces this so the AI treats the date with
    # appropriate skepticism. A follow-up can add write-back to
    # Paperless to correct the field.
    date_mismatch_note: str | None = None

    @property
    def has_content(self) -> bool:
        return bool(self.content_excerpt and self.content_excerpt.strip())


def fetch_document_context(
    conn: sqlite3.Connection,
    *,
    txn_hash: str | None = None,
    posting_date: date | None = None,
    amount: Decimal | str | float | None = None,
    tolerance_days: int = 3,
    max_chars: int = DEFAULT_EXCERPT_CHARS,
) -> DocumentContext | None:
    """Return a DocumentContext for the txn, or None when nothing
    plausible is available.

    * If ``txn_hash`` resolves to a linked receipt via
      ``document_links``, return that one (with a ``date_mismatch_note``
      when the OCR'd receipt_date looks wrong relative to
      ``posting_date``).
    * Else if ``posting_date`` + ``amount`` are provided and
      exactly ONE paperless doc matches on total + date window,
      return it as a candidate.
    * Else return None.
    """
    if txn_hash:
        linked = _fetch_linked(
            conn, txn_hash=txn_hash,
            posting_date=posting_date, max_chars=max_chars,
        )
        if linked is not None:
            return linked
    if posting_date is not None and amount is not None:
        return _fetch_candidate(
            conn,
            posting_date=posting_date,
            amount=amount,
            tolerance_days=tolerance_days,
            max_chars=max_chars,
        )
    return None


# ------------------------------------------------------------------
# Linked-receipt lookup
# ------------------------------------------------------------------


def _fetch_linked(
    conn: sqlite3.Connection,
    *,
    txn_hash: str,
    posting_date: date | None,
    max_chars: int,
) -> DocumentContext | None:
    # Prefer the most confident / most recent link for this txn.
    # ADR-0061: receipt_date column renamed to document_date; the
    # row dict key, dataclass field, and column all share the new
    # name post-Phase 4 (no alias needed).
    row = conn.execute(
        """
        SELECT pdi.paperless_id, pdi.vendor, pdi.total_amount,
               pdi.document_date, pdi.content_excerpt
          FROM document_links rl
          JOIN paperless_doc_index pdi
                 ON pdi.paperless_id = rl.paperless_id
         WHERE rl.txn_hash = ?
         ORDER BY rl.linked_at DESC
         LIMIT 1
        """,
        (txn_hash,),
    ).fetchone()
    if row is None:
        return None
    return _row_to_context(
        row, source="linked",
        confidence_note="linked to this transaction",
        posting_date=posting_date,
        max_chars=max_chars,
    )


# ------------------------------------------------------------------
# Candidate-by-amount+date lookup
# ------------------------------------------------------------------


def _fetch_candidate(
    conn: sqlite3.Connection,
    *,
    posting_date: date,
    amount: Decimal | str | float,
    tolerance_days: int,
    max_chars: int,
) -> DocumentContext | None:
    """Find a single unambiguous receipt matching ``amount`` and
    within ±tolerance_days of ``posting_date``. Returns None when
    zero or multiple receipts match (ambiguous → don't bias)."""
    try:
        amt = amount if isinstance(amount, Decimal) else Decimal(str(amount))
    except Exception:  # noqa: BLE001
        return None
    abs_amt = amt.copy_abs()
    # ADR-0022: paperless_doc_index.total_amount is TEXT post-migration 057.
    # Filter by date in SQL, then exact-amount compare in Python with Decimal
    # (cent threshold) so we don't lose precision through CAST AS REAL.
    # ADR-0061 Phase 4: receipt_date renamed to document_date end-to-end —
    # column, row dict key, and DocumentContext field all share the new name.
    raw_rows = conn.execute(
        """
        SELECT paperless_id, vendor, total_amount, document_date,
               content_excerpt
          FROM paperless_doc_index
         WHERE total_amount IS NOT NULL
           AND document_date IS NOT NULL
           AND ABS(
                 julianday(document_date) - julianday(?)
               ) <= ?
         LIMIT 200
        """,
        (posting_date.isoformat(), int(tolerance_days)),
    ).fetchall()
    cents_threshold = Decimal("0.01")
    rows = []
    for r in raw_rows:
        try:
            d = Decimal(str(r["total_amount"]))
        except Exception:  # noqa: BLE001
            continue
        if abs(d - abs_amt) < cents_threshold:
            rows.append(r)
            if len(rows) > 1:
                break
    if len(rows) != 1:
        return None
    (row,) = rows
    return _row_to_context(
        row, source="candidate",
        confidence_note=(
            f"candidate: unique Paperless receipt matching total "
            f"{row['total_amount']} within ±{tolerance_days} days"
        ),
        posting_date=posting_date,
        max_chars=max_chars,
    )


# ------------------------------------------------------------------
# Shared row → DocumentContext conversion
# ------------------------------------------------------------------


def _date_mismatch_note(
    receipt_date: date | None,
    posting_date: date | None,
) -> str | None:
    """Flag implausible receipt dates for the AI to handle with
    skepticism. The Warehouse Club-receipt-OCR'd-as-2064 case is the
    motivating example — the total is right, the vendor is right,
    but the date is junk. Rather than silently trusting it (and
    letting an AI bias by "this receipt is from 40 years ago"),
    we surface the mismatch in the prompt."""
    if receipt_date is None or posting_date is None:
        return None
    delta_days = abs((receipt_date - posting_date).days)
    # Year-level mismatch (e.g., 2064 vs 2024): almost certainly
    # an OCR error on the date. 3650 ≈ 10 years.
    if delta_days > 3650:
        return (
            f"receipt date {receipt_date} is likely an OCR error — "
            f"transaction posted {posting_date}. Trust the vendor, "
            f"total, and line items but DO NOT weigh the receipt date."
        )
    # Month-level mismatch: could be a pre-auth settlement delay,
    # a late receipt, or an OCR error. Flag softly.
    if delta_days > 30:
        return (
            f"receipt date {receipt_date} is {delta_days} days off "
            f"from the transaction date ({posting_date}). Common "
            f"causes: OCR error, delayed settlement, uploading "
            f"an older receipt by mistake. Weigh the content over "
            f"the dates."
        )
    return None


def _row_to_context(
    row: Any,
    *,
    source: str,
    confidence_note: str,
    posting_date: date | None,
    max_chars: int,
) -> DocumentContext:
    total: Decimal | None = None
    try:
        if row["total_amount"] is not None:
            total = Decimal(str(row["total_amount"]))
    except Exception:  # noqa: BLE001
        total = None
    rdate: date | None = None
    try:
        if row["document_date"]:
            rdate = date.fromisoformat(str(row["document_date"])[:10])
    except Exception:  # noqa: BLE001
        rdate = None
    excerpt = (row["content_excerpt"] or "").strip()
    if len(excerpt) > max_chars:
        excerpt = excerpt[:max_chars].rstrip() + "…"
    return DocumentContext(
        paperless_id=int(row["paperless_id"]),
        vendor=row["vendor"],
        total=total,
        document_date=rdate,
        content_excerpt=excerpt,
        source=source,
        confidence_note=confidence_note,
        date_mismatch_note=_date_mismatch_note(rdate, posting_date),
    )
