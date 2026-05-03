# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Staging-backed review surface — NEXTGEN.md Phase B2 groundwork.

Exposes pending staged rows (rows awaiting a human decision) in a
shape the review UI can consume. Replaces the FIXME-scan-of-the-
ledger approach the existing review page uses for importer and
SimpleFIN rows — once the UI routes are swapped to call this,
FIXME bean lines can stop being emitted for un-paired low-
confidence rows (the full B2 swing).

What counts as "pending" here:
  * staged_transactions.status IN ('new', 'classified', 'matched')
  * joined to staged_decisions where needs_review = 1
  * or no decision yet (status='new' — a row we haven't classified)
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

__all__ = [
    "StagingReviewItem",
    "list_pending_items",
    "count_pending_items",
]


@dataclass(frozen=True)
class StagingReviewItem:
    """A single pending staged row in a form the review UI can render."""
    staged_id: int
    source: str
    source_ref: dict[str, Any]
    source_ref_hash: str | None
    session_id: str | None
    posting_date: str
    amount: Decimal
    currency: str
    payee: str | None
    description: str | None
    proposed_account: str | None
    proposed_confidence: str | None
    proposed_by: str | None
    proposed_rationale: str | None
    status: str
    pair_id: int | None
    pair_kind: str | None
    pair_confidence: str | None
    pair_other_staged_id: int | None
    # ADR-0046 Phase 3b — when ingest's loose synthetic-match fires,
    # this carries {lamella_txn_id, wrong_account, right_account} so
    # /review/staged renders the wrong-account-confirm banner. None
    # for every row that didn't trip the loose match.
    synthetic_match_meta: dict[str, Any] | None
    # ADR-0046 Phase 4b — UUIDv7 minted at stage time per migration
    # 059. Surfaced here so /review's classified rows can render an
    # Undo button that posts to /api/txn/{lamella_txn_id}/reverse-
    # classify. None on rows that pre-date the migration's backfill.
    lamella_txn_id: str | None


def list_pending_items(
    conn: sqlite3.Connection,
    *,
    limit: int = 200,
    source: str | None = None,
) -> list[StagingReviewItem]:
    """Return pending staged rows that need a human decision.

    Sorted by posting_date DESC, most-recent first. Each item carries
    any matched-pair context so the review UI can display "this is
    tentatively paired with row X — confirm?" prompts.
    """
    base_sql = """
        SELECT t.id AS staged_id,
               t.source, t.source_ref, t.source_ref_hash, t.session_id,
               t.posting_date, t.amount, t.currency,
               t.payee, t.description, t.status,
               t.synthetic_match_meta,
               t.lamella_txn_id,
               d.account AS proposed_account,
               d.confidence AS proposed_confidence,
               d.decided_by AS proposed_by,
               d.rationale AS proposed_rationale,
               d.needs_review AS needs_review,
               p.id AS pair_id,
               p.kind AS pair_kind,
               p.confidence AS pair_confidence,
               CASE
                 WHEN p.a_staged_id = t.id THEN p.b_staged_id
                 ELSE p.a_staged_id
               END AS pair_other_id
          FROM staged_transactions t
          LEFT JOIN staged_decisions d ON d.staged_id = t.id
          LEFT JOIN staged_pairs p
                 ON (p.a_staged_id = t.id OR p.b_staged_id = t.id)
         WHERE t.status IN ('new', 'classified', 'matched')
           AND (d.needs_review = 1 OR d.staged_id IS NULL)
    """
    params: list[Any] = []
    if source is not None:
        base_sql += " AND t.source = ?"
        params.append(source)
    base_sql += " ORDER BY t.posting_date DESC, t.id DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(base_sql, params).fetchall()
    out: list[StagingReviewItem] = []
    for r in rows:
        ref = json.loads(r["source_ref"]) if r["source_ref"] else {}
        synth_meta_raw = r["synthetic_match_meta"] if "synthetic_match_meta" in r.keys() else None
        synth_meta: dict[str, Any] | None = None
        if synth_meta_raw:
            try:
                parsed = json.loads(synth_meta_raw)
                if isinstance(parsed, dict):
                    synth_meta = parsed
            except (TypeError, ValueError):
                synth_meta = None
        out.append(
            StagingReviewItem(
                staged_id=int(r["staged_id"]),
                source=r["source"],
                source_ref=ref,
                source_ref_hash=r["source_ref_hash"],
                session_id=r["session_id"],
                posting_date=r["posting_date"],
                amount=Decimal(r["amount"]),
                currency=r["currency"] or "USD",
                payee=r["payee"],
                description=r["description"],
                proposed_account=r["proposed_account"],
                proposed_confidence=r["proposed_confidence"],
                proposed_by=r["proposed_by"],
                proposed_rationale=r["proposed_rationale"],
                status=r["status"],
                pair_id=(
                    int(r["pair_id"]) if r["pair_id"] is not None else None
                ),
                pair_kind=r["pair_kind"],
                pair_confidence=r["pair_confidence"],
                pair_other_staged_id=(
                    int(r["pair_other_id"])
                    if r["pair_other_id"] is not None else None
                ),
                synthetic_match_meta=synth_meta,
                lamella_txn_id=r["lamella_txn_id"] if "lamella_txn_id" in r.keys() else None,
            )
        )
    return out


def count_pending_items(
    conn: sqlite3.Connection, *, source: str | None = None,
) -> int:
    """Fast count of pending staged items for dashboard badges."""
    sql = """
        SELECT COUNT(*) AS n
          FROM staged_transactions t
          LEFT JOIN staged_decisions d ON d.staged_id = t.id
         WHERE t.status IN ('new', 'classified', 'matched')
           AND (d.needs_review = 1 OR d.staged_id IS NULL)
    """
    params: list[Any] = []
    if source is not None:
        sql += " AND t.source = ?"
        params.append(source)
    row = conn.execute(sql, params).fetchone()
    return int(row["n"] or 0)
