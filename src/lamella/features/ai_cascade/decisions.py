# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


class DecisionType(str, Enum):
    CLASSIFY_TXN = "classify_txn"
    MATCH_RECEIPT = "match_receipt"
    PARSE_NOTE = "parse_note"
    RULE_PROMOTION = "rule_promotion"
    COLUMN_MAP = "column_map"
    # Paperless verify-and-writeback (Slice A+B+C). Vision model
    # re-extracts fields from the image; caller diffs and pushes
    # corrections back to Paperless with a "Lamella Fixed" tag.
    RECEIPT_VERIFY = "receipt_verify"
    # Enrichment writeback — we learned something contextual about
    # the document (mileage log pinned it to a specific vehicle,
    # active note tied it to a project/trip) and pushed that back
    # to Paperless as a note/custom field.
    RECEIPT_ENRICH = "receipt_enrich"
    # Work-backwards draft-generation. Given an entity's or
    # account's ledger history, generate a proposed plain-English
    # description. User reviews + edits before anything is
    # persisted.
    DRAFT_DESCRIPTION = "draft_description"
    # Calendar phase 2 — short narrative summarizing one day's
    # activity across sources (txns, notes, mileage, paperless).
    # No schema validation; free-form text response.
    SUMMARIZE_DAY = "summarize_day"
    # Calendar phase 2 — re-run classify_txn against every txn on
    # one day with full day context and compare to the current
    # classification. Read-only audit; no ledger writes.
    AUDIT_DAY = "audit_day"


DECISION_TYPES: frozenset[str] = frozenset(d.value for d in DecisionType)

CACHED_MODEL_SENTINEL = "<cached>"


@dataclass(frozen=True)
class DecisionRow:
    id: int
    decided_at: datetime
    decision_type: str
    input_ref: str
    model: str
    prompt_tokens: int | None
    completion_tokens: int | None
    prompt_hash: str | None
    result: dict[str, Any]
    user_corrected: bool
    user_correction: str | None


def _parse_ts(value) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _row_to_decision(row: sqlite3.Row) -> DecisionRow:
    raw = row["result"]
    try:
        result = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except ValueError:
        result = {"error": "unparseable result", "raw": raw}
    return DecisionRow(
        id=int(row["id"]),
        decided_at=_parse_ts(row["decided_at"]),
        decision_type=row["decision_type"],
        input_ref=row["input_ref"],
        model=row["model"],
        prompt_tokens=row["prompt_tokens"],
        completion_tokens=row["completion_tokens"],
        prompt_hash=row["prompt_hash"],
        result=result if isinstance(result, dict) else {"value": result},
        user_corrected=bool(row["user_corrected"]),
        user_correction=row["user_correction"],
    )


class DecisionsLog:
    """CRUD for `ai_decisions`. Every AI call — real, cached, or errored —
    flows through `log()`. Queries here back `/ai/audit`, `/ai/cost`, and
    the prompt-hash cache lookup in `client.OpenRouterClient`."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def log(
        self,
        *,
        decision_type: str,
        input_ref: str,
        model: str,
        result: dict[str, Any],
        prompt_tokens: int | None = None,
        completion_tokens: int | None = None,
        prompt_hash: str | None = None,
        prompt_system: str | None = None,
        prompt_user: str | None = None,
        user_corrected: bool = False,
        user_correction: str | None = None,
    ) -> int:
        """Persist an AI call outcome.

        ``prompt_system`` / ``prompt_user`` capture the exact strings
        passed to the model so a user can audit what the AI actually
        saw — the user explicitly asked "I don't know how the request
        actually looks like, I don't know if it's sending other
        transactions that it does know along with it." These are
        nullable so existing rows (pre-capture era) stay valid.
        """
        if decision_type not in DECISION_TYPES:
            raise ValueError(f"unknown decision_type: {decision_type!r}")
        cursor = self.conn.execute(
            """
            INSERT INTO ai_decisions
                (decision_type, input_ref, model, prompt_tokens, completion_tokens,
                 prompt_hash, result, user_corrected, user_correction,
                 prompt_system, prompt_user)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_type,
                input_ref,
                model,
                prompt_tokens,
                completion_tokens,
                prompt_hash,
                json.dumps(result, default=str),
                1 if user_corrected else 0,
                user_correction,
                prompt_system,
                prompt_user,
            ),
        )
        return int(cursor.lastrowid)

    def get(self, decision_id: int) -> DecisionRow | None:
        row = self.conn.execute(
            "SELECT * FROM ai_decisions WHERE id = ?", (decision_id,)
        ).fetchone()
        return _row_to_decision(row) if row else None

    def mark_correction(
        self,
        decision_id: int,
        *,
        user_correction: str | None,
    ) -> bool:
        cursor = self.conn.execute(
            """
            UPDATE ai_decisions
               SET user_corrected = 1,
                   user_correction = ?
             WHERE id = ?
            """,
            (user_correction, decision_id),
        )
        return cursor.rowcount > 0

    def recent(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        decision_type: str | None = None,
        user_corrected: bool | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[DecisionRow]:
        clauses: list[str] = []
        args: list[Any] = []
        if decision_type:
            clauses.append("decision_type = ?")
            args.append(decision_type)
        if user_corrected is not None:
            clauses.append("user_corrected = ?")
            args.append(1 if user_corrected else 0)
        if since is not None:
            clauses.append("decided_at >= ?")
            args.append(since.isoformat(timespec="seconds"))
        if until is not None:
            clauses.append("decided_at < ?")
            args.append(until.isoformat(timespec="seconds"))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        args.extend([limit, offset])
        rows = self.conn.execute(
            f"""
            SELECT * FROM ai_decisions
            {where}
            ORDER BY decided_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            tuple(args),
        ).fetchall()
        return [_row_to_decision(r) for r in rows]

    def find_cache_hit(
        self,
        *,
        prompt_hash: str,
        ttl_hours: int,
        decision_type: str | None = None,
    ) -> DecisionRow | None:
        """Return the newest non-error decision with `prompt_hash` whose
        `decided_at` is within `ttl_hours`. Returns None if no match."""
        if ttl_hours <= 0 or not prompt_hash:
            return None
        cutoff = datetime.now(timezone.utc) - timedelta(hours=ttl_hours)
        clauses = ["prompt_hash = ?", "decided_at >= ?"]
        args: list[Any] = [prompt_hash, cutoff.isoformat(timespec="seconds")]
        if decision_type:
            clauses.append("decision_type = ?")
            args.append(decision_type)
        rows = self.conn.execute(
            f"""
            SELECT * FROM ai_decisions
            WHERE {' AND '.join(clauses)}
            ORDER BY decided_at DESC, id DESC
            LIMIT 5
            """,
            tuple(args),
        ).fetchall()
        for row in rows:
            parsed = _row_to_decision(row)
            if "error" in parsed.result:
                continue
            if parsed.model == CACHED_MODEL_SENTINEL:
                # Chain-walk: find the original non-cached, non-error row.
                continue
            return parsed
        return None

    def cost_summary(
        self,
        *,
        since: datetime,
        prompt_price_per_1k: float,
        completion_price_per_1k: float,
    ) -> dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*)                                  AS n,
                COALESCE(SUM(prompt_tokens), 0)           AS prompt_tokens,
                COALESCE(SUM(completion_tokens), 0)       AS completion_tokens
            FROM ai_decisions
            WHERE decided_at >= ? AND model <> ?
            """,
            (since.isoformat(timespec="seconds"), CACHED_MODEL_SENTINEL),
        ).fetchone()
        prompt_tokens = int(row["prompt_tokens"] or 0)
        completion_tokens = int(row["completion_tokens"] or 0)
        cost = (
            (prompt_tokens / 1000.0) * prompt_price_per_1k
            + (completion_tokens / 1000.0) * completion_price_per_1k
        )
        by_type_rows = self.conn.execute(
            """
            SELECT decision_type, COUNT(*) AS n,
                   COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                   COALESCE(SUM(completion_tokens), 0) AS completion_tokens
            FROM ai_decisions
            WHERE decided_at >= ? AND model <> ?
            GROUP BY decision_type
            """,
            (since.isoformat(timespec="seconds"), CACHED_MODEL_SENTINEL),
        ).fetchall()
        by_type = {
            r["decision_type"]: {
                "n": int(r["n"] or 0),
                "prompt_tokens": int(r["prompt_tokens"] or 0),
                "completion_tokens": int(r["completion_tokens"] or 0),
            }
            for r in by_type_rows
        }
        cache_row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM ai_decisions WHERE decided_at >= ? AND model = ?",
            (since.isoformat(timespec="seconds"), CACHED_MODEL_SENTINEL),
        ).fetchone()
        return {
            "since": since,
            "calls": int(row["n"] or 0),
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "cache_hits": int(cache_row["n"] or 0) if cache_row else 0,
            "cost_usd": round(cost, 4),
            "by_type": by_type,
        }
