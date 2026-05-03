# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone


REVIEW_KINDS = frozenset(
    {
        "fixme",
        "receipt_unmatched",
        "ambiguous_match",
        "note_orphan",
        # Phase 4
        "simplefin_unmapped_account",
        # Phase 7
        "import_categorization",
    }
)


@dataclass(frozen=True)
class ReviewItem:
    id: int
    kind: str
    source_ref: str
    created_at: datetime
    resolved_at: datetime | None
    priority: int
    ai_suggestion: str | None
    ai_model: str | None
    user_decision: str | None


class ReviewService:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def enqueue(
        self,
        *,
        kind: str,
        source_ref: str,
        priority: int = 0,
        ai_suggestion: str | None = None,
        ai_model: str | None = None,
    ) -> int:
        if kind not in REVIEW_KINDS:
            raise ValueError(f"unknown review kind: {kind!r}")
        cursor = self.conn.execute(
            """
            INSERT INTO review_queue (kind, source_ref, priority, ai_suggestion, ai_model)
            VALUES (?, ?, ?, ?, ?)
            """,
            (kind, source_ref, priority, ai_suggestion, ai_model),
        )
        return int(cursor.lastrowid)

    def enqueue_resolved(
        self,
        *,
        kind: str,
        source_ref: str,
        user_decision: str,
        priority: int = 0,
        ai_suggestion: str | None = None,
        ai_model: str | None = None,
    ) -> int:
        """Insert a review row that is already resolved. Used for auto-apply
        so every ledger change has a paper trail."""
        if kind not in REVIEW_KINDS:
            raise ValueError(f"unknown review kind: {kind!r}")
        cursor = self.conn.execute(
            """
            INSERT INTO review_queue
                (kind, source_ref, priority, ai_suggestion, ai_model,
                 resolved_at, user_decision)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
            """,
            (kind, source_ref, priority, ai_suggestion, ai_model, user_decision),
        )
        return int(cursor.lastrowid)

    def list_open_by_kind(self, kind: str, *, limit: int = 100) -> list[ReviewItem]:
        rows = self.conn.execute(
            """
            SELECT * FROM review_queue
            WHERE resolved_at IS NULL AND kind = ?
            ORDER BY priority DESC, created_at ASC
            LIMIT ?
            """,
            (kind, limit),
        ).fetchall()
        return [_row_to_item(r) for r in rows]

    def set_suggestion(
        self,
        item_id: int,
        *,
        ai_suggestion: str,
        ai_model: str | None,
    ) -> bool:
        cursor = self.conn.execute(
            """
            UPDATE review_queue
               SET ai_suggestion = ?, ai_model = ?
             WHERE id = ? AND resolved_at IS NULL
            """,
            (ai_suggestion, ai_model, item_id),
        )
        return cursor.rowcount > 0

    def get(self, item_id: int) -> ReviewItem | None:
        row = self.conn.execute(
            "SELECT * FROM review_queue WHERE id = ?", (item_id,)
        ).fetchone()
        return _row_to_item(row) if row else None

    def list_open(self) -> list[ReviewItem]:
        rows = self.conn.execute(
            """
            SELECT * FROM review_queue
            WHERE resolved_at IS NULL
            ORDER BY priority DESC, created_at ASC
            """
        ).fetchall()
        return [_row_to_item(r) for r in rows]

    def count_open(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM review_queue WHERE resolved_at IS NULL"
        ).fetchone()
        return int(row["n"] if row else 0)

    def resolve(self, item_id: int, user_decision: str | None) -> bool:
        # Capture the item first so we can trigger kind-specific side
        # effects after resolution (e.g. clear needs_review on a row from
        # an import).
        item = self.get(item_id)
        cursor = self.conn.execute(
            """
            UPDATE review_queue
               SET resolved_at = CURRENT_TIMESTAMP,
                   user_decision = ?
             WHERE id = ? AND resolved_at IS NULL
            """,
            (user_decision, item_id),
        )
        if cursor.rowcount > 0 and item is not None:
            self._post_resolve(item, user_decision)
        return cursor.rowcount > 0

    def _post_resolve(self, item: ReviewItem, user_decision: str | None) -> None:
        if item.kind != "import_categorization":
            return
        # source_ref format: "import:<import_id>:row:<raw_row_id>"
        try:
            parts = item.source_ref.split(":")
            idx = parts.index("row")
            raw_row_id = int(parts[idx + 1])
        except (ValueError, IndexError):
            return
        self.conn.execute(
            """
            UPDATE categorizations
               SET needs_review = 0,
                   reason = COALESCE(reason, '') || ' | resolved by user',
                   decided_at = datetime('now')
             WHERE raw_row_id = ?
            """,
            (raw_row_id,),
        )


def _row_to_item(row: sqlite3.Row) -> ReviewItem:
    def _parse(value):
        if value is None or isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return None
        return None

    return ReviewItem(
        id=int(row["id"]),
        kind=row["kind"],
        source_ref=row["source_ref"],
        created_at=_parse(row["created_at"]) or datetime.now(timezone.utc),
        resolved_at=_parse(row["resolved_at"]),
        priority=int(row["priority"]),
        ai_suggestion=row["ai_suggestion"],
        ai_model=row["ai_model"],
        user_decision=row["user_decision"],
    )
