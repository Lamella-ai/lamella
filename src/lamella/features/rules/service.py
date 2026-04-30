# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from lamella.features.rules.models import PATTERN_TYPES, RuleRow

# Phase 3: AI-originated rules enter at 0.85 and can only be auto-applied
# after `AI_PROMOTION_THRESHOLD` user acceptances.
AI_INITIAL_CONFIDENCE = 0.85
AI_PROMOTION_THRESHOLD = 3
AI_DEMOTION_FLOOR = 0.30
AI_DEMOTION_STEP = 0.10


def _row_to_rule(row: sqlite3.Row) -> RuleRow:
    last_used = row["last_used"]
    if isinstance(last_used, str):
        try:
            last_used = datetime.fromisoformat(last_used)
        except ValueError:
            last_used = None
    return RuleRow(
        id=int(row["id"]),
        pattern_type=row["pattern_type"],
        pattern_value=row["pattern_value"],
        card_account=row["card_account"],
        target_account=row["target_account"],
        confidence=float(row["confidence"]),
        hit_count=int(row["hit_count"]),
        last_used=last_used if isinstance(last_used, datetime) else None,
        created_by=row["created_by"],
    )


class RuleService:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def list(self) -> list[RuleRow]:
        rows = self.conn.execute(
            """
            SELECT * FROM classification_rules
            ORDER BY hit_count DESC, last_used DESC, id ASC
            """
        ).fetchall()
        return [_row_to_rule(r) for r in rows]

    def iter_active(self) -> Iterable[RuleRow]:
        """Yield rules for engine evaluation. All rules are active in Phase 2."""
        return self.list()

    def get(self, rule_id: int) -> RuleRow | None:
        row = self.conn.execute(
            "SELECT * FROM classification_rules WHERE id = ?", (rule_id,)
        ).fetchone()
        return _row_to_rule(row) if row else None

    def create(
        self,
        *,
        pattern_type: str,
        pattern_value: str,
        target_account: str,
        card_account: str | None = None,
        confidence: float = 1.0,
        created_by: str = "user",
    ) -> int:
        if pattern_type not in PATTERN_TYPES:
            raise ValueError(f"unknown pattern_type: {pattern_type!r}")
        if not pattern_value.strip():
            raise ValueError("pattern_value must not be empty")
        if not target_account.strip():
            raise ValueError("target_account must not be empty")
        cursor = self.conn.execute(
            """
            INSERT INTO classification_rules
                (pattern_type, pattern_value, card_account, target_account,
                 confidence, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            (
                pattern_type,
                pattern_value,
                card_account,
                target_account,
                confidence,
                created_by,
            ),
        )
        if cursor.lastrowid:
            return int(cursor.lastrowid)
        # Already existed — return the existing row's id.
        row = self.conn.execute(
            """
            SELECT id FROM classification_rules
            WHERE pattern_type = ? AND pattern_value = ?
              AND IFNULL(card_account, '') = IFNULL(?, '')
              AND target_account = ?
            """,
            (pattern_type, pattern_value, card_account, target_account),
        ).fetchone()
        return int(row["id"])

    def delete(self, rule_id: int) -> bool:
        cursor = self.conn.execute(
            "DELETE FROM classification_rules WHERE id = ?", (rule_id,)
        )
        return cursor.rowcount > 0

    def bump(self, rule_id: int) -> None:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.conn.execute(
            """
            UPDATE classification_rules
               SET hit_count = hit_count + 1,
                   last_used = ?
             WHERE id = ?
            """,
            (now, rule_id),
        )

    def set_confidence(self, rule_id: int, confidence: float) -> None:
        confidence = max(0.0, min(1.0, float(confidence)))
        self.conn.execute(
            "UPDATE classification_rules SET confidence = ? WHERE id = ?",
            (confidence, rule_id),
        )

    def set_created_by(self, rule_id: int, created_by: str) -> None:
        self.conn.execute(
            "UPDATE classification_rules SET created_by = ? WHERE id = ?",
            (created_by, rule_id),
        )

    def promote_ai_if_eligible(self, rule_id: int) -> bool:
        """Phase 3: after `AI_PROMOTION_THRESHOLD` user acceptances of an
        AI-created rule, promote it to user-tier (confidence=1.0,
        created_by='user'). Returns True if a promotion occurred."""
        rule = self.get(rule_id)
        if rule is None or rule.created_by != "ai":
            return False
        if rule.hit_count < AI_PROMOTION_THRESHOLD:
            return False
        self.set_confidence(rule_id, 1.0)
        self.set_created_by(rule_id, "user")
        return True

    def demote_on_contradiction(self, rule_id: int) -> float | None:
        """Phase 3: when the user overrides a rule's suggestion with a
        different target, lower that rule's confidence by one step, with a
        floor. Only demotes AI-created rules; user rules stay pinned."""
        rule = self.get(rule_id)
        if rule is None or rule.created_by != "ai":
            return None
        new_conf = max(AI_DEMOTION_FLOOR, rule.confidence - AI_DEMOTION_STEP)
        self.set_confidence(rule_id, new_conf)
        return new_conf

    def learn_from_decision(
        self,
        *,
        matched_rule_id: int | None,
        user_target_account: str,
        pattern_type: str = "merchant_contains",
        pattern_value: str,
        card_account: str | None = None,
        create_if_missing: bool = True,
        source: str = "user",
    ) -> int | None:
        """Record the outcome of a user's review decision.

        - If the user's target matches `matched_rule_id`'s target → bump it.
          If that rule is AI-created and has crossed the promotion
          threshold, promote it to user-tier.
        - If it contradicts:
            * AI-created matched rule → demote its confidence (floor 0.3).
            * Either way, insert a fresh user rule at confidence=1.0 (if
              `create_if_missing`).
        - `source='ai'` inserts the new rule as `created_by='ai'` at
          `AI_INITIAL_CONFIDENCE`, used when an AI classification is
          accepted as-is.
        Returns the id of the rule that was bumped OR newly created, else
        None.
        """
        if matched_rule_id is not None:
            existing = self.get(matched_rule_id)
            if existing and existing.target_account == user_target_account:
                self.bump(existing.id)
                # Check for promotion AFTER the bump so the new hit counts.
                self.promote_ai_if_eligible(existing.id)
                return existing.id
            if existing is not None:
                self.demote_on_contradiction(existing.id)

        if not create_if_missing:
            return None
        if not pattern_value.strip():
            return None

        initial_confidence = AI_INITIAL_CONFIDENCE if source == "ai" else 1.0
        created_by = "ai" if source == "ai" else "user"
        new_id = self.create(
            pattern_type=pattern_type,
            pattern_value=pattern_value,
            card_account=card_account,
            target_account=user_target_account,
            confidence=initial_confidence,
            created_by=created_by,
        )
        # Only bump on user-source acceptances; an AI-generated rule starts
        # at zero hits and must be earned via user acceptances.
        if source != "ai":
            self.bump(new_id)
        return new_id
