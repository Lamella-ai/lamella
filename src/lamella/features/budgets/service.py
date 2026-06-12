# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Open

from lamella.features.budgets.models import (
    Budget,
    BudgetPeriod,
    BudgetValidationError,
)


class BudgetService:
    """CRUD for the `budgets` table. Validation is enforced here so the
    HTTP layer can stay thin and so the same checks apply to API and UI
    callers."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    @staticmethod
    def validate_pattern(pattern: str, *, open_accounts: Iterable[str]) -> re.Pattern[str]:
        """Compile ``pattern`` and require ≥1 currently-open account match.
        Catches typos like ``Expenses:Acme:Supples`` before they sit in
        the DB silently aggregating $0."""
        if not pattern or not pattern.strip():
            raise BudgetValidationError("account_pattern is required")
        try:
            compiled = re.compile(pattern)
        except re.error as exc:
            raise BudgetValidationError(f"invalid regex: {exc}") from exc
        accounts = list(open_accounts)
        if not any(compiled.search(a) for a in accounts):
            raise BudgetValidationError(
                f"account_pattern matches no open ledger account ({len(accounts)} checked)"
            )
        return compiled

    @staticmethod
    def open_accounts(entries: Iterable) -> list[str]:
        return sorted({e.account for e in entries if isinstance(e, Open)})

    def create(
        self,
        *,
        label: str,
        entity: str,
        account_pattern: str,
        period: str | BudgetPeriod,
        amount: Decimal,
        alert_threshold: float = 0.8,
        open_accounts: Iterable[str] | None = None,
    ) -> Budget:
        label = (label or "").strip()
        entity = (entity or "").strip()
        if not label:
            raise BudgetValidationError("label is required")
        if not entity:
            raise BudgetValidationError("entity is required")
        if isinstance(period, BudgetPeriod):
            period_value = period.value
        else:
            period_value = (period or "").strip().lower()
        if period_value not in {p.value for p in BudgetPeriod}:
            raise BudgetValidationError(
                f"period must be one of {[p.value for p in BudgetPeriod]}"
            )
        amt = Decimal(str(amount))
        if amt <= 0:
            raise BudgetValidationError("amount must be > 0")
        if not (0.0 < float(alert_threshold) <= 1.0):
            raise BudgetValidationError("alert_threshold must be in (0, 1]")
        if open_accounts is None:
            open_accounts = []
        self.validate_pattern(account_pattern, open_accounts=open_accounts)

        cur = self.conn.execute(
            """
            INSERT INTO budgets (label, entity, account_pattern, period, amount, alert_threshold)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (label, entity, account_pattern, period_value, str(amt), float(alert_threshold)),
        )
        return self.get(int(cur.lastrowid))  # type: ignore[return-value]

    def update(
        self,
        budget_id: int,
        *,
        label: str | None = None,
        account_pattern: str | None = None,
        amount: Decimal | None = None,
        alert_threshold: float | None = None,
        period: str | BudgetPeriod | None = None,
        open_accounts: Iterable[str] | None = None,
    ) -> Budget:
        existing = self.get(budget_id)
        if existing is None:
            raise BudgetValidationError(f"budget {budget_id} not found")
        new_label = (label or existing.label).strip()
        new_pattern = (account_pattern or existing.account_pattern).strip()
        new_amount = Decimal(str(amount)) if amount is not None else existing.amount
        new_threshold = float(alert_threshold) if alert_threshold is not None else existing.alert_threshold
        new_period = (period.value if isinstance(period, BudgetPeriod)
                      else (period or existing.period.value)).strip().lower()
        if new_period not in {p.value for p in BudgetPeriod}:
            raise BudgetValidationError(f"period must be one of {[p.value for p in BudgetPeriod]}")
        if new_amount <= 0:
            raise BudgetValidationError("amount must be > 0")
        if not (0.0 < new_threshold <= 1.0):
            raise BudgetValidationError("alert_threshold must be in (0, 1]")
        if open_accounts is not None:
            self.validate_pattern(new_pattern, open_accounts=open_accounts)
        self.conn.execute(
            """
            UPDATE budgets
               SET label = ?, account_pattern = ?, amount = ?,
                   alert_threshold = ?, period = ?,
                   updated_at = CURRENT_TIMESTAMP
             WHERE id = ?
            """,
            (new_label, new_pattern, str(new_amount), new_threshold, new_period, budget_id),
        )
        return self.get(budget_id)  # type: ignore[return-value]

    def delete(self, budget_id: int) -> bool:
        cur = self.conn.execute("DELETE FROM budgets WHERE id = ?", (budget_id,))
        return cur.rowcount > 0

    def get(self, budget_id: int) -> Budget | None:
        row = self.conn.execute(
            "SELECT * FROM budgets WHERE id = ?", (budget_id,),
        ).fetchone()
        return _row_to_budget(row) if row else None

    def list(self, *, entity: str | None = None) -> list[Budget]:
        if entity:
            rows = self.conn.execute(
                "SELECT * FROM budgets WHERE entity = ? ORDER BY entity, period, label",
                (entity,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM budgets ORDER BY entity, period, label",
            ).fetchall()
        return [_row_to_budget(r) for r in rows]


def _row_to_budget(row: sqlite3.Row) -> Budget:
    period = BudgetPeriod(row["period"])
    return Budget(
        id=int(row["id"]),
        label=row["label"],
        entity=row["entity"],
        account_pattern=row["account_pattern"],
        period=period,
        amount=Decimal(row["amount"]),
        alert_threshold=float(row["alert_threshold"]),
        created_at=_parse_dt(row["created_at"]),
        updated_at=_parse_dt(row["updated_at"]),
    )


def _parse_dt(value) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None
