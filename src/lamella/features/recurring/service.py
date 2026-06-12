# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum


QUARANTINE_DAYS = 90


class RecurringStatus(str, Enum):
    PROPOSED = "proposed"
    CONFIRMED = "confirmed"
    IGNORED = "ignored"
    STOPPED = "stopped"  # subscription ended / cancelled — kept for history


class RecurringValidationError(ValueError):
    pass


@dataclass(frozen=True)
class RecurringExpense:
    id: int
    label: str
    entity: str
    expected_amount: Decimal
    expected_day: int | None
    source_account: str
    merchant_pattern: str
    cadence: str
    status: RecurringStatus
    last_seen: date | None
    next_expected: date | None
    created_at: datetime | None
    confirmed_at: datetime | None
    ignored_at: datetime | None


def _parse_dt(value) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _parse_date(value) -> date | None:
    if value is None or isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _row_to_expense(row: sqlite3.Row) -> RecurringExpense:
    return RecurringExpense(
        id=int(row["id"]),
        label=row["label"],
        entity=row["entity"],
        expected_amount=Decimal(row["expected_amount"]),
        expected_day=row["expected_day"],
        source_account=row["source_account"],
        merchant_pattern=row["merchant_pattern"],
        cadence=row["cadence"],
        status=RecurringStatus(row["status"]),
        last_seen=_parse_date(row["last_seen"]),
        next_expected=_parse_date(row["next_expected"]),
        created_at=_parse_dt(row["created_at"]),
        confirmed_at=_parse_dt(row["confirmed_at"]),
        ignored_at=_parse_dt(row["ignored_at"]),
    )


class RecurringService:
    """CRUD + status transitions for recurring_expenses. The detector
    upserts; this service is what the routes and after-ingest monitor
    use."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def get(self, recurring_id: int) -> RecurringExpense | None:
        row = self.conn.execute(
            "SELECT * FROM recurring_expenses WHERE id = ?", (recurring_id,),
        ).fetchone()
        return _row_to_expense(row) if row else None

    def list(self, *, status: str | None = None) -> list[RecurringExpense]:
        if status:
            rows = self.conn.execute(
                "SELECT * FROM recurring_expenses WHERE status = ? ORDER BY entity, label",
                (status,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM recurring_expenses ORDER BY status, entity, label",
            ).fetchall()
        return [_row_to_expense(r) for r in rows]

    def confirm(
        self,
        recurring_id: int,
        *,
        label: str | None = None,
        expected_day: int | None = None,
        source_account: str | None = None,
        open_accounts: set[str] | None = None,
    ) -> RecurringExpense:
        existing = self.get(recurring_id)
        if existing is None:
            raise RecurringValidationError(f"recurring {recurring_id} not found")
        new_label = (label or existing.label).strip()
        new_account = (source_account or existing.source_account).strip()
        if open_accounts is not None and new_account not in open_accounts:
            raise RecurringValidationError(
                f"source_account {new_account!r} is not opened in the ledger"
            )
        new_day = expected_day if expected_day is not None else existing.expected_day
        if new_day is not None and not (1 <= int(new_day) <= 31):
            raise RecurringValidationError("expected_day must be in 1..31")
        # ADR-0023: write TZ-aware ISO-8601 (+00:00) instead of SQLite's
        # naive CURRENT_TIMESTAMP, so the column round-trips through
        # datetime.fromisoformat to an aware datetime.
        now_iso = datetime.now(UTC).isoformat(timespec="seconds")
        self.conn.execute(
            """
            UPDATE recurring_expenses
               SET label = ?, expected_day = ?, source_account = ?,
                   status = 'confirmed', confirmed_at = ?,
                   ignored_at = NULL
             WHERE id = ?
            """,
            (new_label, new_day, new_account, now_iso, recurring_id),
        )
        return self.get(recurring_id)  # type: ignore[return-value]

    def ignore(self, recurring_id: int) -> RecurringExpense:
        existing = self.get(recurring_id)
        if existing is None:
            raise RecurringValidationError(f"recurring {recurring_id} not found")
        # ADR-0023: TZ-aware ISO-8601 at rest. See note in confirm().
        now_iso = datetime.now(UTC).isoformat(timespec="seconds")
        self.conn.execute(
            """
            UPDATE recurring_expenses
               SET status = 'ignored', ignored_at = ?,
                   confirmed_at = NULL
             WHERE id = ?
            """,
            (now_iso, recurring_id),
        )
        return self.get(recurring_id)  # type: ignore[return-value]

    def update_proposal(
        self,
        recurring_id: int,
        *,
        label: str | None = None,
        expected_day: int | None = None,
    ) -> RecurringExpense:
        """Edit a proposed row before confirming. Only `label` and
        `expected_day` are user-tweakable here; merchant_pattern + cadence
        are detection-derived and stable."""
        existing = self.get(recurring_id)
        if existing is None:
            raise RecurringValidationError(f"recurring {recurring_id} not found")
        if existing.status != RecurringStatus.PROPOSED:
            raise RecurringValidationError(
                f"only proposed rows can be edited; status={existing.status.value}"
            )
        new_label = (label or existing.label).strip()
        new_day = expected_day if expected_day is not None else existing.expected_day
        if new_day is not None and not (1 <= int(new_day) <= 31):
            raise RecurringValidationError("expected_day must be in 1..31")
        self.conn.execute(
            "UPDATE recurring_expenses SET label = ?, expected_day = ? WHERE id = ?",
            (new_label, new_day, recurring_id),
        )
        return self.get(recurring_id)  # type: ignore[return-value]

    def in_quarantine(
        self,
        merchant_pattern: str,
        source_account: str,
        *,
        now: datetime | None = None,
    ) -> bool:
        """Return True if an `ignored` row exists for the pair within
        QUARANTINE_DAYS of now. Detection callers should skip re-proposing
        in that case; after the quarantine expires the proposal returns."""
        row = self.conn.execute(
            """
            SELECT ignored_at FROM recurring_expenses
             WHERE merchant_pattern = ? AND source_account = ?
               AND status = 'ignored'
            """,
            (merchant_pattern, source_account),
        ).fetchone()
        if row is None or not row["ignored_at"]:
            return False
        ignored_at = _parse_dt(row["ignored_at"])
        if ignored_at is None:
            return False
        now = now or datetime.now(UTC)
        # ADR-0023 invariant: ignored_at is TZ-aware UTC at rest (mig 058
        # backfilled the legacy naive CURRENT_TIMESTAMP rows; current
        # writers in confirm()/ignore() emit isoformat(+00:00)). The
        # ADR-0023 bridge that normalized naive-vs-aware comparisons was
        # removed when 058 landed.
        return (now - ignored_at) < timedelta(days=QUARANTINE_DAYS)

    def upsert(
        self,
        *,
        label: str,
        entity: str,
        expected_amount: Decimal,
        expected_day: int | None,
        source_account: str,
        merchant_pattern: str,
        cadence: str,
        last_seen: date,
        next_expected: date,
    ) -> tuple[RecurringExpense, str]:
        """Insert as proposed, or update an existing row. Returns the row
        and one of ``inserted`` | ``updated`` | ``skipped`` (skipped when
        an ignored row is in quarantine)."""
        now = datetime.now(UTC)
        if self.in_quarantine(merchant_pattern, source_account, now=now):
            existing_row = self.conn.execute(
                "SELECT * FROM recurring_expenses WHERE merchant_pattern=? AND source_account=?",
                (merchant_pattern, source_account),
            ).fetchone()
            return _row_to_expense(existing_row), "skipped"

        existing = self.conn.execute(
            "SELECT * FROM recurring_expenses WHERE merchant_pattern=? AND source_account=?",
            (merchant_pattern, source_account),
        ).fetchone()
        if existing is None:
            cur = self.conn.execute(
                """
                INSERT INTO recurring_expenses
                    (label, entity, expected_amount, expected_day, source_account,
                     merchant_pattern, cadence, status, last_seen, next_expected)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'proposed', ?, ?)
                """,
                (
                    label, entity, str(expected_amount), expected_day,
                    source_account, merchant_pattern, cadence,
                    last_seen.isoformat(), next_expected.isoformat(),
                ),
            )
            return self.get(int(cur.lastrowid)), "inserted"  # type: ignore[return-value]

        # Existing row exists — update last_seen / next_expected.
        # If the row was confirmed, do an EMA on expected_amount (per plan).
        existing_obj = _row_to_expense(existing)
        if existing_obj.status == RecurringStatus.CONFIRMED:
            new_amount = (
                Decimal("0.8") * existing_obj.expected_amount
                + Decimal("0.2") * expected_amount
            )
        else:
            new_amount = expected_amount
        self.conn.execute(
            """
            UPDATE recurring_expenses
               SET last_seen = ?, next_expected = ?, expected_amount = ?
             WHERE id = ?
            """,
            (last_seen.isoformat(), next_expected.isoformat(), str(new_amount), existing_obj.id),
        )
        return self.get(existing_obj.id), "updated"  # type: ignore[return-value]

    def mark_seen(
        self,
        recurring_id: int,
        *,
        last_seen: date,
        next_expected: date,
    ) -> bool:
        cur = self.conn.execute(
            """
            UPDATE recurring_expenses
               SET last_seen = ?, next_expected = ?
             WHERE id = ?
            """,
            (last_seen.isoformat(), next_expected.isoformat(), recurring_id),
        )
        return cur.rowcount > 0
