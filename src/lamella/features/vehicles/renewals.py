# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 6 — vehicle renewals (registration / inspection / insurance).

Captures due dates with optional cadence_months for auto-advance on
"mark complete." The data-health panel registers a `renewal_past_due`
check; the detail page surfaces upcoming renewals; a separate helper
feeds the existing weekly notification digest.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date as date_t


VALID_RENEWAL_KINDS = {"registration", "inspection", "insurance", "other"}


@dataclass(frozen=True)
class Renewal:
    id: int
    vehicle_slug: str
    renewal_kind: str
    due_date: date_t
    cadence_months: int | None
    last_completed: date_t | None
    notes: str | None
    is_active: bool


def _coerce_date(raw) -> date_t:
    if isinstance(raw, date_t):
        return raw
    return date_t.fromisoformat(str(raw)[:10])


def _row_to_renewal(row: sqlite3.Row) -> Renewal:
    last = None
    if row["last_completed"]:
        try:
            last = _coerce_date(row["last_completed"])
        except ValueError:
            last = None
    return Renewal(
        id=int(row["id"]),
        vehicle_slug=row["vehicle_slug"],
        renewal_kind=row["renewal_kind"],
        due_date=_coerce_date(row["due_date"]),
        cadence_months=(
            int(row["cadence_months"])
            if row["cadence_months"] is not None else None
        ),
        last_completed=last,
        notes=row["notes"],
        is_active=bool(row["is_active"]),
    )


def add_renewal(
    conn: sqlite3.Connection,
    *,
    vehicle_slug: str,
    renewal_kind: str,
    due_date: date_t,
    cadence_months: int | None = None,
    notes: str | None = None,
    connector_config_path=None,
    main_bean_path=None,
) -> int:
    if renewal_kind not in VALID_RENEWAL_KINDS:
        raise ValueError(f"invalid renewal_kind {renewal_kind!r}")
    cur = conn.execute(
        """
        INSERT INTO vehicle_renewals
            (vehicle_slug, renewal_kind, due_date, cadence_months, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            vehicle_slug, renewal_kind, due_date.isoformat(),
            int(cadence_months) if cadence_months else None,
            notes,
        ),
    )
    if connector_config_path is not None and main_bean_path is not None:
        try:
            from lamella.features.vehicles.writer import append_vehicle_renewal
            append_vehicle_renewal(
                connector_config=connector_config_path,
                main_bean=main_bean_path,
                slug=vehicle_slug, renewal_kind=renewal_kind,
                due_date=due_date,
                cadence_months=int(cadence_months) if cadence_months else None,
                notes=notes, is_active=True,
            )
        except Exception as exc:  # noqa: BLE001
            import logging as _l
            _l.getLogger(__name__).warning(
                "vehicle-renewal directive write failed for %s %s: %s",
                vehicle_slug, renewal_kind, exc,
            )
    return int(cur.lastrowid)


def complete_renewal(
    conn: sqlite3.Connection,
    renewal_id: int,
    *,
    completed_on: date_t | None = None,
) -> Renewal | None:
    """Stamp `last_completed` on the row. If cadence_months is set,
    advance due_date by that many months; otherwise deactivate."""
    row = conn.execute(
        "SELECT * FROM vehicle_renewals WHERE id = ?", (int(renewal_id),),
    ).fetchone()
    if row is None:
        return None
    completed = completed_on or date_t.today()
    cadence = row["cadence_months"]
    if cadence:
        cad_m = int(cadence)
        old = _coerce_date(row["due_date"])
        new_year = old.year + (old.month - 1 + cad_m) // 12
        new_month = (old.month - 1 + cad_m) % 12 + 1
        # Clamp day to the new month's last day.
        from calendar import monthrange
        new_day = min(old.day, monthrange(new_year, new_month)[1])
        new_due = date_t(new_year, new_month, new_day)
        conn.execute(
            """
            UPDATE vehicle_renewals
               SET due_date = ?, last_completed = ?, is_active = 1
             WHERE id = ?
            """,
            (new_due.isoformat(), completed.isoformat(), int(renewal_id)),
        )
    else:
        conn.execute(
            """
            UPDATE vehicle_renewals
               SET last_completed = ?, is_active = 0
             WHERE id = ?
            """,
            (completed.isoformat(), int(renewal_id)),
        )
    updated = conn.execute(
        "SELECT * FROM vehicle_renewals WHERE id = ?", (int(renewal_id),),
    ).fetchone()
    return _row_to_renewal(updated) if updated else None


def delete_renewal(conn: sqlite3.Connection, renewal_id: int) -> bool:
    cur = conn.execute(
        "DELETE FROM vehicle_renewals WHERE id = ?", (int(renewal_id),),
    )
    return bool(cur.rowcount and cur.rowcount > 0)


def list_renewals(
    conn: sqlite3.Connection, vehicle_slug: str,
    *, include_inactive: bool = True,
) -> list[Renewal]:
    clause = "vehicle_slug = ?"
    params: list = [vehicle_slug]
    if not include_inactive:
        clause += " AND is_active = 1"
    rows = conn.execute(
        f"SELECT * FROM vehicle_renewals WHERE {clause} "
        f"ORDER BY is_active DESC, due_date ASC",
        tuple(params),
    ).fetchall()
    return [_row_to_renewal(r) for r in rows]


def list_due_soon(
    conn: sqlite3.Connection,
    *,
    within_days: int = 14,
    today: date_t | None = None,
) -> list[Renewal]:
    """Feed for the notification digest: active renewals whose
    due_date falls within the next `within_days`."""
    today = today or date_t.today()
    from datetime import timedelta
    cutoff = today + timedelta(days=int(within_days))
    rows = conn.execute(
        "SELECT * FROM vehicle_renewals "
        "WHERE is_active = 1 AND due_date <= ? "
        "ORDER BY due_date ASC",
        (cutoff.isoformat(),),
    ).fetchall()
    return [_row_to_renewal(r) for r in rows]
