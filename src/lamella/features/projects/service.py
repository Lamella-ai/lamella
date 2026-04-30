# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Project CRUD + the "active projects relevant to this txn" query
that classify calls into."""
from __future__ import annotations

import json
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

log = logging.getLogger(__name__)


_SLUG_RE = re.compile(r"[a-z0-9][a-z0-9_-]*")


def is_valid_project_slug(slug: str) -> bool:
    return bool(_SLUG_RE.fullmatch((slug or "").lower()))


@dataclass
class Project:
    slug: str
    display_name: str
    description: str | None = None
    entity_slug: str | None = None
    property_slug: str | None = None
    project_type: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    budget_amount: Decimal | None = None
    expected_merchants: list[str] = field(default_factory=list)
    is_active: bool = True
    closed_at: str | None = None
    closeout_json: dict | None = None
    notes: str | None = None
    # When this project is a restart/continuation of an earlier
    # one (paused-then-resumed fence build, etc.), the earlier
    # project's slug. Lets closeout reports roll aggregate
    # spending across the full chain.
    previous_project_slug: str | None = None

    @property
    def is_open(self) -> bool:
        return self.is_active and not self.closed_at

    def covers(self, txn_date: date) -> bool:
        if self.start_date is None:
            return False
        if txn_date < self.start_date:
            return False
        if self.end_date is not None and txn_date > self.end_date:
            return False
        return True

    def matches_merchant(self, merchant_text: str) -> bool:
        if not merchant_text:
            return False
        needle = merchant_text.lower()
        for m in self.expected_merchants:
            if not m:
                continue
            if m.lower() in needle:
                return True
        return False


def _row_to_project(row: sqlite3.Row) -> Project:
    merchants_raw = row["expected_merchants"] or "[]"
    try:
        merchants = json.loads(merchants_raw)
        if not isinstance(merchants, list):
            merchants = []
    except ValueError:
        merchants = []
    closeout_json = None
    if row["closeout_json"]:
        try:
            closeout_json = json.loads(row["closeout_json"])
        except ValueError:
            closeout_json = None
    budget: Decimal | None = None
    if row["budget_amount"]:
        try:
            budget = Decimal(str(row["budget_amount"]))
        except Exception:  # noqa: BLE001
            budget = None
    # `previous_project_slug` is added by migration 029; access
    # defensively so older DBs without the column don't blow up.
    try:
        previous = row["previous_project_slug"]
    except (IndexError, KeyError):
        previous = None
    return Project(
        slug=row["slug"],
        display_name=row["display_name"] or row["slug"],
        description=row["description"],
        entity_slug=row["entity_slug"],
        property_slug=row["property_slug"],
        project_type=row["project_type"],
        start_date=date.fromisoformat(row["start_date"]) if row["start_date"] else None,
        end_date=date.fromisoformat(row["end_date"]) if row["end_date"] else None,
        budget_amount=budget,
        expected_merchants=list(merchants),
        is_active=bool(row["is_active"]),
        closed_at=row["closed_at"],
        closeout_json=closeout_json,
        notes=row["notes"],
        previous_project_slug=previous,
    )


class ProjectService:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    # --- reads ----------------------------------------------------

    def list_all(self, *, active_only: bool = False) -> list[Project]:
        sql = "SELECT * FROM projects"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_active DESC, start_date DESC, slug"
        rows = self.conn.execute(sql).fetchall()
        return [_row_to_project(r) for r in rows]

    def get(self, slug: str) -> Project | None:
        row = self.conn.execute(
            "SELECT * FROM projects WHERE slug = ?", (slug,),
        ).fetchone()
        return _row_to_project(row) if row else None

    def active_for(
        self, *, txn_date: date, merchant_text: str = "",
    ) -> list[Project]:
        """All active projects whose date range covers the txn
        date AND whose expected-merchants list matches the
        merchant text. Used by the classifier."""
        rows = self.conn.execute(
            """
            SELECT * FROM projects
             WHERE is_active = 1
               AND start_date <= ?
               AND (end_date IS NULL OR end_date >= ?)
            """,
            (txn_date.isoformat(), txn_date.isoformat()),
        ).fetchall()
        candidates = [_row_to_project(r) for r in rows]
        if not merchant_text:
            # No merchant to match; return nothing so classify
            # doesn't get every project on every classify call.
            return []
        return [p for p in candidates if p.matches_merchant(merchant_text)]

    # --- writes ---------------------------------------------------

    def upsert(
        self,
        *,
        slug: str,
        display_name: str,
        description: str | None = None,
        entity_slug: str | None = None,
        property_slug: str | None = None,
        project_type: str | None = None,
        start_date: str | date,
        end_date: str | date | None = None,
        budget_amount: str | Decimal | None = None,
        expected_merchants: list[str] | None = None,
        is_active: bool = True,
        notes: str | None = None,
        previous_project_slug: str | None = None,
    ) -> None:
        if not is_valid_project_slug(slug):
            raise ValueError(f"invalid project slug {slug!r}")
        start_iso = _to_iso(start_date)
        end_iso = _to_iso(end_date) if end_date else None
        budget_str = _to_decimal_str(budget_amount)
        merchants_json = json.dumps(expected_merchants or [])
        existing = self.conn.execute(
            "SELECT slug FROM projects WHERE slug = ?", (slug,),
        ).fetchone()
        if existing is None:
            self.conn.execute(
                """
                INSERT INTO projects (
                    slug, display_name, description, entity_slug,
                    property_slug, project_type, start_date, end_date,
                    budget_amount, expected_merchants, is_active, notes,
                    previous_project_slug
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slug, display_name, description, entity_slug,
                    property_slug, project_type, start_iso, end_iso,
                    budget_str, merchants_json,
                    1 if is_active else 0, notes, previous_project_slug,
                ),
            )
        else:
            self.conn.execute(
                """
                UPDATE projects SET
                    display_name = ?,
                    description = ?,
                    entity_slug = ?,
                    property_slug = ?,
                    project_type = ?,
                    start_date = ?,
                    end_date = ?,
                    budget_amount = ?,
                    expected_merchants = ?,
                    is_active = ?,
                    notes = ?,
                    previous_project_slug = ?,
                    updated_at = CURRENT_TIMESTAMP
                 WHERE slug = ?
                """,
                (
                    display_name, description, entity_slug, property_slug,
                    project_type, start_iso, end_iso, budget_str,
                    merchants_json, 1 if is_active else 0, notes,
                    previous_project_slug, slug,
                ),
            )

    def close(self, slug: str, *, closeout: dict) -> None:
        self.conn.execute(
            """
            UPDATE projects
               SET is_active = 0,
                   closed_at = CURRENT_TIMESTAMP,
                   closeout_json = ?,
                   updated_at = CURRENT_TIMESTAMP
             WHERE slug = ?
            """,
            (json.dumps(closeout, default=str), slug),
        )

    def delete(self, slug: str) -> None:
        self.conn.execute("DELETE FROM projects WHERE slug = ?", (slug,))

    # --- txn attribution ------------------------------------------

    def record_txn(
        self,
        *,
        project_slug: str,
        txn_hash: str,
        txn_date: date,
        txn_amount: Decimal,
        merchant_text: str | None = None,
        decided_by: str = "ai",
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO project_txns
                (project_slug, txn_hash, txn_date, txn_amount,
                 merchant_text, decided_by)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                project_slug, txn_hash, txn_date.isoformat(),
                str(txn_amount), merchant_text, decided_by,
            ),
        )

    def txns_for(self, slug: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT txn_hash, txn_date, txn_amount, merchant_text,
                   decided_by, decided_at
              FROM project_txns
             WHERE project_slug = ?
             ORDER BY txn_date DESC
            """,
            (slug,),
        ).fetchall()
        return [dict(r) for r in rows]

    def chain(self, slug: str) -> list[Project]:
        """Return the full continuation chain for `slug`, oldest
        first. Walks `previous_project_slug` backward and any
        descendants forward."""
        seen: set[str] = set()
        # Walk back.
        backward: list[Project] = []
        cur = self.get(slug)
        while cur and cur.slug not in seen:
            seen.add(cur.slug)
            backward.append(cur)
            if not cur.previous_project_slug:
                break
            cur = self.get(cur.previous_project_slug)
        backward.reverse()
        # Walk forward (anyone pointing at us).
        chain = list(backward)
        frontier = [chain[-1].slug] if chain else []
        while frontier:
            next_frontier: list[str] = []
            rows = self.conn.execute(
                f"SELECT * FROM projects WHERE previous_project_slug "
                f"IN ({','.join('?' * len(frontier))})",
                tuple(frontier),
            ).fetchall()
            for r in rows:
                child = _row_to_project(r)
                if child.slug in seen:
                    continue
                seen.add(child.slug)
                chain.append(child)
                next_frontier.append(child.slug)
            frontier = next_frontier
        return chain

    def totals_for(self, slug: str) -> dict[str, Any]:
        # ADR-0022: money sums must be aggregated as Decimal, not via
        # SQL CAST AS REAL (which silently coerces through binary
        # float). Pull rows under the tight project_slug predicate and
        # sum app-side with Decimal.
        rows = self.conn.execute(
            """
            SELECT txn_amount
              FROM project_txns
             WHERE project_slug = ?
            """,
            (slug,),
        ).fetchall()
        total = Decimal("0")
        for r in rows:
            raw = r["txn_amount"]
            if raw is None or raw == "":
                continue
            try:
                total += Decimal(str(raw))
            except Exception:  # noqa: BLE001
                continue
        return {"n": len(rows), "total": total}


def _to_iso(d: str | date | None) -> str | None:
    if d is None:
        return None
    if isinstance(d, date):
        return d.isoformat()
    s = str(d).strip()
    return s or None


def _to_decimal_str(v) -> str | None:
    if v is None or v == "":
        return None
    try:
        if isinstance(v, Decimal):
            return str(v)
        return str(Decimal(str(v)))
    except Exception:  # noqa: BLE001
        return None


# ------------------------------------------------------------------
# Convenience function for classify integration
# ------------------------------------------------------------------


def active_projects_for_txn(
    conn: sqlite3.Connection | None,
    *,
    txn_date: date,
    merchant_text: str,
) -> list[Project]:
    """Return projects that apply to this specific txn (active +
    date-covered + merchant-matched). Classify calls this and
    renders each project's description into the prompt."""
    if conn is None:
        return []
    try:
        return ProjectService(conn).active_for(
            txn_date=txn_date, merchant_text=merchant_text,
        )
    except sqlite3.Error:
        return []
