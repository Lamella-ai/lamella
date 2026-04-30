# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 18: day_reviews reconstruct from `custom "day-review"`.

Mirrors step16 for notes. Each day's review state is round-tripped
through a `custom "day-review"` directive with `lamella-day: YYYY-MM-DD`
as the identifier metadata. Tombstones via `custom "day-review-deleted"`
so dropping a day's review clears the SQLite row.

The free-text day note is NOT stored here — day notes live in the
`notes` table as single-day, unscoped rows, and step16 handles
them. Keeping the two concerns separate keeps one storage + one
pipeline into classify_txn.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date
from typing import Any, Iterable

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    custom_arg,
    custom_meta,
)
from lamella.core.transform.reconstruct import ReconstructReport, register

log = logging.getLogger(__name__)


def _str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _date(v: Any) -> date | None:
    if v is None:
        return None
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v).strip())
    except ValueError:
        return None


def _read_day_reviews(entries: Iterable[Any]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    deleted: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type == "day-review-deleted":
            d = _date(custom_meta(entry, "lamella-day") or custom_arg(entry, 0))
            if d is not None:
                key = d.isoformat()
                deleted.add(key)
                rows.pop(key, None)
            continue
        if entry.type != "day-review":
            continue
        review_date = _date(custom_meta(entry, "lamella-day") or custom_arg(entry, 0))
        if review_date is None or review_date.isoformat() in deleted:
            continue
        rows[review_date.isoformat()] = {
            "review_date": review_date.isoformat(),
            "last_reviewed_at": _str(custom_meta(entry, "lamella-last-reviewed-at")),
            "ai_summary": _str(custom_meta(entry, "lamella-ai-summary")),
            "ai_summary_at": _str(custom_meta(entry, "lamella-ai-summary-at")),
            "ai_audit_result": _str(custom_meta(entry, "lamella-ai-audit-result")),
            "ai_audit_result_at": _str(custom_meta(entry, "lamella-ai-audit-result-at")),
        }
    return list(rows.values())


@register(
    "step18:day_reviews",
    state_tables=["day_reviews"],
)
def reconstruct_day_reviews(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(day_reviews)")]
    if not cols:
        return ReconstructReport(
            pass_name="step18:day_reviews",
            rows_written=0,
            notes=["day_reviews table missing — migration 044 not applied?"],
        )

    written = 0
    for row in _read_day_reviews(entries):
        values = {
            "review_date": row["review_date"],
            "last_reviewed_at": row["last_reviewed_at"],
            "ai_summary": row["ai_summary"],
            "ai_summary_at": row["ai_summary_at"],
            "ai_audit_result": row["ai_audit_result"],
            "ai_audit_result_at": row["ai_audit_result_at"],
        }
        cols_present = [k for k in values if k in cols]
        placeholders = ", ".join("?" for _ in cols_present)
        col_list = ", ".join(cols_present)
        conn.execute(
            f"INSERT OR REPLACE INTO day_reviews ({col_list}) VALUES ({placeholders})",
            tuple(values[k] for k in cols_present),
        )
        written += 1
    return ReconstructReport(
        pass_name="step18:day_reviews",
        rows_written=written,
        notes=[f"rebuilt {written} day review(s)"] if written else [],
    )
