# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from lamella.features.mileage.service import MileageService
from lamella.ports.notification import NotificationEvent, Priority
from lamella.features.notifications.dispatcher import Dispatcher

log = logging.getLogger(__name__)


WEEKDAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


@dataclass(frozen=True)
class WeeklyDigest:
    open_reviews: int
    new_rules: int
    mileage_miles: float
    notifications_sent: int
    iso_week: str  # "YYYY-WW"

    def is_empty(self) -> bool:
        return (
            self.open_reviews == 0
            and self.new_rules == 0
            and self.mileage_miles <= 0
            and self.notifications_sent == 0
        )

    def to_event(self, *, url: str | None = None) -> NotificationEvent:
        body = (
            f"Open reviews: {self.open_reviews}\n"
            f"New rules learned: {self.new_rules}\n"
            f"Mileage logged: {self.mileage_miles:.1f} mi\n"
            f"Notifications sent: {self.notifications_sent}"
        )
        return NotificationEvent(
            dedup_key=f"digest:{self.iso_week}",
            priority=Priority.INFO,
            title=f"Lamella weekly digest ({self.iso_week})",
            body=body,
            url=url,
        )


def _iso_week_key(when: datetime) -> str:
    iso_year, iso_week, _ = when.isocalendar()
    return f"{iso_year:04d}-{iso_week:02d}"


def build_weekly_digest(
    *,
    conn: sqlite3.Connection,
    mileage_csv_path: Path,
    now: datetime | None = None,
) -> WeeklyDigest:
    now = now or datetime.now(timezone.utc)
    week_start = (now - timedelta(days=7)).isoformat(timespec="seconds")

    open_reviews_row = conn.execute(
        "SELECT COUNT(*) AS n FROM review_queue WHERE resolved_at IS NULL"
    ).fetchone()
    open_reviews = int(open_reviews_row["n"]) if open_reviews_row else 0

    new_rules_row = conn.execute(
        """
        SELECT COUNT(*) AS n FROM classification_rules
         WHERE last_used IS NOT NULL AND last_used >= ?
        """,
        (week_start,),
    ).fetchone()
    new_rules = int(new_rules_row["n"]) if new_rules_row else 0

    mileage = MileageService(conn=conn, csv_path=mileage_csv_path)
    try:
        mileage.refresh_cache()
    except Exception as exc:  # noqa: BLE001
        log.warning("weekly digest: mileage refresh failed: %s", exc)
    miles_row = conn.execute(
        """
        SELECT COALESCE(SUM(miles), 0) AS miles FROM mileage_entries
         WHERE entry_date >= ?
        """,
        (week_start[:10],),
    ).fetchone()
    miles = float(miles_row["miles"] or 0)

    notifs_row = conn.execute(
        """
        SELECT COUNT(*) AS n FROM notifications
         WHERE sent_at >= ? AND delivered = 1
        """,
        (week_start,),
    ).fetchone()
    notifs = int(notifs_row["n"]) if notifs_row else 0

    return WeeklyDigest(
        open_reviews=open_reviews,
        new_rules=new_rules,
        mileage_miles=miles,
        notifications_sent=notifs,
        iso_week=_iso_week_key(now),
    )


async def maybe_send_weekly_digest(
    *,
    dispatcher: Dispatcher,
    conn: sqlite3.Connection,
    mileage_csv_path: Path,
    digest_day: str,
    now: datetime | None = None,
) -> WeeklyDigest | None:
    """Build the digest and dispatch it if (a) today matches ``digest_day``
    and (b) the digest has non-zero activity. Returns the digest that was
    considered (None if the day didn't match)."""
    now = now or datetime.now(timezone.utc)
    target = (digest_day or "Monday").strip().capitalize()
    if target not in WEEKDAYS:
        target = "Monday"
    if WEEKDAYS[now.weekday()] != target:
        return None
    digest = build_weekly_digest(conn=conn, mileage_csv_path=mileage_csv_path, now=now)
    if digest.is_empty():
        return digest
    await dispatcher.send(digest.to_event(url="/"))
    return digest
