# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the mileage back-fill audit cache.

The cache is a pure projection of ``mileage_entries.created_at`` vs
``entry_date`` — if the table is dropped, running rebuild regenerates
an identical copy. These tests pin:
  - threshold behavior (gap < 2d not counted, >= 2d counted)
  - aggregation (one row per date, correct count/max)
  - record_backfill purge when the last back-filled row for a date
    goes away
  - rebuild is idempotent
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.mileage.backfill_audit import (
    BACKFILL_THRESHOLD_DAYS,
    list_backfill_dates,
    record_backfill,
    rebuild_mileage_backfill_audit,
)


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _insert_mileage(
    conn, *,
    entry_date: str, created_at: str, vehicle: str = "Car A",
    entity: str = "Personal", miles: float = 10.0,
):
    conn.execute(
        """
        INSERT INTO mileage_entries
            (entry_date, vehicle, miles, entity, created_at, source)
        VALUES (?, ?, ?, ?, ?, 'manual')
        """,
        (entry_date, vehicle, miles, entity, created_at),
    )


class TestRebuild:
    def test_same_day_entries_are_not_back_fill(self, conn):
        # Logged 4 hours after the trip — same calendar day, not a back-fill.
        _insert_mileage(
            conn, entry_date="2026-04-20",
            created_at="2026-04-20 14:00:00",
        )
        n = rebuild_mileage_backfill_audit(conn)
        assert n == 0
        assert list_backfill_dates(conn) == []

    def test_next_day_is_not_back_fill(self, conn):
        # 1-day gap — typical "logged yesterday's trips this morning".
        _insert_mileage(
            conn, entry_date="2026-04-20",
            created_at="2026-04-21 09:00:00",
        )
        n = rebuild_mileage_backfill_audit(conn)
        assert n == 0

    def test_two_day_gap_is_back_fill(self, conn):
        _insert_mileage(
            conn, entry_date="2026-04-18",
            created_at="2026-04-20 09:00:00",
        )
        n = rebuild_mileage_backfill_audit(conn)
        assert n == 1
        rows = list_backfill_dates(conn)
        assert len(rows) == 1
        assert rows[0].entry_date.isoformat() == "2026-04-18"
        assert rows[0].backfill_entry_count == 1
        assert rows[0].gap_days_max == 2

    def test_years_old_import_is_detected(self, conn):
        """The user scenario — importing a paper log from 2019 in 2026."""
        _insert_mileage(
            conn, entry_date="2019-06-15",
            created_at="2026-04-20 10:00:00",
        )
        rebuild_mileage_backfill_audit(conn)
        rows = list_backfill_dates(conn)
        assert len(rows) == 1
        assert rows[0].gap_days_max > 2000

    def test_multiple_rows_on_same_date_aggregate(self, conn):
        _insert_mileage(
            conn, entry_date="2026-04-10",
            created_at="2026-04-15 10:00:00",
        )
        _insert_mileage(
            conn, entry_date="2026-04-10",
            created_at="2026-04-20 10:00:00",
            vehicle="Car B",
        )
        rebuild_mileage_backfill_audit(conn)
        rows = list_backfill_dates(conn)
        assert len(rows) == 1
        assert rows[0].backfill_entry_count == 2
        assert rows[0].gap_days_max == 10  # latest backfill was 10 days later

    def test_rebuild_is_idempotent(self, conn):
        _insert_mileage(
            conn, entry_date="2026-04-10",
            created_at="2026-04-15 10:00:00",
        )
        rebuild_mileage_backfill_audit(conn)
        rows_first = list_backfill_dates(conn)
        rebuild_mileage_backfill_audit(conn)
        rows_second = list_backfill_dates(conn)
        assert len(rows_first) == len(rows_second) == 1
        assert rows_first[0].entry_date == rows_second[0].entry_date

    def test_rebuild_ignores_non_backfill_dates(self, conn):
        """Back-filled + same-day row for the same date — the audit
        row should still exist because one of the rows is a back-fill."""
        _insert_mileage(
            conn, entry_date="2026-04-10",
            created_at="2026-04-10 10:00:00",
        )
        _insert_mileage(
            conn, entry_date="2026-04-10",
            created_at="2026-04-20 10:00:00",
            vehicle="Car B",
        )
        rebuild_mileage_backfill_audit(conn)
        rows = list_backfill_dates(conn)
        assert len(rows) == 1
        # The count is the number of BACK-FILLED rows on this date — not
        # total rows — so the same-day row is excluded.
        assert rows[0].backfill_entry_count == 1

    def test_threshold_constant_matches_default(self):
        assert BACKFILL_THRESHOLD_DAYS == 2


class TestRecordBackfillIncremental:
    def test_record_promotes_a_date(self, conn):
        _insert_mileage(
            conn, entry_date="2026-04-10",
            created_at="2026-04-15 10:00:00",
        )
        # Simulate the MileageService.add_entry hook.
        wrote = record_backfill(conn, entry_date="2026-04-10")
        assert wrote is True
        assert len(list_backfill_dates(conn)) == 1

    def test_record_purges_when_no_backfill_rows_remain(self, conn):
        _insert_mileage(
            conn, entry_date="2026-04-10",
            created_at="2026-04-15 10:00:00",
        )
        record_backfill(conn, entry_date="2026-04-10")
        assert len(list_backfill_dates(conn)) == 1
        # User deletes the back-filled row.
        conn.execute(
            "DELETE FROM mileage_entries WHERE entry_date = ?",
            ("2026-04-10",),
        )
        wrote = record_backfill(conn, entry_date="2026-04-10")
        assert wrote is False
        assert list_backfill_dates(conn) == []

    def test_record_ignores_same_day_entry(self, conn):
        _insert_mileage(
            conn, entry_date="2026-04-20",
            created_at="2026-04-20 10:00:00",
        )
        wrote = record_backfill(conn, entry_date="2026-04-20")
        assert wrote is False
        assert list_backfill_dates(conn) == []


class TestSelfHealing:
    def test_rebuild_heals_a_dropped_table(self, conn):
        """Simulate cache corruption: drop the audit table entirely,
        re-create it (as the migration would on a fresh DB), and
        rebuild. The result must match the pre-drop state — the cache
        is pure projection, so there's nothing to lose."""
        _insert_mileage(
            conn, entry_date="2026-04-10",
            created_at="2026-04-15 10:00:00",
        )
        rebuild_mileage_backfill_audit(conn)
        before = list_backfill_dates(conn)

        conn.execute("DROP TABLE mileage_backfill_audit")
        # Re-apply the schema directly (migrate() tracks applied
        # migrations so it won't re-create a dropped table; the
        # production repair path is "drop + re-create + rebuild").
        conn.executescript(
            Path("migrations/042_mileage_backfill_audit.sql").read_text()
        )
        rebuild_mileage_backfill_audit(conn)
        after = list_backfill_dates(conn)

        assert len(before) == len(after) == 1
        assert before[0].entry_date == after[0].entry_date
        assert before[0].backfill_entry_count == after[0].backfill_entry_count

    def test_rebuild_on_missing_table_is_noop(self, conn):
        """Defensive: if the table is somehow missing at rebuild time,
        the function returns 0 and doesn't crash."""
        conn.execute("DROP TABLE mileage_backfill_audit")
        assert rebuild_mileage_backfill_audit(conn) == 0
        assert list_backfill_dates(conn) == []
