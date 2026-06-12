# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from lamella.ports.notification import Channel, Notifier, NotifierResult
from lamella.features.notifications.digests import (
    WEEKDAYS,
    build_weekly_digest,
    maybe_send_weekly_digest,
)
from lamella.features.notifications.dispatcher import Dispatcher


FIXTURES = Path(__file__).parent / "fixtures" / "mileage"


class _FakeNotifier(Notifier):
    channel = Channel.NTFY

    def __init__(self):
        self.sent = []

    def enabled(self) -> bool:
        return True

    async def send(self, event):
        self.sent.append(event)
        return NotifierResult(ok=True)


def _seed_review(db, count: int) -> None:
    for i in range(count):
        db.execute(
            "INSERT INTO review_queue (kind, source_ref) VALUES (?, ?)",
            ("fixme", f"fixme:test:{i}"),
        )


def _copy_mileage(tmp_path: Path) -> Path:
    src = FIXTURES / "vehicles_two_vehicles.csv"
    dest = tmp_path / "vehicles.csv"
    shutil.copy(src, dest)
    return dest


def test_build_weekly_digest_collects_counts(db, tmp_path: Path):
    _seed_review(db, 3)
    csv_path = _copy_mileage(tmp_path)
    # Use a "now" in the recent past so all the seeded rows are within the
    # last 7 days for the count.
    now = datetime(2026, 4, 20, 9, 0, tzinfo=timezone.utc)
    digest = build_weekly_digest(conn=db, mileage_csv_path=csv_path, now=now)
    assert digest.open_reviews == 3
    assert digest.is_empty() is False


def test_empty_digest_recognized(db, tmp_path: Path):
    digest = build_weekly_digest(conn=db, mileage_csv_path=tmp_path / "missing.csv")
    assert digest.is_empty()


async def test_maybe_send_skips_off_day(db, tmp_path: Path):
    _seed_review(db, 1)
    csv_path = _copy_mileage(tmp_path)
    notifier = _FakeNotifier()
    disp = Dispatcher(conn=db, notifiers=[notifier])
    # Pick a day other than the digest_day.
    now = datetime(2026, 4, 21, 9, 0, tzinfo=timezone.utc)  # Tuesday
    res = await maybe_send_weekly_digest(
        dispatcher=disp,
        conn=db,
        mileage_csv_path=csv_path,
        digest_day="Monday",
        now=now,
    )
    assert res is None
    assert notifier.sent == []


async def test_maybe_send_dispatches_when_day_matches_and_nonzero(db, tmp_path: Path):
    _seed_review(db, 1)
    csv_path = _copy_mileage(tmp_path)
    notifier = _FakeNotifier()
    disp = Dispatcher(conn=db, notifiers=[notifier])
    now = datetime(2026, 4, 20, 9, 0, tzinfo=timezone.utc)  # Monday
    digest = await maybe_send_weekly_digest(
        dispatcher=disp,
        conn=db,
        mileage_csv_path=csv_path,
        digest_day="Monday",
        now=now,
    )
    assert digest is not None
    assert digest.is_empty() is False
    assert len(notifier.sent) == 1


async def test_maybe_send_skips_when_empty_on_correct_day(db, tmp_path: Path):
    notifier = _FakeNotifier()
    disp = Dispatcher(conn=db, notifiers=[notifier])
    now = datetime(2026, 4, 20, 9, 0, tzinfo=timezone.utc)  # Monday
    digest = await maybe_send_weekly_digest(
        dispatcher=disp,
        conn=db,
        mileage_csv_path=tmp_path / "missing.csv",
        digest_day="Monday",
        now=now,
    )
    assert digest is not None  # day matched
    assert digest.is_empty()
    assert notifier.sent == []
