# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from dataclasses import dataclass

import pytest

from lamella.ports.notification import (
    Channel,
    NotificationEvent,
    Notifier,
    NotifierResult,
    Priority,
)
from lamella.features.notifications.dispatcher import Dispatcher


class _FakeClock:
    def __init__(self, t: float = 1_000_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class _FakeNotifier(Notifier):
    def __init__(self, channel: Channel, *, enabled: bool = True, ok: bool = True):
        self.channel = channel
        self._enabled = enabled
        self._ok = ok
        self.sent: list[NotificationEvent] = []

    def enabled(self) -> bool:
        return self._enabled

    async def send(self, event: NotificationEvent) -> NotifierResult:
        self.sent.append(event)
        if self._ok:
            return NotifierResult(ok=True)
        return NotifierResult(ok=False, error="simulated failure")


def _event(*, key: str = "k1", priority: Priority = Priority.WARN) -> NotificationEvent:
    return NotificationEvent(
        dedup_key=key, priority=priority, title="t", body="b",
    )


async def test_send_logs_and_delivers_to_enabled_channels(db):
    ntfy = _FakeNotifier(Channel.NTFY)
    push = _FakeNotifier(Channel.PUSHOVER)
    clock = _FakeClock()
    disp = Dispatcher(conn=db, notifiers=[ntfy, push], clock=clock)
    outcomes = await disp.send(_event(priority=Priority.WARN))
    delivered = [o for o in outcomes if o.delivered]
    assert len(delivered) == 2
    rows = db.execute("SELECT * FROM notifications").fetchall()
    assert len(rows) == 2
    assert all(r["delivered"] for r in rows)


async def test_dedup_within_window_drops_second_attempt(db):
    ntfy = _FakeNotifier(Channel.NTFY)
    clock = _FakeClock()
    disp = Dispatcher(conn=db, notifiers=[ntfy], clock=clock)
    await disp.send(_event(key="dup"))
    clock.advance(60)  # 1 minute later
    outcomes = await disp.send(_event(key="dup"))
    assert all(o.deduped for o in outcomes)
    rows = db.execute(
        "SELECT * FROM notifications WHERE dedup_key = 'dup' ORDER BY id"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["delivered"] == 1
    assert rows[1]["delivered"] == 0
    assert rows[1]["error"] == "deduped"


async def test_dedup_window_expires_after_24h(db):
    ntfy = _FakeNotifier(Channel.NTFY)
    clock = _FakeClock()
    disp = Dispatcher(conn=db, notifiers=[ntfy], clock=clock)
    await disp.send(_event(key="dup"))
    clock.advance(25 * 3600)
    outcomes = await disp.send(_event(key="dup"))
    assert any(o.delivered for o in outcomes)
    delivered_rows = db.execute(
        "SELECT * FROM notifications WHERE dedup_key = 'dup' AND delivered = 1"
    ).fetchall()
    assert len(delivered_rows) == 2


async def test_rate_limit_blocks_after_capacity(db):
    ntfy = _FakeNotifier(Channel.NTFY)
    clock = _FakeClock()
    disp = Dispatcher(
        conn=db, notifiers=[ntfy], clock=clock,
        bucket_capacity=3, bucket_window_s=60.0,
    )
    for i in range(3):
        outcomes = await disp.send(_event(key=f"k{i}"))
        assert any(o.delivered for o in outcomes)
    outcomes = await disp.send(_event(key="overflow"))
    assert all(not o.delivered for o in outcomes)
    last = db.execute(
        "SELECT * FROM notifications WHERE dedup_key = 'overflow'"
    ).fetchone()
    assert "rate_limited" in (last["error"] or "")


async def test_info_priority_prefers_ntfy_only(db):
    ntfy = _FakeNotifier(Channel.NTFY)
    push = _FakeNotifier(Channel.PUSHOVER)
    clock = _FakeClock()
    disp = Dispatcher(conn=db, notifiers=[ntfy, push], clock=clock)
    await disp.send(_event(key="info", priority=Priority.INFO))
    assert len(ntfy.sent) == 1
    assert push.sent == []


async def test_no_channel_logs_failed_row(db):
    ntfy = _FakeNotifier(Channel.NTFY, enabled=False)
    push = _FakeNotifier(Channel.PUSHOVER, enabled=False)
    clock = _FakeClock()
    disp = Dispatcher(conn=db, notifiers=[ntfy, push], clock=clock)
    outcomes = await disp.send(_event(key="orphan"))
    assert all(not o.delivered for o in outcomes)
    rows = db.execute(
        "SELECT * FROM notifications WHERE dedup_key = 'orphan'"
    ).fetchall()
    assert len(rows) == 1
    assert "no channel" in (rows[0]["error"] or "")
