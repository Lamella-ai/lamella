# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Iterable, Sequence

from lamella.ports.notification import (
    Channel,
    NotificationEvent,
    Notifier,
    NotifierResult,
    Priority,
)

log = logging.getLogger(__name__)


DEDUP_WINDOW_SECONDS = 24 * 3600
DEFAULT_BUCKET_CAPACITY = 10
DEFAULT_BUCKET_WINDOW_S = 600  # 10 minutes


@dataclass
class _TokenBucket:
    capacity: int
    window_seconds: float
    events: deque[float]

    def allow(self, now: float) -> bool:
        cutoff = now - self.window_seconds
        while self.events and self.events[0] < cutoff:
            self.events.popleft()
        if len(self.events) >= self.capacity:
            return False
        self.events.append(now)
        return True


@dataclass(frozen=True)
class DispatchOutcome:
    dedup_key: str
    channel: Channel
    delivered: bool
    error: str | None
    deduped: bool = False
    rate_limited: bool = False


def _fan_out_channels(
    event: NotificationEvent,
    notifiers: Sequence[Notifier],
) -> list[Notifier]:
    """URGENT goes to every enabled channel. INFO prefers ntfy only (the
    quieter channel) — this matches the plan's "INFO: ntfy only" rule. WARN
    fans out to all enabled channels. A ``channel_hint`` forces a specific
    channel."""
    enabled = [n for n in notifiers if n.enabled()]
    if not enabled:
        return []
    if event.channel_hint is not None:
        return [n for n in enabled if n.channel == event.channel_hint]
    if event.priority == Priority.URGENT:
        return list(enabled)
    if event.priority == Priority.INFO:
        ntfy = [n for n in enabled if n.channel == Channel.NTFY]
        return ntfy or list(enabled)
    return list(enabled)


class Dispatcher:
    """Fans ``NotificationEvent`` out to the right channels while enforcing
    dedup (24 h window per dedup_key) and a per-channel rate limit.

    The dispatcher records a row in the ``notifications`` table for every
    attempt — including deduped and rate-limited ones — so the /notifications
    page and audit queries are honest."""

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        notifiers: Iterable[Notifier],
        clock=None,
        bucket_capacity: int = DEFAULT_BUCKET_CAPACITY,
        bucket_window_s: float = DEFAULT_BUCKET_WINDOW_S,
        dedup_window_s: float = DEDUP_WINDOW_SECONDS,
    ):
        self.conn = conn
        self.notifiers = list(notifiers)
        self._clock = clock or time.time
        self._buckets: dict[Channel, _TokenBucket] = {
            n.channel: _TokenBucket(bucket_capacity, bucket_window_s, deque())
            for n in self.notifiers
        }
        self.dedup_window_s = dedup_window_s

    async def send(self, event: NotificationEvent) -> list[DispatchOutcome]:
        targets = _fan_out_channels(event, self.notifiers)
        if not targets:
            # No enabled channel. Still log so /notifications reflects the attempt.
            self._log_row(
                event=event,
                channel=(event.channel_hint or Channel.NTFY),
                result=NotifierResult(ok=False, error="no channel configured"),
            )
            return [
                DispatchOutcome(
                    dedup_key=event.dedup_key,
                    channel=(event.channel_hint or Channel.NTFY),
                    delivered=False,
                    error="no channel configured",
                )
            ]

        if self._recently_delivered(event.dedup_key):
            outcomes: list[DispatchOutcome] = []
            for target in targets:
                self._log_row(
                    event=event,
                    channel=target.channel,
                    result=NotifierResult(ok=False, error="deduped"),
                )
                outcomes.append(
                    DispatchOutcome(
                        dedup_key=event.dedup_key,
                        channel=target.channel,
                        delivered=False,
                        error="deduped",
                        deduped=True,
                    )
                )
            return outcomes

        results = await asyncio.gather(
            *[self._send_with_rate_limit(target, event) for target in targets]
        )
        outcomes = []
        for target, res in zip(targets, results):
            self._log_row(event=event, channel=target.channel, result=res)
            outcomes.append(
                DispatchOutcome(
                    dedup_key=event.dedup_key,
                    channel=target.channel,
                    delivered=res.ok,
                    error=res.error,
                    rate_limited=res.error is not None and res.error.startswith("rate_limited:"),
                )
            )
        return outcomes

    async def _send_with_rate_limit(
        self,
        notifier: Notifier,
        event: NotificationEvent,
    ) -> NotifierResult:
        bucket = self._buckets.get(notifier.channel)
        if bucket is not None and not bucket.allow(self._clock()):
            log.warning(
                "notify: rate-limited %s (bucket full) for key=%s",
                notifier.channel.value,
                event.dedup_key,
            )
            return NotifierResult(
                ok=False, error=f"rate_limited: {notifier.channel.value}"
            )
        try:
            return await notifier.send(event)
        except Exception as exc:  # noqa: BLE001
            log.exception("notifier %s crashed", notifier.channel.value)
            return NotifierResult(ok=False, error=f"{type(exc).__name__}: {exc}")

    def _recently_delivered(self, dedup_key: str) -> bool:
        """Return True if this dedup_key has been delivered successfully in
        the last dedup_window_s seconds. Errored / deduped rows do not
        count — we want one genuine delivery to gate the next."""
        row = self.conn.execute(
            """
            SELECT sent_at FROM notifications
             WHERE dedup_key = ? AND delivered = 1
          ORDER BY sent_at DESC
             LIMIT 1
            """,
            (dedup_key,),
        ).fetchone()
        if row is None:
            return False
        raw = row["sent_at"]
        if isinstance(raw, datetime):
            sent_at = raw
        else:
            try:
                sent_at = datetime.fromisoformat(str(raw))
            except ValueError:
                return False
        now_ts = self._clock()
        try:
            age = now_ts - sent_at.timestamp()
        except (OSError, OverflowError):
            return False
        return age < self.dedup_window_s

    def _log_row(
        self,
        *,
        event: NotificationEvent,
        channel: Channel,
        result: NotifierResult,
    ) -> None:
        sent_at = datetime.fromtimestamp(self._clock(), tz=UTC).isoformat(timespec="seconds")
        self.conn.execute(
            """
            INSERT INTO notifications
                (sent_at, channel, priority, dedup_key, title, body, delivered, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sent_at,
                channel.value,
                event.priority.value,
                event.dedup_key,
                event.title,
                event.body,
                1 if result.ok else 0,
                result.error,
            ),
        )

    async def aclose(self) -> None:
        for n in self.notifiers:
            close = getattr(n, "aclose", None)
            if close is not None:
                try:
                    await close()
                except Exception:  # noqa: BLE001
                    log.exception("notifier %s aclose failed", n.channel.value)
