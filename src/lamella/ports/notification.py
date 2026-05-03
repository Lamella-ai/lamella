# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Notification port — Notifier ABC + shared types.

The types (``NotificationEvent``, ``Priority``, ``Channel``,
``NotifierResult``) live here alongside the ABC so that adapters
(``adapters/ntfy/``, ``adapters/pushover/``) can import them without
crossing into ``features/notifications/`` — ADR-0040 layering forbids
``adapters/`` from importing ``features/``.

The concrete adapters live in :mod:`lamella.adapters.ntfy.client` and
:mod:`lamella.adapters.pushover.client` (moved in subgroup 4d).
The dispatcher and digests move to ``features/notifications/`` in 5d.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum


class Priority(str, Enum):
    INFO = "info"
    WARN = "warn"
    URGENT = "urgent"


class Channel(str, Enum):
    NTFY = "ntfy"
    PUSHOVER = "pushover"


@dataclass(frozen=True)
class NotificationEvent:
    """A single logical notification. ``dedup_key`` must be stable per
    "thing" (e.g., one review item, one upcoming-expense prediction) so the
    dispatcher can drop duplicates within the 24h window."""
    dedup_key: str
    priority: Priority
    title: str
    body: str
    channel_hint: Channel | None = None
    url: str | None = None


@dataclass(frozen=True)
class NotifierResult:
    ok: bool
    error: str | None = None


class Notifier(ABC):
    """Abstract base for channel adapters. Concrete implementations post to
    one upstream service (ntfy, Pushover, ...) and return a NotifierResult.
    They must not raise — the dispatcher treats exceptions as failure but
    logs the row regardless."""

    channel: Channel

    @abstractmethod
    async def send(self, event: NotificationEvent) -> NotifierResult: ...

    @abstractmethod
    def enabled(self) -> bool: ...
