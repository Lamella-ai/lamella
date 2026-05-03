# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Timezone helper for the calendar.

Read `APP_TZ` (IANA name, e.g. `America/New_York`) from settings and
resolve it to a `zoneinfo.ZoneInfo`. Falls back to UTC with a log
warning on invalid values so a typo in the env var never crashes
the app.

Usage:

- `app_tz(settings)` → `ZoneInfo`
- `today_local(settings)` → `date` (the user's "today", not UTC)
- `local_date_of(ts, settings)` → `date` from a TZ-aware or naive
  timestamp interpreted as UTC

Load-bearing for the 11pm-mileage acceptance criterion: any code
path that derives a date from a timestamp must use these helpers.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

log = logging.getLogger(__name__)


def app_tz(settings) -> ZoneInfo:
    name = getattr(settings, "app_tz", "UTC") or "UTC"
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        log.warning("APP_TZ=%r is not a valid IANA tz — falling back to UTC", name)
        return ZoneInfo("UTC")


def today_local(settings) -> date:
    return datetime.now(tz=app_tz(settings)).date()


def local_date_of(ts: datetime | None, settings) -> date | None:
    """Convert a timestamp to a local date under APP_TZ.

    Naive timestamps are interpreted as UTC (matches the paperless
    sync behavior, which strips tzinfo after normalizing to UTC)."""
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(app_tz(settings)).date()


def local_now(settings) -> datetime:
    return datetime.now(tz=app_tz(settings))
