# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def render_local_ts(
    value: Any,
    *,
    tz_name: str,
    with_seconds: bool = False,
    fmt: str | None = None,
) -> str:
    """Render temporal values safely.

    - date-only values are preserved as ``YYYY-MM-DD`` (no tz shift)
    - datetime values are converted into ``tz_name``
    - naive datetimes are interpreted as UTC
    - unparseable values are returned as-is
    """
    if value is None:
        return ""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()

    raw = str(value).strip()
    if not raw:
        return ""
    if _DATE_ONLY_RE.match(raw):
        return raw

    try:
        dt = value if isinstance(value, datetime) else datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        return raw

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        tz = ZoneInfo(tz_name or "UTC")
    except ZoneInfoNotFoundError:
        tz = ZoneInfo("UTC")
    local = dt.astimezone(tz)
    out_fmt = fmt or ("%Y-%m-%d %H:%M:%S" if with_seconds else "%Y-%m-%d %H:%M")
    return local.strftime(out_fmt)
