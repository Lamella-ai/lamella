from __future__ import annotations

from lamella.web.routes.status import _fmt_ts


def test_fmt_ts_preserves_date_only():
    assert _fmt_ts("2026-04-28", tz_name="America/Denver") == "2026-04-28"


def test_fmt_ts_converts_datetime():
    assert _fmt_ts("2026-04-30T22:41:00+00:00", tz_name="America/Denver") == "2026-04-30 16:41"
