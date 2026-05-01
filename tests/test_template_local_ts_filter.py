from __future__ import annotations

from datetime import date, datetime, timezone


def test_local_ts_filter_converts_datetime_in_app_tz(app_client):
    app_client.app.state.settings.apply_kv_overrides({"app_tz": "America/Denver"})
    f = app_client.app.state.templates.env.filters["local_ts"]
    v = datetime(2026, 4, 30, 22, 41, tzinfo=timezone.utc)
    assert f(v) == "2026-04-30 16:41"


def test_local_ts_filter_preserves_date_only(app_client):
    app_client.app.state.settings.apply_kv_overrides({"app_tz": "America/Denver"})
    f = app_client.app.state.templates.env.filters["local_ts"]
    assert f("2026-04-28") == "2026-04-28"
    assert f(date(2026, 4, 28)) == "2026-04-28"


def test_local_ts_filter_invalid_tz_falls_back_to_utc(app_client):
    app_client.app.state.settings.apply_kv_overrides({"app_tz": "Not/AZone"})
    f = app_client.app.state.templates.env.filters["local_ts"]
    v = datetime(2026, 4, 30, 22, 41, tzinfo=timezone.utc)
    assert f(v) == "2026-04-30 22:41"


def test_local_ts_filter_custom_format(app_client):
    app_client.app.state.settings.apply_kv_overrides({"app_tz": "America/Denver"})
    f = app_client.app.state.templates.env.filters["local_ts"]
    v = datetime(2026, 4, 30, 22, 41, tzinfo=timezone.utc)
    assert f(v, fmt="%b %d, %H:%M") == "Apr 30, 16:41"
