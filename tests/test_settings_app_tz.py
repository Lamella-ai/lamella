# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the runtime-editable app_tz setting.

Covers:
- app_tz is in EDITABLE_KEYS so store.set() accepts it.
- POST /settings/general with a valid IANA tz updates the store and
  takes effect on the live Settings object.
- POST /settings/general with an invalid IANA tz is silently ignored
  (existing value preserved).
- GET /settings/general renders the current app_tz value.
- apply_kv_overrides() propagates a valid tz to settings.app_tz.
- apply_kv_overrides() ignores an invalid tz (no exception raised).
"""
from __future__ import annotations


class TestAppTzInEditableKeys:
    def test_app_tz_is_editable(self, db):
        from lamella.core.settings.store import AppSettingsStore, EDITABLE_KEYS

        assert "app_tz" in EDITABLE_KEYS

        store = AppSettingsStore(db)
        store.set("app_tz", "America/Denver")
        assert store.get("app_tz") == "America/Denver"

    def test_app_tz_rejects_unknown_key_guard(self, db):
        """Sanity check: non-editable keys still raise ValueError."""
        from lamella.core.settings.store import AppSettingsStore
        import pytest

        store = AppSettingsStore(db)
        with pytest.raises(ValueError, match="not editable"):
            store.set("__not_a_real_key__", "value")


class TestApplyKvOverrides:
    def test_valid_tz_propagates(self, settings):
        settings.apply_kv_overrides({"app_tz": "America/Denver"})
        assert settings.app_tz == "America/Denver"

    def test_invalid_tz_is_ignored(self, settings):
        original = settings.app_tz
        settings.apply_kv_overrides({"app_tz": "Not/AValidTimezone"})
        assert settings.app_tz == original

    def test_empty_tz_is_ignored(self, settings):
        original = settings.app_tz
        settings.apply_kv_overrides({"app_tz": ""})
        assert settings.app_tz == original


class TestSettingsGeneralPage:
    def test_get_renders_app_tz_field(self, app_client):
        r = app_client.get("/settings/general")
        assert r.status_code == 200
        assert 'name="app_tz"' in r.text

    def test_get_renders_default_utc(self, app_client):
        r = app_client.get("/settings/general")
        assert r.status_code == 200
        # Default timezone is UTC
        assert "UTC" in r.text

    def test_post_valid_tz_updates_store(self, app_client):
        r = app_client.post(
            "/settings/general",
            data={"number_locale": "en_US", "app_tz": "America/Chicago"},
            follow_redirects=True,
        )
        assert r.status_code == 200
        # After saving, the page should reflect the new timezone.
        assert "America/Chicago" in r.text

    def test_post_invalid_tz_is_silently_rejected(self, app_client):
        # Set a known-good value first.
        app_client.post(
            "/settings/general",
            data={"number_locale": "en_US", "app_tz": "America/Denver"},
            follow_redirects=True,
        )
        # Attempt to set an invalid timezone; should be ignored.
        r = app_client.post(
            "/settings/general",
            data={"number_locale": "en_US", "app_tz": "Bogus/Timezone"},
            follow_redirects=True,
        )
        assert r.status_code == 200
        # Invalid value must not appear; the previous good value should still be shown.
        assert "Bogus/Timezone" not in r.text
        assert "America/Denver" in r.text

    def test_post_tz_takes_effect_on_live_settings(self, app_client):
        """Saving a timezone via POST updates app.state.settings.app_tz
        without requiring a restart."""
        app_client.post(
            "/settings/general",
            data={"number_locale": "en_US", "app_tz": "Europe/Berlin"},
            follow_redirects=False,
        )
        assert app_client.app.state.settings.app_tz == "Europe/Berlin"

    def test_post_tz_affects_local_ts_filter(self, app_client):
        """After saving app_tz, the |local_ts template filter converts
        timestamps using the new timezone."""
        from datetime import datetime, timezone

        app_client.post(
            "/settings/general",
            data={"number_locale": "en_US", "app_tz": "America/Denver"},
            follow_redirects=False,
        )
        f = app_client.app.state.templates.env.filters["local_ts"]
        v = datetime(2026, 4, 30, 22, 41, tzinfo=timezone.utc)
        # America/Denver is UTC-6 (MDT), so 22:41 UTC => 16:41 local.
        assert f(v) == "2026-04-30 16:41"
