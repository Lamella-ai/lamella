# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0047 + ADR-0048 promotions: loans, entities, properties are
first-class user concepts and live at the top-level URL. Old
``/settings/X`` paths 301-redirect to ``/X``.

The deep sub-routes (``/settings/loans/{slug}/anchors``, etc.) keep
their canonical /settings paths for now — those are POST action
endpoints, not bookmarked URLs. A future cleanup pass moves those
when we have the appetite for the sweep.
"""
from __future__ import annotations


class TestLoansPromotion:
    def test_settings_loans_redirects_to_loans(self, app_client):
        r = app_client.get("/settings/loans", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/loans"

    def test_settings_loans_preserves_querystring(self, app_client):
        r = app_client.get("/settings/loans?prefill_slug=foo", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/loans?prefill_slug=foo"

    def test_loans_landing_renders(self, app_client):
        r = app_client.get("/loans")
        assert r.status_code == 200


class TestEntitiesPromotion:
    def test_settings_entities_redirects_to_entities(self, app_client):
        r = app_client.get("/settings/entities", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/entities"

    def test_entities_landing_renders(self, app_client):
        r = app_client.get("/entities")
        assert r.status_code == 200


class TestPropertiesPromotion:
    def test_settings_properties_redirects_to_properties(self, app_client):
        r = app_client.get("/settings/properties", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/properties"

    def test_properties_landing_renders(self, app_client):
        r = app_client.get("/properties")
        assert r.status_code == 200


class TestSidebarLinks:
    def test_sidebar_uses_canonical_loans_url(self, app_client):
        r = app_client.get("/")
        assert r.status_code == 200
        # Sidebar Loans entry should point at the new canonical path,
        # not the old /settings/loans.
        assert 'href="/loans"' in r.text
        assert 'href="/settings/loans"' not in r.text


class TestSettingsDashboardTiles:
    def test_settings_dashboard_tiles_use_canonical_urls(self, app_client):
        r = app_client.get("/settings")
        assert r.status_code == 200
        # Tiles should point at the new canonical URLs (ADR-0047:
        # settings dashboard tile points at the promoted route).
        assert 'href="/entities"' in r.text
        assert 'href="/properties"' in r.text
        assert 'href="/loans"' in r.text
