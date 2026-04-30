# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""PWA install criteria smoke tests.

The "Install Lamella" prompt that pops up on mobile (Chrome / Edge)
fires when:

1. The page is served over HTTPS (operator's responsibility — Cloudflare
   Tunnel, Tailscale serve, or a reverse proxy with certs).
2. ``<link rel="manifest">`` resolves to a JSON document with ``name``,
   ``icons``, ``start_url``, ``display`` ∈ {standalone, fullscreen,
   minimal-ui}, and at least one icon with ``purpose: any``.
3. A service worker is registered AND has a ``fetch`` handler.
4. The user has interacted with the page (Chrome's engagement
   heuristic — not testable here).

These tests pin (2) + (3) so a regression that breaks either prevents
the install prompt from ever appearing.
"""
from __future__ import annotations

import json


class TestManifest:
    def test_manifest_serves_as_json_with_required_fields(self, app_client):
        r = app_client.get("/static/manifest.webmanifest")
        assert r.status_code == 200
        data = json.loads(r.text)
        # Chrome's bare-minimum install criteria: name, icons array,
        # start_url, display ∈ {standalone, fullscreen, minimal-ui}.
        assert data.get("name"), "manifest.name is required"
        assert data.get("display") in {"standalone", "fullscreen", "minimal-ui"}
        assert data.get("start_url"), "manifest.start_url is required"
        assert isinstance(data.get("icons"), list) and data["icons"], (
            "manifest.icons must be a non-empty list"
        )
        # At least one icon with purpose='any' so Chrome has a usable
        # launcher icon.
        assert any(i.get("purpose") == "any" for i in data["icons"]), (
            "at least one icon must have purpose=any"
        )

    def test_manifest_referenced_from_base(self, app_client):
        r = app_client.get("/")
        assert r.status_code == 200
        # base.html must link the manifest, otherwise the browser
        # never reads it.
        assert 'rel="manifest"' in r.text
        assert "manifest.webmanifest" in r.text


class TestServiceWorker:
    def test_service_worker_served(self, app_client):
        r = app_client.get("/static/service-worker.js")
        assert r.status_code == 200
        body = r.text
        # A SW with a fetch handler is one of the install criteria.
        assert "addEventListener" in body
        assert '"fetch"' in body or "'fetch'" in body

    def test_service_worker_registered_site_wide(self, app_client):
        """The SW used to register only on /note + /mileage. That
        prevented the install prompt from firing on the dashboard,
        which is the most common landing page. Site-wide registration
        is the fix; this test pins the regression."""
        r = app_client.get("/")
        assert r.status_code == 200
        assert "serviceWorker.register" in r.text
        # Should NOT be gated by a path === '/note' check anymore.
        assert "path === \"/note\"" not in r.text
        assert "path === '/note'" not in r.text


class TestInstallPromptButton:
    def test_install_button_present_in_topbar(self, app_client):
        r = app_client.get("/")
        assert r.status_code == 200
        # The button starts hidden; JS unhides it when
        # `beforeinstallprompt` fires.
        assert 'id="installBtn"' in r.text

    def test_install_button_listener_wired(self, app_client):
        r = app_client.get("/")
        assert "beforeinstallprompt" in r.text
        assert "appinstalled" in r.text
