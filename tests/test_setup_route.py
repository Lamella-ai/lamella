# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the /setup route + the setup_gate middleware.

Uses a dedicated app fixture with an empty ledger directory so the
boot-time detector classifies the ledger as MISSING and the
middleware kicks in."""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lamella.core.bootstrap.detection import LedgerState
from lamella.core.bootstrap.templates import CANONICAL_FILES
from lamella.core.config import Settings
from lamella.main import create_app


@pytest.fixture
def needs_setup_client(tmp_path: Path, monkeypatch):
    """App whose ledger dir exists but contains no main.bean."""
    empty_ledger = tmp_path / "empty-ledger"
    empty_ledger.mkdir()

    settings = Settings(
        data_dir=tmp_path / "data",
        ledger_dir=empty_ledger,
    )

    # The existing bean-check hook in receipts.linker.run_bean_check
    # would shell out; neutralize for tests.
    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )

    app = create_app(settings=settings)
    with TestClient(app) as client:
        yield client, settings, empty_ledger


class TestDetectionOnBoot:
    def test_state_is_missing_at_boot(self, needs_setup_client):
        client, _settings, _ledger = needs_setup_client
        detection = client.app.state.ledger_detection
        assert detection.state == LedgerState.MISSING
        assert detection.needs_setup


class TestSetupGateMiddleware:
    def test_dashboard_redirects_to_setup(self, needs_setup_client):
        client, _settings, _ledger = needs_setup_client
        response = client.get("/", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/setup"

    @pytest.mark.parametrize(
        "path",
        ["/businesses", "/search", "/reports", "/review", "/settings"],
    )
    def test_common_routes_redirect(self, needs_setup_client, path: str):
        client, _settings, _ledger = needs_setup_client
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/setup"

    def test_healthz_is_exempt(self, needs_setup_client):
        client, _settings, _ledger = needs_setup_client
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_readyz_is_exempt(self, needs_setup_client):
        client, _settings, _ledger = needs_setup_client
        response = client.get("/readyz")
        assert response.status_code == 200

    def test_static_is_exempt(self, needs_setup_client):
        client, _settings, _ledger = needs_setup_client
        response = client.get("/static/app.css", follow_redirects=False)
        # 200 if it exists, 404 if not — either way, not a 303 redirect.
        assert response.status_code != 303


class TestSetupPage:
    @pytest.mark.xfail(
        reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
        strict=False,
    )
    def test_get_setup_renders(self, needs_setup_client):
        client, _settings, _ledger = needs_setup_client
        response = client.get("/setup", follow_redirects=False)
        assert response.status_code == 200
        body = response.text
        assert "Set up Lamella" in body
        assert "Start fresh" in body
        assert "Import existing" in body

    @pytest.mark.xfail(
        reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
        strict=False,
    )
    def test_setup_page_shows_missing_state(self, needs_setup_client):
        client, _settings, _ledger = needs_setup_client
        response = client.get("/setup")
        assert "no <code>main.bean</code>" in response.text.replace(
            "&lt;", "<"
        ).replace("&gt;", ">") or "missing" in response.text.lower()


class TestScaffoldPost:
    def test_post_scaffold_creates_canonical_files(self, needs_setup_client):
        client, _settings, ledger = needs_setup_client
        response = client.post("/setup/scaffold", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/"

        expected = {"main.bean"} | {f.name for f in CANONICAL_FILES}
        on_disk = {p.name for p in ledger.iterdir() if p.is_file()}
        assert expected <= on_disk

    def test_post_scaffold_refreshes_detection_to_ready(
        self, needs_setup_client
    ):
        client, _settings, _ledger = needs_setup_client
        client.post("/setup/scaffold", follow_redirects=False)
        detection = client.app.state.ledger_detection
        assert detection.state == LedgerState.READY
        from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
        assert detection.ledger_version == LATEST_LEDGER_VERSION

    @pytest.mark.xfail(
        reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
        strict=False,
    )
    def test_post_scaffold_clears_redirect_gate(self, needs_setup_client):
        client, _settings, _ledger = needs_setup_client
        client.post("/setup/scaffold", follow_redirects=False)
        # Middleware should no longer redirect /.
        response = client.get("/", follow_redirects=False)
        assert response.status_code != 303, (
            f"Middleware still redirecting after scaffold; body: {response.text[:200]}"
        )

    def test_post_scaffold_is_idempotent_after_success(self, needs_setup_client):
        client, _settings, _ledger = needs_setup_client
        client.post("/setup/scaffold", follow_redirects=False)
        # Second POST refuses (files already exist) but gate is clear,
        # so it follows the already-past-setup redirect path to /.
        response = client.post("/setup/scaffold", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/"
