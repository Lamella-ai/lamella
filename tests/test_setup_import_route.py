# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the /setup/import UI — form, preview, apply."""
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
    """App whose ledger dir exists but has no main.bean."""
    empty_ledger = tmp_path / "empty-ledger"
    empty_ledger.mkdir()

    settings = Settings(
        data_dir=tmp_path / "data",
        ledger_dir=empty_ledger,
    )

    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )

    app = create_app(settings=settings)
    with TestClient(app) as client:
        yield client, settings, empty_ledger, tmp_path


def _write_main(dir_: Path, content: str) -> None:
    (dir_ / "main.bean").write_text(content, encoding="utf-8")


# --- GET form ---------------------------------------------------------------


class TestImportForm:
    def test_get_without_source_renders_form(self, needs_setup_client):
        client, _, ledger, _ = needs_setup_client
        response = client.get("/setup/import")
        assert response.status_code == 200
        assert "Import existing ledger" in response.text
        assert "Source directory" in response.text
        # Default source input is pre-populated with ledger_dir.
        assert str(ledger).replace("\\", "/") in response.text.replace("\\", "/")

    def test_get_with_invalid_source_shows_error(self, needs_setup_client):
        client, _, _, tmp_path = needs_setup_client
        bad = tmp_path / "does-not-exist"
        response = client.get(f"/setup/import?source={bad}")
        assert response.status_code == 200
        assert "directory does not exist" in response.text


class TestImportPreview:
    def test_preview_shows_transform_count(self, needs_setup_client):
        client, _, _, tmp_path = needs_setup_client
        src = tmp_path / "source-ledger"
        src.mkdir()
        _write_main(
            src,
            'option "operating_currency" "USD"\n'
            '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        response = client.get(f"/setup/import?source={src}")
        assert response.status_code == 200
        # Plan summary appears (KPI tiles labelled Keep / Transform).
        assert "Keep" in response.text
        assert "Transform" in response.text
        # At least one transform.
        assert "fava-extension" in response.text

    def test_preview_shows_block_reason_for_disallowed_plugin(
        self, needs_setup_client
    ):
        client, _, _, tmp_path = needs_setup_client
        src = tmp_path / "bad-source"
        src.mkdir()
        _write_main(
            src,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_share.share"\n',
        )
        response = client.get(f"/setup/import?source={src}")
        assert response.status_code == 200
        assert "Import blocked" in response.text
        assert "beancount_share.share" in response.text


class TestApply:
    def test_apply_succeeds_and_redirects(self, needs_setup_client):
        client, _, ledger, tmp_path = needs_setup_client
        # Seed the ledger dir with a main.bean that has a fava-extension.
        _write_main(
            ledger,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_lazy_plugins.auto_accounts"\n'
            '2010-01-01 custom "fava-extension" "fava_dashboards"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )

        # Middleware should still be gating (app booted with empty dir).
        # Force a state refresh by hitting /setup first.
        client.get("/setup")

        response = client.post(
            "/setup/import/apply",
            data={"source": str(ledger)},
            follow_redirects=False,
        )
        assert response.status_code == 303
        # Apply now hands off to /setup/recovery so the user can finish
        # the rebuild walk before the gate clears.
        assert response.headers["location"].startswith("/setup/recovery")
        assert "imported=" in response.headers["location"]

        # Canonical files exist.
        for cfile in CANONICAL_FILES:
            assert (ledger / cfile.name).exists(), cfile.name
        # Fava extension commented out.
        from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
        main_text = (ledger / "main.bean").read_text(encoding="utf-8")
        assert "; [lamella-removed" in main_text
        assert (
            f'custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"'
            in main_text
        )

    def test_apply_refreshes_detection_to_ready(self, needs_setup_client):
        client, _, ledger, _ = needs_setup_client
        _write_main(
            ledger,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n"
            '2026-01-15 * "x"\n'
            "  Assets:Bank    10 USD\n"
            "  Assets:Bank   -10 USD\n",
        )
        client.get("/setup")  # prime detection
        client.post(
            "/setup/import/apply",
            data={"source": str(ledger)},
            follow_redirects=False,
        )
        detection = client.app.state.ledger_detection
        assert detection.state == LedgerState.READY

    def test_apply_with_missing_source_returns_400(self, needs_setup_client):
        client, _, _, tmp_path = needs_setup_client
        bogus = tmp_path / "nowhere"
        response = client.post(
            "/setup/import/apply",
            data={"source": str(bogus)},
            follow_redirects=False,
        )
        assert response.status_code == 400
        assert "does not exist" in response.text

    def test_apply_with_blocked_analysis_returns_400(self, needs_setup_client):
        client, _, _, tmp_path = needs_setup_client
        src = tmp_path / "blocked"
        src.mkdir()
        _write_main(
            src,
            'option "operating_currency" "USD"\n'
            'plugin "beancount_interpolate.recur"\n',
        )
        response = client.post(
            "/setup/import/apply",
            data={"source": str(src)},
            follow_redirects=False,
        )
        assert response.status_code == 400
        assert "blocked" in response.text.lower() or "beancount_interpolate" in response.text


# --- setup.html "Import existing" link is now live -------------------------


class TestSetupPageImportEnabled:
    def test_setup_page_links_to_import(self, needs_setup_client):
        client, _, _, _ = needs_setup_client
        response = client.get("/setup")
        assert response.status_code == 200
        assert "/setup/import" in response.text
        # "coming soon" placeholder is gone.
        assert "coming soon" not in response.text.lower()
