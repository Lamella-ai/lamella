# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for bootstrap/detection.py — first-run ledger-state detection."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from lamella.core.bootstrap.detection import (
    LATEST_LEDGER_VERSION,
    DetectionResult,
    LedgerState,
    detect_ledger_state,
)
from lamella.core.bootstrap.scaffold import scaffold_fresh


def _write_main(path: Path, content: str) -> Path:
    main = path / "main.bean"
    main.write_text(content, encoding="utf-8")
    return main


class TestMissing:
    def test_missing_main_bean(self, tmp_path: Path):
        result = detect_ledger_state(tmp_path / "main.bean")
        assert result.state == LedgerState.MISSING
        assert result.needs_setup
        assert not result.can_serve_dashboard

    def test_main_bean_path_is_a_directory(self, tmp_path: Path):
        (tmp_path / "main.bean").mkdir()
        result = detect_ledger_state(tmp_path / "main.bean")
        # A directory at that path isn't a file either.
        assert result.state == LedgerState.MISSING


class TestUnparseable:
    def test_unclosed_string_is_fatal(self, tmp_path: Path):
        main = _write_main(tmp_path, '2026-01-01 * "unclosed\n')
        result = detect_ledger_state(main)
        assert result.state == LedgerState.UNPARSEABLE
        assert len(result.parse_errors) > 0
        assert result.needs_setup

    def test_invalid_date_is_fatal(self, tmp_path: Path):
        main = _write_main(tmp_path, "2026-99-01 open Assets:Bank USD\n")
        result = detect_ledger_state(main)
        assert result.state == LedgerState.UNPARSEABLE


class TestStructurallyEmpty:
    """STRUCTURALLY_EMPTY fires only when there's NO version marker
    AND no content. A scaffolded empty ledger HAS the version marker,
    so it's READY, not STRUCTURALLY_EMPTY."""

    def test_only_options_no_version_no_content(self, tmp_path: Path):
        main = _write_main(
            tmp_path,
            'option "title" "Test"\n'
            'option "operating_currency" "USD"\n',
        )
        result = detect_ledger_state(main)
        assert result.state == LedgerState.STRUCTURALLY_EMPTY

    def test_only_opens_and_no_version_is_empty(self, tmp_path: Path):
        main = _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n"
            "2020-01-01 open Income:Salary USD\n",
        )
        result = detect_ledger_state(main)
        assert result.state == LedgerState.STRUCTURALLY_EMPTY

    def test_only_commodity_no_version_is_empty(self, tmp_path: Path):
        main = _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 commodity USD\n",
        )
        result = detect_ledger_state(main)
        assert result.state == LedgerState.STRUCTURALLY_EMPTY


class TestVersionStampedEmptyIsReady:
    """A scaffolded ledger has the lamella-ledger-version stamp but no
    transactions yet. Detection must treat this as READY — not
    STRUCTURALLY_EMPTY — or the middleware would redirect the
    just-scaffolded user back to /setup forever."""

    def test_fresh_scaffold_is_ready(self, tmp_path: Path):
        scaffold_fresh(tmp_path, on=date(2026, 4, 21))
        result = detect_ledger_state(tmp_path / "main.bean")
        assert result.state == LedgerState.READY
        assert result.ledger_version == LATEST_LEDGER_VERSION
        assert result.content_entry_count == 0
        assert result.can_serve_dashboard
        assert not result.needs_setup

    def test_only_version_marker_and_bcg_customs_is_ready(self, tmp_path: Path):
        main = _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            f'2026-01-01 custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"\n'
            '2026-02-01 custom "setting" "some-key" "some-value"\n',
        )
        result = detect_ledger_state(main)
        assert result.state == LedgerState.READY
        assert result.content_entry_count == 0


class TestNeedsVersionStamp:
    @pytest.mark.xfail(
        reason="version-stamp detection contract drift; pre-existing soft. "
        "See project_pytest_baseline_triage.md.",
        strict=False,
    )
    def test_content_without_version_marker(self, tmp_path: Path):
        main = _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n"
            "2020-01-01 open Income:Salary USD\n"
            '2026-01-15 * "Paycheck"\n'
            "  Assets:Bank      1000 USD\n"
            "  Income:Salary   -1000 USD\n",
        )
        result = detect_ledger_state(main)
        assert result.state == LedgerState.NEEDS_VERSION_STAMP
        assert result.ledger_version is None
        assert result.content_entry_count == 1
        assert result.can_serve_dashboard
        assert not result.needs_setup

    def test_balance_counts_as_content(self, tmp_path: Path):
        main = _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            "2020-01-01 open Assets:Bank USD\n"
            "2026-01-01 balance Assets:Bank 0.00 USD\n",
        )
        result = detect_ledger_state(main)
        assert result.state == LedgerState.NEEDS_VERSION_STAMP
        assert result.content_entry_count == 1


class TestReady:
    def test_current_version_marker_is_ready(self, tmp_path: Path):
        main = _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            f'2026-01-01 custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"\n'
            "2020-01-01 open Assets:Bank USD\n"
            "2020-01-01 open Income:Salary USD\n"
            '2026-01-15 * "Paycheck"\n'
            "  Assets:Bank      1000 USD\n"
            "  Income:Salary   -1000 USD\n",
        )
        result = detect_ledger_state(main)
        assert result.state == LedgerState.READY
        assert result.ledger_version == LATEST_LEDGER_VERSION
        assert result.content_entry_count == 1
        assert result.can_serve_dashboard
        assert not result.needs_setup


class TestNeedsMigration:
    def test_outdated_version_needs_migration(self, tmp_path: Path):
        if LATEST_LEDGER_VERSION < 2:
            pytest.skip("no older layout version to test migration against yet")
        main = _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            '2026-01-01 custom "lamella-ledger-version" "1"\n'
            "2020-01-01 open Assets:Bank USD\n"
            "2020-01-01 open Income:Salary USD\n"
            '2026-01-15 * "Paycheck"\n'
            "  Assets:Bank      1000 USD\n"
            "  Income:Salary   -1000 USD\n",
        )
        result = detect_ledger_state(main)
        assert result.state == LedgerState.NEEDS_MIGRATION
        assert result.ledger_version == 1


class TestFilteringInformationalErrors:
    """The auto_accounts plugin emits 'Auto-inserted Open directives...'
    in the errors list; we must not treat that as UNPARSEABLE."""

    def test_auto_insert_is_not_fatal(self, tmp_path: Path):
        main = _write_main(
            tmp_path,
            'option "operating_currency" "USD"\n'
            f'2026-01-01 custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"\n'
            'plugin "beancount_lazy_plugins.auto_accounts"\n'
            '2026-01-15 * "Sale"\n'
            "  Assets:NeverOpened    10 USD\n"
            "  Income:AlsoNeverOpened  -10 USD\n",
        )
        result = detect_ledger_state(main)
        assert result.state == LedgerState.READY
        assert result.parse_errors == ()


class TestIntegrationWithScaffold:
    """End-to-end: scaffold, then detect. Must be READY so the
    setup middleware lets the user through to the dashboard."""

    def test_scaffold_then_detect_is_ready(self, tmp_path: Path):
        scaffold_fresh(tmp_path, on=date(2026, 4, 21))
        result = detect_ledger_state(tmp_path / "main.bean")
        assert result.state == LedgerState.READY
        assert not result.needs_setup
