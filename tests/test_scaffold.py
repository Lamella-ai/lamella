# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for bootstrap/scaffold.py — fresh-ledger scaffolding."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from lamella.core.bootstrap.scaffold import ScaffoldError, scaffold_fresh
from lamella.core.bootstrap.templates import CANONICAL_FILES

_EXPECTED_FILES = {"main.bean"} | {f.name for f in CANONICAL_FILES}


class TestScaffoldFresh:
    def test_creates_all_canonical_files(self, tmp_path: Path):
        result = scaffold_fresh(tmp_path, on=date(2026, 4, 21))
        names = {p.name for p in result.created}
        assert names == _EXPECTED_FILES

    def test_every_file_is_nonempty(self, tmp_path: Path):
        scaffold_fresh(tmp_path, on=date(2026, 4, 21))
        for name in _EXPECTED_FILES:
            assert (tmp_path / name).stat().st_size > 0, name

    def test_main_bean_has_version_marker(self, tmp_path: Path):
        from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
        scaffold_fresh(tmp_path, on=date(2026, 4, 21))
        main = (tmp_path / "main.bean").read_text(encoding="utf-8")
        assert (
            f'custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"'
            in main
        )

    def test_main_bean_declares_only_allowlisted_plugin(self, tmp_path: Path):
        scaffold_fresh(tmp_path, on=date(2026, 4, 21))
        main = (tmp_path / "main.bean").read_text(encoding="utf-8")
        plugin_lines = [l for l in main.splitlines() if l.startswith("plugin ")]
        assert plugin_lines == ['plugin "beancount_lazy_plugins.auto_accounts"']

    def test_user_includes_precede_connector_includes(self, tmp_path: Path):
        scaffold_fresh(tmp_path, on=date(2026, 4, 21))
        main = (tmp_path / "main.bean").read_text(encoding="utf-8")

        def pos(marker: str) -> int:
            return main.index(marker)

        assert pos('include "accounts.bean"') < pos('include "connector_accounts.bean"')
        assert pos('include "manual_transactions.bean"') < pos(
            'include "connector_links.bean"'
        )

    def test_connector_file_has_managed_header(self, tmp_path: Path):
        scaffold_fresh(tmp_path, on=date(2026, 4, 21))
        content = (tmp_path / "connector_links.bean").read_text(encoding="utf-8")
        assert "Managed by Lamella" in content
        assert "Owner:     Lamella" in content
        # Ledger schema version is the live one (currently v3); tests
        # should not pin to a historical value.
        assert "Schema:    lamella-ledger-version=" in content
        assert "File:      connector_links.bean" in content
        assert "2026-04-21" in content

    def test_user_file_has_user_header(self, tmp_path: Path):
        scaffold_fresh(tmp_path, on=date(2026, 4, 21))
        content = (tmp_path / "accounts.bean").read_text(encoding="utf-8")
        assert "User-authored file" in content
        assert "Owner:     user" in content
        assert "File:      accounts.bean" in content
        assert "2026-04-21" in content


class TestRefusal:
    def test_refuses_when_main_bean_exists(self, tmp_path: Path):
        (tmp_path / "main.bean").write_text("existing", encoding="utf-8")
        with pytest.raises(ScaffoldError, match="already exist"):
            scaffold_fresh(tmp_path)

    def test_refuses_when_any_canonical_file_exists(self, tmp_path: Path):
        (tmp_path / "connector_config.bean").write_text("x", encoding="utf-8")
        with pytest.raises(ScaffoldError, match="already exist"):
            scaffold_fresh(tmp_path)

    def test_refuses_when_ledger_dir_missing(self, tmp_path: Path):
        missing = tmp_path / "nowhere"
        with pytest.raises(ScaffoldError, match="does not exist"):
            scaffold_fresh(missing)

    def test_leaves_unrelated_files_alone(self, tmp_path: Path):
        (tmp_path / "readme.txt").write_text("hello", encoding="utf-8")
        scaffold_fresh(tmp_path)
        assert (tmp_path / "readme.txt").read_text(encoding="utf-8") == "hello"


class TestBeanCheckGate:
    def test_passes_hook_the_main_bean_path(self, tmp_path: Path):
        captured: list[Path] = []

        def fake_check(path: Path) -> list[str]:
            captured.append(path)
            return []

        scaffold_fresh(tmp_path, bean_check=fake_check)
        assert len(captured) == 1
        assert captured[0].name == "main.bean"

    def test_rolls_back_on_bean_check_errors(self, tmp_path: Path):
        def failing_check(_: Path) -> list[str]:
            return ["synthetic error A", "synthetic error B"]

        with pytest.raises(ScaffoldError, match="synthetic error"):
            scaffold_fresh(tmp_path, bean_check=failing_check)

        # Nothing the scaffolder touched remains on disk.
        for name in _EXPECTED_FILES:
            assert not (tmp_path / name).exists(), (
                f"{name} was not rolled back on bean-check failure"
            )

    def test_rollback_preserves_unrelated_files(self, tmp_path: Path):
        (tmp_path / "unrelated.txt").write_text("preserved", encoding="utf-8")

        def failing_check(_: Path) -> list[str]:
            return ["boom"]

        with pytest.raises(ScaffoldError):
            scaffold_fresh(tmp_path, bean_check=failing_check)

        assert (tmp_path / "unrelated.txt").read_text(encoding="utf-8") == "preserved"


class TestIntegrationWithRealBeancount:
    """Prove the scaffolded ledger passes a real bean-check parse.

    Uses ``beancount.loader.load_file`` directly — same entry point
    our runtime uses. Empty-with-header files + the canonical
    main.bean should produce zero errors; the ``auto_accounts``
    plugin has nothing to insert because no transactions reference
    any accounts."""

    def test_real_bean_check_passes(self, tmp_path: Path):
        from beancount import loader

        def real_check(path: Path) -> list[str]:
            _entries, errors, _opts = loader.load_file(str(path))
            return [
                getattr(e, "message", str(e))
                for e in errors
                if "Auto-inserted" not in getattr(e, "message", str(e))
            ]

        result = scaffold_fresh(tmp_path, bean_check=real_check)
        assert result.ledger_dir == tmp_path
        assert len(result.created) == len(_EXPECTED_FILES)
