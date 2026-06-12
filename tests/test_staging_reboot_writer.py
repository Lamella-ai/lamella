# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the file-side reboot writer — NEXTGEN.md Phase E2b."""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.features.import_.staging import (
    RebootApplyError,
    RebootWriter,
    noop_cleaner,
)


@pytest.fixture(autouse=True)
def stub_bean_check(monkeypatch):
    monkeypatch.setattr(
        "lamella.features.import_.staging.reboot_writer.capture_bean_check",
        lambda _main: (0, ""),
    )
    monkeypatch.setattr(
        "lamella.features.import_.staging.reboot_writer.run_bean_check_vs_baseline",
        lambda _main, _baseline: None,
    )


def _write_main(dir_: Path, extra_files: dict[str, str] | None = None) -> Path:
    body = 'option "operating_currency" "USD"\n2020-01-01 open Assets:Bank USD\n'
    (dir_ / "main.bean").write_text(body, encoding="utf-8")
    for name, content in (extra_files or {}).items():
        (dir_ / name).write_text(content, encoding="utf-8")
    return dir_ / "main.bean"


class TestPrepare:
    def test_noop_cleaner_produces_empty_diffs(self, tmp_path: Path):
        main = _write_main(tmp_path, {"accounts.bean": "; accounts\n"})
        rw = RebootWriter(ledger_dir=tmp_path, main_bean=main)
        plan = rw.prepare(noop_cleaner)
        assert not plan.has_changes
        # Both .bean files are in the reboot dir, byte-identical.
        for d in plan.diffs:
            assert not d.changed
            assert d.proposed_path.read_bytes() == d.path.read_bytes()

    def test_cleaner_output_lands_in_reboot_dir(self, tmp_path: Path):
        main = _write_main(tmp_path)
        rw = RebootWriter(ledger_dir=tmp_path, main_bean=main)
        rw.prepare(lambda p, t: "; touched\n" + t)
        assert (tmp_path / ".reboot" / "main.bean").exists()
        original = main.read_text(encoding="utf-8")
        proposed = (tmp_path / ".reboot" / "main.bean").read_text(encoding="utf-8")
        assert proposed.startswith("; touched\n")
        # Original untouched.
        assert original == main.read_text(encoding="utf-8")

    def test_plan_reports_changed_files(self, tmp_path: Path):
        main = _write_main(tmp_path, {"accounts.bean": "; old\n"})
        rw = RebootWriter(ledger_dir=tmp_path, main_bean=main)
        def clean_accounts(p, t):
            return "; new\n" if p.name == "accounts.bean" else t
        plan = rw.prepare(clean_accounts)
        changed = [d for d in plan.diffs if d.changed]
        assert len(changed) == 1
        assert changed[0].path.name == "accounts.bean"
        assert "; new" in changed[0].unified_diff


class TestApplyRollback:
    def test_apply_overwrites_originals_with_proposed(self, tmp_path: Path):
        main = _write_main(tmp_path)
        rw = RebootWriter(ledger_dir=tmp_path, main_bean=main)
        rw.prepare(lambda p, t: t + "; appended\n")
        result = rw.apply()
        assert result.files_overwritten >= 1
        # Main.bean now carries the appended comment.
        assert main.read_text(encoding="utf-8").endswith("; appended\n")
        # .reboot/ was cleaned up.
        assert not (tmp_path / ".reboot").exists()
        # A backup dir exists with the original bytes.
        assert result.backup_dir.exists()
        assert "option" in (result.backup_dir / "main.bean").read_text()

    def test_apply_without_prepare_raises(self, tmp_path: Path):
        main = _write_main(tmp_path)
        rw = RebootWriter(ledger_dir=tmp_path, main_bean=main)
        with pytest.raises(RebootApplyError, match="no reboot plan|no proposed"):
            rw.apply()

    def test_rollback_restores_from_latest_backup(self, tmp_path: Path):
        main = _write_main(tmp_path)
        original = main.read_text(encoding="utf-8")
        rw = RebootWriter(ledger_dir=tmp_path, main_bean=main)
        rw.prepare(lambda p, t: t + "; appended\n")
        rw.apply()
        assert "appended" in main.read_text(encoding="utf-8")
        result = rw.rollback()
        assert result.files_restored >= 1
        assert main.read_text(encoding="utf-8") == original

    def test_rollback_with_no_backups_raises(self, tmp_path: Path):
        main = _write_main(tmp_path)
        rw = RebootWriter(ledger_dir=tmp_path, main_bean=main)
        with pytest.raises(RebootApplyError, match="no pre-reboot backups"):
            rw.rollback()

    def test_apply_bean_check_failure_rolls_back(
        self, tmp_path: Path, monkeypatch,
    ):
        main = _write_main(tmp_path)
        original = main.read_bytes()
        rw = RebootWriter(ledger_dir=tmp_path, main_bean=main)
        rw.prepare(lambda p, t: t + "; corrupted\n")
        from lamella.core.ledger_writer import BeanCheckError
        monkeypatch.setattr(
            "lamella.features.import_.staging.reboot_writer.run_bean_check_vs_baseline",
            lambda _main, _baseline: (_ for _ in ()).throw(BeanCheckError("boom")),
        )
        with pytest.raises(RebootApplyError, match="bean-check"):
            rw.apply()
        # Original bytes restored.
        assert main.read_bytes() == original

    def test_clean_ledger_apply_is_idempotent(self, tmp_path: Path):
        """Apply the noop cleaner on a clean ledger; ledger bytes
        equal original bytes after apply. This is the E2 exit-
        criterion idempotency assertion."""
        main = _write_main(tmp_path)
        original = main.read_bytes()
        rw = RebootWriter(ledger_dir=tmp_path, main_bean=main)
        rw.prepare(noop_cleaner)
        rw.apply()
        assert main.read_bytes() == original
