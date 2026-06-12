# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ``with_bean_snapshot`` — the atomic-write envelope.

Phase 3 of /setup/recovery. Replaces the inline snapshot/restore
calls in ``import_apply.py``. Phases 5 (schema migrations) and 6
(bulk repair) reuse this; the tests here pin the contract
behaviors those phases will rely on.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.features.recovery.snapshot import (
    BeanSnapshotCheckError,
    with_bean_snapshot,
)


def _write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


class TestCleanExit:
    def test_writes_persist_when_no_exception(self, tmp_path: Path):
        f = tmp_path / "main.bean"
        _write(f, "before")

        with with_bean_snapshot([f]) as snap:
            f.write_text("after", encoding="utf-8")
            snap.add_touched(f)

        assert f.read_text(encoding="utf-8") == "after"

    def test_touched_files_recorded(self, tmp_path: Path):
        a = tmp_path / "a.bean"
        b = tmp_path / "b.bean"
        _write(a, "A")
        _write(b, "B")

        with with_bean_snapshot([a, b]) as snap:
            a.write_text("A2", encoding="utf-8")
            snap.add_touched(a)

        assert snap.touched_files == (a,)

    def test_touched_dedup(self, tmp_path: Path):
        # Calling add_touched twice for the same path doesn't
        # produce duplicates.
        f = tmp_path / "x.bean"
        _write(f, "x")
        with with_bean_snapshot([f]) as snap:
            snap.add_touched(f)
            snap.add_touched(f)
        assert snap.touched_files == (f,)


class TestExceptionRollback:
    def test_exception_inside_block_restores_files(self, tmp_path: Path):
        f = tmp_path / "main.bean"
        _write(f, "original")

        with pytest.raises(RuntimeError, match="boom"):
            with with_bean_snapshot([f]):
                f.write_text("trash", encoding="utf-8")
                raise RuntimeError("boom")

        # File restored to pre-block state byte-for-byte.
        assert f.read_text(encoding="utf-8") == "original"

    def test_exception_unlinks_files_that_didnt_exist_at_entry(self, tmp_path: Path):
        # Heal actions sometimes create files (a Close directive in a
        # connector_accounts.bean that didn't exist yet). On
        # failure, those files must be removed — leaving them around
        # would shape-mismatch the pre-entry state.
        existing = tmp_path / "existed.bean"
        appearing = tmp_path / "new.bean"
        _write(existing, "kept")
        # appearing does NOT exist at entry.

        with pytest.raises(RuntimeError):
            with with_bean_snapshot([existing, appearing]):
                appearing.write_text("created during block", encoding="utf-8")
                raise RuntimeError("boom")

        assert existing.read_text(encoding="utf-8") == "kept"
        assert not appearing.exists(), (
            "files that didn't exist at entry must be unlinked on rollback"
        )

    def test_multiple_files_all_restored(self, tmp_path: Path):
        a = tmp_path / "a.bean"
        b = tmp_path / "b.bean"
        c = tmp_path / "c.bean"
        _write(a, "A0")
        _write(b, "B0")
        _write(c, "C0")

        with pytest.raises(RuntimeError):
            with with_bean_snapshot([a, b, c]):
                a.write_text("A1", encoding="utf-8")
                b.write_text("B1", encoding="utf-8")
                c.write_text("C1", encoding="utf-8")
                raise RuntimeError("boom")

        assert a.read_text(encoding="utf-8") == "A0"
        assert b.read_text(encoding="utf-8") == "B0"
        assert c.read_text(encoding="utf-8") == "C0"


class TestBeanCheck:
    def test_success_when_check_returns_no_errors(self, tmp_path: Path):
        f = tmp_path / "main.bean"
        _write(f, "original")

        with with_bean_snapshot([f], bean_check=lambda p: []) as snap:
            f.write_text("new", encoding="utf-8")
            snap.add_touched(f)

        assert f.read_text(encoding="utf-8") == "new"

    def test_check_failure_restores_and_raises(self, tmp_path: Path):
        f = tmp_path / "main.bean"
        _write(f, "good")

        def failing_check(_p: Path) -> list[str]:
            return ["synthetic balance error"]

        with pytest.raises(BeanSnapshotCheckError) as exc_info:
            with with_bean_snapshot([f], bean_check=failing_check) as snap:
                f.write_text("bad", encoding="utf-8")
                snap.add_touched(f)

        # File restored — the bad write didn't survive.
        assert f.read_text(encoding="utf-8") == "good"
        # Errors carried on the exception for the caller.
        assert exc_info.value.errors == ["synthetic balance error"]

    def test_check_uses_bean_check_path_when_given(self, tmp_path: Path):
        f1 = tmp_path / "a.bean"
        f2 = tmp_path / "main.bean"
        _write(f1, "a")
        _write(f2, "m")

        seen = {}
        def capturing_check(path: Path) -> list[str]:
            seen["path"] = path
            return []

        with with_bean_snapshot(
            [f1, f2], bean_check=capturing_check, bean_check_path=f2,
        ):
            pass

        assert seen["path"] == f2

    def test_check_defaults_to_first_declared_path(self, tmp_path: Path):
        f1 = tmp_path / "main.bean"
        f2 = tmp_path / "other.bean"
        _write(f1, "x")
        _write(f2, "y")

        seen = {}
        def capturing_check(path: Path) -> list[str]:
            seen["path"] = path
            return []

        with with_bean_snapshot([f1, f2], bean_check=capturing_check):
            pass
        assert seen["path"] == f1

    def test_no_paths_no_check_path_skips_check(self, tmp_path: Path):
        # Edge case: caller passed an empty path set with a
        # bean_check. There's nothing to check; treat as no-op.
        called = {"n": 0}

        def check(_p: Path) -> list[str]:
            called["n"] += 1
            return ["should not see this"]

        with with_bean_snapshot([], bean_check=check):
            pass

        assert called["n"] == 0


class TestNonExistentPaths:
    def test_path_not_existing_at_entry_isnt_an_error(self, tmp_path: Path):
        # A heal action may declare it might create connector_accounts.bean
        # if it doesn't exist. with_bean_snapshot accepts this.
        appearing = tmp_path / "future.bean"
        assert not appearing.exists()

        with with_bean_snapshot([appearing]) as snap:
            appearing.write_text("created", encoding="utf-8")
            snap.add_touched(appearing)

        assert appearing.read_text(encoding="utf-8") == "created"
