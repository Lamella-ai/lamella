# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the ADR-0030 path-safety helper."""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.fs import UnsafePathError, validate_safe_path


def test_safe_path_within_root(tmp_path: Path) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    p = validate_safe_path("file.txt", allowed_roots=[root])
    assert p == (root / "file.txt").resolve()


def test_safe_path_escape_raises(tmp_path: Path) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    with pytest.raises(UnsafePathError):
        validate_safe_path("../escape.txt", allowed_roots=[root])


def test_safe_path_absolute_escape_raises(tmp_path: Path) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    with pytest.raises(UnsafePathError):
        validate_safe_path(str(other / "x.txt"), allowed_roots=[root])


def test_safe_path_double_dot_within_root(tmp_path: Path) -> None:
    root = tmp_path / "allowed"
    sub = root / "sub"
    sub.mkdir(parents=True)
    p = validate_safe_path("sub/../file.txt", allowed_roots=[root])
    assert p == (root / "file.txt").resolve()


def test_safe_path_rejects_symlink_traversal(tmp_path: Path) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    # A symlink inside the root pointing to an outside directory must
    # be refused even when the candidate path lexically stays under root.
    (root / "link").symlink_to(outside)
    with pytest.raises(UnsafePathError):
        validate_safe_path("link/file.txt", allowed_roots=[root])


def test_safe_path_rejects_reserved_backup_dir(tmp_path: Path) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    # External callers must not write into snapshot directory namespace.
    with pytest.raises(UnsafePathError):
        validate_safe_path(
            ".pre-inplace-20260427T120000/foo.bean",
            allowed_roots=[root],
        )


def test_safe_path_multiple_roots(tmp_path: Path) -> None:
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    root_a.mkdir()
    root_b.mkdir()
    p = validate_safe_path(
        str(root_b / "file.txt"), allowed_roots=[root_a, root_b],
    )
    assert p == (root_b / "file.txt").resolve()


def test_safe_path_empty_roots_raises(tmp_path: Path) -> None:
    with pytest.raises(UnsafePathError):
        validate_safe_path("file.txt", allowed_roots=[])
