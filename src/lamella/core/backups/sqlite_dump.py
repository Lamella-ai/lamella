# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)

_BACKUP_NAME_RE = re.compile(r"^connector-(\d{8})\.sqlite$")


@dataclass(frozen=True)
class BackupResult:
    path: Path | None
    created: bool
    pruned: tuple[Path, ...]
    skipped_reason: str | None = None


def _is_writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError:
        return False
    return os.access(path, os.W_OK)


def _prune(backup_dir: Path, keep: int) -> list[Path]:
    candidates: list[tuple[str, Path]] = []
    for child in backup_dir.iterdir():
        m = _BACKUP_NAME_RE.match(child.name)
        if m and child.is_file():
            candidates.append((m.group(1), child))
    candidates.sort(key=lambda t: t[0], reverse=True)
    removed: list[Path] = []
    for _, path in candidates[keep:]:
        try:
            path.unlink()
            removed.append(path)
        except OSError as exc:
            log.warning("failed to prune old backup %s: %s", path, exc)
    return removed


def run_backup(
    *,
    db_path: Path,
    backup_dir: Path,
    today: date | None = None,
    keep: int = 30,
) -> BackupResult:
    """Dump the app's SQLite database to `backup_dir/connector-YYYYMMDD.sqlite`.

    - Skip (no-op) if today's file already exists.
    - Retain the `keep` most-recent dated backups; older files are deleted.
    - Never raise on a non-writable target: log a WARNING and return with
      `skipped_reason` set so the scheduler keeps running.
    """
    day = today or date.today()

    if not db_path.exists():
        return BackupResult(path=None, created=False, pruned=(), skipped_reason="db-missing")

    if not _is_writable(backup_dir):
        log.warning("backup dir %s is not writable; skipping", backup_dir)
        return BackupResult(path=None, created=False, pruned=(), skipped_reason="not-writable")

    target = backup_dir / f"connector-{day.strftime('%Y%m%d')}.sqlite"
    if target.exists():
        pruned = _prune(backup_dir, keep=keep)
        return BackupResult(path=target, created=False, pruned=tuple(pruned), skipped_reason="exists")

    src = sqlite3.connect(str(db_path))
    try:
        dst = sqlite3.connect(str(target))
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()

    pruned = _prune(backup_dir, keep=keep)
    log.info(
        "sqlite backup written to %s (pruned %d old file(s))", target, len(pruned)
    )
    return BackupResult(path=target, created=True, pruned=tuple(pruned))
