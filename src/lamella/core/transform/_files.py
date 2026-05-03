# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Shared helpers for transform passes: enumerate Connector-owned files,
snapshot+rollback them as a group, and defer to the existing bean-check
tolerance helpers in receipts.linker so pre-existing ledger errors don't
look like new regressions."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from lamella.core.config import Settings
from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    run_bean_check_vs_baseline,
)

log = logging.getLogger(__name__)


@dataclass
class FileSnapshot:
    path: Path
    pre_bytes: bytes | None  # None if file didn't exist pre-write

    def restore(self) -> None:
        if self.pre_bytes is None:
            self.path.unlink(missing_ok=True)
        else:
            self.path.write_bytes(self.pre_bytes)


def snapshot(path: Path) -> FileSnapshot:
    return FileSnapshot(
        path=path,
        pre_bytes=path.read_bytes() if path.exists() else None,
    )


def connector_owned_files(settings: Settings) -> list[Path]:
    """Every ledger file the Connector writes to. Anything not on this
    list is user-authored and must never be rewritten by a transform
    pass."""
    out: list[Path] = []
    for p in (
        settings.connector_links_path,
        settings.connector_overrides_path,
        settings.connector_accounts_path,
        settings.simplefin_transactions_path,
        settings.simplefin_preview_path,
        settings.mileage_summary_path,
    ):
        if p.exists():
            out.append(p)
    imports_dir = settings.import_ledger_output_dir_resolved
    if imports_dir.exists():
        for p in sorted(imports_dir.glob("*.bean")):
            out.append(p)
    return out


def run_check_with_rollback(
    main_bean: Path,
    baseline_output: str,
    snapshots: list[FileSnapshot],
    *,
    run_check: bool,
) -> None:
    if not run_check:
        return
    try:
        run_bean_check_vs_baseline(main_bean, baseline_output)
    except BeanCheckError:
        log.error("bean-check regressed after transform; rolling back")
        for snap in snapshots:
            snap.restore()
        raise


def baseline(main_bean: Path, *, run_check: bool) -> str:
    if not run_check:
        return ""
    _, out = capture_bean_check(main_bean)
    return out
