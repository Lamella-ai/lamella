# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""File-side reboot workflow — NEXTGEN.md Phase E2b.

Companion to the reboot scan (E1) and metadata retrofit (E2):
pluggable write pipeline for preparing cleaned copies of every
ledger ``.bean`` file in an isolated ``.reboot/`` directory,
showing the user per-file diffs, and applying with rollback-
safe backups to ``.pre-reboot-<timestamp>/``.

The cleaner is a caller-supplied ``(path, text) -> text`` function.
Phase E2b ships a ``noop_cleaner`` baseline that produces byte-
identical output (the idempotency assertion: a clean ledger
round-trips through prepare+apply unchanged). Phase E3 will add
cleaners that reclassify via current AI context, collapse
retrofitted duplicate lines, etc.

Discipline:
  * ``prepare`` writes only inside ``.reboot/``. Originals are
    not touched.
  * ``apply`` snapshots every ``.bean`` in the ledger dir into
    ``.pre-reboot-<timestamp>/`` before any overwrite.
  * ``bean-check`` runs against the post-apply state; on new
    errors, every file is restored from the backup and
    ``RebootApplyError`` is raised.
  * ``rollback`` restores from the most recent (or specified)
    ``.pre-reboot-<timestamp>/`` directory.
"""
from __future__ import annotations

import difflib
import logging
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable, Iterable

from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    run_bean_check_vs_baseline,
)

log = logging.getLogger(__name__)

__all__ = [
    "RebootApplyError",
    "RebootPlan",
    "RebootWriter",
    "FileDiff",
    "noop_cleaner",
]


Cleaner = Callable[[Path, str], str]


def noop_cleaner(_path: Path, text: str) -> str:
    """Baseline cleaner: output equals input. Apply on a clean
    ledger is a no-op."""
    return text


class RebootApplyError(Exception):
    """Apply refused or failed; files were rolled back."""


@dataclass(frozen=True)
class FileDiff:
    path: Path
    proposed_path: Path
    unified_diff: str
    changed: bool


@dataclass
class RebootPlan:
    """Output of ``prepare``: what would change on apply."""
    reboot_dir: Path
    diffs: tuple[FileDiff, ...] = ()
    files_created: tuple[Path, ...] = ()  # .reboot/<filename>
    errors: list[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return any(d.changed for d in self.diffs)


@dataclass
class RebootApplyResult:
    backup_dir: Path
    files_restored: int = 0
    files_overwritten: int = 0
    bean_check_ok: bool = True


class RebootWriter:
    """Pluggable prepare/apply/rollback for ledger-level reboots."""

    def __init__(
        self,
        *,
        ledger_dir: Path,
        main_bean: Path,
    ):
        self.ledger_dir = ledger_dir
        self.main_bean = main_bean
        self.reboot_dir = ledger_dir / ".reboot"

    # -- prepare -------------------------------------------------------

    def prepare(self, cleaner: Cleaner = noop_cleaner) -> RebootPlan:
        """Run ``cleaner`` over every ``.bean`` file and write the
        result to ``ledger_dir / .reboot / filename``. Returns a
        ``RebootPlan`` carrying per-file diffs."""
        self.reboot_dir.mkdir(parents=True, exist_ok=True)
        diffs: list[FileDiff] = []
        created: list[Path] = []
        errors: list[str] = []

        for path in sorted(self.ledger_dir.glob("*.bean")):
            try:
                original = path.read_text(encoding="utf-8")
            except OSError as exc:
                errors.append(f"{path.name}: read failed ({exc})")
                continue
            try:
                cleaned = cleaner(path, original)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{path.name}: cleaner raised {exc}")
                cleaned = original
            out = self.reboot_dir / path.name
            out.write_text(cleaned, encoding="utf-8")
            created.append(out)
            udiff = "".join(
                difflib.unified_diff(
                    original.splitlines(keepends=True),
                    cleaned.splitlines(keepends=True),
                    fromfile=str(path),
                    tofile=str(out),
                    n=2,
                )
            )
            diffs.append(
                FileDiff(
                    path=path,
                    proposed_path=out,
                    unified_diff=udiff,
                    changed=(cleaned != original),
                )
            )

        return RebootPlan(
            reboot_dir=self.reboot_dir,
            diffs=tuple(diffs),
            files_created=tuple(created),
            errors=errors,
        )

    # -- apply ---------------------------------------------------------

    def apply(self, *, backup_label: str | None = None) -> RebootApplyResult:
        """Move prepared files from ``.reboot/`` into the ledger dir.

        Every ``.bean`` file in the ledger dir is first copied into
        ``.pre-reboot-<timestamp>/`` (or ``<backup_label>`` if
        provided). Then the proposed files from ``.reboot/`` overwrite
        the originals. ``bean-check`` runs against the post-apply
        ledger; on new errors, all files are restored from the backup
        and ``RebootApplyError`` is raised.
        """
        if not self.reboot_dir.is_dir():
            raise RebootApplyError(
                f"no reboot plan found at {self.reboot_dir}; run prepare first"
            )
        proposed = sorted(self.reboot_dir.glob("*.bean"))
        if not proposed:
            raise RebootApplyError(
                f"no proposed files in {self.reboot_dir}; run prepare first"
            )

        ts = backup_label or datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        backup_dir = self.ledger_dir / f".pre-reboot-{ts}"
        backup_dir.mkdir(parents=True, exist_ok=False)

        # 1. Snapshot every .bean in the ledger dir.
        for src in sorted(self.ledger_dir.glob("*.bean")):
            shutil.copy2(src, backup_dir / src.name)

        # 2. bean-check baseline before we touch anything.
        _rc, baseline = capture_bean_check(self.main_bean)

        result = RebootApplyResult(backup_dir=backup_dir)
        touched: list[Path] = []
        try:
            # 3. Overwrite each ledger file with its proposed version.
            for prop in proposed:
                dest = self.ledger_dir / prop.name
                shutil.copy2(prop, dest)
                touched.append(dest)
                result.files_overwritten += 1
            # 4. bean-check the result.
            run_bean_check_vs_baseline(self.main_bean, baseline)
        except BeanCheckError as exc:
            log.warning(
                "reboot apply bean-check failed (%s) — rolling back", exc,
            )
            self._restore_from(backup_dir)
            raise RebootApplyError(f"bean-check failed after apply: {exc}") from exc
        except Exception:
            log.exception("reboot apply raised — rolling back")
            self._restore_from(backup_dir)
            raise

        # 5. Clean up the prepared dir so the next prepare is fresh.
        shutil.rmtree(self.reboot_dir, ignore_errors=True)
        log.info(
            "reboot applied: %d file(s) overwritten, backup at %s",
            result.files_overwritten, backup_dir,
        )
        return result

    # -- rollback ------------------------------------------------------

    def list_backups(self) -> list[Path]:
        """Return all ``.pre-reboot-*`` directories, newest first."""
        return sorted(
            (p for p in self.ledger_dir.glob(".pre-reboot-*") if p.is_dir()),
            reverse=True,
        )

    def rollback(self, backup_dir: Path | None = None) -> RebootApplyResult:
        """Restore from the most recent (or specified) backup dir."""
        if backup_dir is None:
            backups = self.list_backups()
            if not backups:
                raise RebootApplyError("no pre-reboot backups found")
            backup_dir = backups[0]
        if not backup_dir.is_dir():
            raise RebootApplyError(
                f"backup dir does not exist: {backup_dir}"
            )
        count = self._restore_from(backup_dir)
        return RebootApplyResult(
            backup_dir=backup_dir,
            files_restored=count,
        )

    def _restore_from(self, backup_dir: Path) -> int:
        count = 0
        for src in sorted(backup_dir.glob("*.bean")):
            shutil.copy2(src, self.ledger_dir / src.name)
            count += 1
        return count
