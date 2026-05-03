# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Safety primitives for destructive ledger-file operations.

Every cleanup tool in this package writes directly to a ledger file
(simplefin_transactions.bean, connector_overrides.bean, …). Even with
bean-check + in-memory snapshot restore, a silent data-loss bug could
wipe a group of transactions the user meant to keep one of. This
module adds the defense-in-depth the user asked for:

  1. ``archive_before_change`` — write the original bytes of every
     target file to a timestamped backup under ``<ledger>/.backups/``
     before any mutation. Recoverable with a plain ``cp``.
  2. ``assert_would_keep_one_per_group`` — server-side invariant:
     given a list of "remove this id" selections and the detected
     duplicate groups, raise if any group would be emptied.

The goal: a user can run the cleanup, look at the result, and if
something looks wrong — even if the tool somehow got the UI math
backwards — the original data is one ``cp`` away.
"""
from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

log = logging.getLogger(__name__)


class WouldEmptyGroupError(Exception):
    """Raised when a cleanup request would remove every member of a
    detected duplicate group. Caller must include a keep-one per
    group or abort."""


@dataclass(frozen=True)
class ArchiveRecord:
    timestamp: str
    operation: str
    originals: list[Path]
    backups: list[Path]

    def describe(self) -> str:
        return (
            f"{self.timestamp} :: {self.operation} :: "
            f"{len(self.backups)} file(s) backed up"
        )


def archive_before_change(
    *,
    ledger_dir: Path,
    operation: str,
    target_files: list[Path],
) -> ArchiveRecord:
    """Copy every file in ``target_files`` to
    ``<ledger_dir>/.backups/<utc-iso>-<operation>/<filename>``.

    Skips files that don't exist (nothing to back up); the caller
    can rely on the returned ``backups`` list to know what was
    actually preserved. The backup directory is always created
    even when empty, so the archive trail is visible.
    """
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_root = ledger_dir / ".backups" / f"{ts}-{operation}"
    backup_root.mkdir(parents=True, exist_ok=True)
    originals: list[Path] = []
    backups: list[Path] = []
    for src in target_files:
        if not src.exists():
            continue
        dst = backup_root / src.name
        # If two target paths share a name (unusual but possible),
        # disambiguate with the parent dir name.
        if dst.exists():
            dst = backup_root / f"{src.parent.name}__{src.name}"
        shutil.copy2(src, dst)
        originals.append(src)
        backups.append(dst)
    record = ArchiveRecord(
        timestamp=ts, operation=operation,
        originals=originals, backups=backups,
    )
    log.info(
        "archive_before_change: %s; %d file(s) backed up under %s",
        operation, len(backups), backup_root,
    )
    return record


def assert_would_keep_one_per_group(
    *,
    groups: dict[str, list[str]],
    remove_ids: list[str] | set[str],
) -> None:
    """``groups`` is ``{group_key: [member_id, …]}``. Raises
    ``WouldEmptyGroupError`` if any group would have zero members
    after removing ``remove_ids`` — i.e. the user must always keep
    at least one in each group.

    Safe to call with unknown ids in ``remove_ids``; only the ids
    that appear in a detected group participate in the check.
    """
    remove_set = {str(x) for x in remove_ids}
    for key, members in groups.items():
        member_set = {str(m) for m in members}
        would_remove = member_set & remove_set
        if would_remove and would_remove == member_set:
            raise WouldEmptyGroupError(
                f"group {key!r} has {len(member_set)} member(s); "
                f"your request would remove all of them. Keep at least "
                f"one per group."
            )
