# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""``with_bean_snapshot`` context manager — atomic write envelope.

Replaces the inline _snapshot_bean_files / _restore_snapshots
pair that lived in ``bootstrap.import_apply``. Phases 5 (schema
migrations) and 6 (bulk repair apply) reuse this same context
manager; getting the API right in Phase 3 means those phases
inherit working infrastructure rather than each one re-rolling
the snapshot/restore call sequence.

Per SETUP_IMPLEMENTATION.md: takes a **declared path set**, not
the whole ledger. Heal actions know which files they touch;
snapshotting the whole tree on every Close click is wasteful and
hides bugs where an action writes outside its declared set.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Iterator

_LOG = logging.getLogger(__name__)

BeanCheck = Callable[[Path], list[str]]


class BeanSnapshotCheckError(Exception):
    """Raised when the optional ``bean_check`` returned errors after
    a clean with-block exit. The snapshot has already been restored
    by the time this raises — the caller can re-raise as a
    user-facing error or log and move on."""

    def __init__(self, errors: list[str]):
        self.errors = list(errors)
        detail = "; ".join(errors[:3])
        if len(errors) > 3:
            detail += f" ... ({len(errors) - 3} more)"
        super().__init__(f"bean-check failed: {detail}")


@dataclass
class BeanSnapshot:
    """Handle yielded by ``with_bean_snapshot``. Heal actions call
    ``add_touched`` after every write so the result envelope can
    summarize what changed. The snapshot's restore set is fixed at
    entry — files outside the declared set are not snapshotted and
    writing to one is a programmer error."""

    paths: tuple[Path, ...]
    """The fixed declared-path set. Files outside this set are not
    snapshotted. Provided for caller introspection only — caller
    should not mutate."""

    _content: dict[Path, bytes] = field(default_factory=dict, repr=False)
    """The byte-level snapshot taken at entry. Internal."""

    _existed_at_entry: set[Path] = field(default_factory=set, repr=False)
    """Subset of ``paths`` that existed when the snapshot was taken.
    Files that didn't exist get unlinked on restore rather than
    written-back-as-empty."""

    _touched: list[Path] = field(default_factory=list, repr=False)

    @property
    def touched_files(self) -> tuple[Path, ...]:
        return tuple(self._touched)

    def add_touched(self, path: Path) -> None:
        """Record that the caller has written to ``path``. Doesn't
        gate the write — purely informational. Caller is trusted to
        only write to paths inside the declared set; writing
        outside is undefined behavior (the file won't be restored
        on failure)."""
        if path not in self._touched:
            self._touched.append(path)


def _take_snapshot(paths: Iterable[Path]) -> BeanSnapshot:
    snap = BeanSnapshot(paths=tuple(paths))
    for path in snap.paths:
        try:
            if path.is_file():
                snap._content[path] = path.read_bytes()
                snap._existed_at_entry.add(path)
            # Non-existent files: not snapshotted; restored by
            # unlinking if they appeared during the with-block.
        except OSError:
            _LOG.exception("snapshot read failed for %s", path)
            raise
    return snap


def _restore(snap: BeanSnapshot) -> None:
    """Restore every file in the declared set to its pre-entry state.
    Existing-at-entry files get their bytes rewritten; not-existed-at-entry
    files get unlinked if they appeared during the with-block."""
    for path in snap.paths:
        try:
            if path in snap._existed_at_entry:
                path.write_bytes(snap._content[path])
            elif path.exists():
                path.unlink()
        except OSError:
            _LOG.exception("snapshot restore failed for %s", path)


@contextmanager
def with_bean_snapshot(
    paths: Iterable[Path],
    *,
    bean_check: BeanCheck | None = None,
    bean_check_path: Path | None = None,
) -> Iterator[BeanSnapshot]:
    """Snapshot the given files on entry. Restore on exception or
    on a ``bean_check`` failure post-write.

    Args:
        paths: declared file set the action will write to. Iterable
            consumed once at entry. Files in the set that don't yet
            exist are tracked too — they get unlinked on restore if
            they appeared during the block.
        bean_check: optional callable invoked after the with-block
            exits cleanly. If it returns a non-empty list of error
            strings, the snapshot is restored and
            ``BeanSnapshotCheckError`` is raised.
        bean_check_path: the path passed to ``bean_check``. Defaults
            to the first path in the set; pass main.bean explicitly
            when the declared set doesn't include it.

    Yields:
        a ``BeanSnapshot`` the caller uses to ``add_touched(path)``
        after each write.

    Raises:
        BeanSnapshotCheckError: bean_check returned errors. Snapshot
            already restored.
        OSError: snapshot or restore I/O failed. Caller's writes may
            be partially restored — best-effort.
        Anything else the caller raises inside the with-block —
            re-raised after restore completes.
    """
    paths_tuple = tuple(paths)
    snap = _take_snapshot(paths_tuple)
    try:
        yield snap
    except BaseException:
        _restore(snap)
        raise

    if bean_check is None:
        return

    check_path = bean_check_path
    if check_path is None and paths_tuple:
        check_path = paths_tuple[0]
    if check_path is None:
        # No paths declared and no bean_check_path given → nothing
        # to check. Caller asked for a no-op envelope.
        return

    errors = bean_check(check_path)
    if errors:
        _restore(snap)
        raise BeanSnapshotCheckError(errors)
