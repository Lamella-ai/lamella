# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""One-shot rewrite of legacy ``bcg-*`` keys to ``lamella-*`` in
Connector-owned ledger files.

At-load normalization (``lamella._legacy_meta``) handles the legacy
prefix transparently in memory, so this transform is *optional*:
the app reads either prefix correctly. Run this when you want clean
``.bean`` content on disk — typically once, after the rebrand has
soaked for a while and no rollback to the old code is on the table.

Scope — ONLY Connector-owned files are touched (see ``_files.py``).
The user's hand-authored files are never rewritten.

Idempotent: a line that already uses ``lamella-`` does not match
the legacy regex and is left alone. Running this twice is a no-op.

Dry-run by default (prints unified diffs + counts). Pass ``--apply``
to write. Each writable pass snapshots every target file, runs
bean-check vs. a pre-pass baseline, and rolls every file back if a
new error appears.
"""
from __future__ import annotations

import argparse
import difflib
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from lamella.core.config import Settings
from lamella.core.transform._files import (
    FileSnapshot,
    baseline,
    connector_owned_files,
    run_check_with_rollback,
    snapshot,
)

log = logging.getLogger(__name__)


# Matches a metadata continuation line. We rewrite the key only,
# preserving the operator's original whitespace and value formatting.
_META_KEY_RE = re.compile(
    r"^(?P<indent>[ \t]+)bcg-(?P<rest>[A-Za-z][A-Za-z0-9_-]*):(?=[ \t])",
    re.MULTILINE,
)

# Matches a transaction-line tag like ``#bcg-override``. Tags can
# appear anywhere on a transaction header, separated by whitespace.
_TAG_RE = re.compile(r"#bcg-(?P<rest>[A-Za-z][A-Za-z0-9_-]*)")

# Matches a custom directive type like ``custom "bcg-ledger-version"``.
_CUSTOM_TYPE_RE = re.compile(r'(custom\s+)"bcg-(?P<rest>[A-Za-z][A-Za-z0-9_-]*)"')


@dataclass
class FileChange:
    path: Path
    diff: str
    count: int


def rewrite_text(text: str) -> tuple[str, int]:
    """Apply all three rewrites to ``text``. Returns the new text and
    the number of substitutions made."""
    count = 0

    def _meta(m: re.Match) -> str:
        nonlocal count
        count += 1
        return f"{m.group('indent')}lamella-{m.group('rest')}:"

    def _tag(m: re.Match) -> str:
        nonlocal count
        count += 1
        return f"#lamella-{m.group('rest')}"

    def _custom(m: re.Match) -> str:
        nonlocal count
        count += 1
        return f'{m.group(1)}"lamella-{m.group("rest")}"'

    text = _META_KEY_RE.sub(_meta, text)
    text = _TAG_RE.sub(_tag, text)
    text = _CUSTOM_TYPE_RE.sub(_custom, text)
    return text, count


def collect_changes(files: list[Path]) -> list[FileChange]:
    out: list[FileChange] = []
    for path in files:
        if not path.exists():
            continue
        original = path.read_text(encoding="utf-8")
        rewritten, count = rewrite_text(original)
        if count == 0:
            continue
        diff = "".join(
            difflib.unified_diff(
                original.splitlines(keepends=True),
                rewritten.splitlines(keepends=True),
                fromfile=str(path),
                tofile=str(path),
            )
        )
        out.append(FileChange(path=path, diff=diff, count=count))
    return out


def apply_changes(
    settings: Settings, changes: list[FileChange]
) -> tuple[bool, str | None]:
    """Write each change, snapshotting first. Run bean-check vs. the
    pre-pass baseline and roll every file back if a new error appears.
    Returns (ok, error_message)."""
    if not changes:
        return True, None
    snapshots: list[FileSnapshot] = [snapshot(c.path) for c in changes]
    base = baseline(settings.ledger_main)
    for c in changes:
        rewritten, _ = rewrite_text(c.path.read_text(encoding="utf-8"))
        c.path.write_text(rewritten, encoding="utf-8")
    err = run_check_with_rollback(
        main_bean=settings.ledger_main, baseline=base, snapshots=snapshots
    )
    if err is not None:
        return False, err
    return True, None


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write the rewrites (default: dry-run + diffs).",
    )
    args = parser.parse_args(argv)

    settings = Settings()
    files = connector_owned_files(settings)
    changes = collect_changes(files)

    if not changes:
        log.info("no bcg- references found in connector-owned files; nothing to do.")
        return 0

    total = sum(c.count for c in changes)
    log.info(
        "found %d bcg- references across %d connector-owned files",
        total,
        len(changes),
    )
    for c in changes:
        log.info("  %s — %d substitutions", c.path, c.count)
        if not args.apply:
            sys.stdout.write(c.diff)

    if not args.apply:
        log.info("dry-run; pass --apply to write.")
        return 0

    ok, err = apply_changes(settings, changes)
    if not ok:
        log.error("bean-check failed after rewrite — every file rolled back: %s", err)
        return 1
    log.info("applied %d rewrites; bean-check clean.", total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
