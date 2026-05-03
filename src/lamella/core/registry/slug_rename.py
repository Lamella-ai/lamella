# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Cross-file slug rename for entities and vehicles.

Renames `:{old}:` or `:{old}$` segments in every .bean file under the
ledger directory. Used to (a) fix typo'd slugs and (b) merge two slugs
that mean the same thing (FarmCo ↔ FarmCo).

Safety model:
  1. Compute a dry-run diff first — every file + line that would change.
  2. Snapshot every file to be rewritten.
  3. Apply replacements.
  4. Run bean-check. Baseline-compare with the pre-write output so
     pre-existing plugin complaints don't block legitimate renames.
  5. On failure, restore all snapshots atomically.
  6. Update accounts_meta.entity_slug and merchant_memory.entity_slug
     to point at the new slug.

Scope: only rewrites account path segments at a specific index.
Segment index 1 = entity (`Assets:WidgetCo:...`).
Segment index 2 under `Vehicles` = vehicle slug
(`Expenses:Vehicles:2009WorkSUV:...`).
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    run_bean_check_vs_baseline,
)

log = logging.getLogger(__name__)


@dataclass
class PreviewLine:
    file: str
    line_number: int
    before: str
    after: str


@dataclass
class RenamePreview:
    old: str
    new: str
    segment_index: int
    lines: list[PreviewLine]

    @property
    def file_count(self) -> int:
        return len({line.file for line in self.lines})

    @property
    def line_count(self) -> int:
        return len(self.lines)


def _compiled_segment_re(old: str, segment_index: int) -> re.Pattern:
    r"""Match `:old:` or `:old` at end of account path, only where the
    `old` slug appears at the given segment index.

    For segment_index=1 (entity), we look for patterns like
    `{root}:old:` where root is Assets|Liabilities|Expenses|Income|Equity.
    For segment_index=2 under Vehicles, the pattern is
    `{root}:Vehicles:old(:|$)`.

    The lookahead ``(?=[:\s]|$)`` avoids substring matches like
    ``WidgetCo`` matching inside ``WidgetCoOther``.
    """
    if segment_index == 1:
        # (^|\s)(Assets|Liabilities|Expenses|Income|Equity):old(?=[:\s$])
        return re.compile(
            rf"(?P<prefix>(^|\s)(?:Assets|Liabilities|Expenses|Income|Equity)):"
            rf"{re.escape(old)}(?=[:\s]|$)"
        )
    # Vehicle slug (under :Vehicles:)
    return re.compile(
        rf"(?P<prefix>(^|\s)(?:Assets|Liabilities|Expenses|Income|Equity):"
        rf"(?:[A-Za-z0-9-]+:)?Vehicles):{re.escape(old)}(?=[:\s]|$)"
    )


def build_preview(
    *,
    ledger_dir: Path,
    old: str,
    new: str,
    segment_index: int = 1,
) -> RenamePreview:
    """Compute what the rename would change without touching any file."""
    if not ledger_dir.is_dir():
        raise ValueError(f"ledger_dir {ledger_dir} is not a directory")
    if old == new or not old or not new:
        raise ValueError("old and new slugs must differ and be non-empty")
    pattern = _compiled_segment_re(old, segment_index)

    lines: list[PreviewLine] = []
    for path in sorted(ledger_dir.rglob("*.bean")):
        try:
            content = path.read_text(encoding="utf-8")
        except Exception:
            continue
        for lineno, raw in enumerate(content.splitlines(), start=1):
            if pattern.search(raw) is None:
                continue
            replaced = pattern.sub(lambda m: f"{m.group('prefix')}:{new}", raw)
            if replaced != raw:
                lines.append(PreviewLine(
                    file=str(path.relative_to(ledger_dir)),
                    line_number=lineno,
                    before=raw,
                    after=replaced,
                ))
    return RenamePreview(old=old, new=new, segment_index=segment_index, lines=lines)


def apply_rename(
    *,
    main_bean: Path,
    ledger_dir: Path,
    old: str,
    new: str,
    segment_index: int,
    conn: sqlite3.Connection,
    run_check: bool = True,
    data_dir: Path | None = None,
) -> RenamePreview:
    """Apply the rename atomically: snapshot every file, rewrite, run
    bean-check (baseline-compared), roll back on any failure, update
    the DB. Returns the preview of what was changed.

    If `data_dir` is provided, an auto-backup tarball is created first
    so the user can restore via /settings/backups if they later decide
    the rewrite was a mistake.
    """
    preview = build_preview(
        ledger_dir=ledger_dir, old=old, new=new, segment_index=segment_index,
    )
    if not preview.lines:
        return preview

    # Auto-backup every touched-file state before the rewrite. Small
    # cost; enormous upside when the user realizes they renamed wrong.
    if data_dir is not None:
        try:
            from lamella.core.registry.backup import create_backup
            create_backup(
                ledger_dir=ledger_dir,
                data_dir=data_dir,
                label=f"pre-rename-{old}-to-{new}",
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("pre-rename auto-backup failed: %s (proceeding)", exc)

    # Snapshot
    touched_files = sorted({line.file for line in preview.lines})
    snapshots: dict[str, bytes] = {}
    for f in touched_files:
        full = ledger_dir / f
        snapshots[f] = full.read_bytes()

    # Baseline bean-check.
    baseline_output = ""
    if run_check:
        _, baseline_output = capture_bean_check(main_bean)

    # Apply.
    pattern = _compiled_segment_re(old, segment_index)
    try:
        for f in touched_files:
            full = ledger_dir / f
            text = full.read_text(encoding="utf-8")
            rewritten = pattern.sub(lambda m: f"{m.group('prefix')}:{new}", text)
            if rewritten != text:
                full.write_text(rewritten, encoding="utf-8")

        if run_check:
            run_bean_check_vs_baseline(main_bean, baseline_output)
    except BeanCheckError:
        # Restore every snapshot.
        for f, blob in snapshots.items():
            (ledger_dir / f).write_bytes(blob)
        raise
    except Exception:
        for f, blob in snapshots.items():
            (ledger_dir / f).write_bytes(blob)
        raise

    # DB fixup.
    try:
        if segment_index == 1:
            conn.execute(
                "UPDATE accounts_meta SET account_path = REPLACE(account_path, :old, :new), "
                "entity_slug = CASE WHEN entity_slug = :old2 THEN :new2 ELSE entity_slug END "
                "WHERE account_path LIKE '%:' || :old3 || ':%' "
                "OR account_path LIKE '%:' || :old4 "
                "OR entity_slug = :old5",
                {
                    "old": f":{old}:", "new": f":{new}:",
                    "old2": old, "new2": new,
                    "old3": old, "old4": old, "old5": old,
                },
            )
            conn.execute(
                "UPDATE merchant_memory SET entity_slug = :new WHERE entity_slug = :old",
                {"old": old, "new": new},
            )
            conn.execute(
                "UPDATE loans SET entity_slug = :new WHERE entity_slug = :old",
                {"old": old, "new": new},
            )
            conn.execute(
                "UPDATE vehicles SET entity_slug = :new WHERE entity_slug = :old",
                {"old": old, "new": new},
            )
            # Delete or merge the old entity row.
            existing_new = conn.execute(
                "SELECT slug FROM entities WHERE slug = ?", (new,)
            ).fetchone()
            if existing_new:
                # Merge: old row is deleted (metadata already on the new row).
                conn.execute("DELETE FROM entities WHERE slug = ?", (old,))
            else:
                # Rename: change the row.
                conn.execute(
                    "UPDATE entities SET slug = ? WHERE slug = ?",
                    (new, old),
                )
        elif segment_index == 2:
            existing_new = conn.execute(
                "SELECT slug FROM vehicles WHERE slug = ?", (new,)
            ).fetchone()
            if existing_new:
                conn.execute("DELETE FROM vehicles WHERE slug = ?", (old,))
            else:
                conn.execute(
                    "UPDATE vehicles SET slug = ? WHERE slug = ?",
                    (new, old),
                )
    except Exception as exc:  # noqa: BLE001
        log.error("slug rename DB update failed post-file-rewrite: %s", exc)
        # Files are already rewritten; we don't roll them back here since
        # the ledger is self-consistent and bean-check passed. Operator
        # can retry the DB update manually.
    return preview
