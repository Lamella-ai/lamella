# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Find and clean up stacked overrides — multiple ``lamella-override-of``
blocks pointing at the same source ``txn_hash``.

Background: before idempotent replace_existing in OverrideWriter, a
handler like ``/settings/loans/{slug}/record-payment`` would stack a
new override on top of the old one on each re-submit. Every bean-check
parse added *both* blocks, so the target account (loan principal,
interest, etc.) received the payment twice, thrice, etc. The UI showed
"recent payments" pulled from those postings, so the user saw visible
duplicates.

The fix in overrides.py prevents new duplicates. This module surfaces
the existing ones so the user can clean them up. Strategy: keep the
newest block (highest ``lamella-modified-at``) and drop the earlier ones,
since the most recent submission is what the user most likely wanted.
Readers that aren't sure can always undo and re-record.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path


_TXN_HEADER_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+\*")
# Accept the new lamella- prefix, the legacy bcg- prefix, and the
# pre-prefix bare key for one cutover release. Writers always emit
# lamella-* now; the alternates are read-only compatibility.
_OVERRIDE_OF_RE = re.compile(
    r'^\s*(?:lamella-override-of|bcg-override-of|override-of)\s*:\s*"(?P<hash>[^"]+)"\s*$'
)
_MODIFIED_AT_RE = re.compile(
    r'^\s*(?:lamella-modified-at|bcg-modified-at)\s*:\s*"(?P<at>[^"]+)"\s*$'
)


@dataclass
class OverrideBlock:
    target_hash: str
    txn_date: str
    modified_at: str | None
    block_text: str
    start_line: int  # 0-indexed position in the file


@dataclass
class StackedOverrideGroup:
    target_hash: str
    blocks: list[OverrideBlock] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.blocks)

    @property
    def excess(self) -> int:
        return max(0, self.count - 1)


def parse_overrides(overrides_path: Path) -> list[OverrideBlock]:
    """Parse every override block in ``connector_overrides.bean``,
    returning them in file order with their parsed target hash +
    modified_at timestamp (when present)."""
    if not overrides_path.exists():
        return []
    text = overrides_path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    out: list[OverrideBlock] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        m = _TXN_HEADER_RE.match(line)
        if not m:
            i += 1
            continue
        start = i
        block_lines = [line]
        date_str = m.group(1)
        i += 1
        while i < len(lines) and (
            lines[i].startswith("  ") or lines[i].strip() == ""
        ):
            if _TXN_HEADER_RE.match(lines[i]):
                break
            block_lines.append(lines[i])
            i += 1
        # Extract target hash + modified_at from the block.
        target_hash: str | None = None
        modified_at: str | None = None
        for bl in block_lines:
            if target_hash is None:
                hm = _OVERRIDE_OF_RE.match(bl)
                if hm:
                    target_hash = hm.group("hash")
            if modified_at is None:
                mm = _MODIFIED_AT_RE.match(bl)
                if mm:
                    modified_at = mm.group("at")
            if target_hash and modified_at:
                break
        if target_hash is None:
            # Non-override transaction (rare in this file, but be safe).
            continue
        out.append(OverrideBlock(
            target_hash=target_hash,
            txn_date=date_str,
            modified_at=modified_at,
            block_text="".join(block_lines),
            start_line=start,
        ))
    return out


def scan_stacked(overrides_path: Path) -> list[StackedOverrideGroup]:
    """Return every (target_hash) group that appears >1 time in the
    overrides file. Sorted by excess count descending so the worst
    offenders surface first.
    """
    blocks = parse_overrides(overrides_path)
    by_hash: dict[str, list[OverrideBlock]] = {}
    for b in blocks:
        by_hash.setdefault(b.target_hash, []).append(b)
    groups = [
        StackedOverrideGroup(target_hash=h, blocks=bs)
        for h, bs in by_hash.items()
        if len(bs) > 1
    ]
    groups.sort(key=lambda g: (-g.excess, g.target_hash))
    return groups


def dedupe_stacked(
    overrides_path: Path,
    *,
    target_hashes: list[str] | None = None,
    keep_strategy: str = "newest",
) -> int:
    """Collapse every stacked group down to a single block.

    ``target_hashes`` — if provided, only dedupe those; otherwise
    dedupe every stacked group in the file.
    ``keep_strategy`` — ``"newest"`` keeps the one with the latest
    ``lamella-modified-at`` (falls back to last in file when modified_at
    missing); ``"oldest"`` keeps the first. ``"newest"`` default
    because it reflects the user's most recent submission.

    Returns the number of override blocks removed.
    """
    groups = scan_stacked(overrides_path)
    if target_hashes is not None:
        wanted = set(target_hashes)
        groups = [g for g in groups if g.target_hash in wanted]
    if not groups:
        return 0

    # Build a set of (start_line, block_text) tuples to drop. We keep
    # the chosen block and delete every other block with the same hash.
    drop_starts: set[int] = set()
    for g in groups:
        if keep_strategy == "oldest":
            keeper = g.blocks[0]
        else:
            # newest: pick the block with the max modified_at string
            # (ISO timestamps compare lexicographically). When
            # modified_at is missing on some, prefer the one with a
            # timestamp; fall back to last in file.
            def _key(b: OverrideBlock) -> tuple:
                return (b.modified_at or "", b.start_line)
            keeper = max(g.blocks, key=_key)
        for b in g.blocks:
            if b is keeper:
                continue
            drop_starts.add(b.start_line)

    # Rewrite the file without the dropped blocks.
    text = overrides_path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    out_lines: list[str] = []
    i = 0
    removed = 0
    while i < len(lines):
        if i in drop_starts:
            # Skip through this entire block.
            i += 1
            while i < len(lines) and (
                lines[i].startswith("  ") or lines[i].strip() == ""
            ):
                if _TXN_HEADER_RE.match(lines[i]):
                    break
                i += 1
            removed += 1
            continue
        out_lines.append(lines[i])
        i += 1
    new_text = "".join(out_lines)
    # Collapse any 3+ blank-line runs left behind.
    new_text = re.sub(r"\n{3,}", "\n\n", new_text)
    overrides_path.write_text(new_text, encoding="utf-8")
    return removed
