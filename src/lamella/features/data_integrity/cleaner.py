# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Physically remove duplicate transactions from
``simplefin_transactions.bean`` and its preview-mode sibling.

Contract:
  - Only touches files we own (``simplefin_transactions.bean``,
    ``simplefin_transactions.connector_preview.bean``).
  - Snapshot + restore: on bean-check failure, both files revert
    byte-for-byte.
  - bean-check runs against baseline so pre-existing unrelated
    errors don't block legitimate removals.
  - Returns (removed_ids, skipped_ids) — skipped means "not found
    in the file"; that's expected when an id belonged to an older
    ingest or a different source.

Block detection: the writer emits each txn as
``{date} * "..." "..."\\n  lamella-simplefin-id: "<id>"\\n  ... postings ...\\n``
followed by a blank line. We walk the file line-by-line, identify
date-headed blocks, parse out the simplefin id if present, and keep
only the blocks whose id is NOT in the remove set.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    run_bean_check_vs_baseline,
)

log = logging.getLogger(__name__)

# Any line starting with a date followed by `*` (or `!`, or `txn`)
# is a new transaction block header.
_TXN_HEADER_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\s+[*!]")

# Legacy txn-level SimpleFIN id line (pre-Phase-7b on-disk content).
_SFID_RE = re.compile(
    r'^\s*(?:lamella-simplefin-id|simplefin-id)\s*:\s*"(?P<sfid>[^"]+)"\s*$'
)
# New schema: posting-level paired source meta. Two paired keys with
# matching index N — `lamella-source-N: "simplefin"` and
# `lamella-source-reference-id-N: "<id>"`.
_SOURCE_NAME_RE = re.compile(
    r'^(?P<indent>\s*)lamella-source-(?P<idx>\d+)\s*:\s*"(?P<src>[^"]+)"\s*$'
)
_SOURCE_REF_RE = re.compile(
    r'^(?P<indent>\s*)lamella-source-reference-id-(?P<idx>\d+)\s*:\s*"(?P<ref>[^"]+)"\s*$'
)
# Lineage line — used as the insertion anchor for aliases on
# post-Phase-7b entries that no longer carry the legacy SFID line.
_TXN_ID_RE = re.compile(
    r'^(?P<indent>\s*)lamella-txn-id\s*:\s*"(?P<id>[^"]+)"\s*$'
)


@dataclass
class CleanupResult:
    removed_ids: list[str] = field(default_factory=list)
    skipped_ids: list[str] = field(default_factory=list)
    files_rewritten: list[str] = field(default_factory=list)

    @property
    def removed_count(self) -> int:
        return len(self.removed_ids)


_ALIASES_RE = re.compile(
    r'^(?P<indent>\s*)lamella-simplefin-aliases\s*:\s*"(?P<aliases>[^"]*)"\s*$'
)


def _extract_sfid_from_block(block_lines: list[str]) -> str | None:
    """Return the SimpleFIN reference id from a transaction block,
    matching either the legacy txn-level `lamella-simplefin-id` line
    or the post-Phase-7 paired source meta on a posting (any index N
    where `lamella-source-N: "simplefin"` is paired with
    `lamella-source-reference-id-N: "<id>"`).

    First match wins. None if the block has no SimpleFIN provenance.
    """
    sources_by_idx: dict[str, str] = {}
    refs_by_idx: dict[str, str] = {}
    for bl in block_lines:
        m = _SFID_RE.match(bl)
        if m:
            return m.group("sfid")
        sm = _SOURCE_NAME_RE.match(bl)
        if sm:
            sources_by_idx[sm.group("idx")] = sm.group("src")
            continue
        rm = _SOURCE_REF_RE.match(bl)
        if rm:
            refs_by_idx[rm.group("idx")] = rm.group("ref")
    for idx, src in sources_by_idx.items():
        if src == "simplefin" and idx in refs_by_idx:
            return refs_by_idx[idx]
    return None


def _aliases_insertion_index(block_lines: list[str]) -> tuple[int, str]:
    """Pick the (insert-after-index, indent) for a new
    `lamella-simplefin-aliases` line on a block that doesn't already
    carry one.

    Anchor preference, in order:
      1. Legacy `lamella-simplefin-id` line (puts aliases right next
         to the id they alias — readable).
      2. `lamella-txn-id` lineage line (post-Phase-7b new-format
         entries — aliases live at txn level alongside lineage).
      3. Header line itself (very old hand-edited content with
         no Lamella meta).
    Returns ``(insert_after_idx, indent_str)``. Caller inserts the
    aliases line at ``insert_after_idx + 1``.
    """
    header_idx = 0  # block_lines[0] is always the date header
    sfid_idx: int | None = None
    txn_id_idx: int | None = None
    txn_id_indent = "  "
    for idx, bl in enumerate(block_lines):
        if sfid_idx is None and _SFID_RE.match(bl):
            sfid_idx = idx
        m = _TXN_ID_RE.match(bl)
        if m and txn_id_idx is None:
            txn_id_idx = idx
            txn_id_indent = m.group("indent")
    if sfid_idx is not None:
        indent_m = re.match(r"^(\s*)", block_lines[sfid_idx])
        return sfid_idx, indent_m.group(1) if indent_m else "  "
    if txn_id_idx is not None:
        return txn_id_idx, txn_id_indent
    return header_idx, "  "


def _inject_aliases_into_block(
    block_lines: list[str], new_aliases: set[str],
) -> list[str]:
    """Given the lines of one txn block + a set of new alias ids,
    return a new list with the aliases merged into the block's
    ``lamella-simplefin-aliases`` metadata (created if missing,
    deduped + sorted if present).

    The aliases line lands right after the txn's primary identifier:
    legacy ``lamella-simplefin-id`` if present, else ``lamella-txn-id``,
    else right after the header. Aliases live at txn-meta level
    regardless of which schema the block uses — readers in
    ``simplefin/dedup`` look there.
    """
    if not new_aliases:
        return block_lines
    # Extract existing aliases (if any) so we can merge + dedupe.
    existing: set[str] = set()
    aliases_idx: int | None = None
    for idx, bl in enumerate(block_lines):
        m = _ALIASES_RE.match(bl)
        if m:
            aliases_idx = idx
            for tok in re.split(r"[,\s]+", m.group("aliases").strip()):
                if tok:
                    existing.add(tok)
            break
    merged = sorted(existing | set(new_aliases))
    rendered = " ".join(merged)
    if aliases_idx is not None:
        # Preserve the indent of the existing line.
        m = _ALIASES_RE.match(block_lines[aliases_idx])
        indent = m.group("indent") if m else "  "
        block_lines = list(block_lines)
        block_lines[aliases_idx] = (
            f'{indent}lamella-simplefin-aliases: "{rendered}"\n'
        )
        return block_lines
    # No existing aliases line — insert right after the chosen anchor.
    insert_after, indent = _aliases_insertion_index(block_lines)
    new_line = f'{indent}lamella-simplefin-aliases: "{rendered}"\n'
    out = list(block_lines)
    out.insert(insert_after + 1, new_line)
    return out


def _rewrite_file_without_sfids(
    path: Path, remove_ids: set[str],
    *,
    alias_targets: dict[str, str] | None = None,
) -> tuple[str, set[str]]:
    """Return (new_contents, removed_ids_seen_in_this_file). The
    caller handles snapshot + bean-check; this just produces the
    new text.

    ``alias_targets`` — ``{removed_id: keeper_id}``. For each
    removed id, the keeper block gets ``removed_id`` appended to
    its ``lamella-simplefin-aliases`` metadata. That way a future
    ingest where SimpleFIN re-issues the same event under yet
    another fresh id is still caught by dedup against the aliases.
    """
    if not path.exists():
        return "", set()
    text = path.read_text(encoding="utf-8")
    if not remove_ids:
        return text, set()
    alias_targets = alias_targets or {}
    # Build the reverse: {keeper_id: {removed_id, …}}
    keeper_to_removed: dict[str, set[str]] = {}
    for removed, keeper in alias_targets.items():
        keeper_to_removed.setdefault(keeper, set()).add(removed)

    out_lines: list[str] = []
    removed_here: set[str] = set()
    lines = text.splitlines(keepends=True)
    i = 0
    while i < len(lines):
        line = lines[i]
        if not _TXN_HEADER_RE.match(line):
            out_lines.append(line)
            i += 1
            continue
        # Collect the whole block — header + every following indented or
        # blank line until we hit another date-headed block OR
        # end-of-file. A block's trailing blank lines stay with it so
        # removal doesn't leave a stranded empty line.
        block_lines = [line]
        i += 1
        while i < len(lines):
            nxt = lines[i]
            if _TXN_HEADER_RE.match(nxt):
                break
            block_lines.append(nxt)
            i += 1
        # Parse out the simplefin id from this block (first match wins).
        # Accepts both legacy txn-level `lamella-simplefin-id` and the
        # post-Phase-7 paired source meta on a posting.
        sfid_here = _extract_sfid_from_block(block_lines)
        if sfid_here is not None and sfid_here in remove_ids:
            removed_here.add(sfid_here)
            # Drop the block entirely. Don't emit a blank line either;
            # we'll normalize trailing newlines at the end.
            continue
        # Keeper block — merge aliases if this id is targeted.
        if sfid_here is not None and sfid_here in keeper_to_removed:
            block_lines = _inject_aliases_into_block(
                block_lines, keeper_to_removed[sfid_here],
            )
        out_lines.extend(block_lines)
    new_text = "".join(out_lines)
    # Collapse any run of 3+ blank lines left behind by removals.
    new_text = re.sub(r"\n{3,}", "\n\n", new_text)
    # Ensure single trailing newline.
    if new_text and not new_text.endswith("\n"):
        new_text += "\n"
    return new_text, removed_here


def remove_duplicate_sfids(
    *,
    main_bean: Path,
    simplefin_transactions: Path,
    simplefin_preview: Path | None = None,
    remove_ids: list[str] | set[str],
    alias_targets: dict[str, str] | None = None,
    run_check: bool = True,
) -> CleanupResult:
    """Strip every transaction block whose lamella-simplefin-id is in
    ``remove_ids`` from ``simplefin_transactions.bean`` (and the
    preview file if given). Atomic: bean-check failure reverts both.

    ``alias_targets`` (``{removed_id: keeper_id}``) — for each
    removed id, the keeper block gains a ``lamella-simplefin-aliases``
    entry so SimpleFIN re-delivering the same event with yet another
    fresh id still gets caught by dedup. Dropping this arg degrades
    back to a simple physical removal (callers that aren't
    alias-aware, e.g. migration scripts).
    """
    remove_set = {str(x) for x in remove_ids if x}
    result = CleanupResult()
    if not remove_set:
        return result

    # Snapshot every file we might touch.
    targets: list[tuple[Path, bytes | None]] = []
    targets.append(
        (simplefin_transactions,
         simplefin_transactions.read_bytes()
         if simplefin_transactions.exists() else None)
    )
    if simplefin_preview is not None:
        targets.append(
            (simplefin_preview,
             simplefin_preview.read_bytes()
             if simplefin_preview.exists() else None)
        )
    main_before = main_bean.read_bytes() if main_bean.exists() else None

    _, baseline_output = (
        capture_bean_check(main_bean) if run_check and main_bean.exists()
        else (0, "")
    )

    def _restore() -> None:
        if main_before is not None:
            main_bean.write_bytes(main_before)
        for p, before in targets:
            if before is None:
                p.unlink(missing_ok=True)
            else:
                p.write_bytes(before)

    # Apply the rewrite to each target file.
    all_removed: set[str] = set()
    for path, _before in targets:
        new_text, removed_here = _rewrite_file_without_sfids(
            path, remove_set, alias_targets=alias_targets,
        )
        if removed_here:
            path.write_text(new_text, encoding="utf-8")
            all_removed |= removed_here
            result.files_rewritten.append(str(path))

    if run_check and main_bean.exists():
        try:
            run_bean_check_vs_baseline(main_bean, baseline_output)
        except BeanCheckError:
            _restore()
            raise

    result.removed_ids = sorted(all_removed)
    result.skipped_ids = sorted(remove_set - all_removed)
    return result
