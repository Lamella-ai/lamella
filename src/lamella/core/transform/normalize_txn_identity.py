# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 4 of NORMALIZE_TXN_IDENTITY.md — one-shot transform that
brings the on-disk ledger into the post-normalization schema:

  * Mints ``lamella-txn-id`` (UUIDv7) on every transaction that
    lacks one. The minted id is what every downstream subsystem
    (AI decisions, override pointers) keys off, so this is the
    universal change.
  * Migrates the legacy transaction-level source keys
    (``lamella-simplefin-id`` / bare ``simplefin-id`` /
    ``lamella-import-txn-id``) down to the source-side (first)
    posting as paired indexed source meta
    (``lamella-source-N`` + ``lamella-source-reference-id-N``).
  * Drops retired keys that violate the reconstruct rule:
    ``lamella-import-id`` (a SQLite PK) and ``lamella-import-source``
    (free-form debug).
  * Backfills ``ai_decisions.input_ref`` so per-txn AI history can
    do a single-column equality lookup again. Decisions logged
    under a SimpleFIN id, importer composite, or txn_hash get
    rewritten to the entry's new lineage id.

Discipline (mirrors ``rewrite/txn_inplace.py``):

  * Snapshot every modified file to ``.pre-normalize-<ISO-timestamp>/``
    BEFORE any byte changes.
  * Line-based edits preserve whitespace, comments, and surviving
    meta lines exactly.
  * Run ``bean-check`` against a pre-pass baseline; on any new
    error, every file rolls back from its snapshot.
  * Dry-run by default; ``--apply`` performs writes.

The agent will not run ``--apply`` unprompted. Modifying the user's
production ledger is the destructive-operation line per CLAUDE.md /
Auto Mode. The user runs it explicitly when ready.
"""
from __future__ import annotations

import argparse
import logging
import re
import shutil
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

from beancount.core.data import Transaction
from beancount.parser import parser

from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.config import Settings
from lamella.core.identity import (
    REF_KEY,
    SOURCE_KEY,
    TXN_ID_KEY,
    mint_txn_id,
)
from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    run_bean_check_vs_baseline,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Legacy txn-level keys we migrate down to posting-level paired source meta.
_LEGACY_SIMPLEFIN_KEYS = ("lamella-simplefin-id", "simplefin-id")
_LEGACY_IMPORT_TXN_ID_KEY = "lamella-import-txn-id"

# Retired entirely (reconstruct violation or pure debug):
_RETIRED_TXN_KEYS = frozenset({
    "lamella-import-id",       # SQLite PK
    "lamella-import-source",   # free-form "source=X row=Y" debug
})

# Directory name prefixes we never descend into.
_FORBIDDEN_DIR_PREFIXES = (
    "_archive",
    ".pre-inplace",
    ".pre-normalize",
    ".pre-reboot",
    ".reboot",
    ".git",
    "__pycache__",
)

# Regex for a posting line: indented account followed by amount/currency.
# Matches the same shape as rewrite/txn_inplace's _POSTING_LINE_RE.
_POSTING_LINE_RE = re.compile(
    r"^(?P<indent>\s+)(?P<account>[A-Z][A-Za-z0-9:_\-]+)(?P<rest>\s+.*)$"
)

# Regex for a meta-style line: indented "key: value". Beancount allows
# bare and quoted values; we capture the lot and only ever rewrite the
# key half.
_META_LINE_RE = re.compile(
    r"^(?P<indent>\s+)(?P<key>[a-zA-Z][\w-]*):\s*(?P<value>.*)$"
)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class _PostingBlock:
    """One posting + its trailing meta lines, as parsed from raw text."""
    posting_idx: int                          # 0-indexed line of the posting
    indent: str                               # leading whitespace
    account: str
    meta_lines: list[tuple[int, str, str, str]] = field(default_factory=list)
    # tuples are (line_idx, indent, key, value)


@dataclass
class _TxnEdit:
    """All edits to make to one transaction's text region."""
    header_idx: int                           # 0-indexed header line
    delete_lines: set[int] = field(default_factory=set)
    # Map of `insert_before_idx` → list of new line strings (with newline).
    insertions: dict[int, list[str]] = field(default_factory=dict)
    # Lineage id minted (None if the txn already had one).
    lineage_minted: str | None = None
    # Lineage id resolved for this txn — present whether minted or pre-existing.
    lineage_resolved: str | None = None
    # Identifiers we migrated; used to backfill ai_decisions.
    migrated_simplefin_ids: list[str] = field(default_factory=list)
    migrated_csv_ids: list[str] = field(default_factory=list)
    # The composite "source=X row=Y" string we dropped, if any. Parsed
    # back to (source_id, row_num) for the importer-row backfill map.
    dropped_import_source: str | None = None
    # txn_hash of the entry, computed from the parsed entry — invariant
    # under metadata-only edits, so safe to compute once.
    txn_hash_value: str | None = None


@dataclass
class _FileChange:
    path: Path
    txn_edits: list[_TxnEdit]
    new_text: str
    pre_text: str

    @property
    def n_lineage_minted(self) -> int:
        return sum(1 for t in self.txn_edits if t.lineage_minted)

    @property
    def n_simplefin_migrated(self) -> int:
        return sum(len(t.migrated_simplefin_ids) for t in self.txn_edits)

    @property
    def n_csv_migrated(self) -> int:
        return sum(len(t.migrated_csv_ids) for t in self.txn_edits)

    @property
    def n_changed_txns(self) -> int:
        return sum(
            1 for t in self.txn_edits
            if t.delete_lines or t.insertions or t.lineage_minted
        )


@dataclass
class _BackfillMaps:
    """Identity → lineage maps used to rewrite ai_decisions.input_ref."""
    simplefin_to_lineage: dict[str, str] = field(default_factory=dict)
    csv_to_lineage: dict[str, str] = field(default_factory=dict)
    # Composite key "source_id:row_num" → lineage. Built from the
    # ``lamella-import-source`` debug string before we drop it.
    import_composite_to_lineage: dict[str, str] = field(default_factory=dict)
    txn_hash_to_lineage: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# File enumeration
# ---------------------------------------------------------------------------

def collect_bean_files(ledger_dir: Path) -> list[Path]:
    """Every ``*.bean`` file under ``ledger_dir`` worth touching.

    Skips snapshot/reboot directories and similar so we never edit
    a backup of ourselves into a confused state.
    """
    out: list[Path] = []
    if not ledger_dir.exists():
        return out
    for path in sorted(ledger_dir.rglob("*.bean")):
        rel_parts = path.relative_to(ledger_dir).parts
        if any(
            part.startswith(_FORBIDDEN_DIR_PREFIXES) for part in rel_parts[:-1]
        ):
            continue
        out.append(path)
    return out


# ---------------------------------------------------------------------------
# Per-file planning
# ---------------------------------------------------------------------------

def _strip_string_value(raw: str) -> str:
    """Strip surrounding quotes off a beancount-rendered string value.

    The parser hands us the unquoted form, but we rebuild edits from raw
    text where values are still quoted. Used only when we need to compare
    to a parsed string (e.g. checking whether a posting already has a
    given source pair).
    """
    s = raw.strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        return s[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    return s


def _q(value: str) -> str:
    """Quote a string for inclusion in a beancount meta value."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _walk_txn_block(
    lines: list[str], header_idx: int,
) -> tuple[list[tuple[int, str, str, str]], list[_PostingBlock], int]:
    """Walk the indented block following a transaction header.

    Returns ``(txn_meta, posting_blocks, end_idx)`` where:
      * ``txn_meta`` is a list of ``(line_idx, indent, key, value)``
        tuples for meta lines that appear BEFORE the first posting.
      * ``posting_blocks`` is one entry per posting; each has its own
        list of meta lines that follow it.
      * ``end_idx`` is the index of the first line NOT part of this
        transaction (blank, un-indented, or end-of-file).
    """
    txn_meta: list[tuple[int, str, str, str]] = []
    posting_blocks: list[_PostingBlock] = []
    n = len(lines)
    j = header_idx + 1
    while j < n:
        line = lines[j]
        stripped = line.strip()
        if not stripped:
            break
        if not line.startswith((" ", "\t")):
            # Comment-only lines starting with ; need to be tolerated
            # as part of the txn block — beancount allows them.
            if stripped.startswith(";"):
                j += 1
                continue
            break
        # Posting line first — its leading account name has the same
        # indent shape as a meta line, but the regex requires capital
        # letter + colon-separated account path which meta keys lack.
        pm = _POSTING_LINE_RE.match(line)
        if pm:
            posting_blocks.append(_PostingBlock(
                posting_idx=j,
                indent=pm.group("indent"),
                account=pm.group("account"),
            ))
            j += 1
            continue
        mm = _META_LINE_RE.match(line)
        if mm:
            entry = (j, mm.group("indent"), mm.group("key"), mm.group("value"))
            if posting_blocks:
                posting_blocks[-1].meta_lines.append(entry)
            else:
                txn_meta.append(entry)
            j += 1
            continue
        # Indented comment / unknown — pass through unchanged.
        j += 1
    return txn_meta, posting_blocks, j


def _existing_source_pairs_on_posting(
    posting_block: _PostingBlock,
) -> tuple[set[tuple[str, str]], set[int]]:
    """Read paired indexed source meta off a posting block.

    Returns ``(pairs, used_indexes)`` where:
      * ``pairs`` is the set of ``(source_name, reference_id)`` tuples
        already present on the posting.
      * ``used_indexes`` is the set of integers ``N`` for which an
        indexed key (either ``-N`` half) is present. Bare un-indexed
        pairs land in ``pairs`` but contribute no integer.
    """
    indexed: dict[int, dict[str, str]] = {}
    bare: dict[str, str] = {}
    used_indexes: set[int] = set()
    for (_, _, key, raw_value) in posting_block.meta_lines:
        value = _strip_string_value(raw_value)
        m = re.match(rf"^{re.escape(SOURCE_KEY)}-(\d+)$", key)
        if m:
            i = int(m.group(1))
            indexed.setdefault(i, {})["src"] = value
            used_indexes.add(i)
            continue
        m = re.match(rf"^{re.escape(REF_KEY)}-(\d+)$", key)
        if m:
            i = int(m.group(1))
            indexed.setdefault(i, {})["ref"] = value
            used_indexes.add(i)
            continue
        if key == SOURCE_KEY:
            bare["src"] = value
        elif key == REF_KEY:
            bare["ref"] = value
    pairs: set[tuple[str, str]] = set()
    for slot in indexed.values():
        if "src" in slot and "ref" in slot:
            pairs.add((slot["src"], slot["ref"]))
    if "src" in bare and "ref" in bare:
        pairs.add((bare["src"], bare["ref"]))
    return pairs, used_indexes


def _next_free_index(used: set[int]) -> int:
    """Lowest non-negative integer not in ``used``."""
    i = 0
    while i in used:
        i += 1
    return i


def _build_source_meta_lines(
    indent: str, source: str, reference_id: str, index: int, newline: str,
) -> list[str]:
    """Render the two paired indexed-source meta lines for a posting."""
    return [
        f'{indent}{SOURCE_KEY}-{index}: "{_q(source)}"{newline}',
        f'{indent}{REF_KEY}-{index}: "{_q(reference_id)}"{newline}',
    ]


def _detect_newline(line: str) -> str:
    if line.endswith("\r\n"):
        return "\r\n"
    if line.endswith("\n"):
        return "\n"
    return "\n"


def _txn_indent(lines: list[str], txn_meta_lines: list, posting_blocks: list[_PostingBlock]) -> str:
    """Pick the indent string used for txn-level meta lines.

    Prefer an existing meta line's indent (matches the source file's
    convention). Fall back to the first posting's indent. Default to
    two spaces if the txn has neither.
    """
    if txn_meta_lines:
        return txn_meta_lines[0][1]
    if posting_blocks:
        return posting_blocks[0].indent
    return "  "


def _existing_lineage_in_txn_meta(
    txn_meta_lines: list[tuple[int, str, str, str]],
) -> str | None:
    """Read ``lamella-txn-id`` out of the raw txn-meta block. Returns
    None if absent. Lets us plan an edit without needing a parsed
    Transaction (the in-place rewriter caller has lines + header_idx
    but no parsed entry)."""
    for (_idx, _indent, key, raw_value) in txn_meta_lines:
        if key == TXN_ID_KEY:
            value = _strip_string_value(raw_value)
            return value or None
    return None


def _plan_txn_from_lines(
    lines: list[str], header_idx: int,
    *,
    backfill: _BackfillMaps | None = None,
) -> _TxnEdit | None:
    """Compute identity-normalization edits for one transaction
    using only its raw lines + header line index.

    No parsed Transaction required — used both by the bulk transform
    (which adds ``txn_hash`` separately for the AI-decisions backfill
    map) and by the in-place rewriter (which doesn't need backfill).
    Returns ``None`` if the transaction is already normalized.
    """
    if header_idx < 0 or header_idx >= len(lines):
        return None
    txn_meta, posting_blocks, _end_idx = _walk_txn_block(lines, header_idx)
    edit = _TxnEdit(header_idx=header_idx)

    # ------------------------------------------------------------------
    # Lineage — mint if absent.
    # ------------------------------------------------------------------
    existing_lineage = _existing_lineage_in_txn_meta(txn_meta)
    if existing_lineage:
        lineage = existing_lineage
        edit.lineage_resolved = lineage
    else:
        lineage = mint_txn_id()
        edit.lineage_resolved = lineage
        edit.lineage_minted = lineage
        # Insert a `  lamella-txn-id: "..."` line right after the header.
        # Use the canonical txn-meta indent for the file.
        indent = _txn_indent(lines, txn_meta, posting_blocks)
        # Use the header line's newline style.
        newline = _detect_newline(lines[header_idx])
        new_line = f'{indent}{TXN_ID_KEY}: "{lineage}"{newline}'
        edit.insertions.setdefault(header_idx + 1, []).append(new_line)

    # ------------------------------------------------------------------
    # Identify legacy txn-level source keys + plan deletions.
    # ------------------------------------------------------------------
    sf_ref: str | None = None
    csv_ref: str | None = None
    for (line_idx, _indent, key, raw_value) in txn_meta:
        value = _strip_string_value(raw_value)
        if key in _LEGACY_SIMPLEFIN_KEYS:
            if value and sf_ref is None:
                sf_ref = value
            edit.delete_lines.add(line_idx)
        elif key == _LEGACY_IMPORT_TXN_ID_KEY:
            if value and csv_ref is None:
                csv_ref = value
            edit.delete_lines.add(line_idx)
        elif key in _RETIRED_TXN_KEYS:
            edit.delete_lines.add(line_idx)
            if key == "lamella-import-source":
                # Format: "source=X row=Y" — captured for ai_decisions
                # backfill of legacy importer rows.
                edit.dropped_import_source = value

    if backfill is not None:
        if sf_ref:
            backfill.simplefin_to_lineage[sf_ref] = lineage
            edit.migrated_simplefin_ids.append(sf_ref)
        if csv_ref:
            backfill.csv_to_lineage[csv_ref] = lineage
            edit.migrated_csv_ids.append(csv_ref)
        if edit.dropped_import_source:
            composite = _parse_import_source(edit.dropped_import_source)
            if composite is not None:
                backfill.import_composite_to_lineage[composite] = lineage
    else:
        if sf_ref:
            edit.migrated_simplefin_ids.append(sf_ref)
        if csv_ref:
            edit.migrated_csv_ids.append(csv_ref)

    # ------------------------------------------------------------------
    # Stamp paired source meta on the source-side posting.
    # ------------------------------------------------------------------
    if (sf_ref or csv_ref) and posting_blocks:
        target_posting = posting_blocks[0]
        existing_pairs, used_indexes = _existing_source_pairs_on_posting(
            target_posting
        )
        # Posting's first meta line indent (or 4-space default if none).
        if target_posting.meta_lines:
            posting_indent = target_posting.meta_lines[0][1]
        else:
            posting_indent = target_posting.indent + "  "
        newline = _detect_newline(lines[target_posting.posting_idx])
        # Inserted lines go right after the posting line, before any
        # existing meta lines on that posting.
        insert_before = target_posting.posting_idx + 1
        new_lines: list[str] = []
        if sf_ref and ("simplefin", sf_ref) not in existing_pairs:
            idx = _next_free_index(used_indexes)
            used_indexes.add(idx)
            new_lines.extend(_build_source_meta_lines(
                posting_indent, "simplefin", sf_ref, idx, newline,
            ))
        if csv_ref and ("csv", csv_ref) not in existing_pairs:
            idx = _next_free_index(used_indexes)
            used_indexes.add(idx)
            new_lines.extend(_build_source_meta_lines(
                posting_indent, "csv", csv_ref, idx, newline,
            ))
        if new_lines:
            edit.insertions.setdefault(insert_before, []).extend(new_lines)

    if not edit.delete_lines and not edit.insertions:
        # Nothing to do — pre-normalized entry.
        return None
    return edit


def _plan_txn(
    lines: list[str],
    txn,
    *,
    backfill: _BackfillMaps,
) -> _TxnEdit | None:
    """Bulk-transform per-txn planner. Wraps the line-only planner and
    additionally records ``txn_hash → lineage`` for the AI-decisions
    backfill (only needed by the bulk path)."""
    edit = _plan_txn_from_lines(lines, txn.meta["lineno"] - 1, backfill=backfill)
    if edit is None:
        return None
    th = txn_hash(txn)
    edit.txn_hash_value = th
    if th and edit.lineage_resolved:
        backfill.txn_hash_to_lineage[th] = edit.lineage_resolved
    return edit


def normalize_one_transaction_in_lines(
    lines: list[str], header_idx: int,
) -> tuple[list[str], bool]:
    """Apply identity normalization to one transaction in-place
    within the given lines list.

    Used by the in-place rewriter so that any txn we touch for some
    other reason (FIXME → category, M→N posting rewrite) also gets
    its legacy identity meta cleaned up — the system converges
    legacy entries onto the new schema as the user goes, no bulk
    run required.

    Returns ``(new_lines, changed)``. When ``changed`` is False the
    returned list is the same object as ``lines``.
    """
    edit = _plan_txn_from_lines(lines, header_idx, backfill=None)
    if edit is None:
        return lines, False
    new_text = _apply_edits_to_lines(lines, [edit])
    return new_text.splitlines(keepends=True), True


def _parse_import_source(value: str) -> str | None:
    """Parse a ``lamella-import-source`` value (``"source=X row=Y"``)
    into the canonical ``"X:Y"`` composite key used in
    ``import_composite_to_lineage``.

    Returns ``None`` if the value doesn't match the expected shape — we
    can't safely backfill the AI decision in that case.
    """
    if not value:
        return None
    src_match = re.search(r"source=(\S+)", value)
    row_match = re.search(r"row=(\S+)", value)
    if not src_match or not row_match:
        return None
    return f"{src_match.group(1)}:{row_match.group(1)}"


def _apply_edits_to_lines(
    lines: list[str], edits: list[_TxnEdit],
) -> str:
    """Apply line-level inserts + deletes from every edit and return the
    resulting text. Edits are commutative (insertions are keyed by
    ``insert_before_idx``, deletes by ``line_idx``) so we can build
    output in a single forward pass."""
    delete_set: set[int] = set()
    insert_map: dict[int, list[str]] = {}
    for e in edits:
        delete_set.update(e.delete_lines)
        for idx, payload in e.insertions.items():
            insert_map.setdefault(idx, []).extend(payload)
    out: list[str] = []
    for i, line in enumerate(lines):
        if i in insert_map:
            out.extend(insert_map[i])
        if i not in delete_set:
            out.append(line)
    # Insertions targeted at end-of-file (index == len(lines)).
    if len(lines) in insert_map:
        out.extend(insert_map[len(lines)])
    return "".join(out)


def plan_file(
    path: Path, *, backfill: _BackfillMaps,
) -> _FileChange | None:
    """Parse one ``.bean`` file and compute its edit plan. Returns
    ``None`` when the file is already normalized (no diff)."""
    pre_text = path.read_text(encoding="utf-8")
    lines = pre_text.splitlines(keepends=True)
    try:
        entries, _errors, _options = parser.parse_file(str(path))
    except Exception as exc:  # noqa: BLE001
        log.warning("normalize: failed to parse %s — skipping. %s", path, exc)
        return None

    txn_edits: list[_TxnEdit] = []
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        # Some plugins synthesize entries with meta from a different
        # file; skip anything not anchored to this file.
        if e.meta.get("filename") and Path(e.meta["filename"]) != path:
            continue
        edit = _plan_txn(lines, e, backfill=backfill)
        if edit is not None:
            txn_edits.append(edit)
    if not txn_edits:
        return None
    new_text = _apply_edits_to_lines(lines, txn_edits)
    if new_text == pre_text:
        return None
    return _FileChange(
        path=path, txn_edits=txn_edits, new_text=new_text, pre_text=pre_text,
    )


# ---------------------------------------------------------------------------
# Whole-pass orchestration
# ---------------------------------------------------------------------------

@dataclass
class TransformResult:
    files_planned: int
    files_changed: int
    txns_changed: int
    lineage_minted: int
    simplefin_migrated: int
    csv_migrated: int
    ai_decisions_backfilled: int
    snapshot_dir: Path | None
    applied: bool
    bean_check_error: str | None = None


def _snapshot_files(
    changes: list[_FileChange], ledger_dir: Path,
) -> Path:
    """Copy every changing file to ``.pre-normalize-<ISO-timestamp>/``
    under ``ledger_dir`` BEFORE any byte changes. Returns the snapshot
    root."""
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    snapshot_root = ledger_dir / f".pre-normalize-{ts}"
    snapshot_root.mkdir(parents=True, exist_ok=False)
    for c in changes:
        rel = c.path.relative_to(ledger_dir)
        dest = snapshot_root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(c.path, dest)
    return snapshot_root


def _restore_from_snapshot(
    changes: list[_FileChange], snapshot_root: Path, ledger_dir: Path,
) -> None:
    for c in changes:
        rel = c.path.relative_to(ledger_dir)
        src = snapshot_root / rel
        if src.exists():
            shutil.copy2(src, c.path)


def _backfill_ai_decisions(
    conn: sqlite3.Connection, maps: _BackfillMaps,
) -> int:
    """Rewrite ``ai_decisions.input_ref`` to the entry's lineage id when
    we can resolve the existing input_ref to a known map. Decisions we
    can't resolve are left alone (legacy unreachable rows or entries
    that no longer exist).

    Returns the number of rows updated.
    """
    cur = conn.execute("SELECT id, input_ref FROM ai_decisions")
    rows = cur.fetchall()
    updated = 0
    for row in rows:
        decision_id = row[0]
        ref = row[1]
        if not ref:
            continue
        new_ref: str | None = None
        # Already a lineage id? lamella-txn-id is a UUIDv7 — 36 chars
        # with the canonical 8-4-4-4-12 hex layout. Skip those.
        if _looks_like_uuid(ref):
            continue
        if ref in maps.simplefin_to_lineage:
            new_ref = maps.simplefin_to_lineage[ref]
        elif ref in maps.csv_to_lineage:
            new_ref = maps.csv_to_lineage[ref]
        elif ref in maps.txn_hash_to_lineage:
            new_ref = maps.txn_hash_to_lineage[ref]
        else:
            # Importer composite: "import:<id>:row:<row_id>"
            m = re.match(r"^import:(\S+):row:(\S+)$", ref)
            if m:
                composite = f"{m.group(1)}:{m.group(2)}"
                new_ref = maps.import_composite_to_lineage.get(composite)
        if new_ref and new_ref != ref:
            conn.execute(
                "UPDATE ai_decisions SET input_ref = ? WHERE id = ?",
                (new_ref, decision_id),
            )
            updated += 1
    if updated:
        conn.commit()
    return updated


_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _looks_like_uuid(value: str) -> bool:
    return bool(_UUID_RE.match(value))


def run(
    settings: Settings,
    *,
    apply: bool,
    run_check: bool = True,
    db_conn: sqlite3.Connection | None = None,
) -> TransformResult:
    """Plan + (optionally) apply the normalization across the user's
    ledger.

    ``apply=False`` is the default — produces a plan + diff summary,
    writes nothing. ``apply=True`` writes through with snapshot +
    bean-check guarding.

    ``db_conn`` lets tests inject an in-memory connection. Production
    callers pass their app-state ``conn`` so the AI-decisions backfill
    runs against the live DB. ``None`` skips the backfill entirely
    (useful for ledger-only dry runs).
    """
    ledger_dir = settings.ledger_dir
    files = collect_bean_files(ledger_dir)
    backfill = _BackfillMaps()
    changes: list[_FileChange] = []
    for path in files:
        change = plan_file(path, backfill=backfill)
        if change is not None:
            changes.append(change)

    files_planned = len(files)
    files_changed = len(changes)
    txns_changed = sum(c.n_changed_txns for c in changes)
    lineage_minted = sum(c.n_lineage_minted for c in changes)
    sf_migrated = sum(c.n_simplefin_migrated for c in changes)
    csv_migrated = sum(c.n_csv_migrated for c in changes)

    if not apply:
        return TransformResult(
            files_planned=files_planned,
            files_changed=files_changed,
            txns_changed=txns_changed,
            lineage_minted=lineage_minted,
            simplefin_migrated=sf_migrated,
            csv_migrated=csv_migrated,
            ai_decisions_backfilled=0,
            snapshot_dir=None,
            applied=False,
        )

    if not changes and db_conn is None:
        return TransformResult(
            files_planned=files_planned,
            files_changed=0,
            txns_changed=0,
            lineage_minted=0,
            simplefin_migrated=0,
            csv_migrated=0,
            ai_decisions_backfilled=0,
            snapshot_dir=None,
            applied=True,
        )

    # ------------------------------------------------------------------
    # Apply: snapshot, write, bean-check, rollback on failure.
    # ------------------------------------------------------------------
    snapshot_root: Path | None = None
    if changes:
        snapshot_root = _snapshot_files(changes, ledger_dir)
        baseline = ""
        if run_check:
            _, baseline = capture_bean_check(settings.ledger_main)
        try:
            for c in changes:
                c.path.write_text(c.new_text, encoding="utf-8")
            if run_check:
                try:
                    run_bean_check_vs_baseline(settings.ledger_main, baseline)
                except BeanCheckError as exc:
                    _restore_from_snapshot(changes, snapshot_root, ledger_dir)
                    return TransformResult(
                        files_planned=files_planned,
                        files_changed=files_changed,
                        txns_changed=txns_changed,
                        lineage_minted=lineage_minted,
                        simplefin_migrated=sf_migrated,
                        csv_migrated=csv_migrated,
                        ai_decisions_backfilled=0,
                        snapshot_dir=snapshot_root,
                        applied=False,
                        bean_check_error=str(exc),
                    )
        except Exception:
            _restore_from_snapshot(changes, snapshot_root, ledger_dir)
            raise

    # ------------------------------------------------------------------
    # Backfill ai_decisions only after the ledger writes are durable.
    # If the backfill itself errors we leave the ledger as-is (it's
    # already been bean-check validated) and surface the exception.
    # ------------------------------------------------------------------
    backfilled = 0
    if db_conn is not None:
        backfilled = _backfill_ai_decisions(db_conn, backfill)

    return TransformResult(
        files_planned=files_planned,
        files_changed=files_changed,
        txns_changed=txns_changed,
        lineage_minted=lineage_minted,
        simplefin_migrated=sf_migrated,
        csv_migrated=csv_migrated,
        ai_decisions_backfilled=backfilled,
        snapshot_dir=snapshot_root,
        applied=True,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _format_diff_summary(changes: list[_FileChange]) -> str:
    """Human-readable per-file summary. Mirrors the bcg_to_lamella style:
    one line per file with the kind + count of edits, then a unified
    diff for inspection."""
    import difflib
    parts: list[str] = []
    for c in changes:
        parts.append(
            f"\n  {c.path} — {c.n_changed_txns} txn(s) changed: "
            f"+lineage={c.n_lineage_minted} "
            f"+simplefin={c.n_simplefin_migrated} "
            f"+csv={c.n_csv_migrated}"
        )
        diff = "".join(difflib.unified_diff(
            c.pre_text.splitlines(keepends=True),
            c.new_text.splitlines(keepends=True),
            fromfile=str(c.path),
            tofile=str(c.path),
            n=2,
        ))
        parts.append(diff)
    return "".join(parts)


def _open_db_if_present(settings: Settings) -> sqlite3.Connection | None:
    """Open the runtime SQLite if it exists. Returns None when the DB
    file isn't there (fresh install, or running against a ledger-only
    snapshot)."""
    db_path = settings.connector_data_dir / "lamella.sqlite"
    if not db_path.exists():
        return None
    return sqlite3.connect(str(db_path))


def main(argv: Iterable[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser_ = argparse.ArgumentParser(description=__doc__)
    parser_.add_argument(
        "--apply",
        action="store_true",
        help="Actually write the rewrites (default: dry-run).",
    )
    parser_.add_argument(
        "--no-bean-check",
        action="store_true",
        help="Skip bean-check vs baseline (use for tests / ledgers without main.bean).",
    )
    parser_.add_argument(
        "--no-ai-backfill",
        action="store_true",
        help="Skip the ai_decisions.input_ref backfill (ledger writes only).",
    )
    args = parser_.parse_args(list(argv) if argv is not None else None)

    settings = Settings()
    db_conn: sqlite3.Connection | None = None
    if not args.no_ai_backfill:
        db_conn = _open_db_if_present(settings)

    # Plan first so dry-run can show the diff regardless of --apply.
    files = collect_bean_files(settings.ledger_dir)
    backfill = _BackfillMaps()
    plan: list[_FileChange] = []
    for path in files:
        change = plan_file(path, backfill=backfill)
        if change is not None:
            plan.append(change)

    log.info(
        "scanned %d .bean files; %d need changes",
        len(files), len(plan),
    )
    if plan:
        log.info(
            "totals: txns_changed=%d lineage_minted=%d simplefin_migrated=%d csv_migrated=%d",
            sum(c.n_changed_txns for c in plan),
            sum(c.n_lineage_minted for c in plan),
            sum(c.n_simplefin_migrated for c in plan),
            sum(c.n_csv_migrated for c in plan),
        )
        sys.stdout.write(_format_diff_summary(plan))
        sys.stdout.write("\n")
    if db_conn is not None:
        # Count how many ai_decisions rows the backfill WOULD touch.
        would = _count_ai_backfill_targets(db_conn, backfill)
        log.info("ai_decisions: %d row(s) would be backfilled", would)

    if not args.apply:
        log.info("dry-run; pass --apply to write.")
        if db_conn is not None:
            db_conn.close()
        return 0

    result = run(
        settings,
        apply=True,
        run_check=not args.no_bean_check,
        db_conn=db_conn,
    )
    if db_conn is not None:
        db_conn.close()
    if result.bean_check_error:
        log.error(
            "bean-check rejected the rewrite — every file restored from "
            "snapshot %s. detail: %s",
            result.snapshot_dir, result.bean_check_error,
        )
        return 1
    log.info(
        "applied: files_changed=%d txns_changed=%d lineage_minted=%d "
        "simplefin_migrated=%d csv_migrated=%d ai_decisions_backfilled=%d",
        result.files_changed, result.txns_changed, result.lineage_minted,
        result.simplefin_migrated, result.csv_migrated,
        result.ai_decisions_backfilled,
    )
    if result.snapshot_dir is not None:
        log.info("snapshot of pre-write files: %s", result.snapshot_dir)
    return 0


def _count_ai_backfill_targets(
    conn: sqlite3.Connection, maps: _BackfillMaps,
) -> int:
    """How many ai_decisions rows would the backfill rewrite if applied?
    Mirrors the matching logic in _backfill_ai_decisions but without
    issuing UPDATEs."""
    cur = conn.execute("SELECT input_ref FROM ai_decisions")
    n = 0
    for row in cur.fetchall():
        ref = row[0]
        if not ref or _looks_like_uuid(ref):
            continue
        if ref in maps.simplefin_to_lineage:
            n += 1
            continue
        if ref in maps.csv_to_lineage:
            n += 1
            continue
        if ref in maps.txn_hash_to_lineage:
            n += 1
            continue
        m = re.match(r"^import:(\S+):row:(\S+)$", ref)
        if m and f"{m.group(1)}:{m.group(2)}" in maps.import_composite_to_lineage:
            n += 1
    return n


if __name__ == "__main__":
    raise SystemExit(main())
