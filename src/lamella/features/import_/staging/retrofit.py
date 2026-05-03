# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Metadata retrofit — NEXTGEN.md Phase E2 (retrofit half).

When the reboot scan flags a duplicate group — two or more ledger
transactions that share a ``content_fingerprint`` — the retrofit
writer stamps ``lamella-source-ref: "<fingerprint>"`` onto each member's
ledger line. That's the "exit condition that makes the problem go
away forever": every future import from any source (SimpleFIN,
CSV, paste, reboot) can compute the same fingerprint and check
the ledger for ``lamella-source-ref`` matches to dedup on an exact
key rather than falling through to the fuzzy fingerprint path.

The fuzzy system stays in place for newly-arriving data that
hasn't been retrofitted yet, but after retrofit it becomes a
safety net instead of the primary mechanism.

Discipline:
* Every file touched gets a byte-level snapshot before writing.
* ``bean-check`` runs (baseline-subtracted) after the batch.
* Any failure restores every file from its snapshot.
* Idempotent: re-running on a line that already carries
  ``lamella-source-ref`` is a no-op.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from json import loads as _json_loads
from pathlib import Path
from typing import Iterable

from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    run_bean_check_vs_baseline,
)

log = logging.getLogger(__name__)

__all__ = [
    "RetrofitError",
    "RetrofitResult",
    "retrofit_fingerprint",
]


class RetrofitError(Exception):
    """Retrofit refused or bean-check failed with rollback."""


@dataclass
class RetrofitResult:
    fingerprint: str
    lines_targeted: int = 0
    lines_stamped: int = 0
    lines_already_tagged: int = 0
    files_touched: tuple[Path, ...] = ()
    errors: list[str] = field(default_factory=list)


# --- ledger-line surgery ------------------------------------------------


# A txn header line starts with a date + a flag + optional payee/narration.
# The parser accepts the narration pattern loosely; we only need to know
# "does this line start a transaction at column zero?" for insertion.
_TXN_HEADER = re.compile(r"^\d{4}-\d{2}-\d{2}\s+[*!txn]")
# A posting line begins with an indented account path.
_POSTING_LINE = re.compile(r"^\s+[A-Z][A-Za-z0-9_\-]*:")
# A metadata line is indented, starts with an identifier, then a colon.
_META_LINE = re.compile(r"^\s+[A-Za-z][A-Za-z0-9\-_]*:")


def _is_blank(line: str) -> bool:
    return line.strip() == ""


def _is_comment(line: str) -> bool:
    s = line.lstrip()
    return s.startswith(";")


def _scan_txn_metadata_and_postings(
    lines: list[str], header_idx: int,
) -> tuple[int, set[str]]:
    """Given the 0-indexed line number of a txn header, scan forward
    and return:
      * the index of the first posting line (where txn-level metadata
        stops and postings begin), and
      * the set of metadata keys already present at the txn level
        (so retrofit can skip when ``lamella-source-ref`` is already there).

    Handles comment lines and blanks by skipping them. Stops scanning
    at the first posting line or the next txn header or end of file.
    """
    keys: set[str] = set()
    i = header_idx + 1
    while i < len(lines):
        line = lines[i]
        if _is_blank(line) or _is_comment(line):
            i += 1
            continue
        if _TXN_HEADER.match(line):
            return i, keys
        if _POSTING_LINE.match(line):
            return i, keys
        m = _META_LINE.match(line)
        if m:
            # Pull the key out (everything up to the colon, stripped).
            stripped = line.lstrip()
            key = stripped.split(":", 1)[0].strip()
            keys.add(key)
            i += 1
            continue
        # Something we don't recognize (stray text?). Stop to be safe.
        break
    return i, keys


def _insert_metadata_line(
    lines: list[str], *, after_header_idx: int, key: str, value: str,
) -> None:
    """Insert ``  key: "value"`` immediately after the txn header.

    Indent matches Beancount's preferred format (two spaces). We
    always insert at index ``after_header_idx + 1`` so the metadata
    appears before any existing metadata / postings — the order
    within a txn's metadata block is not semantically significant
    to Beancount, so this keeps the insertion simple and
    deterministic.
    """
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    new_line = f'  {key}: "{escaped}"\n'
    header = lines[after_header_idx]
    if not header.endswith("\n"):
        lines[after_header_idx] = header + "\n"
    lines.insert(after_header_idx + 1, new_line)


# --- per-file retrofit --------------------------------------------------


@dataclass
class _TargetLine:
    """One ledger line to stamp. Grouped by file so we can read-write
    each file exactly once per retrofit call."""
    file: Path
    lineno: int   # 1-based, as Beancount emits
    staged_id: int


def _collect_targets(
    conn: sqlite3.Connection, fingerprint: str,
) -> list[_TargetLine]:
    """Find every reboot-sourced staged row whose fingerprint matches.

    We retrofit reboot-source rows because those carry (file, lineno)
    in source_ref. Rows from other sources (simplefin, csv, paste)
    have their own source-specific dedup metadata (lamella-simplefin-id,
    lamella-import-raw-row) and don't need an additional stamp.
    """
    from lamella.features.import_.staging.intake import content_fingerprint

    rows = conn.execute(
        "SELECT id, source, source_ref, posting_date, amount, description "
        "FROM staged_transactions WHERE source = 'reboot'"
    ).fetchall()
    targets: list[_TargetLine] = []
    for r in rows:
        try:
            amt = Decimal(r["amount"])
        except (InvalidOperation, ValueError):
            continue
        if content_fingerprint(
            posting_date=r["posting_date"],
            amount=amt,
            description=r["description"],
        ) != fingerprint:
            continue
        try:
            ref = _json_loads(r["source_ref"])
        except Exception:  # noqa: BLE001
            continue
        file = ref.get("file") if isinstance(ref, dict) else None
        lineno = ref.get("lineno") if isinstance(ref, dict) else None
        if not file or not isinstance(lineno, int) or lineno <= 0:
            continue
        targets.append(
            _TargetLine(
                file=Path(file),
                lineno=int(lineno),
                staged_id=int(r["id"]),
            )
        )
    return targets


def _retrofit_one_file(
    file: Path,
    *,
    line_fingerprints: dict[int, str],
    result: RetrofitResult,
) -> None:
    """Apply retrofit to every target line in a single file.

    ``line_fingerprints`` maps (1-based) lineno → fingerprint to stamp.
    Insertions shift subsequent linenos; we process target lines in
    descending order so earlier insertions don't perturb the index of
    later ones.
    """
    if not file.is_file():
        result.errors.append(f"{file}: not a file")
        return
    text = file.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    # Sort descending so our insertion doesn't shift the remaining
    # target indices.
    for lineno in sorted(line_fingerprints.keys(), reverse=True):
        fingerprint = line_fingerprints[lineno]
        # Beancount meta carries 1-based linenos; our list is 0-based.
        idx = lineno - 1
        if idx < 0 or idx >= len(lines):
            result.errors.append(f"{file}:{lineno}: line out of range")
            continue
        header = lines[idx]
        if not _TXN_HEADER.match(header):
            result.errors.append(
                f"{file}:{lineno}: line is not a transaction header — "
                "skipping retrofit"
            )
            continue
        _first_posting_idx, keys_present = _scan_txn_metadata_and_postings(
            lines, header_idx=idx,
        )
        if "lamella-source-ref" in keys_present:
            result.lines_already_tagged += 1
            continue
        _insert_metadata_line(
            lines,
            after_header_idx=idx,
            key="lamella-source-ref",
            value=fingerprint,
        )
        result.lines_stamped += 1

    file.write_text("".join(lines), encoding="utf-8")


# --- public API ---------------------------------------------------------


def retrofit_fingerprint(
    conn: sqlite3.Connection,
    *,
    fingerprint: str,
    main_bean: Path,
) -> RetrofitResult:
    """Stamp ``lamella-source-ref: "<fingerprint>"`` onto every ledger line
    whose content matches ``fingerprint``.

    Runs bean-check against a baseline after the batch. If new errors
    appear, every file touched is restored from its byte-level
    snapshot and the function raises ``RetrofitError``. Idempotent:
    lines that already carry ``lamella-source-ref`` are skipped without
    any file modification.
    """
    targets = _collect_targets(conn, fingerprint)
    result = RetrofitResult(fingerprint=fingerprint, lines_targeted=len(targets))
    if not targets:
        return result

    # Group targets by file.
    by_file: dict[Path, dict[int, str]] = {}
    for t in targets:
        by_file.setdefault(t.file, {})[t.lineno] = fingerprint

    # Snapshot every file before we touch it.
    snapshots: dict[Path, bytes] = {}
    for file in by_file:
        try:
            snapshots[file] = file.read_bytes()
        except OSError as exc:
            result.errors.append(f"{file}: snapshot failed ({exc})")
            continue

    _rc, baseline = capture_bean_check(main_bean)

    try:
        for file, line_fingerprints in by_file.items():
            if file not in snapshots:
                continue
            _retrofit_one_file(
                file, line_fingerprints=line_fingerprints, result=result,
            )
        # One bean-check for the whole batch.
        run_bean_check_vs_baseline(main_bean, baseline)
    except BeanCheckError as exc:
        log.warning(
            "retrofit bean-check failed (%s) — reverting all %d file(s)",
            exc, len(snapshots),
        )
        for file, blob in snapshots.items():
            try:
                file.write_bytes(blob)
            except OSError:
                log.exception("retrofit rollback: failed to restore %s", file)
        raise RetrofitError(f"bean-check failed after retrofit: {exc}") from exc
    except Exception:
        # Any other write error: also rollback to be safe.
        for file, blob in snapshots.items():
            try:
                file.write_bytes(blob)
            except OSError:
                log.exception("retrofit rollback: failed to restore %s", file)
        raise

    result.files_touched = tuple(sorted(by_file.keys()))

    # Dismiss the staged rows we just stamped so they stop showing up
    # as "unresolved duplicate" in future reboot scans. The ledger line
    # carries the authoritative ``lamella-source-ref`` now; the staged row
    # has done its job.
    from lamella.features.import_.staging.service import StagingService
    svc = StagingService(conn)
    for t in targets:
        try:
            current = svc.get(t.staged_id)
        except Exception:  # noqa: BLE001
            continue
        if current.status == "dismissed":
            continue
        try:
            svc.dismiss(
                t.staged_id,
                reason=f"retrofitted: lamella-source-ref={fingerprint}",
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "retrofit: dismiss(%d) failed: %s", t.staged_id, exc,
            )

    log.info(
        "retrofit fingerprint=%s: stamped=%d already_tagged=%d files=%d",
        fingerprint, result.lines_stamped, result.lines_already_tagged,
        len(result.files_touched),
    )
    return result
