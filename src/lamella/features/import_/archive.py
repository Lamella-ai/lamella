# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Imported-file archive — ADR-0060.

Every tabular file (CSV, OFX, QIF, IIF, XLSX, ODS) and every paste
is copied verbatim under ``<ledger_dir>/imports/`` with a stable
monotonic id, then referenced by every staged row that came from
it. The directory is the source of truth (ADR-0001); the
``imported_files`` SQLite table is a reconstructible index over it.

Filename pattern: ``<5-digit-id>-<sanitized-original>.<ext>``. The
numeric prefix is the file's permanent identity — `file_id` — used
by every staged row's source_ref so "which file did this row come
from?" has a one-step answer that survives a DB wipe.

This module is intentionally thin: archive a file, return its id;
look up an archived file's path; list the archive. Wiring it into
each importer is per-importer work tracked under ADR-0060
follow-ups.
"""
from __future__ import annotations

import hashlib
import logging
import re
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

__all__ = [
    "ALLOWED_FORMATS",
    "ArchivedFile",
    "ArchiveError",
    "archive_file",
    "compute_sha256",
    "find_by_sha256",
    "get_archived_path",
    "list_archived",
    "sanitize_filename",
]


ALLOWED_FORMATS: frozenset[str] = frozenset({
    "csv",
    "ofx",
    "qif",
    "iif",
    "xlsx",
    "ods",
    "paste",
})

# Inside-the-archive-dir layout. Always relative to ledger_dir so
# the manifest's archived_path is portable across machines and
# survives ledger-dir relocation.
ARCHIVE_SUBDIR = "imports"

_FILENAME_RE = re.compile(r"^(\d{5})-(.+)$")
_SANITIZE_RE = re.compile(r"[^A-Za-z0-9._-]")


class ArchiveError(Exception):
    """Raised when an archive operation refuses input — bad format,
    unreadable bytes, mid-write collision, etc."""


@dataclass(frozen=True)
class ArchivedFile:
    """One row from ``imported_files`` joined with its on-disk
    facts. The dataclass is the natural read shape for callers."""
    file_id: int
    original_filename: str
    archived_path: str  # relative to ledger_dir
    content_sha256: str
    source_format: str
    byte_size: int
    imported_at: str
    imported_by: str | None
    notes: str | None


def sanitize_filename(name: str) -> str:
    """Reduce the user-supplied filename to a portable token.

    Allowed: ASCII letters, digits, dot, underscore, dash. Anything
    else collapses to a single dash. Empty / dot-only inputs map
    to ``"file"`` so the archive filename never starts or ends with
    a dash. Length capped to keep the archive directory tidy on
    filesystems with strict path limits."""
    if not name:
        return "file"
    # Strip any leading directory component the browser may have
    # included on Windows ("C:\\Users\\...\\statement.csv").
    name = name.replace("\\", "/").rsplit("/", 1)[-1]
    cleaned = _SANITIZE_RE.sub("-", name).strip("-.")
    if not cleaned:
        return "file"
    # Beancount + most editors handle 200-char filenames fine; a
    # cap stops a pathological 4kB filename from crashing the FS.
    return cleaned[:200]


def compute_sha256(path_or_bytes) -> str:
    """SHA-256 hex digest of a file's bytes. Accepts a path or a
    bytes object so paste content can be hashed without a temp
    write."""
    h = hashlib.sha256()
    if isinstance(path_or_bytes, (bytes, bytearray, memoryview)):
        h.update(bytes(path_or_bytes))
        return h.hexdigest()
    p = Path(path_or_bytes)
    with p.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def find_by_sha256(
    conn: sqlite3.Connection,
    *,
    content_sha256: str,
) -> ArchivedFile | None:
    """Return the existing archive row for these exact bytes if one
    exists. Used by ``archive_file`` to skip the copy on full-file
    dedup; also useful to callers that want to preflight a re-import
    decision."""
    row = conn.execute(
        "SELECT id, original_filename, archived_path, content_sha256, "
        "source_format, byte_size, imported_at, imported_by, notes "
        "FROM imported_files WHERE content_sha256 = ?",
        (content_sha256,),
    ).fetchone()
    if row is None:
        return None
    return _row_to_archived(row)


def get_archived_path(
    conn: sqlite3.Connection,
    *,
    ledger_dir: Path,
    file_id: int,
) -> Path | None:
    """Resolve an archive id to its absolute on-disk path. Returns
    ``None`` if the id isn't in the manifest. Doesn't check
    existence — callers that need that should ``.exists()`` the
    return value."""
    row = conn.execute(
        "SELECT archived_path FROM imported_files WHERE id = ?",
        (file_id,),
    ).fetchone()
    if row is None:
        return None
    return ledger_dir / row["archived_path"]


def list_archived(
    conn: sqlite3.Connection,
    *,
    source_format: str | None = None,
    limit: int = 500,
) -> list[ArchivedFile]:
    """List archive rows newest-first, optionally filtered by
    format. The default page is large enough that pagination is
    rarely needed in practice; the UI can switch to keyset paging
    if a deployment exceeds it."""
    sql = (
        "SELECT id, original_filename, archived_path, content_sha256, "
        "source_format, byte_size, imported_at, imported_by, notes "
        "FROM imported_files"
    )
    params: list = []
    if source_format is not None:
        sql += " WHERE source_format = ?"
        params.append(source_format)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [_row_to_archived(r) for r in rows]


def archive_file(
    conn: sqlite3.Connection,
    *,
    ledger_dir: Path,
    source_path: Path | None = None,
    content: bytes | None = None,
    original_filename: str,
    source_format: str,
    imported_by: str | None = None,
    notes: str | None = None,
) -> ArchivedFile:
    """Copy an imported file into the archive and return its
    manifest row. Provide EITHER ``source_path`` (a file already on
    disk; gets copied) OR ``content`` (raw bytes; written
    directly — used by paste intake).

    Full-file dedup: if the exact content already exists in the
    archive, no copy happens. The existing archive row is returned
    and the caller proceeds as if it had just landed. Re-uploading
    the same statement is therefore idempotent — the same file_id
    flows into staging, the staging upsert keys off
    ``(source, source_ref_hash)`` and updates in place.

    Raises ``ArchiveError`` if neither ``source_path`` nor
    ``content`` is supplied, the file is unreadable, or the format
    isn't on the allowlist.
    """
    if source_format not in ALLOWED_FORMATS:
        raise ArchiveError(
            f"unknown source_format {source_format!r}; allowed: "
            f"{sorted(ALLOWED_FORMATS)}"
        )
    if (source_path is None) == (content is None):
        raise ArchiveError(
            "exactly one of source_path / content must be supplied"
        )

    # Hash before allocating an id. Lets us short-circuit on
    # full-file dedup without minting a wasted id and without
    # writing a file we'd then have to delete.
    if content is not None:
        sha = compute_sha256(content)
        size = len(content)
    else:
        if not source_path.exists():
            raise ArchiveError(f"source file does not exist: {source_path}")
        sha = compute_sha256(source_path)
        size = source_path.stat().st_size

    existing = find_by_sha256(conn, content_sha256=sha)
    if existing is not None:
        log.info(
            "archive: full-file dedup hit (sha256=%s) — reusing "
            "file_id=%s (%s)",
            sha[:12], existing.file_id, existing.original_filename,
        )
        return existing

    archive_dir = ledger_dir / ARCHIVE_SUBDIR
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Mint a new file_id. The PK is monotonic; SQLite's autoincrement
    # behavior on INSERT-without-id assigns max(id)+1. Use that
    # rather than a separate sequence so reconstruct from the
    # filename prefix is bidirectional with the live mint.
    cursor = conn.execute(
        "INSERT INTO imported_files "
        "(original_filename, archived_path, content_sha256, "
        " source_format, byte_size, imported_at, imported_by, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            original_filename,
            "",  # filled in after we know the id
            sha,
            source_format,
            size,
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            imported_by,
            notes,
        ),
    )
    file_id = int(cursor.lastrowid)
    safe_name = sanitize_filename(original_filename)
    archive_filename = f"{file_id:05d}-{safe_name}"
    archived_rel = f"{ARCHIVE_SUBDIR}/{archive_filename}"
    archived_abs = ledger_dir / archived_rel

    # Write content / copy file. Done after the row insert so a
    # failed commit (DB locked, etc.) doesn't leak orphan archive
    # files. If the copy fails, we roll back the row.
    try:
        if content is not None:
            archived_abs.write_bytes(content)
        else:
            shutil.copy2(source_path, archived_abs)
    except OSError as exc:
        conn.execute(
            "DELETE FROM imported_files WHERE id = ?", (file_id,),
        )
        raise ArchiveError(
            f"failed to copy {original_filename!r} into archive: {exc}"
        ) from exc

    conn.execute(
        "UPDATE imported_files SET archived_path = ? WHERE id = ?",
        (archived_rel, file_id),
    )
    conn.commit()

    log.info(
        "archive: stored file_id=%s (%s, %d bytes, sha256=%s)",
        file_id, archive_filename, size, sha[:12],
    )

    return ArchivedFile(
        file_id=file_id,
        original_filename=original_filename,
        archived_path=archived_rel,
        content_sha256=sha,
        source_format=source_format,
        byte_size=size,
        imported_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        imported_by=imported_by,
        notes=notes,
    )


def _row_to_archived(row) -> ArchivedFile:
    return ArchivedFile(
        file_id=int(row["id"]),
        original_filename=row["original_filename"],
        archived_path=row["archived_path"],
        content_sha256=row["content_sha256"],
        source_format=row["source_format"],
        byte_size=int(row["byte_size"]),
        imported_at=row["imported_at"],
        imported_by=row["imported_by"],
        notes=row["notes"],
    )


def parse_archive_filename(name: str) -> tuple[int, str] | None:
    """Pull the ``(file_id, original_filename)`` pair out of an
    archive filename. Returns None if the name doesn't match the
    canonical ``NNNNN-rest`` shape — used by reconstruct to skip
    files the user manually dropped into the archive directory
    without renaming."""
    m = _FILENAME_RE.match(name)
    if not m:
        return None
    try:
        return int(m.group(1)), m.group(2)
    except ValueError:
        return None
