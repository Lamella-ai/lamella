# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 25: imported_files reconstruct from ``<ledger_dir>/imports/``.

Per ADR-0060, every imported tabular file is archived under
``<ledger_dir>/imports/`` with a ``NNNNN-original-name.ext`` filename
pattern. The directory is the source of truth (ADR-0001); the
``imported_files`` SQLite table is a reconstructible index over it.

This step walks the archive directory and rebuilds the manifest
when the DB is wiped. The reconstruct is idempotent — re-running
with the table fully populated is a no-op (UPSERT on the unique
``content_sha256`` index).

ADR-0015 invariant: every archive file matching the canonical
filename pattern produces exactly one ``imported_files`` row, and
no extra rows. ``imported_at`` and ``imported_by`` legitimately
drift between live and rebuilt because they aren't recoverable from
filename alone — those columns are tolerated as cache drift.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import (
    TablePolicy,
    register as register_policy,
)
from lamella.features.import_.archive import (
    ALLOWED_FORMATS,
    ARCHIVE_SUBDIR,
    compute_sha256,
    parse_archive_filename,
)

log = logging.getLogger(__name__)


@register(
    "step25:imported_files",
    state_tables=["imported_files"],
)
def reconstruct_imported_files(
    conn: sqlite3.Connection, entries: list,
) -> ReconstructReport:
    """Walk ``<ledger_dir>/imports/`` and upsert one row per archive
    file. The Beancount entries argument is unused — this is a
    purely filesystem-driven reconstruct.

    The ledger_dir is read out of ``app.state.settings.ledger_dir``
    when the function is invoked through the live reconstruct
    runner; tests pass it explicitly via the ``LAMELLA_LEDGER_DIR``
    env var or by writing settings into a fixture conn beforehand.
    Direct callers can monkey-patch ``_resolve_ledger_dir`` if
    needed.
    """
    cols = [
        r[1] for r in conn.execute(
            "PRAGMA table_info(imported_files)"
        )
    ]
    if not cols:
        return ReconstructReport(
            pass_name="step25:imported_files", rows_written=0,
            notes=["imported_files table not present — skip"],
        )

    ledger_dir = _resolve_ledger_dir()
    if ledger_dir is None:
        return ReconstructReport(
            pass_name="step25:imported_files", rows_written=0,
            notes=[
                "ledger_dir not resolvable — set LAMELLA_LEDGER_DIR "
                "or run inside the app context"
            ],
        )

    archive_dir = ledger_dir / ARCHIVE_SUBDIR
    if not archive_dir.exists():
        return ReconstructReport(
            pass_name="step25:imported_files", rows_written=0,
            notes=[f"archive dir {archive_dir} does not exist — nothing to do"],
        )

    written = 0
    skipped_unparseable = 0
    skipped_unknown_format = 0
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for path in sorted(archive_dir.iterdir()):
        if not path.is_file():
            continue
        parsed = parse_archive_filename(path.name)
        if parsed is None:
            # Filename doesn't match the canonical NNNNN-rest shape.
            # Files dropped manually into the archive without renaming
            # don't get manifest rows; the user can rename them and
            # rerun reconstruct to pick them up.
            skipped_unparseable += 1
            continue
        file_id, rest = parsed
        # Format inferred from the extension. Anything not on the
        # allowlist gets skipped — the live archive_file() helper
        # validates at insert time, so an unknown-extension file in
        # the archive directory is a manual drop we don't want to
        # silently legitimize.
        ext = path.suffix.lower().lstrip(".")
        source_format = ext if ext in ALLOWED_FORMATS else None
        if source_format is None:
            # 'paste' archives use the .csv extension by convention,
            # so a bare .csv file's format is ambiguous from the
            # extension alone — but the live ingest would have stamped
            # the format at archive time, and reconstruct only fires
            # when the live row is missing. Default to 'csv' for the
            # fallback to keep round-trip from filename possible; the
            # live row's stored format wins when both exist.
            source_format = "csv" if ext == "csv" else None
        if source_format is None:
            skipped_unknown_format += 1
            continue

        sha = compute_sha256(path)
        size = path.stat().st_size
        archived_rel = f"{ARCHIVE_SUBDIR}/{path.name}"

        # Original filename: strip the NNNNN- prefix from the on-disk
        # name. Doesn't recover the user's exact pre-sanitization
        # original (sanitization is lossy), but it's the best we have
        # without persistent metadata. Live row's original_filename
        # wins when both exist.
        original_filename = rest

        conn.execute(
            """
            INSERT INTO imported_files
                (id, original_filename, archived_path, content_sha256,
                 source_format, byte_size, imported_at, imported_by, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            ON CONFLICT(content_sha256) DO UPDATE SET
                archived_path = excluded.archived_path,
                source_format = excluded.source_format,
                byte_size     = excluded.byte_size
            """,
            (
                file_id,
                original_filename,
                archived_rel,
                sha,
                source_format,
                size,
                now_iso,
            ),
        )
        written += 1

    notes = [
        f"rebuilt {written} imported_files row(s) from {archive_dir}",
    ]
    if skipped_unparseable:
        notes.append(
            f"skipped {skipped_unparseable} file(s) not matching "
            "canonical NNNNN-rest shape"
        )
    if skipped_unknown_format:
        notes.append(
            f"skipped {skipped_unknown_format} file(s) with extensions "
            f"not on the allowlist {sorted(ALLOWED_FORMATS)}"
        )
    return ReconstructReport(
        pass_name="step25:imported_files", rows_written=written,
        notes=notes,
    )


def _resolve_ledger_dir() -> Path | None:
    """Find ledger_dir for the reconstruct walk.

    Resolution order:
      1. ``LAMELLA_LEDGER_DIR`` env var (legacy ``CONNECTOR_LEDGER_DIR``
         also accepted for back-compat).
      2. The Settings instance the app already loaded (read via
         lazy import to avoid a hard dependency on the web stack
         when reconstruct is run from a CLI tool).

    Returns None if neither is available; the reconstruct step
    treats that as a no-op rather than crashing.
    """
    import os
    from pathlib import Path as _P
    val = os.environ.get("LAMELLA_LEDGER_DIR") or os.environ.get(
        "CONNECTOR_LEDGER_DIR"
    )
    if val:
        return _P(val)
    try:
        from lamella.core.config import Settings
        return Settings().ledger_dir
    except Exception:  # noqa: BLE001
        return None


def _allow_imported_files_drift(live_rows, rebuilt_rows):
    """Drift policy: imported_at, imported_by, notes, and the exact
    pre-sanitization original_filename aren't recoverable from the
    archive directory alone. Live values for these survive on disk
    only in the SQLite manifest; reconstruct produces NULL or a
    sanitized echo. Tolerate that drift on the cache columns;
    everything else (id, archived_path, content_sha256,
    source_format, byte_size) is state and must match exactly.
    """
    tolerated = []
    by_id_live = {r["id"]: r for r in live_rows}
    by_id_rebuilt = {r["id"]: r for r in rebuilt_rows}
    for file_id, live in by_id_live.items():
        rebuilt = by_id_rebuilt.get(file_id)
        if rebuilt is None:
            continue
        cache_diff_fields = (
            "imported_at",
            "imported_by",
            "notes",
            "original_filename",
        )
        for col in cache_diff_fields:
            if live.get(col) != rebuilt.get(col):
                tolerated.append(
                    f"(file_id={file_id}) {col} differs — not "
                    "recoverable from filename + content"
                )
    return tolerated


register_policy(TablePolicy(
    table="imported_files",
    kind="cache",
    primary_key=("id",),
    allow_drift=_allow_imported_files_drift,
))
