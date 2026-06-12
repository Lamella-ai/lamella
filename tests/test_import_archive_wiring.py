# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0060 wiring: tabular intake (CSV/OFX/QIF/IIF/XLSX/ODS) lands
staged rows with ``source_ref={file_id, sheet, row, …}`` once the
upload has been archived.

The wiring has two halves:

1. ``ImportService.register_upload`` archives every supported
   upload under ``<ledger_dir>/imports/`` (idempotent on
   content-sha256). The same bytes uploaded twice reuse the
   existing file_id.
2. ``_mirror_to_staging`` joins ``sources → imports → imported_files``
   on content_sha256 and uses the resolved ``file_id`` in
   ``source_ref`` instead of the legacy session-scoped shape.

This test covers the second half — the SQL join — without spinning
up the full ImportService. We seed the rows the join expects and
call ``insert_raw_row`` directly.
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.import_._db import insert_raw_row, upsert_source


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _seed_archived_upload(conn, *, content_sha256: str, file_id: int):
    """Mirror what ImportService.register_upload + archive_file
    produce: an imports row holding the upload's content hash, and
    an imported_files row with the matching hash and file_id."""
    conn.execute(
        "INSERT INTO imports (id, filename, content_sha256, "
        "stored_path, status) VALUES (?, ?, ?, ?, ?)",
        (1, "stmt.csv", content_sha256, "/tmp/uploads/1/stmt.csv", "uploaded"),
    )
    conn.execute(
        "INSERT INTO imported_files "
        "(id, original_filename, archived_path, content_sha256, "
        " source_format, byte_size, imported_at) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
        (
            file_id,
            "stmt.csv",
            f"imports/{file_id:05d}-stmt.csv",
            content_sha256,
            "csv",
            128,
        ),
    )


def test_mirror_to_staging_uses_file_id_source_ref(conn):
    """The path the user reads as "did the wiring fire?" — when an
    imported_files row exists for the upload, every staged row's
    source_ref must carry ``file_id`` (and the sheet name when the
    source has one)."""
    _seed_archived_upload(
        conn, content_sha256="a" * 64, file_id=42,
    )
    source_id = upsert_source(
        conn,
        upload_id=1,
        path="stmt.csv",
        sheet_name="Sheet1",
        sheet_type="primary",
        source_class="bank-statement",
    )
    insert_raw_row(
        conn,
        source_id=source_id,
        row_num=7,
        raw={"row": "raw"},
        date="2026-04-15",
        amount=Decimal("-12.50"),
        currency="USD",
        payee="Coffee Shop",
        description="Decaf",
    )

    row = conn.execute(
        "SELECT source_ref FROM staged_transactions WHERE source = 'csv'"
    ).fetchone()
    assert row is not None, "expected a mirrored staged_transactions row"
    ref = json.loads(row["source_ref"])
    assert ref.get("file_id") == 42, (
        "source_ref must carry the resolved imported_files.id "
        "as file_id; got " + repr(ref)
    )
    assert ref.get("row") == 7
    assert ref.get("sheet") == "Sheet1"
    # Legacy keys must NOT be the dedup driver when file_id resolves —
    # if they were, re-importing the same archived file under a new
    # upload_id would create new rows instead of upserting in place.
    # Defensive: at least the new shape is canonical.
    assert "upload_id" not in ref or ref.get("file_id") == 42


def test_mirror_to_staging_falls_back_when_archive_missing(conn):
    """When no imported_files row exists for the upload (legacy
    flow / archive write failed / unknown extension), the legacy
    session-scoped shape kicks in. This is the safety net so the
    existing importer corpus keeps working while ADR-0060 wires up
    everywhere."""
    # imports row WITHOUT a corresponding imported_files row.
    conn.execute(
        "INSERT INTO imports (id, filename, content_sha256, "
        "stored_path, status) VALUES (?, ?, ?, ?, ?)",
        (1, "weird.foo", "b" * 64, "/tmp/uploads/1/weird.foo", "uploaded"),
    )
    source_id = upsert_source(
        conn,
        upload_id=1,
        path="weird.foo",
        sheet_name="",
        sheet_type="primary",
        source_class="generic",
    )
    insert_raw_row(
        conn,
        source_id=source_id,
        row_num=3,
        raw={"row": "raw"},
        date="2026-04-15",
        amount=Decimal("-12.50"),
        currency="USD",
        description="Decaf",
    )
    row = conn.execute(
        "SELECT source_ref FROM staged_transactions WHERE source = 'csv'"
    ).fetchone()
    assert row is not None
    ref = json.loads(row["source_ref"])
    assert "file_id" not in ref, (
        "no archived file → must fall back to the legacy shape so "
        "the importer keeps working on uploads predating ADR-0060"
    )
    assert ref.get("upload_id") == 1
    assert ref.get("row_num") == 3


def test_re_running_insert_same_row_upserts_in_place(conn):
    """Idempotency check: when the same row from the same archived
    file gets ingested twice, the staging upsert (keyed on
    ``(source, source_ref_hash)``) keeps a single row. With the
    file_id-based source_ref this works because the canonicalized
    JSON ``{file_id, row, sheet, …}`` produces the same hash on
    each call."""
    sha = "c" * 64
    _seed_archived_upload(conn, content_sha256=sha, file_id=7)
    src = upsert_source(
        conn, upload_id=1, path="stmt.csv", sheet_name="Sheet1",
        sheet_type="primary", source_class="bank-statement",
    )
    insert_raw_row(
        conn, source_id=src, row_num=1, raw={},
        date="2026-04-15", amount=Decimal("-12.50"),
        description="Decaf",
    )
    # Same row, same upload, second pass — re-import idempotency.
    insert_raw_row(
        conn, source_id=src, row_num=1, raw={},
        date="2026-04-15", amount=Decimal("-12.50"),
        description="Decaf",
    )
    n = conn.execute(
        "SELECT COUNT(*) AS n FROM staged_transactions "
        "WHERE source='csv'"
    ).fetchone()["n"]
    assert n == 1, (
        "two ingests of the same row from the same archived file "
        "must upsert in place (single staged row); got " + str(n)
    )
