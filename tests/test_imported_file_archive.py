# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the imported-file archive — ADR-0060.

The archive helper is the foundation of the multi-format importer
provenance story. Per ADR-0060 it must:

* Copy / write the file into ``<ledger_dir>/imports/`` with a
  monotonic ``NNNNN-original-name.ext`` filename.
* Mint a stable ``file_id`` that lives in ``imported_files`` and
  shows up in every staged row's ``source_ref``.
* Dedup on full-file content hash so re-importing the same bytes
  reuses the existing file_id without copying twice.
* Sanitize the original filename (Windows path components, awkward
  characters) before constructing the archive name.
* Be reconstructible — a DB wipe + reconstruct walk over the
  archive directory rebuilds the manifest.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.import_.archive import (
    ALLOWED_FORMATS,
    ARCHIVE_SUBDIR,
    ArchiveError,
    archive_file,
    compute_sha256,
    find_by_sha256,
    get_archived_path,
    list_archived,
    parse_archive_filename,
    sanitize_filename,
)


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


@pytest.fixture()
def ledger_dir(tmp_path: Path) -> Path:
    """Fresh ledger root per test. The archive subdir is created
    lazily by ``archive_file`` so we don't pre-create it here —
    asserting that creation works is part of the contract."""
    return tmp_path


# --- pure helpers -------------------------------------------------------


class TestSanitizeFilename:

    def test_passthrough_for_safe_input(self):
        assert (
            sanitize_filename("bankone-statement-2026-04.csv")
            == "bankone-statement-2026-04.csv"
        )

    def test_collapses_whitespace_to_dash(self):
        assert (
            sanitize_filename("My Statement Apr 2026.csv")
            == "My-Statement-Apr-2026.csv"
        )

    def test_strips_windows_path_prefix(self):
        # Browser file inputs on Windows can include the full path.
        assert (
            sanitize_filename("C:\\Users\\Aj\\Downloads\\stmt.csv")
            == "stmt.csv"
        )

    def test_strips_unix_path_prefix(self):
        assert (
            sanitize_filename("/tmp/uploads/stmt.csv")
            == "stmt.csv"
        )

    def test_collapses_special_chars(self):
        assert (
            sanitize_filename("weird*file?name<>!.csv")
            == "weird-file-name---.csv"
        )

    def test_empty_input_falls_back_to_file(self):
        assert sanitize_filename("") == "file"

    def test_dots_only_falls_back_to_file(self):
        assert sanitize_filename("...") == "file"

    def test_caps_long_name(self):
        long = "a" * 500
        assert len(sanitize_filename(long + ".csv")) <= 200


class TestParseArchiveFilename:

    def test_canonical_pattern(self):
        result = parse_archive_filename(
            "00042-bankone-statement-2026-04.csv"
        )
        assert result == (42, "bankone-statement-2026-04.csv")

    def test_non_canonical_returns_none(self):
        # User dropped a file directly without renaming.
        assert parse_archive_filename("some-random-file.csv") is None
        assert parse_archive_filename(".hidden") is None
        assert parse_archive_filename("4-too-short-prefix.csv") is None


class TestComputeSha256:

    def test_bytes_and_path_agree(self, tmp_path):
        content = b"date,amount,description\n2026-04-15,-12.50,Coffee\n"
        path = tmp_path / "stmt.csv"
        path.write_bytes(content)
        assert compute_sha256(content) == compute_sha256(path)

    def test_different_content_different_hash(self):
        a = compute_sha256(b"row 1")
        b = compute_sha256(b"row 2")
        assert a != b


# --- archive_file end-to-end -------------------------------------------


class TestArchiveFile:

    def test_archive_from_path_creates_directory_and_row(
        self, conn, ledger_dir,
    ):
        # Write a fixture CSV outside the archive.
        src = ledger_dir / "incoming" / "stmt.csv"
        src.parent.mkdir()
        src.write_text("date,amount\n2026-04-15,-12.50\n", encoding="utf-8")

        result = archive_file(
            conn,
            ledger_dir=ledger_dir,
            source_path=src,
            original_filename="stmt.csv",
            source_format="csv",
        )
        # File ended up under <ledger_dir>/imports/00001-stmt.csv.
        archived = ledger_dir / result.archived_path
        assert archived.exists()
        assert archived.parent.name == ARCHIVE_SUBDIR
        assert archived.name == f"{result.file_id:05d}-stmt.csv"
        # Row landed in the manifest.
        row = conn.execute(
            "SELECT * FROM imported_files WHERE id = ?",
            (result.file_id,),
        ).fetchone()
        assert row is not None
        assert row["source_format"] == "csv"
        assert row["original_filename"] == "stmt.csv"
        assert (
            row["content_sha256"]
            == compute_sha256(src)
        )

    def test_archive_from_content_writes_bytes_directly(
        self, conn, ledger_dir,
    ):
        # Paste intake hashes & writes directly without a source
        # file on disk.
        content = b"date,amount\n2026-04-15,-12.50\n"
        result = archive_file(
            conn,
            ledger_dir=ledger_dir,
            content=content,
            original_filename="paste-2026-04-29.csv",
            source_format="paste",
        )
        archived = ledger_dir / result.archived_path
        assert archived.read_bytes() == content
        assert result.byte_size == len(content)

    def test_full_file_dedup_reuses_existing_id(
        self, conn, ledger_dir,
    ):
        content = b"date,amount\n2026-04-15,-12.50\n"
        first = archive_file(
            conn,
            ledger_dir=ledger_dir,
            content=content,
            original_filename="stmt.csv",
            source_format="csv",
        )
        # Second import of the same bytes — different filename even.
        second = archive_file(
            conn,
            ledger_dir=ledger_dir,
            content=content,
            original_filename="duplicate-statement.csv",
            source_format="csv",
        )
        assert second.file_id == first.file_id, (
            "same content must reuse the existing archive id "
            "instead of creating a second copy"
        )
        # Only one file actually on disk.
        archive_dir = ledger_dir / ARCHIVE_SUBDIR
        assert len(list(archive_dir.iterdir())) == 1

    def test_monotonic_ids(self, conn, ledger_dir):
        ids = []
        for i in range(3):
            result = archive_file(
                conn,
                ledger_dir=ledger_dir,
                content=f"row {i}\n".encode(),
                original_filename=f"f{i}.csv",
                source_format="csv",
            )
            ids.append(result.file_id)
        assert ids == sorted(ids)
        # Specifically: monotonic from 1.
        assert ids == [1, 2, 3]

    def test_filename_sanitizes_into_archive_name(
        self, conn, ledger_dir,
    ):
        result = archive_file(
            conn,
            ledger_dir=ledger_dir,
            content=b"x\n",
            original_filename="My Bank Statement (2026-04).csv",
            source_format="csv",
        )
        archived = ledger_dir / result.archived_path
        assert archived.name == (
            f"{result.file_id:05d}-My-Bank-Statement--2026-04-.csv"
        )

    def test_unknown_format_rejected(self, conn, ledger_dir):
        with pytest.raises(ArchiveError, match="unknown source_format"):
            archive_file(
                conn,
                ledger_dir=ledger_dir,
                content=b"x\n",
                original_filename="weird.foo",
                source_format="foo",
            )

    def test_neither_source_nor_content_rejected(
        self, conn, ledger_dir,
    ):
        with pytest.raises(
            ArchiveError, match="exactly one of source_path / content"
        ):
            archive_file(
                conn,
                ledger_dir=ledger_dir,
                original_filename="x.csv",
                source_format="csv",
            )

    def test_both_source_and_content_rejected(
        self, conn, ledger_dir,
    ):
        with pytest.raises(
            ArchiveError, match="exactly one of source_path / content"
        ):
            archive_file(
                conn,
                ledger_dir=ledger_dir,
                source_path=ledger_dir / "stmt.csv",
                content=b"x",
                original_filename="x.csv",
                source_format="csv",
            )

    def test_missing_source_path_rejected(self, conn, ledger_dir):
        with pytest.raises(ArchiveError, match="does not exist"):
            archive_file(
                conn,
                ledger_dir=ledger_dir,
                source_path=ledger_dir / "missing.csv",
                original_filename="missing.csv",
                source_format="csv",
            )


# --- read paths --------------------------------------------------------


class TestReadPaths:

    def test_find_by_sha256_round_trip(self, conn, ledger_dir):
        result = archive_file(
            conn,
            ledger_dir=ledger_dir,
            content=b"row\n",
            original_filename="f.csv",
            source_format="csv",
        )
        looked = find_by_sha256(
            conn, content_sha256=result.content_sha256,
        )
        assert looked is not None
        assert looked.file_id == result.file_id

    def test_find_by_sha256_miss_returns_none(self, conn):
        assert find_by_sha256(
            conn, content_sha256="0" * 64,
        ) is None

    def test_get_archived_path_resolves_absolute(
        self, conn, ledger_dir,
    ):
        result = archive_file(
            conn,
            ledger_dir=ledger_dir,
            content=b"row\n",
            original_filename="f.csv",
            source_format="csv",
        )
        path = get_archived_path(
            conn, ledger_dir=ledger_dir, file_id=result.file_id,
        )
        assert path is not None
        assert path.is_absolute()
        assert path.exists()
        assert path.read_bytes() == b"row\n"

    def test_get_archived_path_unknown_id_returns_none(
        self, conn, ledger_dir,
    ):
        assert get_archived_path(
            conn, ledger_dir=ledger_dir, file_id=999,
        ) is None

    def test_list_archived_newest_first(self, conn, ledger_dir):
        for i in range(3):
            archive_file(
                conn,
                ledger_dir=ledger_dir,
                content=f"row {i}\n".encode(),
                original_filename=f"f{i}.csv",
                source_format="csv",
            )
        rows = list_archived(conn)
        assert len(rows) == 3
        assert [r.file_id for r in rows] == [3, 2, 1]

    def test_list_archived_filter_by_format(self, conn, ledger_dir):
        archive_file(
            conn, ledger_dir=ledger_dir, content=b"a\n",
            original_filename="a.csv", source_format="csv",
        )
        archive_file(
            conn, ledger_dir=ledger_dir, content=b"b\n",
            original_filename="b.xlsx", source_format="xlsx",
        )
        csvs = list_archived(conn, source_format="csv")
        assert len(csvs) == 1
        assert csvs[0].source_format == "csv"


# --- reconstruct -------------------------------------------------------


class TestReconstruct:
    """ADR-0001 / ADR-0015 invariant: a wipe + walk rebuilds the
    manifest. The archive directory is the source of truth."""

    def test_walk_archive_dir_rebuilds_manifest(
        self, conn, ledger_dir, monkeypatch,
    ):
        # Archive two files.
        a = archive_file(
            conn, ledger_dir=ledger_dir, content=b"row a\n",
            original_filename="a.csv", source_format="csv",
        )
        b = archive_file(
            conn, ledger_dir=ledger_dir, content=b"row b\n",
            original_filename="b.csv", source_format="csv",
        )
        # Wipe the manifest as if SQLite had been deleted.
        conn.execute("DELETE FROM imported_files")
        conn.commit()
        # Run the reconstruct step. ledger_dir is resolved via env
        # var so we can point it at our fixture.
        monkeypatch.setenv("LAMELLA_LEDGER_DIR", str(ledger_dir))
        from lamella.core.transform.steps.step25_imported_files import (
            reconstruct_imported_files,
        )
        report = reconstruct_imported_files(conn, entries=[])
        assert report.rows_written == 2
        # Both ids restored.
        rows = list_archived(conn)
        assert {r.file_id for r in rows} == {a.file_id, b.file_id}

    def test_reconstruct_skips_unparseable_files(
        self, conn, ledger_dir, monkeypatch,
    ):
        archive_dir = ledger_dir / ARCHIVE_SUBDIR
        archive_dir.mkdir()
        # User dropped a non-canonical file in the archive dir
        # (e.g., a manual restore from backup with a different
        # naming scheme).
        (archive_dir / "manually-renamed.csv").write_bytes(b"x\n")
        (archive_dir / "00001-real.csv").write_bytes(b"real\n")
        monkeypatch.setenv("LAMELLA_LEDGER_DIR", str(ledger_dir))
        from lamella.core.transform.steps.step25_imported_files import (
            reconstruct_imported_files,
        )
        report = reconstruct_imported_files(conn, entries=[])
        assert report.rows_written == 1
        assert any("not matching" in n for n in report.notes)


# --- format allowlist sanity -------------------------------------------


def test_allowed_formats_covers_user_visible_intakes():
    """ADR-0060 enumerates the allowed source_format tags. Make
    sure the runtime allowlist matches the ADR exactly so a future
    rename of the contract surfaces here."""
    assert ALLOWED_FORMATS == frozenset({
        "csv", "ofx", "qif", "iif", "xlsx", "ods", "paste",
    })
