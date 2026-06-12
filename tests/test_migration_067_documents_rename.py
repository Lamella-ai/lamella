# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for migration 067 — ADR-0061 Phase 2 (DB schema renames +
``document_type`` discriminator).

Pins down:

- Table renames: ``receipt_links`` → ``document_links``,
  ``receipt_dismissals`` → ``document_dismissals``,
  ``receipt_link_blocks`` → ``document_link_blocks``. After
  migration the OLD names raise ``sqlite3.OperationalError`` and
  the NEW names are queryable.
- Column rename: ``paperless_doc_index.receipt_date`` →
  ``paperless_doc_index.document_date``. Old name absent, new name
  present.
- New column: ``paperless_doc_index.document_type`` exists. Legacy
  rows are backfilled to ``'receipt'``; new rows default to NULL.
- Row data preservation: counts and key column values survive the
  rename intact (no data loss).
- Idempotency: ``migrate()`` is the framework-level guard
  (``schema_migrations`` version table); re-running ``migrate()``
  on a v67-applied DB is a no-op. Pinned via ``applied == []`` on
  the second invocation.
- Index rename: the post-migration index names follow the new
  ``document_*`` convention; old index names are gone.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lamella.core.db import _migration_files, migrate


def _apply_through(db: sqlite3.Connection, *, max_version: int) -> None:
    """Run only the migrations with version <= max_version. Used to
    set up a v3 (pre-067) schema state we can then transform with
    the 067 migration."""
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    for version, _name, sql in _migration_files():
        if version > max_version:
            continue
        db.executescript("BEGIN;\n" + sql + "\nCOMMIT;")
        db.execute(
            "INSERT OR IGNORE INTO schema_migrations (version) VALUES (?)",
            (version,),
        )


def _v3_db(tmp_path: Path) -> sqlite3.Connection:
    """Return a connection with migrations 1..66 applied (the
    pre-Phase-2 schema state). Tests then replay 067 manually so we
    can assert exactly the diff that 067 introduces."""
    db = sqlite3.connect(tmp_path / "v3.sqlite")
    db.row_factory = sqlite3.Row
    _apply_through(db, max_version=66)
    return db


def _seed_pre_rename_data(db: sqlite3.Connection) -> dict[str, int]:
    """Insert rows into the OLD-named tables + the OLD column shape so
    the 067 migration has something to preserve and backfill. Returns
    the counts so tests can assert preservation post-migration."""
    db.execute(
        "INSERT INTO receipt_links "
        "(paperless_id, txn_hash, txn_date, txn_amount, match_method, match_confidence) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (101, "hash-receipt-link-1", "2026-04-01", "12.34", "manual", 1.0),
    )
    db.execute(
        "INSERT INTO receipt_links "
        "(paperless_id, txn_hash, txn_date, txn_amount, match_method, match_confidence) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (102, "hash-receipt-link-2", "2026-04-02", "56.78", "auto", 0.91),
    )
    db.execute(
        "INSERT INTO receipt_dismissals (txn_hash, reason, dismissed_by) "
        "VALUES (?, ?, ?)",
        ("hash-dismiss-1", "no receipt expected", "user"),
    )
    db.execute(
        "INSERT INTO receipt_link_blocks (paperless_id, txn_hash, reason) "
        "VALUES (?, ?, ?)",
        (103, "hash-blocked-1", "user_unlink"),
    )
    # paperless_doc_index rows with the OLD `receipt_date` column.
    db.execute(
        "INSERT INTO paperless_doc_index "
        "(paperless_id, title, vendor, total_amount, receipt_date) "
        "VALUES (?, ?, ?, ?, ?)",
        (201, "Hardware Store receipt", "Acme Hardware", "12.34", "2026-04-01"),
    )
    db.execute(
        "INSERT INTO paperless_doc_index "
        "(paperless_id, title, vendor, total_amount, receipt_date) "
        "VALUES (?, ?, ?, ?, ?)",
        (202, "Coffee Shop", "Acme Coffee", "5.50", "2026-04-02"),
    )
    db.commit()
    return {
        "receipt_links": 2,
        "receipt_dismissals": 1,
        "receipt_link_blocks": 1,
        "paperless_doc_index": 2,
    }


def _apply_067(db: sqlite3.Connection) -> None:
    """Run migration 067 against a DB that already has migrations
    1..66 applied. Mirrors what ``migrate()`` would do for a single
    version step."""
    sql = next(s for v, _n, s in _migration_files() if v == 67)
    db.executescript("BEGIN;\n" + sql + "\nCOMMIT;")
    db.execute(
        "INSERT OR IGNORE INTO schema_migrations (version) VALUES (?)", (67,)
    )
    db.commit()


# ---------------------------------------------------------------------------
# Table renames
# ---------------------------------------------------------------------------


def test_tables_renamed(tmp_path: Path):
    db = _v3_db(tmp_path)
    _seed_pre_rename_data(db)
    _apply_067(db)

    # New names queryable.
    db.execute("SELECT 1 FROM document_links LIMIT 1").fetchone()
    db.execute("SELECT 1 FROM document_dismissals LIMIT 1").fetchone()
    db.execute("SELECT 1 FROM document_link_blocks LIMIT 1").fetchone()

    # Old names gone.
    for old_name in (
        "receipt_links", "receipt_dismissals", "receipt_link_blocks",
    ):
        with pytest.raises(sqlite3.OperationalError):
            db.execute(f"SELECT 1 FROM {old_name}").fetchone()


def test_row_counts_preserved(tmp_path: Path):
    db = _v3_db(tmp_path)
    counts = _seed_pre_rename_data(db)
    _apply_067(db)

    new_for_old = {
        "receipt_links": "document_links",
        "receipt_dismissals": "document_dismissals",
        "receipt_link_blocks": "document_link_blocks",
    }
    for old_table, new_table in new_for_old.items():
        n = db.execute(f"SELECT COUNT(*) AS n FROM {new_table}").fetchone()["n"]
        assert n == counts[old_table], (
            f"row count mismatch for {old_table} → {new_table}: "
            f"expected {counts[old_table]}, got {n}"
        )


def test_row_data_preserved(tmp_path: Path):
    db = _v3_db(tmp_path)
    _seed_pre_rename_data(db)
    _apply_067(db)

    # Spot-check that the actual values survived intact.
    row = db.execute(
        "SELECT paperless_id, txn_hash, txn_amount, match_method "
        "FROM document_links WHERE paperless_id = 101"
    ).fetchone()
    assert row["txn_hash"] == "hash-receipt-link-1"
    assert row["txn_amount"] == "12.34"
    assert row["match_method"] == "manual"

    row = db.execute(
        "SELECT reason, dismissed_by FROM document_dismissals "
        "WHERE txn_hash = 'hash-dismiss-1'"
    ).fetchone()
    assert row["reason"] == "no receipt expected"
    assert row["dismissed_by"] == "user"

    row = db.execute(
        "SELECT reason FROM document_link_blocks "
        "WHERE paperless_id = 103 AND txn_hash = 'hash-blocked-1'"
    ).fetchone()
    assert row["reason"] == "user_unlink"


# ---------------------------------------------------------------------------
# Column rename: receipt_date → document_date
# ---------------------------------------------------------------------------


def test_receipt_date_column_renamed_to_document_date(tmp_path: Path):
    db = _v3_db(tmp_path)
    _seed_pre_rename_data(db)
    _apply_067(db)

    cols = {
        row["name"]
        for row in db.execute("PRAGMA table_info(paperless_doc_index)")
    }
    assert "document_date" in cols, "document_date column missing post-migration"
    assert "receipt_date" not in cols, "receipt_date column should be gone"


def test_document_date_values_preserved(tmp_path: Path):
    db = _v3_db(tmp_path)
    _seed_pre_rename_data(db)
    _apply_067(db)

    row = db.execute(
        "SELECT document_date FROM paperless_doc_index WHERE paperless_id = 201"
    ).fetchone()
    assert row["document_date"] == "2026-04-01"

    row = db.execute(
        "SELECT document_date FROM paperless_doc_index WHERE paperless_id = 202"
    ).fetchone()
    assert row["document_date"] == "2026-04-02"


# ---------------------------------------------------------------------------
# document_type discriminator
# ---------------------------------------------------------------------------


def test_document_type_column_exists_and_legacy_rows_backfilled(tmp_path: Path):
    db = _v3_db(tmp_path)
    _seed_pre_rename_data(db)
    _apply_067(db)

    cols = {
        row["name"]
        for row in db.execute("PRAGMA table_info(paperless_doc_index)")
    }
    assert "document_type" in cols, "document_type column missing"

    # Both legacy rows backfilled to 'receipt' per the §4 default for
    # pre-Phase-2 data.
    rows = db.execute(
        "SELECT paperless_id, document_type FROM paperless_doc_index "
        "ORDER BY paperless_id"
    ).fetchall()
    assert all(row["document_type"] == "receipt" for row in rows), rows


def test_new_rows_default_to_null_document_type(tmp_path: Path):
    """A row inserted AFTER the migration without an explicit
    ``document_type`` value gets NULL — only legacy rows are
    backfilled. The Paperless sync populates ``document_type`` from
    ``paperless_doc_type_roles`` when the user has classified the
    Paperless doc-type."""
    db = _v3_db(tmp_path)
    _seed_pre_rename_data(db)
    _apply_067(db)

    db.execute(
        "INSERT INTO paperless_doc_index "
        "(paperless_id, title, total_amount, document_date) "
        "VALUES (?, ?, ?, ?)",
        (999, "freshly-synced", "1.00", "2026-05-01"),
    )
    row = db.execute(
        "SELECT document_type FROM paperless_doc_index WHERE paperless_id = 999"
    ).fetchone()
    assert row["document_type"] is None


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_migrate_is_idempotent(tmp_path: Path):
    """``migrate()`` short-circuits via the schema_migrations version
    gate — re-running on an already-migrated DB is a no-op. This is
    the project's standard idempotency contract; the 067 migration
    leans on it (no in-script guard)."""
    db_path = tmp_path / "idem.sqlite"
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    first = migrate(db)
    assert 67 in first, "067 should have applied on first migrate()"

    second = migrate(db)
    assert second == [], (
        "second migrate() must be a no-op; got newly-applied: "
        f"{second!r}"
    )

    # Sanity: the renamed tables are still queryable, the old ones still gone.
    db.execute("SELECT 1 FROM document_links LIMIT 1").fetchone()
    db.execute("SELECT 1 FROM document_dismissals LIMIT 1").fetchone()
    db.execute("SELECT 1 FROM document_link_blocks LIMIT 1").fetchone()
    cols = {
        row["name"]
        for row in db.execute("PRAGMA table_info(paperless_doc_index)")
    }
    assert "document_type" in cols
    assert "document_date" in cols
    assert "receipt_date" not in cols
    db.close()


# ---------------------------------------------------------------------------
# Index renames
# ---------------------------------------------------------------------------


def test_indexes_renamed_to_document_prefix(tmp_path: Path):
    db = _v3_db(tmp_path)
    _seed_pre_rename_data(db)
    _apply_067(db)

    index_names = {
        row["name"]
        for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index'"
        )
    }
    # New index names exist.
    assert "document_links_paperless_idx" in index_names
    assert "document_links_hash_idx" in index_names
    assert "idx_document_link_blocks_txn" in index_names
    assert "paperless_doc_index_document_date_idx" in index_names
    # Old index names are gone.
    assert "receipt_links_paperless_idx" not in index_names
    assert "receipt_links_hash_idx" not in index_names
    assert "idx_receipt_link_blocks_txn" not in index_names
    assert "paperless_doc_index_receipt_date_idx" not in index_names
