# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for migration 054 (setup_repair_state) — Phase 6.1.1.

Pins down the locked-spec discipline before any consumer code
touches the table:

- Table lands with the correct shape.
- IF NOT EXISTS is real: re-applying the migration with an
  existing row preserves the row.
- Default state_json is the empty-blob shape so first-write code
  can read-then-merge instead of having to handle NULL.
- Reconstruct semantics: dropping the table mid-session and re-
  running migrations re-creates it empty (not a regression — that
  IS the desired behavior when SQLite is wiped wholesale). The
  reconstruct-doesn't-drop discipline is enforced at the
  reconstruct module level (not the migration), so this test
  documents the migration's own contract: "if the table doesn't
  exist, create it with empty defaults; if it does, leave it
  alone."
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from lamella.core.db import migrate


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    yield db
    db.close()


# ---------------------------------------------------------------------------
# Schema shape
# ---------------------------------------------------------------------------


def test_table_exists_after_migrate(conn):
    """Migration 054 ran as part of the standard migrate() chain
    and the table is present."""
    row = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name='setup_repair_state'"
    ).fetchone()
    assert row is not None, "setup_repair_state table must exist after migrate"


def test_columns_match_spec(conn):
    """Column types match the Phase 6 spec lock — TEXT primary key
    on session_id, TEXT state_json with the empty-blob default,
    two TIMESTAMP columns auto-stamped."""
    rows = conn.execute(
        "PRAGMA table_info(setup_repair_state)"
    ).fetchall()
    by_name = {r["name"]: r for r in rows}

    assert "session_id" in by_name
    assert by_name["session_id"]["type"].upper() == "TEXT"
    assert by_name["session_id"]["pk"] == 1
    assert by_name["session_id"]["notnull"] == 1

    assert "state_json" in by_name
    assert by_name["state_json"]["type"].upper() == "TEXT"
    assert by_name["state_json"]["notnull"] == 1

    assert "created_at" in by_name
    assert "TIMESTAMP" in by_name["created_at"]["type"].upper()
    assert by_name["created_at"]["notnull"] == 1

    assert "updated_at" in by_name
    assert "TIMESTAMP" in by_name["updated_at"]["type"].upper()
    assert by_name["updated_at"]["notnull"] == 1


def test_state_json_default_is_empty_blob_shape(conn):
    """The default value mirrors the spec's blob shape so first-
    write code can `json.loads(state_json)` without a NULL guard.
    Insert with only the primary key, verify the default lands."""
    conn.execute(
        "INSERT INTO setup_repair_state (session_id) VALUES (?)",
        ("current",),
    )
    conn.commit()
    row = conn.execute(
        "SELECT state_json FROM setup_repair_state WHERE session_id = ?",
        ("current",),
    ).fetchone()
    parsed = json.loads(row["state_json"])
    assert parsed == {"findings": {}, "applied_history": []}


# ---------------------------------------------------------------------------
# Idempotency / re-application
# ---------------------------------------------------------------------------


def test_re_applying_migrate_preserves_existing_rows(tmp_path):
    """The IF NOT EXISTS guard means a second migrate() call (e.g.
    after an upgrade that adds new files) doesn't drop or
    recreate the table, so any draft in flight survives."""
    db_path = tmp_path / "repair.sqlite"
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    migrate(db)

    # Write a non-default draft.
    draft = {
        "findings": {
            "schema_drift:abcdef123456": {
                "action": "apply",
                "edit_payload": None,
            }
        },
        "applied_history": [],
    }
    db.execute(
        "INSERT INTO setup_repair_state (session_id, state_json) "
        "VALUES (?, ?)",
        ("current", json.dumps(draft)),
    )
    db.commit()
    db.close()

    # Re-open + re-migrate.
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    applied = migrate(db)
    # Nothing newly applied (the migration's already in
    # schema_migrations).
    assert applied == []

    # The draft survived.
    row = db.execute(
        "SELECT state_json FROM setup_repair_state WHERE session_id = ?",
        ("current",),
    ).fetchone()
    assert row is not None
    assert json.loads(row["state_json"]) == draft
    db.close()


def test_create_with_if_not_exists_on_pre_existing_table(tmp_path):
    """A direct re-run of the 054 SQL file against a DB that
    already has the table doesn't error and doesn't drop the row.
    Defends the reconstruct-doesn't-drop contract — even if the
    migration runner replayed 054 standalone, the existing draft
    would survive."""
    from lamella.core.db import _migration_files

    db = sqlite3.connect(tmp_path / "redo.sqlite")
    db.row_factory = sqlite3.Row
    migrate(db)

    db.execute(
        "INSERT INTO setup_repair_state (session_id, state_json) "
        "VALUES (?, ?)",
        ("current", '{"findings":{"x":{"action":"dismiss"}},"applied_history":[]}'),
    )
    db.commit()

    # Replay 054 directly.
    sql_054 = next(
        sql for v, _n, sql in _migration_files() if v == 54
    )
    db.executescript("BEGIN;\n" + sql_054 + "\nCOMMIT;")

    row = db.execute(
        "SELECT state_json FROM setup_repair_state WHERE session_id = ?",
        ("current",),
    ).fetchone()
    assert row is not None
    parsed = json.loads(row["state_json"])
    assert parsed["findings"] == {"x": {"action": "dismiss"}}
    db.close()


# ---------------------------------------------------------------------------
# Constraint behavior
# ---------------------------------------------------------------------------


def test_session_id_primary_key_uniqueness(conn):
    """A second insert with the same session_id raises — the
    blob is single-row-per-session by design. Callers UPDATE
    instead of double-inserting; that's the contract Phase 6.1.2's
    write helper enforces."""
    conn.execute(
        "INSERT INTO setup_repair_state (session_id) VALUES (?)",
        ("current",),
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO setup_repair_state (session_id) VALUES (?)",
            ("current",),
        )
        conn.commit()


def test_session_id_supports_alternate_values(conn):
    """v1 uses the literal "current" but the column is plain TEXT
    — a future expansion to multiple parallel repair sessions can
    use real session IDs without a schema change."""
    for session_id in ("current", "session-a", "session-b"):
        conn.execute(
            "INSERT INTO setup_repair_state (session_id) VALUES (?)",
            (session_id,),
        )
    conn.commit()
    rows = conn.execute(
        "SELECT session_id FROM setup_repair_state ORDER BY session_id"
    ).fetchall()
    assert [r["session_id"] for r in rows] == [
        "current", "session-a", "session-b",
    ]
