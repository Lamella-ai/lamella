# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ``setup_repair_state`` CRUD — Phase 6.1.2.

Pins down: (a) read of an empty install returns the empty-blob
shape (not None), (b) write-then-read round-trips byte-identical,
(c) clear deletes and a follow-up read returns empty, (d) validate
rejects malformed shapes before they hit disk, (e) UPSERT
preserves created_at across writes (we record updated_at on
every replace), (f) malformed blob already stored raises on read
rather than silently masquerading as empty.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from lamella.features.recovery.repair_state import (
    DEFAULT_SESSION_ID,
    EMPTY_BLOB,
    RepairStateValidationError,
    clear_repair_state,
    read_repair_state,
    write_repair_state,
)
from lamella.core.db import migrate


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    yield db
    db.close()


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


def test_read_empty_install_returns_empty_blob(conn):
    """A fresh install has no row — read returns the canonical
    empty shape rather than None or raising."""
    state = read_repair_state(conn)
    assert state == {"findings": {}, "applied_history": []}


def test_read_returns_fresh_copy_not_module_constant(conn):
    """Mutating a read result must not leak into the next read.
    Defends against the easy bug where every reader gets the
    same dict identity."""
    state1 = read_repair_state(conn)
    state1["findings"]["leak"] = {"action": "apply", "edit_payload": None}
    state2 = read_repair_state(conn)
    assert "leak" not in state2["findings"]
    # And the module constant is also untouched.
    assert "leak" not in EMPTY_BLOB["findings"]


def test_read_alternate_session_id_independent(conn):
    """Two distinct session_ids hold distinct blobs. Forward-
    compatibility for the multi-session expansion."""
    write_repair_state(
        conn,
        {
            "findings": {"a:1": {"action": "apply", "edit_payload": None}},
            "applied_history": [],
        },
        session_id="alpha",
    )
    write_repair_state(
        conn,
        {
            "findings": {"b:2": {"action": "dismiss", "edit_payload": None}},
            "applied_history": [],
        },
        session_id="beta",
    )
    a = read_repair_state(conn, session_id="alpha")
    b = read_repair_state(conn, session_id="beta")
    assert "a:1" in a["findings"]
    assert "b:2" in b["findings"]
    assert "a:1" not in b["findings"]


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def test_write_then_read_round_trip(conn):
    """Round-trip across the full blob shape — findings dict with
    every action variant, applied_history with one entry."""
    state = {
        "findings": {
            "schema_drift:abcd123456": {
                "action": "apply",
                "edit_payload": None,
            },
            "legacy_path:efgh789abc": {
                "action": "edit",
                "edit_payload": {"canonical": "Assets:Acme:Vehicle:V1"},
            },
            "missing_scaffold:xyz0987654": {
                "action": "dismiss",
                "edit_payload": None,
            },
        },
        "applied_history": [
            {
                "group": "schema",
                "committed_at": "2026-04-26T10:30:00Z",
                "applied_finding_ids": ["schema_drift:abcd123456"],
                "failed_finding_ids": [],
            },
        ],
    }
    write_repair_state(conn, state)
    got = read_repair_state(conn)
    assert got == state


def test_write_replaces_in_full_does_not_merge(conn):
    """The contract is replace-not-merge. A second write with
    fewer findings drops the absent ones — callers that want
    merge semantics must read-modify-write."""
    write_repair_state(conn, {
        "findings": {
            "a:1": {"action": "apply", "edit_payload": None},
            "b:2": {"action": "apply", "edit_payload": None},
        },
        "applied_history": [],
    })
    write_repair_state(conn, {
        "findings": {
            "a:1": {"action": "dismiss", "edit_payload": None},
        },
        "applied_history": [],
    })
    got = read_repair_state(conn)
    assert got["findings"] == {
        "a:1": {"action": "dismiss", "edit_payload": None},
    }


def test_write_preserves_created_at_and_advances_updated_at(conn):
    """Per the spec, created_at is the per-session forensic stamp
    ("when did this draft start") and updated_at is the activity
    stamp ("when did the user last touch the draft"). UPSERT must
    leave created_at fixed and advance updated_at on every write.

    The resume-from-failed-group UX may surface updated_at to ask
    "Resume the batch you started 3 hours ago?" — that question is
    only meaningful if updated_at actually moves."""
    import time

    write_repair_state(conn, EMPTY_BLOB)
    first = conn.execute(
        "SELECT created_at, updated_at FROM setup_repair_state "
        "WHERE session_id = ?", (DEFAULT_SESSION_ID,),
    ).fetchone()

    # SQLite's CURRENT_TIMESTAMP has 1-second granularity; sleep
    # enough that the second write's stamp is provably later.
    time.sleep(1.05)
    write_repair_state(conn, {
        "findings": {"x:1": {"action": "apply", "edit_payload": None}},
        "applied_history": [],
    })
    second = conn.execute(
        "SELECT created_at, updated_at FROM setup_repair_state "
        "WHERE session_id = ?", (DEFAULT_SESSION_ID,),
    ).fetchone()

    # created_at frozen at the first write's timestamp.
    assert second["created_at"] == first["created_at"]
    # updated_at advanced — strictly greater than the first
    # write's updated_at, not just >= created_at.
    assert second["updated_at"] > first["updated_at"]


# ---------------------------------------------------------------------------
# Clear
# ---------------------------------------------------------------------------


def test_clear_deletes_row(conn):
    write_repair_state(conn, {
        "findings": {"x:1": {"action": "apply", "edit_payload": None}},
        "applied_history": [],
    })
    clear_repair_state(conn)
    assert read_repair_state(conn) == {"findings": {}, "applied_history": []}


def test_clear_idempotent_on_empty(conn):
    """Calling clear when no row exists is a silent no-op."""
    clear_repair_state(conn)  # First call: nothing to delete.
    clear_repair_state(conn)  # Second call: still nothing.
    assert read_repair_state(conn) == EMPTY_BLOB


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_write_rejects_non_dict(conn):
    with pytest.raises(RepairStateValidationError):
        write_repair_state(conn, ["not", "a", "dict"])  # type: ignore[arg-type]


def test_write_rejects_missing_findings(conn):
    with pytest.raises(RepairStateValidationError):
        write_repair_state(conn, {"applied_history": []})  # type: ignore[arg-type]


def test_write_rejects_missing_applied_history(conn):
    with pytest.raises(RepairStateValidationError):
        write_repair_state(conn, {"findings": {}})  # type: ignore[arg-type]


def test_write_rejects_invalid_action(conn):
    with pytest.raises(RepairStateValidationError, match="action"):
        write_repair_state(conn, {
            "findings": {
                "x:1": {"action": "delete", "edit_payload": None},
            },
            "applied_history": [],
        })


def test_write_rejects_non_string_finding_id(conn):
    with pytest.raises(RepairStateValidationError):
        write_repair_state(conn, {
            "findings": {
                42: {"action": "apply", "edit_payload": None},  # type: ignore[dict-item]
            },
            "applied_history": [],
        })


def test_write_rejects_invalid_edit_payload_type(conn):
    with pytest.raises(RepairStateValidationError, match="edit_payload"):
        write_repair_state(conn, {
            "findings": {
                "x:1": {"action": "edit", "edit_payload": "string-not-dict"},
            },
            "applied_history": [],
        })


def test_write_rejects_history_entry_missing_field(conn):
    """The spec lists four required fields per applied_history
    entry. Missing any of them is rejected."""
    with pytest.raises(RepairStateValidationError):
        write_repair_state(conn, {
            "findings": {},
            "applied_history": [
                {
                    "group": "schema",
                    # Missing committed_at + applied_finding_ids +
                    # failed_finding_ids.
                },
            ],
        })


def test_write_rejects_history_lists_as_strings(conn):
    """applied_finding_ids and failed_finding_ids must be lists.
    A common bug shape: serializing them as comma-separated
    strings."""
    with pytest.raises(RepairStateValidationError):
        write_repair_state(conn, {
            "findings": {},
            "applied_history": [{
                "group": "schema",
                "committed_at": "2026-04-26T10:30:00Z",
                "applied_finding_ids": "a,b,c",  # type: ignore[dict-item]
                "failed_finding_ids": [],
            }],
        })


# ---------------------------------------------------------------------------
# Read-side malformed-blob handling
# ---------------------------------------------------------------------------


def test_read_raises_on_corrupted_stored_blob(conn):
    """A blob written by some external process that bypassed
    validation. Surface it loudly — silent empty-fallback would
    mask the upstream bug."""
    conn.execute(
        "INSERT INTO setup_repair_state (session_id, state_json) "
        "VALUES (?, ?)",
        (DEFAULT_SESSION_ID, json.dumps([1, 2, 3])),
    )
    conn.commit()
    with pytest.raises(RepairStateValidationError, match="not a JSON object"):
        read_repair_state(conn)


def test_read_raises_on_blob_missing_findings(conn):
    conn.execute(
        "INSERT INTO setup_repair_state (session_id, state_json) "
        "VALUES (?, ?)",
        (DEFAULT_SESSION_ID, json.dumps({"applied_history": []})),
    )
    conn.commit()
    with pytest.raises(RepairStateValidationError, match="findings"):
        read_repair_state(conn)
