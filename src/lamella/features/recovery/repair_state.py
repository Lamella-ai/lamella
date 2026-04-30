# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""``setup_repair_state`` CRUD — Phase 6.1.2 of /setup/recovery.

Pure read/write/clear over the JSON blob from migration 054. The
bulk-apply orchestrator (Phase 6.1.3) consumes this layer; the
bulk-review UI (Phase 6.1.5) writes drafts through it on field
change. No business logic here — every state transition that
isn't a literal blob mutation belongs upstream.

Single-row contract: every operation keys on ``session_id`` and
v1 uses the literal string ``"current"``. The functions accept a
``session_id`` parameter for forward compatibility (Phase 7+ may
introduce multi-session) but default to ``"current"``.

Read-side returns the canonical empty-blob shape when no row
exists, mirroring the migration's column default. Callers can
``read_repair_state(conn)`` unconditionally and treat the result
as a populated dict; they don't need to branch on row presence.

Write-side validates the blob shape (top-level ``findings`` dict,
``applied_history`` list) before serializing. A malformed write
raises :class:`RepairStateValidationError` rather than landing a
shape the orchestrator would later choke on.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any


__all__ = [
    "DEFAULT_SESSION_ID",
    "EMPTY_BLOB",
    "RepairStateValidationError",
    "clear_repair_state",
    "read_repair_state",
    "write_repair_state",
]


_LOG = logging.getLogger(__name__)


DEFAULT_SESSION_ID = "current"
"""v1 single-session sentinel. See Phase 6 spec freeze for the
multi-session future expansion path."""


# Canonical empty shape — every read returns at minimum this. Matches
# the migration's column default exactly.
EMPTY_BLOB: dict[str, Any] = {
    "findings": {},
    "applied_history": [],
}


class RepairStateValidationError(ValueError):
    """Raised by :func:`write_repair_state` when the supplied blob
    doesn't match the locked Phase 6 shape. The orchestrator
    expects ``findings: dict[str, ...]`` + ``applied_history:
    list[...]`` and the read path can't recover from a write that
    violates either invariant."""


# --- read ------------------------------------------------------------------


def read_repair_state(
    conn: sqlite3.Connection,
    *,
    session_id: str = DEFAULT_SESSION_ID,
) -> dict[str, Any]:
    """Return the parsed blob for ``session_id``. Returns
    :data:`EMPTY_BLOB` (a fresh copy — caller may mutate) when no
    row exists yet. Never returns None.

    Doesn't raise on a malformed stored blob — that would mean
    earlier write logic skipped validation, which the writer in
    this module makes impossible. If a caller is reading rows
    written by some external process and they're malformed, that
    process is the bug; here we surface the malformation as a
    visible parse error rather than silently returning empty
    (which would mask the problem)."""
    row = conn.execute(
        "SELECT state_json FROM setup_repair_state "
        "WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    if row is None:
        # Return a fresh copy so callers mutating the result don't
        # accidentally mutate the module-level constant.
        return _empty_blob_copy()

    state_json = row["state_json"] if isinstance(row, sqlite3.Row) else row[0]
    parsed = json.loads(state_json)
    # Defense-in-depth: a row's state_json could be a primitive
    # (legacy fixture, hand-edited row); coerce to the expected
    # shape rather than letting the type confusion propagate.
    if not isinstance(parsed, dict):
        raise RepairStateValidationError(
            f"setup_repair_state.state_json for session_id={session_id!r} "
            f"is not a JSON object (got {type(parsed).__name__}). "
            "This indicates an upstream write bypassed validation."
        )
    if "findings" not in parsed or not isinstance(parsed["findings"], dict):
        raise RepairStateValidationError(
            "stored state_json missing 'findings' dict — "
            "use clear_repair_state(conn) and re-write a fresh draft"
        )
    if "applied_history" not in parsed or not isinstance(
        parsed["applied_history"], list,
    ):
        raise RepairStateValidationError(
            "stored state_json missing 'applied_history' list — "
            "use clear_repair_state(conn) and re-write a fresh draft"
        )
    return parsed


# --- write -----------------------------------------------------------------


def write_repair_state(
    conn: sqlite3.Connection,
    state: dict[str, Any],
    *,
    session_id: str = DEFAULT_SESSION_ID,
) -> None:
    """Replace the blob for ``session_id`` with ``state``. UPSERT
    semantics — creates the row if missing, replaces in full if
    present.

    Validates the blob shape before serializing. **Replace-not-
    merge is intentional**, not a "we should merge for safety"
    oversight: the blob is a draft snapshot, not an accumulating
    log. Merge semantics would mean a concurrent draft edit could
    resurrect a finding the user just dismissed (concurrent write
    A reads pre-dismissal, write B dismisses, write A merges its
    pre-dismissal view back in, dismissal is lost). Replace puts
    each writer's full intent on disk; callers that want read-
    modify-write semantics call :func:`read_repair_state` first,
    mutate the dict, and pass the mutated copy in.

    Auto-stamps ``updated_at = CURRENT_TIMESTAMP`` on every write
    so a Phase 7+ "abandoned drafts" pass can prune sessions
    untouched for N days.

    Raises:
        RepairStateValidationError: blob doesn't match shape.
        sqlite3.OperationalError: table missing (migration 054
            didn't run — programmer error, not user-facing).
    """
    _validate(state)
    payload = json.dumps(state, ensure_ascii=False)
    # UPSERT via INSERT OR REPLACE keeps the migration default
    # behavior: a fresh row gets created_at = now via the column
    # default, an existing row keeps its created_at (no — actually
    # INSERT OR REPLACE deletes and re-inserts, so created_at
    # would reset). Use the explicit ON CONFLICT path to preserve
    # created_at across replaces.
    conn.execute(
        """
        INSERT INTO setup_repair_state (session_id, state_json)
        VALUES (?, ?)
        ON CONFLICT(session_id) DO UPDATE SET
            state_json = excluded.state_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (session_id, payload),
    )
    conn.commit()


# --- clear -----------------------------------------------------------------


def clear_repair_state(
    conn: sqlite3.Connection,
    *,
    session_id: str = DEFAULT_SESSION_ID,
) -> None:
    """Delete the row for ``session_id``. Idempotent — silent
    if no row exists. After this, :func:`read_repair_state`
    returns :data:`EMPTY_BLOB` until the next write."""
    conn.execute(
        "DELETE FROM setup_repair_state WHERE session_id = ?",
        (session_id,),
    )
    conn.commit()


# --- helpers ---------------------------------------------------------------


def _empty_blob_copy() -> dict[str, Any]:
    """Deep copy of :data:`EMPTY_BLOB` so mutating a read result
    doesn't leak into subsequent reads or into the module
    constant. Copy via ``copy.deepcopy`` (instead of literal-
    duplicating the shape) so a future change to ``EMPTY_BLOB``
    only touches one place."""
    import copy
    return copy.deepcopy(EMPTY_BLOB)


def _validate(state: dict[str, Any]) -> None:
    """Validate the blob shape against the locked Phase 6 schema.
    Raises :class:`RepairStateValidationError` on any violation."""
    if not isinstance(state, dict):
        raise RepairStateValidationError(
            f"state must be a dict, got {type(state).__name__}"
        )
    findings = state.get("findings")
    if not isinstance(findings, dict):
        raise RepairStateValidationError(
            "state['findings'] must be a dict mapping finding_id → "
            "{action, edit_payload}"
        )
    for fid, entry in findings.items():
        if not isinstance(fid, str):
            raise RepairStateValidationError(
                f"finding key must be str, got {type(fid).__name__}"
            )
        if not isinstance(entry, dict):
            raise RepairStateValidationError(
                f"findings[{fid!r}] must be a dict"
            )
        action = entry.get("action")
        if action not in ("apply", "edit", "dismiss"):
            raise RepairStateValidationError(
                f"findings[{fid!r}].action must be one of "
                f"('apply', 'edit', 'dismiss'), got {action!r}"
            )
        edit_payload = entry.get("edit_payload")
        if edit_payload is not None and not isinstance(edit_payload, dict):
            raise RepairStateValidationError(
                f"findings[{fid!r}].edit_payload must be None or a dict, "
                f"got {type(edit_payload).__name__}"
            )

    history = state.get("applied_history")
    if not isinstance(history, list):
        raise RepairStateValidationError(
            "state['applied_history'] must be a list"
        )
    for i, entry in enumerate(history):
        if not isinstance(entry, dict):
            raise RepairStateValidationError(
                f"applied_history[{i}] must be a dict"
            )
        # Per the spec: each entry has group / committed_at /
        # applied_finding_ids / failed_finding_ids.
        for required in ("group", "committed_at", "applied_finding_ids",
                         "failed_finding_ids"):
            if required not in entry:
                raise RepairStateValidationError(
                    f"applied_history[{i}] missing required "
                    f"field {required!r}"
                )
        if not isinstance(entry["applied_finding_ids"], list):
            raise RepairStateValidationError(
                f"applied_history[{i}].applied_finding_ids must be a list"
            )
        if not isinstance(entry["failed_finding_ids"], list):
            raise RepairStateValidationError(
                f"applied_history[{i}].failed_finding_ids must be a list"
            )
