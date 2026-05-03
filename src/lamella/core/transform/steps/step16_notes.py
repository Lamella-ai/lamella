# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 16: notes reconstruct from `custom "note"`.

Supersedes step7_note_coverage's "notes are ephemeral" decision. The
notes table is now state: every captured note and its AI-derived
hints survive a DB wipe. Tombstones via `custom "note-deleted" <id>`.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Iterable

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    custom_arg,
    custom_meta,
)
from lamella.core.transform.reconstruct import ReconstructReport, register

log = logging.getLogger(__name__)


def _str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def _bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def _read_notes(entries: Iterable[Any]) -> list[dict[str, Any]]:
    rows: dict[int, dict[str, Any]] = {}
    deleted: set[int] = set()
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type == "note-deleted":
            nid = _int(custom_arg(entry, 0))
            if nid is not None:
                deleted.add(nid)
                rows.pop(nid, None)
            continue
        if entry.type != "note":
            continue
        nid = _int(custom_arg(entry, 0))
        if nid is None or nid in deleted:
            continue
        body = _str(custom_meta(entry, "lamella-note-body"))
        if not body:
            continue
        rows[nid] = {
            "id": nid,
            "captured_at": entry.date.isoformat(),
            "body": body,
            "merchant_hint": _str(custom_meta(entry, "lamella-note-merchant-hint")),
            "entity_hint": _str(custom_meta(entry, "lamella-note-entity-hint")),
            "active_from": _str(custom_meta(entry, "lamella-note-active-from")),
            "active_to": _str(custom_meta(entry, "lamella-note-active-to")),
            "keywords": _str(custom_meta(entry, "lamella-note-keywords")),
            "card_override": _bool(custom_meta(entry, "lamella-note-card-override")),
            "status": _str(custom_meta(entry, "lamella-note-status")) or "open",
            "resolved_txn": _str(custom_meta(entry, "lamella-note-resolved-txn")),
            "resolved_receipt": _int(custom_meta(entry, "lamella-note-resolved-receipt")),
            "txn_hash": _str(custom_meta(entry, "lamella-note-txn-hash")),
        }
    return list(rows.values())


@register(
    "step16:notes",
    state_tables=["notes"],
)
def reconstruct_notes(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(notes)")]
    written = 0
    for row in _read_notes(entries):
        # Introspect schema so extra columns from 022_notes_active_window
        # are honored if present, ignored if not.
        values = {
            "id": row["id"],
            "captured_at": row["captured_at"],
            "body": row["body"],
            "entity_hint": row["entity_hint"],
            "merchant_hint": row["merchant_hint"],
            "resolved_txn": row["resolved_txn"],
            "resolved_receipt": row["resolved_receipt"],
            "status": row["status"],
            "active_from": row["active_from"],
            "active_to": row["active_to"],
            "keywords": row["keywords"],
            "card_override": 1 if row["card_override"] else 0 if row["card_override"] is not None else None,
            "txn_hash": row.get("txn_hash"),
        }
        cols_present = [k for k in values if k in cols]
        placeholders = ", ".join("?" for _ in cols_present)
        col_list = ", ".join(cols_present)
        conn.execute(
            f"INSERT OR REPLACE INTO notes ({col_list}) VALUES ({placeholders})",
            tuple(values[k] for k in cols_present),
        )
        written += 1
    return ReconstructReport(
        pass_name="step16:notes", rows_written=written,
        notes=[f"rebuilt {written} notes"] if written else [],
    )
