# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timezone


@dataclass(frozen=True)
class NoteRow:
    id: int
    captured_at: datetime
    body: str
    entity_hint: str | None
    merchant_hint: str | None
    resolved_txn: str | None
    resolved_receipt: int | None
    status: str
    # Active-window fields (migration 022). active_from/active_to are
    # the inclusive date bounds during which the note is "in effect"
    # for classification. A note without these set falls back to a
    # single-day window on its captured_at date at query time.
    active_from: date | None = None
    active_to: date | None = None
    entity_scope: str | None = None    # null = global
    card_scope: str | None = None      # null = any card
    card_override: bool = False
    keywords: tuple[str, ...] = field(default_factory=tuple)
    # Migration 045 — per-txn memo. When set, the note is always
    # active context for that specific transaction's classify call,
    # regardless of date proximity or active-window settings.
    txn_hash: str | None = None


class NoteService:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create(
        self,
        body: str,
        *,
        entity_hint: str | None = None,
        merchant_hint: str | None = None,
        captured_at: datetime | None = None,
        active_from: date | None = None,
        active_to: date | None = None,
        entity_scope: str | None = None,
        card_scope: str | None = None,
        card_override: bool = False,
        keywords: tuple[str, ...] | list[str] | None = None,
        txn_hash: str | None = None,
    ) -> int:
        """Insert a note. ``captured_at`` defaults to the DB's
        CURRENT_TIMESTAMP when omitted. Active-window fields are
        optional; when absent, ``notes_active_on`` falls back to a
        single-day window on the captured_at date.

        ``txn_hash`` pins the note to a specific transaction — the
        classifier picks it up whenever that txn runs through
        ``build_classify_context``. Leave None for date-scoped or
        entity-scoped notes.
        """
        body = body.strip()
        if not body:
            raise ValueError("note body is empty")
        kws = json.dumps(list(keywords) if keywords else []) if keywords else None
        ts = captured_at.isoformat(sep=" ") if captured_at else None
        # Probe for the txn_hash column so the service stays back-compat
        # with an unmigrated DB (e.g. test fixtures built before 045).
        cols = [r[1] for r in self.conn.execute("PRAGMA table_info(notes)")]
        has_txn_hash = "txn_hash" in cols
        if ts is not None:
            if has_txn_hash:
                cursor = self.conn.execute(
                    """
                    INSERT INTO notes
                        (body, entity_hint, merchant_hint, captured_at,
                         active_from, active_to, entity_scope, card_scope,
                         card_override, keywords_json, txn_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        body, entity_hint, merchant_hint, ts,
                        active_from.isoformat() if active_from else None,
                        active_to.isoformat() if active_to else None,
                        entity_scope, card_scope,
                        1 if card_override else 0,
                        kws, txn_hash,
                    ),
                )
            else:
                cursor = self.conn.execute(
                    """
                    INSERT INTO notes
                        (body, entity_hint, merchant_hint, captured_at,
                         active_from, active_to, entity_scope, card_scope,
                         card_override, keywords_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        body, entity_hint, merchant_hint, ts,
                        active_from.isoformat() if active_from else None,
                        active_to.isoformat() if active_to else None,
                        entity_scope, card_scope,
                        1 if card_override else 0,
                        kws,
                    ),
                )
        else:
            if has_txn_hash:
                cursor = self.conn.execute(
                    """
                    INSERT INTO notes
                        (body, entity_hint, merchant_hint,
                         active_from, active_to, entity_scope, card_scope,
                         card_override, keywords_json, txn_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        body, entity_hint, merchant_hint,
                        active_from.isoformat() if active_from else None,
                        active_to.isoformat() if active_to else None,
                        entity_scope, card_scope,
                        1 if card_override else 0,
                        kws, txn_hash,
                    ),
                )
            else:
                cursor = self.conn.execute(
                    """
                    INSERT INTO notes
                        (body, entity_hint, merchant_hint,
                         active_from, active_to, entity_scope, card_scope,
                         card_override, keywords_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        body, entity_hint, merchant_hint,
                        active_from.isoformat() if active_from else None,
                        active_to.isoformat() if active_to else None,
                        entity_scope, card_scope,
                        1 if card_override else 0,
                        kws,
                    ),
                )
        return int(cursor.lastrowid)

    def list(self, *, limit: int = 50, status: str | None = None) -> list[NoteRow]:
        if status:
            rows = self.conn.execute(
                "SELECT * FROM notes WHERE status = ? ORDER BY captured_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM notes ORDER BY captured_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_row_to_note(r) for r in rows]

    def count_open(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM notes WHERE status = 'open'"
        ).fetchone()
        return int(row["n"] if row else 0)

    def get(self, note_id: int) -> NoteRow | None:
        row = self.conn.execute("SELECT * FROM notes WHERE id = ?", (note_id,)).fetchone()
        return _row_to_note(row) if row else None

    def update_hints(
        self,
        note_id: int,
        *,
        merchant_hint: str | None,
        entity_hint: str | None,
        active_from: date | None = None,
        active_to: date | None = None,
        keywords: tuple[str, ...] | list[str] | None = None,
        card_override: bool | None = None,
    ) -> bool:
        """Background AI parse writes inferred hints back. Existing
        user-provided hints are preserved (NULL columns only); active-
        window fields are written whenever the parse produced a
        concrete value."""
        kws = None
        if keywords is not None:
            kws = json.dumps(list(keywords))
        cursor = self.conn.execute(
            """
            UPDATE notes
               SET merchant_hint = COALESCE(merchant_hint, ?),
                   entity_hint   = COALESCE(entity_hint, ?),
                   active_from   = COALESCE(active_from, ?),
                   active_to     = COALESCE(active_to, ?),
                   keywords_json = COALESCE(?, keywords_json),
                   card_override = CASE WHEN ? IS NULL THEN card_override ELSE ? END
             WHERE id = ?
            """,
            (
                merchant_hint, entity_hint,
                active_from.isoformat() if active_from else None,
                active_to.isoformat() if active_to else None,
                kws,
                None if card_override is None else 1,
                None if card_override is None else (1 if card_override else 0),
                note_id,
            ),
        )
        return cursor.rowcount > 0

    # -- active-window queries ---------------------------------------

    def notes_active_on(
        self,
        txn_date: date | str,
        *,
        entity: str | None = None,
        card: str | None = None,
        proximity_days: int = 3,
        txn_hash: str | None = None,
    ) -> list[NoteRow]:
        """Return notes that give context to a transaction on ``txn_date``.

        A note is included if EITHER of these holds:

        1. The note carries an explicit ``active_from..active_to`` window
           (e.g., parse_note extracted "April 14 to April 20" from
           "in Atlanta for the trade show") AND ``txn_date`` falls
           inside it.
        2. The note was captured within ``±proximity_days`` of
           ``txn_date`` — the "what was happening around this time"
           context. Users jot notes throughout a trip or a period
           of interest; expecting every note to declare an explicit
           range is unrealistic.

        This returns **context**, not a scoping constraint. Notes are
        weighted priors the classifier consumes alongside similar-
        history, card binding, and the account whitelist (see
        ``feedback_rules_directional.md``). A note about a
        convention does not force a mortgage-payment auto-draft in
        the same window to misclassify — the AI sees the note and
        weighs it, and "autopay from WF Checking to Mortgage
        account" has overwhelming prior support to land where it
        always lands.

        ``entity`` filters: notes with a matching ``entity_scope``
        OR ``entity_scope IS NULL`` (global). ``None`` skips.
        ``card`` filters similarly against ``card_scope``.
        """
        iso = txn_date if isinstance(txn_date, str) else txn_date.isoformat()
        # Txn-scoped memos short-circuit the date logic — if the user
        # pinned a note to this specific transaction, that note is
        # always active context for it regardless of date.
        cols = [r[1] for r in self.conn.execute("PRAGMA table_info(notes)")]
        has_txn_hash_col = "txn_hash" in cols
        if has_txn_hash_col:
            sql = """
                SELECT * FROM notes
                 WHERE (
                     (txn_hash IS NOT NULL AND ? IS NOT NULL AND txn_hash = ?)
                     OR (
                         (active_from IS NOT NULL AND active_to IS NOT NULL
                          AND ? BETWEEN active_from AND active_to)
                         OR
                         ABS(julianday(substr(captured_at, 1, 10)) - julianday(?)) <= ?
                     )
                 )
            """
            params: list = [txn_hash, txn_hash, iso, iso, int(proximity_days)]
        else:
            sql = """
                SELECT * FROM notes
                 WHERE (
                     (active_from IS NOT NULL AND active_to IS NOT NULL
                      AND ? BETWEEN active_from AND active_to)
                     OR
                     ABS(julianday(substr(captured_at, 1, 10)) - julianday(?)) <= ?
                 )
            """
            params = [iso, iso, int(proximity_days)]
        if entity is not None:
            sql += " AND (entity_scope IS NULL OR entity_scope = ?)"
            params.append(entity)
        if card is not None:
            sql += " AND (card_scope IS NULL OR card_scope = ?)"
            params.append(card)
        sql += " ORDER BY captured_at DESC"
        rows = self.conn.execute(sql, params).fetchall()
        return [_row_to_note(r) for r in rows]


def _coerce_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _row_to_note(row: sqlite3.Row) -> NoteRow:
    captured = row["captured_at"]
    if isinstance(captured, str):
        try:
            captured = datetime.fromisoformat(captured)
        except ValueError:
            captured = datetime.now(timezone.utc)
    keys = row.keys() if hasattr(row, "keys") else []
    active_from = (
        _coerce_date(row["active_from"]) if "active_from" in keys else None
    )
    active_to = (
        _coerce_date(row["active_to"]) if "active_to" in keys else None
    )
    entity_scope = row["entity_scope"] if "entity_scope" in keys else None
    card_scope = row["card_scope"] if "card_scope" in keys else None
    card_override = bool(row["card_override"]) if "card_override" in keys else False
    kws_raw = row["keywords_json"] if "keywords_json" in keys else None
    keywords: tuple[str, ...] = ()
    if kws_raw:
        try:
            keywords = tuple(json.loads(kws_raw))
        except (ValueError, TypeError):
            keywords = ()
    txn_hash_val = row["txn_hash"] if "txn_hash" in keys else None
    return NoteRow(
        id=int(row["id"]),
        captured_at=captured,
        body=row["body"],
        entity_hint=row["entity_hint"],
        merchant_hint=row["merchant_hint"],
        resolved_txn=row["resolved_txn"],
        resolved_receipt=row["resolved_receipt"],
        status=row["status"],
        active_from=active_from,
        active_to=active_to,
        entity_scope=entity_scope,
        card_scope=card_scope,
        card_override=card_override,
        keywords=keywords,
        txn_hash=txn_hash_val,
    )
