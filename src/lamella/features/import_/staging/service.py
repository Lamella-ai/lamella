# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Staging service — read/write/promote the unified staging tables.

All new intake sources go through this service. The goal is that
a single ``StagingService`` instance can accept a row from any
source (SimpleFIN, CSV, paste, reboot) and shepherd it through
the lifecycle ``new → classified → matched → promoted`` with
uniform semantics.

Lifecycle states:
  * **new**        — raw row landed, no classification attempted yet.
  * **classified** — rule/AI decided on an account (may still need
    review, depending on confidence).
  * **matched**    — part of a detected transfer or duplicate pair.
  * **promoted**   — written to a .bean file. Terminal.
  * **failed**     — write or bean-check failed and was rolled back.
  * **dismissed**  — user explicitly discarded the row.

Dedup is handled via ``(source, source_ref_hash)``: if the same
row is stage'd twice (e.g., two SimpleFIN fetches covering the
same window), the second call updates-in-place instead of
inserting a duplicate. Callers can also query by
``source_ref_hash`` to check presence before fetching.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

log = logging.getLogger(__name__)


# Allowed source tags. Expand with caution — adding one here means
# every downstream consumer should handle it.
SOURCES: frozenset[str] = frozenset(
    {"simplefin", "csv", "ods", "xlsx", "paste", "reboot"}
)

# Allowed lifecycle states for staged_transactions.status.
# ``likely_duplicate`` (ADR-0058) marks a row whose content matched an
# existing staged row OR ledger entry at intake time. These rows live
# off the main review queue; the user resolves them on
# ``/review/duplicates`` (confirm = dismissed-as-dup, release = back to
# 'new').
STATES: frozenset[str] = frozenset(
    {
        "new",
        "classified",
        "matched",
        "promoted",
        "failed",
        "dismissed",
        "likely_duplicate",
    }
)

# Confidence bands reused by StagedDecision.
CONFIDENCES: frozenset[str] = frozenset(
    {"high", "medium", "low", "unresolved"}
)


class StagingError(Exception):
    """Staging refused an operation (bad source, bad state transition, …)."""


# --- dataclasses ---------------------------------------------------------


@dataclass(frozen=True)
class StagedRow:
    id: int
    source: str
    source_ref: dict[str, Any]
    source_ref_hash: str
    session_id: str | None
    posting_date: str
    amount: Decimal
    currency: str
    payee: str | None
    description: str | None
    memo: str | None
    raw: dict[str, Any]
    status: str
    promoted_to_file: str | None
    promoted_txn_hash: str | None
    promoted_at: str | None
    created_at: str
    updated_at: str
    # Immutable identity (UUIDv7) per ADR-0019. Minted on insert; the
    # value is stamped on the eventual ledger entry's lamella-txn-id
    # meta when the row is promoted, so /txn/{token} resolves to the
    # same URL pre- and post-promotion.
    lamella_txn_id: str | None = None


@dataclass(frozen=True)
class StagedDecision:
    staged_id: int
    account: str | None
    confidence: str
    confidence_score: float | None
    decided_by: str
    rule_id: int | None
    ai_decision_id: int | None
    rationale: str | None
    needs_review: bool
    decided_at: str


@dataclass(frozen=True)
class StagedPair:
    id: int
    kind: str
    confidence: str
    a_staged_id: int
    b_staged_id: int | None
    b_ledger_hash: str | None
    reason: str | None
    created_at: str


# --- helpers -------------------------------------------------------------


def _canonical_ref_hash(source: str, ref: dict[str, Any]) -> str:
    """Stable hash over ``source`` + normalized JSON of ``ref``.

    Key ordering must be deterministic so the same logical row produces
    the same hash across fetches. Uses ``sort_keys=True`` and strict
    separators; values are coerced to strings to survive JSON type
    differences (int vs str) from different source schemas.
    """
    payload = json.dumps(
        {"source": source, "ref": ref},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha1(payload.encode("utf-8"), usedforsecurity=False).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _row_to_staged(row: sqlite3.Row) -> StagedRow:
    # Defensive: legacy connections that bypass core.db.migrate's
    # backfill won't have the column. The keys() check is one cheap
    # introspection, not per-column.
    keys = row.keys() if hasattr(row, "keys") else []
    lamella_txn_id = row["lamella_txn_id"] if "lamella_txn_id" in keys else None
    return StagedRow(
        id=int(row["id"]),
        source=row["source"],
        source_ref=json.loads(row["source_ref"]) if row["source_ref"] else {},
        source_ref_hash=row["source_ref_hash"],
        session_id=row["session_id"],
        posting_date=row["posting_date"],
        amount=Decimal(row["amount"]),
        currency=row["currency"] or "USD",
        payee=row["payee"],
        description=row["description"],
        memo=row["memo"],
        raw=json.loads(row["raw_json"]) if row["raw_json"] else {},
        status=row["status"],
        promoted_to_file=row["promoted_to_file"],
        promoted_txn_hash=row["promoted_txn_hash"],
        promoted_at=row["promoted_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        lamella_txn_id=lamella_txn_id,
    )


def _row_to_decision(row: sqlite3.Row) -> StagedDecision:
    return StagedDecision(
        staged_id=int(row["staged_id"]),
        account=row["account"],
        confidence=row["confidence"],
        confidence_score=(
            float(row["confidence_score"])
            if row["confidence_score"] is not None
            else None
        ),
        decided_by=row["decided_by"],
        rule_id=int(row["rule_id"]) if row["rule_id"] is not None else None,
        ai_decision_id=(
            int(row["ai_decision_id"])
            if row["ai_decision_id"] is not None
            else None
        ),
        rationale=row["rationale"],
        needs_review=bool(row["needs_review"]),
        decided_at=row["decided_at"],
    )


def _row_to_pair(row: sqlite3.Row) -> StagedPair:
    return StagedPair(
        id=int(row["id"]),
        kind=row["kind"],
        confidence=row["confidence"],
        a_staged_id=int(row["a_staged_id"]),
        b_staged_id=(
            int(row["b_staged_id"])
            if row["b_staged_id"] is not None
            else None
        ),
        b_ledger_hash=row["b_ledger_hash"],
        reason=row["reason"],
        created_at=row["created_at"],
    )


# --- service -------------------------------------------------------------


class StagingService:
    """Source-agnostic CRUD + lifecycle for staged transactions.

    Not async; staging writes happen inside ingest paths that already
    run sync against the single-writer SQLite.
    """

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    # -- create --------------------------------------------------------

    def stage(
        self,
        *,
        source: str,
        source_ref: dict[str, Any],
        posting_date: str,
        amount: Decimal | str | int | float,
        currency: str = "USD",
        payee: str | None = None,
        description: str | None = None,
        memo: str | None = None,
        raw: dict[str, Any] | None = None,
        session_id: str | None = None,
        lamella_txn_id: str | None = None,
        dedup_check: bool = False,
        ledger_reader=None,
    ) -> StagedRow:
        """Insert-or-update a staged row.

        If ``(source, source_ref)`` already exists, its mutable fields
        are refreshed and ``updated_at`` bumped — the row is not
        duplicated and its status is preserved. Returns the row as it
        now exists in the DB.

        ``lamella_txn_id`` is normally minted fresh on first sight.
        Reboot rows reference an existing ledger entry that already
        carries a ``lamella-txn-id`` meta; passing the entry's id
        here keeps /txn/{id} resolving to the same URL pre- and
        post-promotion (the override write doesn't change the entry's
        id, so leaving the staged row with a fresh-minted UUID would
        invalidate any bookmark made before promotion).

        ADR-0058 — when ``dedup_check=True`` the service consults the
        cross-source dedup oracle BEFORE the upsert. If the incoming
        ``(date, amount, description)`` triple matches an existing
        staged row (any source) or, when ``ledger_reader`` is given,
        a ledger ``Transaction``, the new row is staged with
        ``status='likely_duplicate'`` instead of ``'new'`` and a
        ``dedup_match`` block is recorded under ``raw_json``. The
        re-stage path (existing ``source_ref_hash``) leaves an
        already-existing row's status alone — the user's prior
        decision (confirm / release) is sticky.
        """
        if source not in SOURCES:
            raise StagingError(
                f"unknown source {source!r}; allowed: {sorted(SOURCES)}"
            )

        ref_hash = _canonical_ref_hash(source, source_ref)
        amount_text = str(
            amount if isinstance(amount, Decimal) else Decimal(str(amount))
        )

        # Pre-stage dedup oracle (ADR-0058). Skipped on re-stage of an
        # existing row — the upsert below preserves the existing
        # status, so re-running an ingest does not re-flip a row the
        # user has already confirmed/released.
        initial_status = "new"
        dedup_payload: dict[str, Any] | None = None
        inherited_txn_id: str | None = None
        if dedup_check:
            existing_id_row = self.conn.execute(
                "SELECT id FROM staged_transactions "
                "WHERE source = ? AND source_ref_hash = ?",
                (source, ref_hash),
            ).fetchone()
            if existing_id_row is None:
                # Late import to avoid a module-load cycle (oracle
                # imports content_fingerprint from intake, which
                # transitively touches the staging package).
                from lamella.features.import_.staging.dedup_oracle import (
                    find_match,
                )
                hit = find_match(
                    self.conn,
                    posting_date=posting_date,
                    amount=amount,
                    description=description,
                    payee=payee,
                    reader=ledger_reader,
                )
                if hit is not None:
                    initial_status = "likely_duplicate"
                    # Inherit the matched record's lamella-txn-id so
                    # multi-source observations of one event share one
                    # event identity. This is the foundation for the
                    # paired ``lamella-source-N`` / ``lamella-source-
                    # reference-id-N`` writeback (ADR-0019): when the
                    # user confirms "same event," the ledger entry's
                    # bank-side posting accumulates a new source pair
                    # at the next free index, and every staged row
                    # observing this event already shares its lineage.
                    inherited_txn_id = hit.matched_lamella_txn_id
                    dedup_payload = {
                        "kind": hit.kind,
                        "fingerprint": hit.fingerprint,
                        "matched_date": hit.matched_date,
                        "matched_description": hit.matched_description,
                        "confidence": hit.confidence,
                        "why": hit.why,
                        "matched_lamella_txn_id": (
                            hit.matched_lamella_txn_id
                        ),
                        "matched_account": hit.matched_account,
                        "staged_id": hit.staged_id,
                        "staged_source": hit.staged_source,
                        "txn_hash": hit.txn_hash,
                        "filename": hit.filename,
                        "lineno": hit.lineno,
                    }

        raw_with_dedup = dict(raw or {})
        if dedup_payload is not None:
            raw_with_dedup["dedup_match"] = dedup_payload
        raw_json = json.dumps(
            raw_with_dedup, default=str, sort_keys=True,
        )
        ref_json = json.dumps(source_ref, default=str, sort_keys=True)
        now = _now()
        # Use the caller-supplied id when present (reboot rows mirror
        # the existing ledger entry's lamella-txn-id), else mint fresh.
        # ON CONFLICT preserves whichever value is already on the row,
        # so repeat-stage stays bookmark-stable.
        from lamella.core.identity import mint_txn_id
        # Lineage precedence: caller-supplied id (reboot rows pass the
        # ledger entry's existing id) → oracle-inherited id (this row
        # observes a leg another source already saw) → fresh mint.
        # That order keeps multi-source observations on a single event
        # without overriding a deliberate caller pin.
        new_lamella_txn_id = (
            lamella_txn_id or inherited_txn_id or mint_txn_id()
        )

        cursor = self.conn.execute(
            """
            INSERT INTO staged_transactions (
                source, source_ref, source_ref_hash, session_id,
                posting_date, amount, currency, payee, description, memo,
                raw_json, status, lamella_txn_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, source_ref_hash) DO UPDATE SET
                session_id     = excluded.session_id,
                posting_date   = excluded.posting_date,
                amount         = excluded.amount,
                currency       = excluded.currency,
                payee          = excluded.payee,
                description    = excluded.description,
                memo           = excluded.memo,
                raw_json       = excluded.raw_json,
                lamella_txn_id = COALESCE(
                    staged_transactions.lamella_txn_id, excluded.lamella_txn_id
                ),
                updated_at     = excluded.updated_at
            RETURNING id
            """,
            (
                source, ref_json, ref_hash, session_id,
                posting_date, amount_text, currency, payee, description, memo,
                raw_json, initial_status, new_lamella_txn_id, now, now,
            ),
        )
        staged_id = int(cursor.fetchone()["id"])
        return self.get(staged_id)

    # -- read ----------------------------------------------------------

    def get(self, staged_id: int) -> StagedRow:
        row = self.conn.execute(
            "SELECT * FROM staged_transactions WHERE id = ?", (staged_id,)
        ).fetchone()
        if row is None:
            raise StagingError(f"no staged row with id {staged_id}")
        return _row_to_staged(row)

    def get_by_ref(
        self, source: str, source_ref: dict[str, Any]
    ) -> StagedRow | None:
        ref_hash = _canonical_ref_hash(source, source_ref)
        row = self.conn.execute(
            "SELECT * FROM staged_transactions "
            "WHERE source = ? AND source_ref_hash = ?",
            (source, ref_hash),
        ).fetchone()
        return _row_to_staged(row) if row else None

    def get_by_lamella_txn_id(self, lamella_txn_id: str) -> StagedRow | None:
        """Resolve a staged row by its immutable identity. Returns None
        if no row matches — including the post-promotion case where the
        same identity now lives on a ledger entry only. Callers that
        want "staged OR ledger" semantics should walk to the ledger
        next.
        """
        if not lamella_txn_id:
            return None
        row = self.conn.execute(
            "SELECT * FROM staged_transactions WHERE lamella_txn_id = ?",
            (lamella_txn_id,),
        ).fetchone()
        return _row_to_staged(row) if row else None

    def list_by_status(
        self, *statuses: str, source: str | None = None, limit: int = 500,
    ) -> list[StagedRow]:
        if not statuses:
            statuses = ("new", "classified", "matched")
        placeholders = ",".join("?" for _ in statuses)
        sql = (
            f"SELECT * FROM staged_transactions WHERE status IN ({placeholders})"
        )
        params: list[Any] = list(statuses)
        if source is not None:
            sql += " AND source = ?"
            params.append(source)
        sql += " ORDER BY posting_date DESC, id DESC LIMIT ?"
        params.append(limit)
        return [
            _row_to_staged(r) for r in self.conn.execute(sql, params).fetchall()
        ]

    def list_by_date_amount(
        self,
        *,
        posting_date: str,
        amount: Decimal | str,
        tolerance_days: int = 0,
    ) -> list[StagedRow]:
        """Pair-matching helper — find staged rows with the same amount
        whose posting_date is within ``tolerance_days`` of ``posting_date``.
        Used by the transfer matcher in Phase C; put here so the storage
        layer owns the index usage."""
        amt = str(amount) if isinstance(amount, Decimal) else str(Decimal(str(amount)))
        if tolerance_days <= 0:
            rows = self.conn.execute(
                "SELECT * FROM staged_transactions "
                "WHERE posting_date = ? AND amount = ?",
                (posting_date, amt),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM staged_transactions "
                "WHERE amount = ? AND "
                "      date(posting_date) BETWEEN date(?, ?) AND date(?, ?)",
                (
                    amt,
                    posting_date, f"-{tolerance_days} days",
                    posting_date, f"+{tolerance_days} days",
                ),
            ).fetchall()
        return [_row_to_staged(r) for r in rows]

    # -- decisions -----------------------------------------------------

    def record_decision(
        self,
        *,
        staged_id: int,
        account: str | None,
        confidence: str,
        decided_by: str,
        confidence_score: float | None = None,
        rule_id: int | None = None,
        ai_decision_id: int | None = None,
        rationale: str | None = None,
        needs_review: bool = False,
    ) -> StagedDecision:
        if confidence not in CONFIDENCES:
            raise StagingError(
                f"invalid confidence {confidence!r}; allowed: {sorted(CONFIDENCES)}"
            )
        # Upsert: one decision per staged row. Last write wins (re-classification
        # after a human override, for example).
        self.conn.execute(
            """
            INSERT INTO staged_decisions (
                staged_id, account, confidence, confidence_score, decided_by,
                rule_id, ai_decision_id, rationale, needs_review, decided_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(staged_id) DO UPDATE SET
                account          = excluded.account,
                confidence       = excluded.confidence,
                confidence_score = excluded.confidence_score,
                decided_by       = excluded.decided_by,
                rule_id          = excluded.rule_id,
                ai_decision_id   = excluded.ai_decision_id,
                rationale        = excluded.rationale,
                needs_review     = excluded.needs_review,
                decided_at       = excluded.decided_at
            """,
            (
                staged_id, account, confidence, confidence_score, decided_by,
                rule_id, ai_decision_id, rationale, 1 if needs_review else 0,
                _now(),
            ),
        )
        # Moving from 'new' to 'classified' is implicit: if the row was
        # still in 'new', classification advances the lifecycle.
        self.conn.execute(
            "UPDATE staged_transactions SET status = 'classified', updated_at = ? "
            "WHERE id = ? AND status = 'new'",
            (_now(), staged_id),
        )
        row = self.conn.execute(
            "SELECT * FROM staged_decisions WHERE staged_id = ?", (staged_id,)
        ).fetchone()
        return _row_to_decision(row)

    def get_decision(self, staged_id: int) -> StagedDecision | None:
        row = self.conn.execute(
            "SELECT * FROM staged_decisions WHERE staged_id = ?", (staged_id,)
        ).fetchone()
        return _row_to_decision(row) if row else None

    # -- pairs ---------------------------------------------------------

    def record_pair(
        self,
        *,
        kind: str,
        confidence: str,
        a_staged_id: int,
        b_staged_id: int | None = None,
        b_ledger_hash: str | None = None,
        reason: str | None = None,
    ) -> StagedPair:
        if kind not in {"transfer", "duplicate"}:
            raise StagingError(f"unknown pair kind {kind!r}")
        if confidence not in {"high", "medium", "low"}:
            raise StagingError(f"invalid pair confidence {confidence!r}")
        if b_staged_id is None and not b_ledger_hash:
            raise StagingError(
                "pair must reference either a second staged row or a ledger hash"
            )
        cursor = self.conn.execute(
            """
            INSERT INTO staged_pairs (
                kind, confidence, a_staged_id, b_staged_id, b_ledger_hash,
                reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (
                kind, confidence, a_staged_id, b_staged_id, b_ledger_hash,
                reason, _now(),
            ),
        )
        pair_id = int(cursor.fetchone()["id"])
        # Advance both sides to 'matched' if they're still early in lifecycle.
        self.conn.execute(
            "UPDATE staged_transactions SET status = 'matched', updated_at = ? "
            "WHERE id = ? AND status IN ('new', 'classified')",
            (_now(), a_staged_id),
        )
        if b_staged_id is not None:
            self.conn.execute(
                "UPDATE staged_transactions SET status = 'matched', updated_at = ? "
                "WHERE id = ? AND status IN ('new', 'classified')",
                (_now(), b_staged_id),
            )
        row = self.conn.execute(
            "SELECT * FROM staged_pairs WHERE id = ?", (pair_id,)
        ).fetchone()
        return _row_to_pair(row)

    def pairs_for(self, staged_id: int) -> list[StagedPair]:
        rows = self.conn.execute(
            "SELECT * FROM staged_pairs "
            "WHERE a_staged_id = ? OR b_staged_id = ? "
            "ORDER BY id ASC",
            (staged_id, staged_id),
        ).fetchall()
        return [_row_to_pair(r) for r in rows]

    # -- lifecycle -----------------------------------------------------

    def mark_promoted(
        self,
        staged_id: int,
        *,
        promoted_to_file: str,
        promoted_txn_hash: str | None = None,
    ) -> StagedRow:
        """Mark a staged row as written to Beancount. Terminal state."""
        now = _now()
        cursor = self.conn.execute(
            "UPDATE staged_transactions SET "
            "  status = 'promoted', "
            "  promoted_to_file = ?, "
            "  promoted_txn_hash = ?, "
            "  promoted_at = ?, "
            "  updated_at = ? "
            "WHERE id = ?",
            (promoted_to_file, promoted_txn_hash, now, now, staged_id),
        )
        if cursor.rowcount == 0:
            raise StagingError(f"no staged row with id {staged_id}")
        return self.get(staged_id)

    def mark_failed(self, staged_id: int, *, reason: str) -> StagedRow:
        now = _now()
        self.conn.execute(
            "UPDATE staged_transactions SET status = 'failed', updated_at = ? "
            "WHERE id = ?",
            (now, staged_id),
        )
        # Stamp the reason into the decision rationale for auditability.
        existing = self.get_decision(staged_id)
        if existing is not None:
            self.conn.execute(
                "UPDATE staged_decisions SET rationale = ?, decided_at = ? "
                "WHERE staged_id = ?",
                (
                    (existing.rationale + " | " if existing.rationale else "")
                    + f"FAILED: {reason}",
                    now,
                    staged_id,
                ),
            )
        return self.get(staged_id)

    def dismiss(self, staged_id: int, *, reason: str | None = None) -> StagedRow:
        """Mark the row as **ignored** — soft-state, fully reversible.

        Sets ``status = 'dismissed'`` (the underlying column name is
        kept for migration/compat reasons; the user-facing word is
        "Ignored"). The row stays in the database forever — never
        auto-deleted — so reconciling against a bank statement later
        can still see "yes I knew about this, I told it to skip."

        Use :meth:`restore` to bring an ignored row back to
        ``pending`` if the user changes their mind.
        """
        self.conn.execute(
            "UPDATE staged_transactions SET status = 'dismissed', updated_at = ? "
            "WHERE id = ?",
            (_now(), staged_id),
        )
        if reason:
            existing = self.get_decision(staged_id)
            self.conn.execute(
                """
                INSERT INTO staged_decisions
                    (staged_id, confidence, decided_by, rationale, decided_at)
                VALUES (?, 'unresolved', 'human', ?, ?)
                ON CONFLICT(staged_id) DO UPDATE SET
                    rationale = excluded.rationale,
                    decided_at = excluded.decided_at
                """,
                (staged_id, f"Ignored: {reason}", _now()),
            )
        return self.get(staged_id)

    def restore(self, staged_id: int) -> StagedRow:
        """Flip an ignored row back to ``pending`` so it re-enters the
        review queue. Reverses :meth:`dismiss`. Idempotent — if the
        row isn't currently ``dismissed`` (e.g. already promoted),
        leaves status as-is and returns the row unchanged."""
        self.conn.execute(
            "UPDATE staged_transactions "
            "   SET status = 'new', updated_at = ? "
            " WHERE id = ? AND status = 'dismissed'",
            (_now(), staged_id),
        )
        # Stamp the restoration into the decision rationale for
        # auditability — same shape as dismiss(reason=...) so the
        # /review row reflects the toggle.
        existing = self.get_decision(staged_id)
        if existing is not None:
            self.conn.execute(
                """
                UPDATE staged_decisions
                   SET rationale = ?,
                       decided_at = ?
                 WHERE staged_id = ?
                """,
                (
                    (existing.rationale + " | " if existing.rationale else "")
                    + "Restored from ignored",
                    _now(),
                    staged_id,
                ),
            )
        return self.get(staged_id)

    # -- maintenance ---------------------------------------------------

    def cleanup_terminal(
        self, *, older_than_days: int = 30, limit: int = 1000
    ) -> int:
        """Delete **promoted** rows older than ``older_than_days``.

        Promoted rows have a ledger transaction as their durable
        backup, so removing the staging-side breadcrumb after 30 days
        is safe — the ledger keeps the record. Dismissed (ignored)
        rows are intentionally NEVER auto-deleted: they're the
        provenance for "I deliberately skipped this line on the bank
        statement," and reconciliation against a future statement
        balance needs them.

        Returns the count deleted.
        """
        cursor = self.conn.execute(
            "DELETE FROM staged_transactions "
            "WHERE status = 'promoted' "
            "  AND updated_at < datetime('now', ?) "
            "  AND id IN ("
            "    SELECT id FROM staged_transactions "
            "    WHERE status = 'promoted' "
            "      AND updated_at < datetime('now', ?) "
            "    LIMIT ?"
            "  )",
            (f"-{older_than_days} days", f"-{older_than_days} days", limit),
        )
        return cursor.rowcount or 0
