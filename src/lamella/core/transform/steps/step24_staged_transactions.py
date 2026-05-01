# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 24: staged_transactions reconstruct from ``custom "staged-txn"``
directives (and their post-classification ``custom "staged-txn-promoted"``
counterparts).

Per ADR-0043 and ADR-0043b, an unclassified bank row lands in the ledger
as a ``custom "staged-txn"`` directive — no monetary posting, no balance
sheet impact, just a metadata-only audit anchor. The corresponding
``staged_transactions`` row in SQLite holds the source payload and review
state. This reconstruct step rebuilds that table when SQLite is wiped,
walking the ledger for both directive flavours and upserting one row per
directive.

ADR-0015 invariant: the staged_transactions row count for non-promoted
status must equal the count of ``custom "staged-txn"`` directives across
connector-owned files. Verified by tests.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from datetime import date
from typing import Any

from beancount.core.data import Custom

from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import (
    TablePolicy,
    register as register_policy,
)

log = logging.getLogger(__name__)

# Directive type names (the string after `custom`).
STAGED_TXN_TYPE = "staged-txn"
STAGED_TXN_PROMOTED_TYPE = "staged-txn-promoted"


def _meta_get(meta: dict | None, key: str) -> str | None:
    """Pull a meta value as a string, tolerating None and missing keys."""
    if not meta:
        return None
    val = meta.get(key)
    if val is None:
        return None
    return str(val)


def _ref_hash(source: str, source_ref_id: str) -> str:
    """Same source_ref_hash recipe staging uses: sha1(source + ":" +
    source_ref_id). Keeps reconstruct rows deduplicating against any
    pre-existing rows from a re-import or a prior reconstruct pass."""
    return hashlib.sha1(
        f"{source}:{source_ref_id}".encode("utf-8")
    ).hexdigest()


def _extract_directive_fields(entry: Custom) -> dict[str, Any] | None:
    """Pull the ADR-0043b fields out of a Custom directive's meta. Returns
    None for directives that are missing required fields — those land in
    the ReconstructReport notes for the operator to investigate, but
    don't block the rest of the rebuild."""
    if not isinstance(entry, Custom):
        return None
    if entry.type not in (STAGED_TXN_TYPE, STAGED_TXN_PROMOTED_TYPE):
        return None
    meta = entry.meta or {}
    txn_id = _meta_get(meta, "lamella-txn-id")
    # The directive header carries the source as a positional arg
    # (per ADR-0043b — works around the beancount pad-plugin
    # NoneType-iter bug). The meta value is also present; the
    # positional value wins when both are present and disagree
    # because it's the more visible / grep-able placement.
    positional_source: str | None = None
    if entry.values:
        for v in entry.values:
            if isinstance(v.value, str):
                positional_source = v.value
                break
    source = positional_source or _meta_get(meta, "lamella-source")
    source_ref_id = _meta_get(meta, "lamella-source-reference-id")
    txn_amount_raw = _meta_get(meta, "lamella-txn-amount")
    source_account = _meta_get(meta, "lamella-source-account")
    narration = _meta_get(meta, "lamella-txn-narration")
    if not (txn_id and source and source_ref_id and txn_amount_raw and source_account):
        return None
    # `lamella-txn-amount` is rendered as `<decimal> <currency>` per
    # ADR-0043b §3 (bare beancount Amount). We split on whitespace; if
    # the amount didn't survive parsing as an Amount, treat the meta
    # value as the textual fallback.
    amount_str = txn_amount_raw
    currency = "USD"
    if " " in txn_amount_raw:
        parts = txn_amount_raw.rsplit(" ", 1)
        if len(parts) == 2:
            amount_str, currency = parts[0].strip(), parts[1].strip()
    txn_date_raw = _meta_get(meta, "lamella-txn-date") or entry.date.isoformat()
    return {
        "lamella_txn_id": txn_id,
        "source": source,
        "source_ref_id": source_ref_id,
        "amount": amount_str,
        "currency": currency,
        "posting_date": txn_date_raw,
        "source_account": source_account,
        "narration": narration or "",
        "directive_date": entry.date,
        "is_promoted": entry.type == STAGED_TXN_PROMOTED_TYPE,
    }


@register(
    "step24:staged_transactions",
    state_tables=["staged_transactions"],
)
def reconstruct_staged_transactions(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    """Walk the ledger for ``custom "staged-txn"`` and
    ``custom "staged-txn-promoted"`` directives; upsert one row per
    directive into ``staged_transactions``."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(staged_transactions)")]
    if not cols:
        return ReconstructReport(
            pass_name="step24:staged_transactions", rows_written=0,
            notes=["staged_transactions table not present — skip"],
        )
    written = 0
    skipped_malformed = 0
    promoted_count = 0
    pending_count = 0
    for entry in entries:
        fields = _extract_directive_fields(entry)
        if fields is None:
            if isinstance(entry, Custom) and entry.type in (
                STAGED_TXN_TYPE, STAGED_TXN_PROMOTED_TYPE,
            ):
                skipped_malformed += 1
            continue
        # source_ref is a JSON envelope per the schema convention so
        # readers can extract the canonical id without parsing free
        # text. Mirror what the live importer writes.
        source_ref = json.dumps({"id": fields["source_ref_id"]})
        source_ref_hash = _ref_hash(fields["source"], fields["source_ref_id"])
        status = "promoted" if fields["is_promoted"] else "new"
        if fields["is_promoted"]:
            promoted_count += 1
        else:
            pending_count += 1
        # Pull promotion meta if present so reconstruct preserves the
        # audit trail end-to-end. These fields are tolerated as
        # missing on the unpromoted form.
        promoted_at = None
        if fields["is_promoted"]:
            promoted_at = _meta_get(entry.meta, "lamella-promoted-at")
        conn.execute(
            """
            INSERT INTO staged_transactions
                (source, source_ref, source_ref_hash, posting_date,
                 amount, currency, payee, description, raw_json,
                 status, promoted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (source, source_ref_hash) DO UPDATE SET
                posting_date = excluded.posting_date,
                amount       = excluded.amount,
                currency     = excluded.currency,
                payee        = excluded.payee,
                description  = excluded.description,
                status       = excluded.status,
                promoted_at  = COALESCE(excluded.promoted_at,
                                        staged_transactions.promoted_at),
                updated_at   = datetime('now')
            """,
            (
                fields["source"],
                source_ref,
                source_ref_hash,
                fields["posting_date"],
                fields["amount"],
                fields["currency"],
                fields["narration"],     # payee — we don't separate payee from narration in the directive
                fields["narration"],     # description mirrors narration
                source_ref,              # raw_json — for now the source_ref envelope is enough
                status,
                promoted_at,
            ),
        )
        written += 1
    notes: list[str] = []
    if written:
        notes.append(
            f"rebuilt {written} staged rows "
            f"({pending_count} pending + {promoted_count} promoted)"
        )
    if skipped_malformed:
        notes.append(
            f"skipped {skipped_malformed} malformed staged-txn directive(s) "
            "missing required ADR-0043b fields"
        )
    return ReconstructReport(
        pass_name="step24:staged_transactions", rows_written=written,
        notes=notes,
    )


def _allow_session_id_drift(live_rows, rebuilt_rows):
    """``session_id`` and the autoincrement ``id`` are runtime-assigned
    at ingest time and are not persisted in the directive shape per
    ADR-0043b. Reconstruct legitimately produces rows with
    ``session_id IS NULL`` and a fresh ``id``; live rows carry
    whichever values were minted at ingest. Tolerate that drift on
    the cache columns; everything else (source / source_ref_hash /
    posting_date / amount / status / promoted_at) is state and must
    match exactly."""
    tolerated = []
    by_key_live = {(r["source"], r["source_ref_hash"]): r for r in live_rows}
    by_key_rebuilt = {(r["source"], r["source_ref_hash"]): r for r in rebuilt_rows}
    for key, live in by_key_live.items():
        rebuilt = by_key_rebuilt.get(key)
        if rebuilt is None:
            continue
        # Only the cache columns are allowed to differ.
        cache_diff_fields = ("session_id", "id", "created_at", "updated_at")
        for col in cache_diff_fields:
            if live.get(col) != rebuilt.get(col):
                tolerated.append(
                    f"({key[0]}, {key[1][:8]}) {col} differs "
                    "(cache column — not encoded in directive)"
                )
    return tolerated


register_policy(TablePolicy(
    table="staged_transactions",
    kind="cache",
    primary_key=("source", "source_ref_hash"),
    allow_drift=_allow_session_id_drift,
))
