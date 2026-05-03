# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.features.reports._pdf import render_html, render_pdf
from lamella.features.reports.line_map import LineMap
from lamella.features.reports.receipt_fetcher import FetchedReceipt, fetch_for_audit
from lamella.features.reports.schedule_c import build_schedule_c

log = logging.getLogger(__name__)


@dataclass
class TransactionRow:
    line: int | str
    line_description: str
    date: date
    account: str
    amount: Decimal
    narration: str
    note_body: str | None
    paperless_id: int | None
    receipt_data_url: str | None
    receipt_link: str | None


def _entity_of(account: str) -> str | None:
    parts = account.split(":")
    if len(parts) < 2:
        return None
    if parts[0] not in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
        return None
    return parts[1]


def _receipt_for_txn(
    conn: sqlite3.Connection,
    hash_: str,
    *,
    fallback_hashes: tuple[str, ...] = (),
    txn_date: date | None = None,
    txn_amount: Decimal | None = None,
) -> int | None:
    """Find a receipt link for this transaction.

    ``document_links.txn_hash`` is the Beancount content hash, which
    changes whenever the txn's postings or narration change. There
    are two drift shapes the audit PDF needs to recover from:

    1. ``lamella-override-of`` chain — when a user reclassifies a
       FIXME, an override block is stamped with
       ``lamella-override-of: "<original-hash>"`` and the override
       entry has a NEW hash. The receipt was attached to the ORIGINAL
       hash. Caller passes the override chain via ``fallback_hashes``.
    2. In-place rewrite or staged-txn promotion — same entry edited,
       no override-of meta to chain through. Caller passes
       ``txn_date`` + ``txn_amount`` for a (date, amount) fuzzy
       fallback against ``document_links.txn_date`` + ``txn_amount``.
       Falls back ONLY when the exact-hash + chain lookup misses,
       and only matches when exactly one receipt sits at that
       (date, amount) coordinate, so we never silently swap a
       receipt onto the wrong txn.
    """
    candidates = (hash_, *fallback_hashes)
    placeholders = ",".join("?" * len(candidates))
    row = conn.execute(
        f"SELECT paperless_id FROM document_links "
        f"WHERE txn_hash IN ({placeholders}) ORDER BY id ASC LIMIT 1",
        candidates,
    ).fetchone()
    if row:
        return int(row["paperless_id"])
    if txn_date is None or txn_amount is None:
        return None
    # Fuzzy fallback. ``txn_amount`` is stored as a TEXT column with
    # the canonical Decimal string (ADR-0022); compare via CAST to
    # tolerate ``"10"`` vs ``"10.00"`` round-trips. Match on the
    # absolute value because some link rows stored the cashflow sign
    # flipped (older bug). Only return a hit when exactly one row
    # matches; ambiguous coordinates fall through to "no link found"
    # rather than guessing.
    fuzzy = conn.execute(
        "SELECT paperless_id FROM document_links "
        "WHERE txn_date = ? "
        "  AND ABS(CAST(txn_amount AS REAL) - ?) < 0.005",
        (txn_date.isoformat(), float(abs(txn_amount))),
    ).fetchall()
    if len(fuzzy) == 1:
        return int(fuzzy[0]["paperless_id"])
    return None


def _override_chain(entry) -> tuple[str, ...]:
    """Walk ``lamella-override-of`` meta to collect every prior hash
    this entry has been an override of. Used as fallback identities
    for receipt-link / note lookups so reclassified txns don't lose
    their attached receipts."""
    chain: list[str] = []
    seen: set[str] = set()
    meta = getattr(entry, "meta", None) or {}
    cursor = meta.get("lamella-override-of")
    while isinstance(cursor, str) and cursor and cursor not in seen:
        chain.append(cursor)
        seen.add(cursor)
        # If we walk beyond the first override, we'd need the FULL
        # entries list to keep chasing. The first link covers the
        # common case (receipt linked to the original FIXME txn,
        # override created on classify). Multi-step override-of
        # chains are rare.
        break
    return tuple(chain)


def _note_for_txn(
    conn: sqlite3.Connection,
    hash_: str,
    *,
    fallback_hashes: tuple[str, ...] = (),
) -> str | None:
    candidates = (hash_, *fallback_hashes)
    placeholders = ",".join("?" * len(candidates))
    row = conn.execute(
        f"SELECT body FROM notes WHERE resolved_txn IN ({placeholders}) "
        f"ORDER BY id ASC LIMIT 1",
        candidates,
    ).fetchone()
    return row["body"] if row else None


def collect_transactions(
    *,
    entity: str,
    year: int,
    entries: Iterable,
    line_map: LineMap,
    conn: sqlite3.Connection | None = None,
) -> list[TransactionRow]:
    materialized = list(entries)
    rows: list[TransactionRow] = []
    for entry in materialized:
        if not isinstance(entry, Transaction):
            continue
        if entry.date.year != year:
            continue
        for posting in entry.postings:
            if not posting.account.startswith("Expenses:"):
                continue
            if _entity_of(posting.account) != entity:
                continue
            units = posting.units
            if units is None or units.number is None:
                continue
            if units.currency and units.currency != "USD":
                continue
            classification = line_map.classify(posting.account)
            if classification is None:
                continue
            hash_ = txn_hash(entry)
            # Receipt links are keyed on the BEANCOUNT CONTENT HASH,
            # which changes when the txn gets reclassified (override
            # block stamped with lamella-override-of). Pass the
            # override-of chain as fallback identities so a
            # reclassified txn still finds the receipt that was
            # attached to its original FIXME form.
            chain = _override_chain(entry)
            paperless_id = (
                _receipt_for_txn(conn, hash_, fallback_hashes=chain)
                if conn is not None else None
            )
            note_body = (
                _note_for_txn(conn, hash_, fallback_hashes=chain)
                if conn is not None else None
            )
            rows.append(
                TransactionRow(
                    line=classification.line,
                    line_description=classification.description,
                    date=entry.date,
                    account=posting.account,
                    amount=Decimal(units.number),
                    narration=entry.narration or "",
                    note_body=note_body,
                    paperless_id=paperless_id,
                    receipt_data_url=None,
                    receipt_link=None,
                )
            )
    rows.sort(key=lambda r: (_sort_key(r.line), r.date, r.account))
    return rows


def _sort_key(line):
    if isinstance(line, int):
        return (0, line)
    try:
        return (0, int(str(line).strip()))
    except ValueError:
        return (1, str(line))


async def attach_receipts(
    rows: list[TransactionRow],
    *,
    paperless_client,
    max_bytes: int,
) -> None:
    """For every row that has a paperless_id, fetch and embed (or fall
    back to a link). Mutates rows in place."""
    if paperless_client is None:
        return
    for row in rows:
        if row.paperless_id is None:
            continue
        result: FetchedReceipt = await fetch_for_audit(
            paperless_client,
            paperless_id=row.paperless_id,
            max_bytes=max_bytes,
        )
        row.receipt_data_url = result.data_url
        row.receipt_link = result.fallback_link


def render_audit_html(
    *,
    entity: str,
    year: int,
    entries: Iterable,
    line_map: LineMap,
    rows: list[TransactionRow],
) -> str:
    report = build_schedule_c(
        entity=entity, year=year, entries=list(entries), line_map=line_map,
    )
    summary = report.summary
    line_count = len(summary)
    total = sum((row.amount for row in summary), Decimal("0"))
    return render_html(
        "audit_portfolio.html",
        entity=entity,
        year=year,
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        summary=summary,
        total=total,
        line_count=line_count,
        txn_count=len(rows),
        transactions=rows,
    )


async def render_audit_pdf(
    *,
    entity: str,
    year: int,
    entries: Iterable,
    line_map: LineMap,
    conn: sqlite3.Connection,
    paperless_client,
    max_receipt_bytes: int,
) -> bytes:
    rows = collect_transactions(
        entity=entity, year=year, entries=entries, line_map=line_map, conn=conn,
    )
    await attach_receipts(rows, paperless_client=paperless_client, max_bytes=max_receipt_bytes)
    html = render_audit_html(
        entity=entity, year=year, entries=entries, line_map=line_map, rows=rows,
    )
    return render_pdf(html)
