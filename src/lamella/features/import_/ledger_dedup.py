# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Cross-ledger dedup: drop import rows that already exist in the ledger.

For every row we'd post to account `A` on date `D` with amount `X`, check
whether the live ledger has a matching posting in the same account +/- 1
day with |amount| within $0.01. If so, mark that `classifications.status`
as `deduped` with a reason string so the emit pass skips it.

Scoping: matches are account-scoped — we don't flag a Checking deposit
against a coincidental Savings deposit of the same amount/date.

Accounts that are NOT yet open in the ledger can't be duplicates, so rows
whose target source_account is missing from the ledger are left alone
(they'll flag the import for review in `service.commit_import`).
"""
from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Open, Transaction

from lamella.core.beancount_io import LedgerReader
from lamella.features.import_.emit import sanitize_account, source_account

log = logging.getLogger(__name__)


@dataclass
class LedgerDedupResult:
    dropped: int = 0
    kept: int = 0
    missing_accounts: set[str] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.missing_accounts is None:
            self.missing_accounts = set()


def _as_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def _gather_ledger_postings(entries: list):
    """Build a dict (account, date) -> list[Decimal amount] for fast lookup."""
    postings: dict[tuple[str, date], list[Decimal]] = defaultdict(list)
    opens: set[str] = set()
    for entry in entries:
        if isinstance(entry, Open):
            opens.add(entry.account)
            continue
        if not isinstance(entry, Transaction):
            continue
        for posting in entry.postings:
            if posting.units is None or posting.units.number is None:
                continue
            try:
                amt = Decimal(str(posting.units.number))
            except Exception:
                continue
            postings[(posting.account, entry.date)].append(amt)
    return postings, opens


def _match(
    postings: dict[tuple[str, date], list[Decimal]],
    *,
    account: str,
    when: date,
    amount: Decimal,
    window_days: int = 1,
    tolerance: Decimal = Decimal("0.01"),
) -> bool:
    for offset in range(-window_days, window_days + 1):
        key = (account, when + timedelta(days=offset))
        for existing in postings.get(key, ()):
            if abs(existing - amount) <= tolerance:
                return True
            if abs(existing + amount) <= tolerance:
                # Beancount postings balance, so the counter leg appears
                # with flipped sign. Count that as a hit too.
                return True
    return False


def drop_duplicates(
    conn: sqlite3.Connection,
    *,
    import_id: int,
    reader: LedgerReader,
) -> LedgerDedupResult:
    """Mark classifications.status='deduped' for any raw_row whose tentative
    source posting collides with a ledger transaction within ±1 day.
    """
    ledger = reader.load()
    postings, opens = _gather_ledger_postings(ledger.entries)
    result = LedgerDedupResult()

    rows = conn.execute(
        """
        SELECT rr.id, rr.date, rr.amount, rr.payee, rr.description,
               rr.payment_method, rr.raw_json,
               s.source_class, s.path, s.entity AS source_entity,
               cat.entity AS cat_entity,
               COALESCE(cls.status, 'imported') AS status
          FROM raw_rows rr
          JOIN sources s ON s.id = rr.source_id
          LEFT JOIN categorizations cat ON cat.raw_row_id = rr.id
          LEFT JOIN classifications cls ON cls.raw_row_id = rr.id
         WHERE s.upload_id = ?
        """,
        (import_id,),
    ).fetchall()

    for row in rows:
        if row["status"] in ("deduped", "skipped", "zero"):
            continue
        when = _as_date(row["date"])
        if when is None or row["amount"] is None:
            result.kept += 1
            continue
        entity_hint = row["cat_entity"] or row["source_entity"]
        raw_account = source_account(
            row["source_class"],
            entity_hint,
            row["path"],
            row["payment_method"],
            row["raw_json"],
        )
        account = sanitize_account(raw_account)
        amount = Decimal(str(row["amount"]))
        if account not in opens:
            result.missing_accounts.add(account)
            result.kept += 1
            continue
        if _match(postings, account=account, when=when, amount=amount):
            conn.execute(
                """
                UPDATE classifications
                   SET status = 'deduped',
                       dedup_reason = ?,
                       decided_at = datetime('now')
                 WHERE raw_row_id = ?
                """,
                (f"matched live ledger at {account} ±1d", int(row["id"])),
            )
            conn.execute(
                """
                INSERT INTO classifications (raw_row_id, status, dedup_reason)
                     SELECT ?, 'deduped', ?
                      WHERE NOT EXISTS (
                          SELECT 1 FROM classifications WHERE raw_row_id = ?)
                """,
                (int(row["id"]), f"matched live ledger at {account} ±1d", int(row["id"])),
            )
            result.dropped += 1
        else:
            result.kept += 1
    log.info(
        "ledger_dedup import_id=%d dropped=%d kept=%d missing_accounts=%d",
        import_id, result.dropped, result.kept, len(result.missing_accounts),
    )
    return result
