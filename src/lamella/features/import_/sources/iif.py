# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""IIF (Intuit Interchange Format) ingester.

QuickBooks Desktop's tab-separated, header-row-driven import format.
Multi-section: a single file can interleave chart-of-accounts rows
(``ACCNT``), customer rows (``CUST``), vendor rows (``VEND``), and
transactions (``TRNS``/``SPL``/``ENDTRNS``). Each section's column
order is declared by a leading ``!HEADER`` row.

Transaction shape: a ``TRNS`` row is the parent (total amount,
primary account); one or more ``SPL`` rows are the offsetting
splits (already double-entry); ``ENDTRNS`` terminates the group.
This is the one source format whose on-disk model maps cleanly to
the staging model — IIF *is* a posting list.

The format is simple enough that there's no maintained Python
library worth pulling in. We parse with the standard library: the
``csv`` module with ``delimiter='\t'`` plus a small state machine
that dispatches on the first column of each row.

Out of scope: the ``ACCNT``, ``CUST``, and ``VEND`` sections are
*read* and stashed in the raw envelope so a future "import
QuickBooks chart of accounts" feature can pick them up, but they
do not produce raw_rows — only ``TRNS``/``SPL`` blocks do.
"""
from __future__ import annotations

import csv
import io
import logging
import sqlite3
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from lamella.features.import_._db import insert_raw_row, stable_hash

log = logging.getLogger(__name__)


@dataclass
class _Split:
    fields: dict[str, str] = field(default_factory=dict)


@dataclass
class _Transaction:
    fields: dict[str, str] = field(default_factory=dict)
    splits: list[_Split] = field(default_factory=list)


@dataclass
class _ParsedIif:
    transactions: list[_Transaction] = field(default_factory=list)
    accounts: list[dict[str, str]] = field(default_factory=list)
    customers: list[dict[str, str]] = field(default_factory=list)
    vendors: list[dict[str, str]] = field(default_factory=list)


def _zip_row(headers: list[str], row: list[str]) -> dict[str, str]:
    """Pair a data row with its declared headers, dropping the row's
    leading record-type token (``TRNS`` etc.) so the resulting dict
    has no synthetic key.
    """
    # row[0] is the record type token; headers[0] is its tag name
    # (e.g. ``!TRNS`` -> ``TRNS``). Both are dropped from the dict.
    fields: dict[str, str] = {}
    for i, name in enumerate(headers):
        if i == 0:
            continue
        if i >= len(row):
            break
        fields[name] = row[i]
    return fields


def _parse_amount(raw: str | None) -> Decimal | None:
    if raw is None:
        return None
    txt = raw.strip().replace("$", "").replace(",", "").replace(" ", "")
    if not txt:
        return None
    if txt.startswith("(") and txt.endswith(")"):
        txt = "-" + txt[1:-1]
    try:
        return Decimal(txt)
    except (InvalidOperation, ValueError):
        return None


def _parse_iif_date(raw: str | None) -> str | None:
    """IIF dates are typically ``MM/DD/YYYY`` (US locale). Support
    YY-shorthand and ISO too for files that have been touched up.
    """
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None
    # ISO first.
    if len(s) >= 8 and s[4] == "-" and s[7] == "-":
        try:
            y, m, d = s[:10].split("-")
            return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        except (ValueError, IndexError):
            pass
    # M/D/Y or M-D-Y.
    parts: list[str] = []
    for sep in ("/", "-"):
        if sep in s:
            parts = s.split(sep)
            break
    if len(parts) == 3:
        try:
            m, d, y = (int(p) for p in parts)
        except ValueError:
            return None
        if y < 100:
            y += 2000 if y < 70 else 1900
        if 1 <= m <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{m:02d}-{d:02d}"
    return None


def parse(text: str) -> _ParsedIif:
    """State-machine parse. The leading ``!``-token of each header
    row sets the active schema for subsequent matching data rows.
    """
    parsed = _ParsedIif()
    # Active header schemas, keyed by record-type token.
    headers: dict[str, list[str]] = {}
    cur_txn: _Transaction | None = None

    reader = csv.reader(io.StringIO(text), delimiter="\t", quoting=csv.QUOTE_NONE)
    for row in reader:
        if not row:
            continue
        # csv may produce an empty trailing column from a trailing tab.
        while row and row[-1] == "":
            row.pop()
        if not row:
            continue
        marker = row[0]
        if marker.startswith("!"):
            # Header row — strip the leading ``!`` to derive the
            # record-type key, then stash the column names for
            # future data rows of that type.
            tag = marker[1:].upper()
            # The ``!`` is on the type token only; the rest of the
            # row is the column-name list. We retain the type token
            # at index 0 for parity with the data rows so _zip_row
            # can drop both consistently.
            headers[tag] = [marker] + list(row[1:])
            continue

        rtype = marker.upper()
        cols = headers.get(rtype)
        if cols is None:
            # Data row without a preceding header — skip silently.
            # Exporting tools occasionally emit blank delimiter rows
            # before re-declaring headers, and we shouldn't blow up.
            continue
        fields = _zip_row(cols, row)
        if rtype == "TRNS":
            # New transaction — close any orphan one first. Some
            # exports drop ENDTRNS on the last record.
            if cur_txn is not None:
                parsed.transactions.append(cur_txn)
            cur_txn = _Transaction(fields=fields)
        elif rtype == "SPL":
            if cur_txn is None:
                # Stray SPL — wrap it in a synthetic txn so we don't
                # lose the data; downstream will see it as a single-
                # leg posting and likely route to review.
                cur_txn = _Transaction()
            cur_txn.splits.append(_Split(fields=fields))
        elif rtype == "ENDTRNS":
            if cur_txn is not None:
                parsed.transactions.append(cur_txn)
                cur_txn = None
        elif rtype == "ACCNT":
            parsed.accounts.append(fields)
        elif rtype == "CUST":
            parsed.customers.append(fields)
        elif rtype == "VEND":
            parsed.vendors.append(fields)
        # Other record types (CLASS, INVITEM, BUD, etc.) are ignored.

    # Flush a trailing transaction missing its ENDTRNS.
    if cur_txn is not None:
        parsed.transactions.append(cur_txn)
    return parsed


def _read_file(path: Path) -> str:
    """IIF is canonically Windows-1252 (QuickBooks Desktop's locale).
    Try UTF-8 then fall back to cp1252 then Latin-1 — Latin-1 always
    decodes so the read can't raise.
    """
    with path.open("rb") as fh:
        blob = fh.read()
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            return blob.decode(enc)
        except UnicodeDecodeError:
            continue
    return blob.decode("latin-1", errors="replace")


def ingest_sheet(
    conn: sqlite3.Connection,
    source_id: int,
    path: Path,
    sheet_name: str,
    *,
    column_map: dict | None = None,
) -> int:
    """Parse `path` as IIF and insert one raw_row per transaction.

    Each ``TRNS`` (with its trailing ``SPL`` rows) becomes one
    raw_row. The split list is preserved in the raw envelope so the
    downstream categorizer / posting builder can choose to honor
    the IIF-supplied legs verbatim instead of re-categorizing —
    that's the one path where the source data already encodes the
    user's intended postings.
    """
    text = _read_file(path)
    parsed = parse(text)

    n = 0
    for idx, txn in enumerate(parsed.transactions, start=1):
        f = txn.fields
        date = _parse_iif_date(f.get("DATE"))
        amount = _parse_amount(f.get("AMOUNT"))
        if date is None and amount is None:
            continue
        trnsid = f.get("TRNSID") or None
        payee = f.get("NAME") or None
        memo = f.get("MEMO") or None
        docnum = f.get("DOCNUM") or None
        account = f.get("ACCNT") or None
        cls = f.get("CLASS") or None

        # External id: TRNSID when QuickBooks exported one (live
        # files always do); fall back to a content hash so files
        # hand-crafted in spreadsheet apps still dedup.
        if trnsid:
            ext_id = f"iif:{trnsid}"
        else:
            ext_id = "iif:" + stable_hash(date, amount, payee, account, docnum)

        split_payload = []
        for s in txn.splits:
            split_payload.append({
                "account": s.fields.get("ACCNT"),
                "amount": _parse_amount(s.fields.get("AMOUNT")),
                "memo": s.fields.get("MEMO"),
                "class": s.fields.get("CLASS"),
                "name": s.fields.get("NAME"),
            })

        desc_parts = [p for p in (payee, memo, docnum) if p]
        desc = " | ".join(desc_parts)[:500] or None

        raw = {
            "_iif": {
                "_format": "iif",
                "_trnsid": trnsid,
                "_account": account,
                "_class": cls,
                "_docnum": docnum,
                "_payee": payee,
                "_memo": memo,
                "_splits": split_payload or None,
            }
        }
        insert_raw_row(
            conn,
            source_id=source_id,
            row_num=idx,
            raw=raw,
            date=date,
            amount=amount,
            payee=payee,
            description=desc,
            memo=memo,
            transaction_id=ext_id,
        )
        n += 1
    log.info("iif.ingest_sheet source_id=%d rows=%d txn_blocks=%d",
             source_id, n, len(parsed.transactions))
    return n
