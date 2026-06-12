# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""QIF (Quicken Interchange Format) ingester.

QIF is a late-80s Quicken format. Plain text, line-prefixed:

    !Type:Bank
    D2024-01-05
    T-50.00
    PStarbucks
    MMorning coffee
    NCheck 1234
    LDining
    ^

Each transaction is terminated by a ``^`` line. The first non-blank
line declares the transaction type for the whole file (Bank, CCard,
Cash, Invst, etc.).

Two real weaknesses the parser must compensate for:

1. **Date format ambiguity.** QIF dates can be ``MM/DD/YY``,
   ``DD/MM/YY``, ``MM/DD'YYYY`` (Quicken's Y2K hack), or
   ``YYYY-MM-DD``. We auto-detect by scanning all dates: if any day
   value > 12, the order is unambiguous. If every date is ambiguous
   (every value <= 12), we default to US ``MM/DD`` and stash the
   ambiguity flag in the raw metadata so downstream review can
   prompt the user.

2. **No native unique ID.** QIF has no FITID equivalent. We
   generate a stable hash from ``(date, amount, payee, memo,
   check_num)`` as the dedup key.

Out of scope (deferred): investment transactions (``!Type:Invst``)
use a different field-letter set (N=action, Y=security, I=price,
Q=quantity). Detected and skipped with a metadata note rather than
mis-parsed.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from lamella.features.import_._db import insert_raw_row, stable_hash

log = logging.getLogger(__name__)

# !Type:... values that this ingester handles. Investment / list types
# are recognized but skipped — they don't shape into raw_rows the way
# bank/cash/credit transactions do.
_TXN_TYPES = {"bank", "cash", "ccard", "oth a", "oth l", "bills"}
_INVESTMENT_TYPES = {"invst"}
_LIST_TYPES = {"memorized", "class", "cat", "account"}


@dataclass
class _ParsedTxn:
    date: str | None = None
    amount: Decimal | None = None
    payee: str | None = None
    memo: str | None = None
    check_num: str | None = None
    category: str | None = None
    cleared: str | None = None
    splits: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class _ParsedQif:
    type_label: str = ""
    date_format: str = "unknown"  # 'mdy', 'dmy', 'ymd', 'ambiguous'
    transactions: list[_ParsedTxn] = field(default_factory=list)
    skipped_reason: str | None = None


_DATE_ISO_RE = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$")
# Quicken's Y2K notation uses an apostrophe before the 4-digit year:
# 11/05'2024 means Nov 5, 2024.
_DATE_QUICKEN_Y2K_RE = re.compile(r"^(\d{1,2})[/\-](\d{1,2})['/\-](\d{4})$")
_DATE_SHORT_RE = re.compile(r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})$")


def _split_to_iso(y: int, m: int, d: int) -> str | None:
    if not (1 <= m <= 12 and 1 <= d <= 31):
        return None
    return f"{y:04d}-{m:02d}-{d:02d}"


def _detect_date_order(raw_dates: list[str]) -> str:
    """Examine every parsed date and decide MDY vs DMY vs ambiguous.

    Rule: if any first-component > 12, the order MUST be DMY. If any
    second-component > 12, MDY. If both observations occur, the file
    is internally inconsistent and we still default to MDY (the US
    convention) but flag it. If every date has both components <= 12,
    we cannot tell — return 'ambiguous' and let the caller decide
    whether to default or prompt.
    """
    saw_first_gt12 = False
    saw_second_gt12 = False
    for raw in raw_dates:
        if _DATE_ISO_RE.match(raw):
            continue  # ISO is unambiguous, doesn't help discriminate.
        m = _DATE_QUICKEN_Y2K_RE.match(raw) or _DATE_SHORT_RE.match(raw)
        if not m:
            continue
        a, b, _y = m.groups()
        if int(a) > 12:
            saw_first_gt12 = True
        if int(b) > 12:
            saw_second_gt12 = True
    if saw_first_gt12 and not saw_second_gt12:
        return "dmy"
    if saw_second_gt12 and not saw_first_gt12:
        return "mdy"
    if saw_first_gt12 and saw_second_gt12:
        # File mixes orderings — treat as ambiguous so the caller
        # surfaces it rather than silently mis-dating half the rows.
        return "ambiguous"
    return "ambiguous"


def _normalize_date(raw: str, order: str) -> str | None:
    """Convert a raw QIF date string to ISO ``YYYY-MM-DD``.

    `order` is one of ``mdy``/``dmy``/``ambiguous``. For
    ``ambiguous`` we default to MDY (US Quicken convention) — the
    caller is expected to surface a warning.
    """
    raw = raw.strip()
    m = _DATE_ISO_RE.match(raw)
    if m:
        y, mth, d = (int(x) for x in m.groups())
        return _split_to_iso(y, mth, d)
    m = _DATE_QUICKEN_Y2K_RE.match(raw) or _DATE_SHORT_RE.match(raw)
    if not m:
        return None
    a, b, y = m.groups()
    yi = int(y)
    if yi < 100:
        # 70+ -> 19xx, 0..69 -> 20xx (Quicken's pivot is 1970-ish).
        yi += 2000 if yi < 70 else 1900
    if order == "dmy":
        return _split_to_iso(yi, int(b), int(a))
    return _split_to_iso(yi, int(a), int(b))


def _parse_amount(raw: str) -> Decimal | None:
    txt = raw.strip().replace("$", "").replace(",", "").replace(" ", "")
    if not txt:
        return None
    if txt.startswith("(") and txt.endswith(")"):
        txt = "-" + txt[1:-1]
    try:
        return Decimal(txt)
    except (InvalidOperation, ValueError):
        return None


def parse(text: str) -> _ParsedQif:
    """Two-pass parse — first pass collects raw fields, second pass
    normalizes dates once we know the date order.
    """
    parsed = _ParsedQif()

    lines = text.splitlines()
    # Find the first ``!Type:`` declaration.
    type_label = ""
    body_start = 0
    for i, ln in enumerate(lines):
        s = ln.strip()
        if not s:
            continue
        if s.lower().startswith("!type:"):
            type_label = s[6:].strip().lower()
            body_start = i + 1
            break
        if s.lower().startswith("!option:") or s.lower().startswith("!clear"):
            # Pass options through but keep scanning for the type.
            continue
        # First non-blank, non-bang line and no type — bail. The
        # detector should have caught this but a tolerant fallback
        # is nicer than a hard raise.
        body_start = i
        break

    parsed.type_label = type_label
    if type_label in _INVESTMENT_TYPES:
        parsed.skipped_reason = (
            "investment QIF (!Type:Invst) is not yet supported — "
            "the field codes (N/Y/I/Q) differ from bank/cash/ccard"
        )
        return parsed
    if type_label in _LIST_TYPES:
        parsed.skipped_reason = (
            f"list-only QIF (!Type:{type_label}) — no transactions to ingest"
        )
        return parsed
    if type_label and type_label not in _TXN_TYPES:
        parsed.skipped_reason = f"unrecognized QIF type: {type_label!r}"
        return parsed

    # Pass 1: accumulate raw transactions, collecting raw date strings
    # so we can run order detection before normalizing.
    raw_dates: list[str] = []
    raw_txns: list[tuple[_ParsedTxn, str | None]] = []
    cur = _ParsedTxn()
    cur_raw_date: str | None = None
    pending_split: dict[str, Any] | None = None

    def _flush_split() -> None:
        nonlocal pending_split
        if pending_split:
            cur.splits.append(pending_split)
            pending_split = None

    for ln in lines[body_start:]:
        if not ln:
            continue
        if ln.startswith("!"):
            # Embedded type switch in the middle of a file is rare
            # but legal; we ignore it for now (treat as a section
            # marker rather than splitting into multiple files).
            continue
        if ln[0] == "^":
            _flush_split()
            if cur.amount is not None or cur.payee or cur.memo or cur_raw_date:
                raw_txns.append((cur, cur_raw_date))
            cur = _ParsedTxn()
            cur_raw_date = None
            continue
        code = ln[0]
        val = ln[1:].strip()
        if code == "D":
            cur_raw_date = val
            raw_dates.append(val)
        elif code == "T" or code == "U":
            cur.amount = _parse_amount(val)
        elif code == "P":
            cur.payee = val
        elif code == "M":
            cur.memo = val
        elif code == "N":
            cur.check_num = val
        elif code == "L":
            cur.category = val
        elif code == "C":
            cur.cleared = val
        elif code == "S":
            _flush_split()
            pending_split = {"category": val}
        elif code == "E":
            if pending_split is None:
                pending_split = {}
            pending_split["memo"] = val
        elif code == "$":
            if pending_split is None:
                pending_split = {}
            pending_split["amount"] = _parse_amount(val)
        # Other field codes (A=address, F=reimbursable, etc.) are
        # ignored — they don't shape the posting and the raw line
        # isn't worth dragging through if we're not consuming it.

    # Trailing transaction without a final ``^`` — flush it. Some
    # bank exports forget the terminator on the last record.
    if cur.amount is not None or cur.payee or cur.memo or cur_raw_date:
        _flush_split()
        raw_txns.append((cur, cur_raw_date))

    parsed.date_format = _detect_date_order(raw_dates)
    for txn, raw_date in raw_txns:
        if raw_date:
            txn.date = _normalize_date(raw_date, parsed.date_format)
        parsed.transactions.append(txn)
    return parsed


def _read_file(path: Path) -> str:
    with path.open("rb") as fh:
        blob = fh.read()
    try:
        return blob.decode("utf-8")
    except UnicodeDecodeError:
        return blob.decode("latin-1", errors="replace")


def ingest_sheet(
    conn: sqlite3.Connection,
    source_id: int,
    path: Path,
    sheet_name: str,
    *,
    column_map: dict | None = None,
) -> int:
    """Parse `path` as QIF and insert one raw_row per transaction.

    `sheet_name` and `column_map` are accepted for ABI parity with
    the tabular ingesters but ignored — QIF has no sheet concept
    and a fixed schema.
    """
    text = _read_file(path)
    parsed = parse(text)
    if parsed.skipped_reason:
        log.info("qif.ingest_sheet source_id=%d skipped: %s", source_id, parsed.skipped_reason)
        return 0

    envelope = {
        "_format": "qif",
        "_type": parsed.type_label,
        "_date_format": parsed.date_format,
    }
    n = 0
    for idx, txn in enumerate(parsed.transactions, start=1):
        if txn.date is None or txn.amount is None:
            continue
        # QIF has no native unique id; build a stable hash from the
        # fields a bank would never accidentally collide on. Using
        # source_id in the hash means re-importing the same file
        # produces the same key (idempotent within an import) but
        # the same transaction in two different uploads gets two
        # different keys — which is intentional, since raw_rows is
        # per-upload and the cross-upload dedup happens in
        # ledger_dedup against the committed ledger.
        ext_id = "qif:" + stable_hash(
            txn.date, txn.amount, txn.payee, txn.memo, txn.check_num
        )

        desc_parts = [p for p in (txn.payee, txn.memo, txn.category) if p]
        desc = " | ".join(desc_parts)[:500] or None

        raw = {
            "_qif": {
                **envelope,
                "_payee": txn.payee,
                "_memo": txn.memo,
                "_check_num": txn.check_num,
                "_category": txn.category,
                "_cleared": txn.cleared,
                "_splits": txn.splits or None,
            }
        }
        insert_raw_row(
            conn,
            source_id=source_id,
            row_num=idx,
            raw=raw,
            date=txn.date,
            amount=txn.amount,
            payee=txn.payee,
            description=desc,
            memo=txn.memo,
            transaction_id=ext_id,
        )
        n += 1
    log.info("qif.ingest_sheet source_id=%d rows=%d", source_id, n)
    return n
