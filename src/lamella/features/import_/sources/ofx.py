# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""OFX / QFX / QBO ingester.

OFX (Open Financial Exchange) is the dominant US bank-export format.
Two on-disk shapes coexist:

  * OFX 1.x — SGML, header block then unclosed-tag content.
  * OFX 2.x — proper XML preceded by ``<?xml?>`` and ``<?OFX?>`` PIs.

QFX and QBO are Intuit-flavored OFX with an extra ``INTU.BID`` header
field. They're byte-compatible — same parser handles all three. The
caller distinguishes them by ``source_class`` only for traceability so
a re-import of the same export is recognizable in the imports list.

Strategy: rather than maintain two parsers (one SGML, one XML), this
module extracts fields with regular expressions. OFX's flat
transaction record (each ``<STMTTRN>...</STMTTRN>`` block contains a
short list of ``<TAG>VALUE`` pairs at the same depth) is regular
enough that regex extraction is both shorter and more tolerant of the
SGML "missing closing tag" edge cases than ElementTree on the
SGML-to-XML-rewritten variant.

Dedup: ``FITID`` per-account is the canonical OFX identifier and is
written to ``raw_rows.transaction_id``.

Out of scope (deferred to a phase 2): ``<INVTRAN>`` investment
transactions inside ``<INVSTMTMSGSRSV1>``. The parser ignores the
investment envelope entirely — bank + credit-card statements cover
the long tail that motivates this importer.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from lamella.features.import_._db import insert_raw_row

log = logging.getLogger(__name__)


# Tag-extraction patterns. Each one captures up to the next ``<`` or
# end-of-line, which works for both SGML 1.x (no closing tag) and XML
# 2.x (closing tag is just another ``<``).
def _tag_re(name: str) -> re.Pattern[str]:
    return re.compile(rf"<{name}>([^<\r\n]+)", re.I)


_RE_FITID = _tag_re("FITID")
_RE_DTPOSTED = _tag_re("DTPOSTED")
_RE_DTUSER = _tag_re("DTUSER")
_RE_TRNAMT = _tag_re("TRNAMT")
_RE_TRNTYPE = _tag_re("TRNTYPE")
_RE_NAME = _tag_re("NAME")
_RE_MEMO = _tag_re("MEMO")
_RE_CHECKNUM = _tag_re("CHECKNUM")
_RE_REFNUM = _tag_re("REFNUM")
_RE_PAYEE_NAME = re.compile(r"<PAYEE>.*?<NAME>([^<\r\n]+)", re.I | re.S)

# Envelope tags.
_RE_CURDEF = _tag_re("CURDEF")
_RE_BANKID = _tag_re("BANKID")
_RE_ACCTID = _tag_re("ACCTID")
_RE_CCACCTID = _tag_re("CCACCTID")
_RE_ACCTTYPE = _tag_re("ACCTTYPE")
_RE_BALAMT = _tag_re("BALAMT")
_RE_DTASOF = _tag_re("DTASOF")

# Transaction blocks live inside <BANKTRANLIST> for both bank and
# credit-card statements per the OFX spec — the credit-card envelope
# differs only in the wrapper. <STMTTRN> is the canonical record name.
_RE_STMTTRN_BLOCK = re.compile(r"<STMTTRN>(.*?)</STMTTRN>", re.I | re.S)
# Some legacy banks emit <CCSTMTTRN> for credit-card transactions.
_RE_CCSTMTTRN_BLOCK = re.compile(r"<CCSTMTTRN>(.*?)</CCSTMTTRN>", re.I | re.S)
# <LEDGERBAL> envelope — captured for future balance-assertion writes.
_RE_LEDGERBAL_BLOCK = re.compile(r"<LEDGERBAL>(.*?)</LEDGERBAL>", re.I | re.S)


@dataclass
class _ParsedOfx:
    currency: str = "USD"
    bank_id: str | None = None
    acct_id: str | None = None
    acct_type: str | None = None
    is_credit_card: bool = False
    balance_amount: Decimal | None = None
    balance_date: str | None = None
    transactions: list[dict[str, Any]] = field(default_factory=list)


def _strip_header(text: str) -> str:
    """Drop the OFX 1.x header block (everything before ``<OFX>``).

    For 2.x XML, the ``<?xml?>`` and ``<?OFX?>`` PIs are also above
    the root element — same treatment. Returns the string unchanged
    if no ``<OFX>`` is found, so a malformed file still parses what
    it can without raising.
    """
    m = re.search(r"<OFX\b", text, re.I)
    if not m:
        return text
    return text[m.start():]


def _first(pat: re.Pattern[str], text: str) -> str | None:
    m = pat.search(text)
    return m.group(1).strip() if m else None


def _parse_ofx_date(raw: str | None) -> str | None:
    """OFX dates are ``YYYYMMDDHHMMSS[.SSS][TZ:NAME]``. We only need
    the date portion; everything past the 8th character is ignored.
    """
    if not raw:
        return None
    digits = raw.strip()
    if len(digits) < 8 or not digits[:8].isdigit():
        return None
    y, mth, d = digits[0:4], digits[4:6], digits[6:8]
    try:
        return f"{int(y):04d}-{int(mth):02d}-{int(d):02d}"
    except ValueError:
        return None


def _parse_amount(raw: str | None) -> Decimal | None:
    if raw is None:
        return None
    txt = raw.strip().replace(",", "")
    if not txt:
        return None
    try:
        return Decimal(txt)
    except (InvalidOperation, ValueError):
        return None


def parse(text: str) -> _ParsedOfx:
    """Parse an OFX/QFX/QBO document into a flat envelope + txn list.

    Tolerant of trailing garbage, missing closing tags, missing
    fields. A malformed file produces a partial result rather than
    raising — the caller treats absence of transactions as an empty
    ingest, not an error.
    """
    body = _strip_header(text)
    parsed = _ParsedOfx()

    cur = _first(_RE_CURDEF, body)
    if cur:
        parsed.currency = cur.upper()
    parsed.bank_id = _first(_RE_BANKID, body)
    # CCACCTID wins over ACCTID when both are present — credit-card
    # envelope exposes the card number via CCACCTID.
    cc_acct = _first(_RE_CCACCTID, body)
    if cc_acct:
        parsed.acct_id = cc_acct
        parsed.is_credit_card = True
    else:
        parsed.acct_id = _first(_RE_ACCTID, body)
    parsed.acct_type = _first(_RE_ACCTTYPE, body)

    bal_block = _RE_LEDGERBAL_BLOCK.search(body)
    if bal_block:
        parsed.balance_amount = _parse_amount(_first(_RE_BALAMT, bal_block.group(1)))
        parsed.balance_date = _parse_ofx_date(_first(_RE_DTASOF, bal_block.group(1)))

    blocks = _RE_STMTTRN_BLOCK.findall(body) + _RE_CCSTMTTRN_BLOCK.findall(body)
    for block in blocks:
        fitid = _first(_RE_FITID, block)
        date = _parse_ofx_date(_first(_RE_DTPOSTED, block) or _first(_RE_DTUSER, block))
        amount = _parse_amount(_first(_RE_TRNAMT, block))
        if fitid is None and date is None and amount is None:
            # Empty/garbage block — skip silently.
            continue
        ttype = _first(_RE_TRNTYPE, block)
        # <PAYEE><NAME>X</NAME></PAYEE> wins over a bare <NAME> when
        # both happen to be present (some banks emit a structured
        # PAYEE block with the merchant name).
        payee = _first(_RE_PAYEE_NAME, block) or _first(_RE_NAME, block)
        memo = _first(_RE_MEMO, block)
        check = _first(_RE_CHECKNUM, block)
        ref = _first(_RE_REFNUM, block)
        parsed.transactions.append({
            "fitid": fitid,
            "date": date,
            "amount": amount,
            "type": ttype,
            "payee": payee,
            "memo": memo,
            "check_number": check,
            "ref_number": ref,
        })
    return parsed


def _read_file(path: Path) -> str:
    """Read the file as text. OFX files can be Latin-1, UTF-8, or
    Windows-1252 depending on the bank. Latin-1 is the safe
    superset — every byte decodes — and the markers we extract are
    all 7-bit ASCII so the worst case is mojibake in payee strings,
    which we still capture verbatim.
    """
    with path.open("rb") as fh:
        blob = fh.read()
    # Try UTF-8 first since most modern exports use it; fall back to
    # Latin-1 which never raises.
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
    """Parse `path` as OFX/QFX/QBO and insert one raw_row per <STMTTRN>.

    `sheet_name` and `column_map` are accepted for ABI parity with
    the other ingesters but ignored — OFX files have a single
    statement per envelope and a fixed schema.
    """
    text = _read_file(path)
    parsed = parse(text)
    n = 0

    envelope = {
        "_format": "ofx",
        "_currency": parsed.currency,
        "_bank_id": parsed.bank_id,
        "_acct_id": parsed.acct_id,
        "_acct_type": parsed.acct_type,
        "_is_credit_card": parsed.is_credit_card,
        "_ledger_balance": parsed.balance_amount,
        "_ledger_balance_date": parsed.balance_date,
    }

    for idx, txn in enumerate(parsed.transactions, start=1):
        fitid = txn["fitid"]
        date = txn["date"]
        amount = txn["amount"]
        if date is None or amount is None:
            # OFX guarantees both for valid transactions; skip rather
            # than emit a half-row that will fail downstream classify.
            continue
        # NAME and MEMO are wildly inconsistent across banks: some
        # use NAME for the merchant and MEMO for additional context;
        # others put everything in MEMO. We keep both fields raw
        # in the metadata and concatenate for the description so
        # downstream rules / AI see the full text.
        payee = txn["payee"] or None
        memo = txn["memo"] or None
        desc_parts = [p for p in (payee, memo, txn["type"]) if p]
        desc = " | ".join(desc_parts)[:500] or None

        raw = {
            "_ofx": {
                **envelope,
                "_fitid": fitid,
                "_trntype": txn["type"],
                "_check_number": txn["check_number"],
                "_ref_number": txn["ref_number"],
                "_name": payee,
                "_memo": memo,
            }
        }
        insert_raw_row(
            conn,
            source_id=source_id,
            row_num=idx,
            raw=raw,
            date=date,
            amount=amount,
            currency=parsed.currency,
            payee=payee,
            description=desc,
            memo=memo,
            transaction_id=fitid,
        )
        n += 1
    log.info("ofx.ingest_sheet source_id=%d rows=%d", source_id, n)
    return n
