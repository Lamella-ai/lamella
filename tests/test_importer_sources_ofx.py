# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""OFX / QFX / QBO ingester tests.

Covers both on-disk shapes (1.x SGML with unclosed tags, 2.x XML with
proper close tags), the Intuit ``INTU.BID`` variant for QFX, the
credit-card envelope (``CCACCTID``), the ``LEDGERBAL`` capture, and
the FITID-as-transaction-id contract.
"""
from __future__ import annotations

from pathlib import Path

from lamella.features.import_ import _structured
from lamella.features.import_._db import upsert_source
from lamella.features.import_.sources import ofx


OFX_1X_BANK = """OFXHEADER:100
DATA:OFXSGML
VERSION:102
SECURITY:NONE
ENCODING:USASCII
CHARSET:1252
COMPRESSION:NONE
OLDFILEUID:NONE
NEWFILEUID:NONE

<OFX>
<SIGNONMSGSRSV1>
<SONRS>
<STATUS><CODE>0<SEVERITY>INFO</STATUS>
<DTSERVER>20240131120000
<LANGUAGE>ENG
</SONRS>
</SIGNONMSGSRSV1>
<BANKMSGSRSV1>
<STMTTRNRS>
<TRNUID>1
<STATUS><CODE>0<SEVERITY>INFO</STATUS>
<STMTRS>
<CURDEF>USD
<BANKACCTFROM>
<BANKID>123456789
<ACCTID>0001234567
<ACCTTYPE>CHECKING
</BANKACCTFROM>
<BANKTRANLIST>
<DTSTART>20240101000000
<DTEND>20240131000000
<STMTTRN>
<TRNTYPE>DEBIT
<DTPOSTED>20240105120000[-5:EST]
<TRNAMT>-50.00
<FITID>2024010500001
<NAME>CAFEONE #1234
<MEMO>Coffee
</STMTTRN>
<STMTTRN>
<TRNTYPE>CREDIT
<DTPOSTED>20240115
<TRNAMT>1500.00
<FITID>2024011500002
<NAME>PAYROLL DEPOSIT
</STMTTRN>
<STMTTRN>
<TRNTYPE>CHECK
<DTPOSTED>20240120
<TRNAMT>-200.00
<FITID>2024012000003
<CHECKNUM>1234
<NAME>RENT
</STMTTRN>
</BANKTRANLIST>
<LEDGERBAL>
<BALAMT>2750.00
<DTASOF>20240131120000
</LEDGERBAL>
</STMTRS>
</STMTTRNRS>
</BANKMSGSRSV1>
</OFX>
"""


OFX_2X_XML = """<?xml version="1.0" encoding="UTF-8"?>
<?OFX OFXHEADER="200" VERSION="200" SECURITY="NONE" OLDFILEUID="NONE" NEWFILEUID="NONE"?>
<OFX>
  <BANKMSGSRSV1>
    <STMTTRNRS>
      <STMTRS>
        <CURDEF>EUR</CURDEF>
        <BANKACCTFROM>
          <BANKID>987654321</BANKID>
          <ACCTID>9999</ACCTID>
          <ACCTTYPE>SAVINGS</ACCTTYPE>
        </BANKACCTFROM>
        <BANKTRANLIST>
          <STMTTRN>
            <TRNTYPE>DEBIT</TRNTYPE>
            <DTPOSTED>20250215000000</DTPOSTED>
            <TRNAMT>-12.34</TRNAMT>
            <FITID>XML-FITID-A</FITID>
            <NAME>BAGUETTE</NAME>
            <MEMO>Bakery</MEMO>
          </STMTTRN>
        </BANKTRANLIST>
      </STMTRS>
    </STMTTRNRS>
  </BANKMSGSRSV1>
</OFX>
"""


QFX_INTU_BID = """OFXHEADER:100
DATA:OFXSGML
VERSION:102
INTU.BID:01234
INTU.USERID:foo

<OFX>
<BANKMSGSRSV1>
<STMTTRNRS>
<STMTRS>
<CURDEF>USD
<BANKACCTFROM>
<BANKID>1
<ACCTID>2
</BANKACCTFROM>
<BANKTRANLIST>
<STMTTRN>
<TRNTYPE>DEBIT
<DTPOSTED>20240301
<TRNAMT>-7.50
<FITID>QFX-1
<NAME>QFX TEST
</STMTTRN>
</BANKTRANLIST>
</STMTRS>
</STMTTRNRS>
</BANKMSGSRSV1>
</OFX>
"""


OFX_CREDIT_CARD = """OFXHEADER:100
DATA:OFXSGML
VERSION:102

<OFX>
<CREDITCARDMSGSRSV1>
<CCSTMTTRNRS>
<CCSTMTRS>
<CURDEF>USD
<CCACCTFROM>
<CCACCTID>4111XXXXXXXX1111
</CCACCTFROM>
<BANKTRANLIST>
<STMTTRN>
<TRNTYPE>DEBIT
<DTPOSTED>20240410
<TRNAMT>-99.99
<FITID>CC-1
<NAME>HARDWARE STORE
</STMTTRN>
</BANKTRANLIST>
</CCSTMTRS>
</CCSTMTTRNRS>
</CREDITCARDMSGSRSV1>
</OFX>
"""


def _prep_import(db, path: Path, source_class: str = "ofx") -> int:
    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES (?, ?, ?, 'classified')",
        (path.name, "ee" * 32, str(path)),
    )
    import_id = cur.lastrowid
    return upsert_source(
        db,
        upload_id=import_id,
        path=path.name,
        sheet_name=f"({source_class})",
        sheet_type="primary",
        source_class=source_class,
    )


def test_sniff_detects_ofx_1x_sgml(tmp_path):
    p = tmp_path / "stmt.ofx"
    p.write_text(OFX_1X_BANK)
    assert _structured.detect(p) == "ofx"


def test_sniff_detects_ofx_2x_xml(tmp_path):
    p = tmp_path / "stmt.ofx"
    p.write_text(OFX_2X_XML)
    assert _structured.detect(p) == "ofx"


def test_sniff_qfx_via_intu_bid(tmp_path):
    p = tmp_path / "stmt.qfx"
    p.write_text(QFX_INTU_BID)
    # INTU.BID present → 'qfx' regardless of extension.
    assert _structured.detect(p) == "qfx"


def test_sniff_qbo_extension_overrides_qfx(tmp_path):
    p = tmp_path / "stmt.qbo"
    p.write_text(QFX_INTU_BID)
    # Same content, different extension — refines to 'qbo'.
    assert _structured.detect(p) == "qbo"


def test_sniff_renamed_file_still_detected_by_content(tmp_path):
    # User renames a real OFX file to .txt; content sniff still wins.
    p = tmp_path / "renamed.txt"
    p.write_text(OFX_1X_BANK)
    assert _structured.detect(p) == "ofx"


def test_ingest_ofx_1x_bank(db, tmp_path):
    p = tmp_path / "bank.ofx"
    p.write_text(OFX_1X_BANK)
    src_id = _prep_import(db, p, "ofx")
    n = ofx.ingest_sheet(db, src_id, p, "(ofx)")
    assert n == 3
    rows = db.execute(
        "SELECT * FROM raw_rows WHERE source_id = ? ORDER BY row_num", (src_id,)
    ).fetchall()
    amounts = sorted(float(r["amount"]) for r in rows)
    assert amounts == [-200.00, -50.00, 1500.00]
    # FITID lands on transaction_id.
    fitids = sorted(r["transaction_id"] for r in rows)
    assert fitids == ["2024010500001", "2024011500002", "2024012000003"]
    # Date with timezone suffix [-5:EST] still parses.
    starbucks = [r for r in rows if r["transaction_id"] == "2024010500001"][0]
    assert starbucks["date"] == "2024-01-05"
    assert starbucks["currency"] == "USD"
    assert starbucks["payee"] == "CAFEONE #1234"
    assert "Coffee" in (starbucks["memo"] or "")


def test_ingest_ofx_2x_xml(db, tmp_path):
    p = tmp_path / "bank.ofx"
    p.write_text(OFX_2X_XML)
    src_id = _prep_import(db, p, "ofx")
    n = ofx.ingest_sheet(db, src_id, p, "(ofx)")
    assert n == 1
    row = db.execute(
        "SELECT * FROM raw_rows WHERE source_id = ?", (src_id,)
    ).fetchone()
    assert row["transaction_id"] == "XML-FITID-A"
    assert row["currency"] == "EUR"
    assert float(row["amount"]) == -12.34


def test_ingest_qfx_routes_through_same_parser(db, tmp_path):
    p = tmp_path / "intuit.qfx"
    p.write_text(QFX_INTU_BID)
    src_id = _prep_import(db, p, "qfx")
    # The QFX source_class binds to ofx.ingest_sheet via INGESTERS.
    n = ofx.ingest_sheet(db, src_id, p, "(qfx)")
    assert n == 1
    row = db.execute(
        "SELECT * FROM raw_rows WHERE source_id = ?", (src_id,)
    ).fetchone()
    assert row["transaction_id"] == "QFX-1"


def test_ingest_credit_card_envelope(db, tmp_path):
    p = tmp_path / "cc.ofx"
    p.write_text(OFX_CREDIT_CARD)
    src_id = _prep_import(db, p, "ofx")
    n = ofx.ingest_sheet(db, src_id, p, "(ofx)")
    assert n == 1
    row = db.execute(
        "SELECT * FROM raw_rows WHERE source_id = ?", (src_id,)
    ).fetchone()
    import json as _json
    raw = _json.loads(row["raw_json"])
    # CCACCTID flows through and is_credit_card flag is set.
    assert raw["_ofx"]["_acct_id"] == "4111XXXXXXXX1111"
    assert raw["_ofx"]["_is_credit_card"] is True


def test_ledger_balance_captured_in_raw(db, tmp_path):
    p = tmp_path / "bank.ofx"
    p.write_text(OFX_1X_BANK)
    src_id = _prep_import(db, p, "ofx")
    ofx.ingest_sheet(db, src_id, p, "(ofx)")
    row = db.execute(
        "SELECT raw_json FROM raw_rows WHERE source_id = ? LIMIT 1", (src_id,)
    ).fetchone()
    import json as _json
    raw = _json.loads(row["raw_json"])
    assert raw["_ofx"]["_ledger_balance"] == 2750.0
    assert raw["_ofx"]["_ledger_balance_date"] == "2024-01-31"


def test_malformed_ofx_returns_empty_without_raising(db, tmp_path):
    p = tmp_path / "junk.ofx"
    p.write_text("OFXHEADER:100\n\n<OFX>not really ofx</OFX>")
    src_id = _prep_import(db, p, "ofx")
    n = ofx.ingest_sheet(db, src_id, p, "(ofx)")
    assert n == 0
