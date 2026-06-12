# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""QIF ingester tests.

QIF is line-prefixed text terminated by ``^``. The parser must
handle ISO dates, US ``MM/DD/YY`` dates, the Quicken Y2K ``MM/DD'YYYY``
notation, splits (``S``/``E``/``$``), and ambiguous-date detection.
"""
from __future__ import annotations

from pathlib import Path

from lamella.features.import_ import _structured
from lamella.features.import_._db import upsert_source
from lamella.features.import_.sources import qif


QIF_BANK_ISO = """!Type:Bank
D2024-01-05
T-50.00
PCafeOne
MMorning coffee
^
D2024-01-15
T1500.00
PPayroll Deposit
^
D2024-01-20
T-200.00
PRent
N1234
LHousing
^
"""


QIF_BANK_USDATE = """!Type:Bank
D01/05/2024
T-50.00
PCafeOne
^
D01/15'2024
T1500.00
PPayroll
^
D11/20/24
T-200.00
PRent
^
"""


QIF_BANK_DMY_UNAMBIGUOUS = """!Type:Bank
D25/12/2024
T-100.00
PHoliday Gift
^
D31/12/2024
T-50.00
PNew Year
^
"""


QIF_WITH_SPLITS = """!Type:Bank
D2024-03-01
T-100.00
PHardware Store
SSupplies
$-60.00
ESmall tools
STools
$-40.00
EDrill bit
^
"""


QIF_INVESTMENT = """!Type:Invst
D2024-04-01
NBuy
YACME
I50.00
Q10
T-500.00
^
"""


def _prep_import(db, path: Path) -> int:
    import hashlib
    sha = hashlib.sha256(str(path).encode("utf-8")).hexdigest()
    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES (?, ?, ?, 'classified')",
        (path.name, sha, str(path)),
    )
    import_id = cur.lastrowid
    return upsert_source(
        db,
        upload_id=import_id,
        path=path.name,
        sheet_name="(qif)",
        sheet_type="primary",
        source_class="qif",
    )


def test_sniff_detects_qif_by_type_marker(tmp_path):
    p = tmp_path / "stmt.qif"
    p.write_text(QIF_BANK_ISO)
    assert _structured.detect(p) == "qif"


def test_sniff_qif_renamed_extension(tmp_path):
    p = tmp_path / "renamed.dat"
    p.write_text(QIF_BANK_ISO)
    assert _structured.detect(p) == "qif"


def test_ingest_qif_iso_dates(db, tmp_path):
    p = tmp_path / "bank.qif"
    p.write_text(QIF_BANK_ISO)
    src_id = _prep_import(db, p)
    n = qif.ingest_sheet(db, src_id, p, "(qif)")
    assert n == 3
    rows = db.execute(
        "SELECT * FROM raw_rows WHERE source_id = ? ORDER BY row_num", (src_id,)
    ).fetchall()
    dates = sorted(r["date"] for r in rows)
    assert dates == ["2024-01-05", "2024-01-15", "2024-01-20"]
    rent = [r for r in rows if r["payee"] == "Rent"][0]
    # QIF has no native id; we synthesize "qif:<hash>" so dedup
    # works across re-imports of the same file.
    assert rent["transaction_id"].startswith("qif:")


def test_ingest_qif_us_dates_including_y2k_notation(db, tmp_path):
    p = tmp_path / "bank.qif"
    p.write_text(QIF_BANK_USDATE)
    src_id = _prep_import(db, p)
    n = qif.ingest_sheet(db, src_id, p, "(qif)")
    assert n == 3
    dates = sorted(
        r["date"] for r in db.execute(
            "SELECT date FROM raw_rows WHERE source_id = ?", (src_id,)
        ).fetchall()
    )
    # 11/20/24 (US 2-digit) → 2024-11-20 because 24 < 70.
    assert dates == ["2024-01-05", "2024-01-15", "2024-11-20"]


def test_ingest_qif_unambiguous_dmy_dates(db, tmp_path):
    """When any date's first component > 12, parser MUST infer DMY."""
    p = tmp_path / "bank.qif"
    p.write_text(QIF_BANK_DMY_UNAMBIGUOUS)
    src_id = _prep_import(db, p)
    n = qif.ingest_sheet(db, src_id, p, "(qif)")
    assert n == 2
    dates = sorted(
        r["date"] for r in db.execute(
            "SELECT date FROM raw_rows WHERE source_id = ?", (src_id,)
        ).fetchall()
    )
    # 25/12/2024 → 2024-12-25, 31/12/2024 → 2024-12-31
    assert dates == ["2024-12-25", "2024-12-31"]


def test_ingest_qif_splits_preserved_in_raw(db, tmp_path):
    p = tmp_path / "bank.qif"
    p.write_text(QIF_WITH_SPLITS)
    src_id = _prep_import(db, p)
    n = qif.ingest_sheet(db, src_id, p, "(qif)")
    assert n == 1
    row = db.execute(
        "SELECT raw_json, amount FROM raw_rows WHERE source_id = ?", (src_id,)
    ).fetchone()
    assert float(row["amount"]) == -100.00
    import json as _json
    raw = _json.loads(row["raw_json"])
    splits = raw["_qif"]["_splits"]
    assert splits is not None
    assert len(splits) == 2
    cats = sorted(s["category"] for s in splits)
    assert cats == ["Supplies", "Tools"]
    amts = sorted(s["amount"] for s in splits)
    assert amts == [-60.0, -40.0]


def test_ingest_qif_investment_skipped(db, tmp_path):
    p = tmp_path / "invst.qif"
    p.write_text(QIF_INVESTMENT)
    src_id = _prep_import(db, p)
    n = qif.ingest_sheet(db, src_id, p, "(qif)")
    # !Type:Invst is deferred to a later phase — skip rather than
    # mis-parse the N/Y/I/Q field codes.
    assert n == 0


def test_qif_dedup_id_stable_across_imports(db, tmp_path):
    """The same transaction in two separate uploads should produce
    the same transaction_id so ledger_dedup can match across them."""
    p1 = tmp_path / "first.qif"
    p1.write_text(QIF_BANK_ISO)
    p2 = tmp_path / "second.qif"
    p2.write_text(QIF_BANK_ISO)
    src1 = _prep_import(db, p1)
    src2 = _prep_import(db, p2)
    qif.ingest_sheet(db, src1, p1, "(qif)")
    qif.ingest_sheet(db, src2, p2, "(qif)")
    ids1 = {r["transaction_id"] for r in db.execute(
        "SELECT transaction_id FROM raw_rows WHERE source_id = ?", (src1,)
    ).fetchall()}
    ids2 = {r["transaction_id"] for r in db.execute(
        "SELECT transaction_id FROM raw_rows WHERE source_id = ?", (src2,)
    ).fetchall()}
    assert ids1 == ids2
