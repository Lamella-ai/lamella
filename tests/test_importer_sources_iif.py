# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""IIF ingester tests.

QuickBooks Desktop's tab-separated multi-section format. The parser
must respect the leading ``!HEADER`` rows that declare the column
order for each subsequent record-type, and must group ``TRNS`` →
``SPL`` → ``ENDTRNS`` triples into a single transaction with its
splits attached.
"""
from __future__ import annotations

from pathlib import Path

from lamella.features.import_ import _structured
from lamella.features.import_._db import upsert_source
from lamella.features.import_.sources import iif


def _iif_lines(*lines: str) -> str:
    """Build an IIF body. Tabs only, no trailing newline rules to fight."""
    return "\n".join(lines) + "\n"


# Minimal IIF file: one TRNS + two SPL legs + ENDTRNS.
IIF_BASIC = _iif_lines(
    "!TRNS\tTRNSID\tTRNSTYPE\tDATE\tACCNT\tNAME\tAMOUNT\tDOCNUM\tMEMO",
    "!SPL\tSPLID\tTRNSTYPE\tDATE\tACCNT\tNAME\tAMOUNT\tMEMO\tCLASS",
    "!ENDTRNS",
    "TRNS\t101\tCHECK\t02/15/2024\tChecking\tHardware Store\t-150.00\t1234\tMixed run",
    "SPL\t201\tCHECK\t02/15/2024\tSupplies\t\t100.00\tShelving\tShop",
    "SPL\t202\tCHECK\t02/15/2024\tTools\t\t50.00\tDrill bit\tShop",
    "ENDTRNS",
)


# Two transactions, second one missing ENDTRNS (real exports do this
# on the trailing record). Headers re-declared partway through to
# exercise the state machine.
IIF_MULTIPLE = _iif_lines(
    "!TRNS\tTRNSID\tTRNSTYPE\tDATE\tACCNT\tNAME\tAMOUNT\tDOCNUM\tMEMO",
    "!SPL\tSPLID\tTRNSTYPE\tDATE\tACCNT\tNAME\tAMOUNT\tMEMO",
    "!ENDTRNS",
    "TRNS\t1\tCHECK\t01/05/2024\tChecking\tCafeOne\t-50.00\t\tCoffee",
    "SPL\t1001\tCHECK\t01/05/2024\tMeals\t\t50.00\t",
    "ENDTRNS",
    "TRNS\t2\tCHECK\t01/15/2024\tChecking\tPayroll\t1500.00\t\tDeposit",
    "SPL\t1002\tCHECK\t01/15/2024\tIncome\t\t-1500.00\t",
)


# Mixed file with a chart-of-accounts section that should NOT produce
# raw_rows, plus one transaction that should.
IIF_WITH_ACCNT = _iif_lines(
    "!ACCNT\tNAME\tACCNTTYPE\tDESC",
    "ACCNT\tChecking\tBANK\tPrimary checking",
    "ACCNT\tSupplies\tEXP\tOffice supplies",
    "!TRNS\tTRNSID\tTRNSTYPE\tDATE\tACCNT\tNAME\tAMOUNT",
    "!SPL\tSPLID\tTRNSTYPE\tDATE\tACCNT\tNAME\tAMOUNT",
    "!ENDTRNS",
    "TRNS\t99\tCHECK\t03/01/2024\tChecking\tVendor\t-25.00",
    "SPL\t199\tCHECK\t03/01/2024\tSupplies\t\t25.00",
    "ENDTRNS",
)


def _prep_import(db, path: Path) -> int:
    """Stage an IIF file as a fresh import row.

    `content_sha256` is unique per call (incorporates the existing
    imports row count) so the dedup test can register the same file
    twice without tripping the imports.content_sha256 UNIQUE
    constraint.
    """
    import hashlib
    n_existing = db.execute("SELECT COUNT(*) AS n FROM imports").fetchone()["n"]
    sha = hashlib.sha256(
        f"{path}|iif|{n_existing}".encode("utf-8")
    ).hexdigest()
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
        sheet_name="(iif)",
        sheet_type="primary",
        source_class="iif",
    )


def test_sniff_detects_iif_by_header(tmp_path):
    p = tmp_path / "stmt.iif"
    p.write_text(IIF_BASIC)
    assert _structured.detect(p) == "iif"


def test_sniff_iif_renamed_extension(tmp_path):
    # Even with a misleading .csv extension, content sniff wins.
    p = tmp_path / "renamed.csv"
    p.write_text(IIF_BASIC)
    assert _structured.detect(p) == "iif"


def test_ingest_basic_trns_with_splits(db, tmp_path):
    p = tmp_path / "qb.iif"
    p.write_text(IIF_BASIC)
    src_id = _prep_import(db, p)
    n = iif.ingest_sheet(db, src_id, p, "(iif)")
    assert n == 1
    row = db.execute(
        "SELECT * FROM raw_rows WHERE source_id = ?", (src_id,)
    ).fetchone()
    assert row["date"] == "2024-02-15"
    assert float(row["amount"]) == -150.00
    assert row["payee"] == "Hardware Store"
    assert row["transaction_id"] == "iif:101"
    import json as _json
    raw = _json.loads(row["raw_json"])
    splits = raw["_iif"]["_splits"]
    assert splits is not None
    assert len(splits) == 2
    accts = sorted(s["account"] for s in splits)
    assert accts == ["Supplies", "Tools"]


def test_ingest_multiple_transactions_handles_trailing_no_endtrns(db, tmp_path):
    p = tmp_path / "qb.iif"
    p.write_text(IIF_MULTIPLE)
    src_id = _prep_import(db, p)
    n = iif.ingest_sheet(db, src_id, p, "(iif)")
    # Both transactions captured, even though the second lacks ENDTRNS.
    assert n == 2
    rows = db.execute(
        "SELECT transaction_id, amount FROM raw_rows WHERE source_id = ? "
        "ORDER BY row_num", (src_id,)
    ).fetchall()
    ids = [r["transaction_id"] for r in rows]
    assert ids == ["iif:1", "iif:2"]
    amounts = [float(r["amount"]) for r in rows]
    assert amounts == [-50.0, 1500.0]


def test_accnt_section_does_not_produce_raw_rows(db, tmp_path):
    p = tmp_path / "qb.iif"
    p.write_text(IIF_WITH_ACCNT)
    src_id = _prep_import(db, p)
    n = iif.ingest_sheet(db, src_id, p, "(iif)")
    # Two ACCNT rows ignored; one TRNS becomes one raw_row.
    assert n == 1
    row = db.execute(
        "SELECT transaction_id FROM raw_rows WHERE source_id = ?", (src_id,)
    ).fetchone()
    assert row["transaction_id"] == "iif:99"


def test_iif_dedup_uses_trnsid(db, tmp_path):
    """TRNSID should be the canonical external_id for IIF — verifies
    re-ingestion produces the same transaction_id (idempotent)."""
    p = tmp_path / "qb.iif"
    p.write_text(IIF_BASIC)
    src_id_a = _prep_import(db, p)
    iif.ingest_sheet(db, src_id_a, p, "(iif)")
    src_id_b = _prep_import(db, p)
    iif.ingest_sheet(db, src_id_b, p, "(iif)")
    ids_a = {r["transaction_id"] for r in db.execute(
        "SELECT transaction_id FROM raw_rows WHERE source_id = ?", (src_id_a,)
    ).fetchall()}
    ids_b = {r["transaction_id"] for r in db.execute(
        "SELECT transaction_id FROM raw_rows WHERE source_id = ?", (src_id_b,)
    ).fetchall()}
    assert ids_a == ids_b == {"iif:101"}
