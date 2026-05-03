# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

from lamella.features.import_._db import upsert_source
from lamella.features.import_.sources import generic


FIXTURES = Path(__file__).parent / "fixtures" / "imports"


def test_generic_ingest_uses_user_confirmed_mapping(db):
    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES (?, ?, ?, 'mapped')",
        ("generic_custom_bank.csv", "cc" * 32, str(FIXTURES / "generic_custom_bank.csv")),
    )
    import_id = cur.lastrowid
    src_id = upsert_source(
        db,
        upload_id=import_id,
        path="generic_custom_bank.csv",
        sheet_name="(csv)",
        sheet_type="primary",
        source_class="generic_csv",
    )
    column_map = {
        "Trans Date": "date",
        "Description": "description",
        "Amt": "amount",
        "Ref No": "transaction_id",
    }
    n = generic.ingest_sheet(
        db,
        src_id,
        FIXTURES / "generic_custom_bank.csv",
        None,
        column_map=column_map,
    )
    assert n == 4
    rows = db.execute(
        "SELECT * FROM raw_rows WHERE source_id = ? ORDER BY row_num", (src_id,)
    ).fetchall()
    amounts = [float(r["amount"]) for r in rows]
    assert amounts == [2500.00, -5.25, -48.17, -500.00]
    assert rows[0]["description"].startswith("ACH Deposit")
    assert rows[2]["transaction_id"] == "A003"


def test_generic_requires_column_map(db):
    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES (?, ?, ?, 'mapped')",
        ("x.csv", "dd" * 32, str(FIXTURES / "generic_custom_bank.csv")),
    )
    import_id = cur.lastrowid
    src_id = upsert_source(
        db,
        upload_id=import_id,
        path="x.csv",
        sheet_name="(csv)",
        sheet_type="primary",
        source_class="generic_csv",
    )
    try:
        generic.ingest_sheet(db, src_id, FIXTURES / "generic_custom_bank.csv", None)
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
