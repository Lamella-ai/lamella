# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

from lamella.features.import_._db import upsert_source
from lamella.features.import_.sources import wf


FIXTURES = Path(__file__).parent / "fixtures" / "imports"


def _prep_import(db):
    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES (?, ?, ?, 'classified')",
        ("wf_2024_sample.csv", "aa" * 32, str(FIXTURES / "wf_2024_sample.csv")),
    )
    import_id = cur.lastrowid
    src_id = upsert_source(
        db,
        upload_id=import_id,
        path="wf_2024_sample.csv",
        sheet_name="(csv)",
        sheet_type="primary",
        source_class="wf_annotated",
        entity="Acme",
    )
    return import_id, src_id


def test_wf_ingests_three_rows(db):
    _, src_id = _prep_import(db)
    n = wf.ingest_sheet(db, src_id, FIXTURES / "wf_2024_sample.csv", None)
    assert n == 3
    rows = db.execute(
        "SELECT * FROM raw_rows WHERE source_id = ? ORDER BY row_num", (src_id,)
    ).fetchall()
    assert len(rows) == 3
    # Signed amounts: outflow negative, deposit positive.
    amounts = sorted(float(r["amount"]) for r in rows)
    assert amounts == [-42.17, -15.5, 2500.00]
    # Annotation preserved for the Hardware Store row.
    hd = [r for r in rows if r["ann_expense_category"] == "Supplies"][0]
    assert hd["ann_business"] == "Acme"
    assert hd["ann_business_expense"].lower() == "yes"


def test_wf_skips_rows_without_date_or_amount(db, tmp_path):
    # Craft a sheet with a total-style row (amount but no date).
    csv_path = tmp_path / "wf_extra.csv"
    csv_path.write_text(
        "Master Category,Subcategory,Date,Location,Payee,Description,"
        "Payment Method,Amount,Business Expense?,Business,Expense Category,"
        "Amount.1,Expense Memo\n"
        "Food,Dining,2024-02-01,,Coffee,Morning,PRIME CHECKING,-4.75,,,,,\n"
        "Totals,,,,,,,2500.00,,,,,\n",
        encoding="utf-8",
    )
    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES (?, ?, ?, 'classified')",
        ("wf_extra.csv", "ab" * 32, str(csv_path)),
    )
    import_id = cur.lastrowid
    src_id = upsert_source(
        db,
        upload_id=import_id,
        path="wf_extra.csv",
        sheet_name="(csv)",
        sheet_type="primary",
        source_class="wf_annotated",
    )
    n = wf.ingest_sheet(db, src_id, csv_path, None)
    # The totals row is dropped (has amount but no date).
    assert n == 1
