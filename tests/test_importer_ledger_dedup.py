# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

from lamella.core.beancount_io.reader import LedgerReader
from lamella.features.import_._db import insert_raw_row, upsert_source
from lamella.features.import_ import ledger_dedup


def _seed_upload(db):
    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES ('x', 'de', '/tmp/x', 'categorized')"
    )
    return cur.lastrowid


def test_match_against_existing_ledger_txn(db, ledger_dir: Path):
    # Open the routed account and seed a ledger transaction at the same
    # account on 2024-06-05 so dedup has something to match.
    accounts = ledger_dir / "accounts.bean"
    accounts.write_text(
        accounts.read_text(encoding="utf-8")
        + "\n2023-01-01 open Assets:Acme:BankOne:Checking USD\n",
        encoding="utf-8",
    )
    (ledger_dir / "connector_links.bean").write_text(
        "2024-06-05 * \"Hardware Store\" \"Seed\"\n"
        "  Assets:Acme:BankOne:Checking  -42.17 USD\n"
        "  Expenses:Acme:Supplies 42.17 USD\n",
        encoding="utf-8",
    )
    main = ledger_dir / "main.bean"
    main.write_text(
        main.read_text(encoding="utf-8")
        + "\ninclude \"connector_links.bean\"\n",
        encoding="utf-8",
    )

    upload_id = _seed_upload(db)
    src_id = upsert_source(
        db, upload_id=upload_id, path="x.csv", sheet_name="(csv)",
        sheet_type="primary", source_class="wf_annotated", entity="Acme",
    )
    row_id = insert_raw_row(
        db, source_id=src_id, row_num=1, raw={},
        date="2024-06-06", amount=-42.17,
        payee="Hardware Store", description="Screws",
        payment_method="ACME LLC CHECKING",
    )
    db.execute(
        "INSERT INTO categorizations (raw_row_id, account, confidence) "
        "VALUES (?, 'Expenses:Acme:Supplies', 'rule')",
        (row_id,),
    )

    reader = LedgerReader(main)
    reader.load(force=True)
    result = ledger_dedup.drop_duplicates(db, import_id=upload_id, reader=reader)
    assert result.dropped == 1
    cls = db.execute("SELECT * FROM classifications WHERE raw_row_id = ?", (row_id,)).fetchone()
    assert cls["status"] == "deduped"
    assert "matched live ledger" in (cls["dedup_reason"] or "")


def test_rows_against_closed_account_not_dropped(db, ledger_dir: Path):
    # Use an account that's NOT open in the fixture ledger: Acme:BankOne:Checking.
    upload_id = _seed_upload(db)
    src_id = upsert_source(
        db, upload_id=upload_id, path="x.csv", sheet_name="(csv)",
        sheet_type="primary", source_class="wf_annotated", entity="Acme",
    )
    row_id = insert_raw_row(
        db, source_id=src_id, row_num=1, raw={},
        date="2024-07-01", amount=-100.0,
        payee="Unknown", description="X",
        payment_method="ACME LLC CHECKING",  # routes to Assets:Acme:BankOne:Checking
    )
    db.execute(
        "INSERT INTO categorizations (raw_row_id, account, confidence) "
        "VALUES (?, 'Expenses:Uncategorized', 'review')",
        (row_id,),
    )
    reader = LedgerReader(ledger_dir / "main.bean")
    reader.load(force=True)
    result = ledger_dedup.drop_duplicates(db, import_id=upload_id, reader=reader)
    assert result.dropped == 0
    assert result.missing_accounts  # recorded so the UI can surface
