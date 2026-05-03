# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

from lamella.features.import_ import emit as emit_mod
from lamella.features.import_._db import insert_raw_row, upsert_source


def _seed(db, source_class="wf_annotated"):
    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES ('x', 'fe', '/tmp/x', 'categorized')"
    )
    upload_id = cur.lastrowid
    src_id = upsert_source(
        db, upload_id=upload_id, path="x.csv", sheet_name="(csv)",
        sheet_type="primary", source_class=source_class, entity="Acme",
    )
    return upload_id, src_id


def test_build_postings_emits_two_legs_per_row(db):
    upload_id, src_id = _seed(db)
    for i, (date, amt) in enumerate(
        [("2024-08-01", -42.17), ("2024-08-02", 100.0)], start=1
    ):
        rid = insert_raw_row(
            db, source_id=src_id, row_num=i, raw={},
            date=date, amount=amt,
            payee="Hardware Store" if amt < 0 else "Customer",
            description="X",
            payment_method="ACME LLC CHECKING",
        )
        db.execute(
            "INSERT INTO categorizations (raw_row_id, account, confidence) "
            "VALUES (?, 'Expenses:Acme:Supplies', 'rule')",
            (rid,),
        )
    n = emit_mod.build_postings(db, upload_id)
    assert n == 2
    legs = db.execute("SELECT * FROM txn_postings ORDER BY raw_row_id, leg_idx").fetchall()
    assert len(legs) == 4
    # Posting sums to zero.
    by_row: dict[int, list[float]] = {}
    for leg in legs:
        by_row.setdefault(int(leg["raw_row_id"]), []).append(float(leg["amount"]))
    for amounts in by_row.values():
        assert round(sum(amounts), 2) == 0.0


def test_emit_to_ledger_writes_idempotent_includes(db, ledger_dir: Path, monkeypatch):
    # Skip the actual bean-check shell out.
    monkeypatch.setattr(
        "lamella.features.import_.emit.run_bean_check", lambda main_bean: None
    )

    upload_id, src_id = _seed(db)
    rid = insert_raw_row(
        db, source_id=src_id, row_num=1, raw={},
        date="2024-09-05", amount=-10.0,
        payee="Test", description="X", payment_method="ACME LLC CHECKING",
    )
    db.execute(
        "INSERT INTO categorizations (raw_row_id, account, confidence) "
        "VALUES (?, 'Expenses:Acme:Supplies', 'rule')",
        (rid,),
    )
    out_dir = ledger_dir / "connector_imports"
    result = emit_mod.emit_to_ledger(
        db, import_id=upload_id, main_bean=ledger_dir / "main.bean",
        output_dir=out_dir, run_check=True,
    )
    assert (out_dir / "2024.bean").exists()
    all_bean = out_dir / "_all.bean"
    assert all_bean.exists()
    # First emit added the include to main.bean.
    main_text_1 = (ledger_dir / "main.bean").read_text(encoding="utf-8")
    assert 'include "connector_imports/_all.bean"' in main_text_1

    # Second emit should NOT re-add the include or duplicate the year include.
    rid2 = insert_raw_row(
        db, source_id=src_id, row_num=2, raw={},
        date="2024-09-06", amount=-20.0,
        payee="Test2", description="Y", payment_method="ACME LLC CHECKING",
    )
    db.execute(
        "INSERT INTO categorizations (raw_row_id, account, confidence) "
        "VALUES (?, 'Expenses:Acme:Supplies', 'rule')",
        (rid2,),
    )
    emit_mod.emit_to_ledger(
        db, import_id=upload_id, main_bean=ledger_dir / "main.bean",
        output_dir=out_dir, run_check=True,
    )
    main_text_2 = (ledger_dir / "main.bean").read_text(encoding="utf-8")
    # include line appears exactly once.
    assert main_text_2.count('include "connector_imports/_all.bean"') == 1
    assert all_bean.read_text(encoding="utf-8").count('include "2024.bean"') == 1
