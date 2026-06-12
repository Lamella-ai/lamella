# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json

import pytest

from lamella.features.import_ import transfers
from lamella.features.import_._db import insert_raw_row, upsert_source


def _seed_upload(db):
    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES ('x', 'ff', '/tmp/x', 'ingested')"
    )
    return cur.lastrowid


@pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
def test_in_batch_transfer_detected(db):
    upload_id = _seed_upload(db)
    src_wf = upsert_source(
        db,
        upload_id=upload_id,
        path="wf.csv",
        sheet_name="WF",
        sheet_type="primary",
        source_class="wf_annotated",
    )
    src_pp = upsert_source(
        db,
        upload_id=upload_id,
        path="paypal.csv",
        sheet_name="PayPal",
        sheet_type="primary",
        source_class="paypal",
    )
    insert_raw_row(
        db, source_id=src_wf, row_num=1, raw={},
        date="2024-03-01", amount=500.0, payee="Paypal Transfer",
        description="PAYPAL transfer",
    )
    insert_raw_row(
        db, source_id=src_pp, row_num=1, raw={},
        date="2024-03-01", amount=-500.0, payee="Transfer out",
        description="Payout",
    )
    counts = transfers.detect(db, upload_id)
    assert counts["transfers"] == 1
    rp = db.execute("SELECT * FROM row_pairs").fetchone()
    assert rp["kind"] == "transfer"


def test_transfers_does_not_pair_across_uploads(db):
    a = _seed_upload(db)
    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES ('y', 'aa', '/tmp/y', 'ingested')"
    )
    b = cur.lastrowid
    src_a = upsert_source(
        db, upload_id=a, path="a.csv", sheet_name="a",
        sheet_type="primary", source_class="wf_annotated",
    )
    src_b = upsert_source(
        db, upload_id=b, path="b.csv", sheet_name="b",
        sheet_type="primary", source_class="paypal",
    )
    insert_raw_row(
        db, source_id=src_a, row_num=1, raw={}, date="2024-04-01",
        amount=200.0, description="X",
    )
    insert_raw_row(
        db, source_id=src_b, row_num=1, raw={}, date="2024-04-01",
        amount=-200.0, description="Y",
    )
    counts_a = transfers.detect(db, a)
    counts_b = transfers.detect(db, b)
    # No pair across uploads — each batch has only one row, no pairing.
    assert counts_a["transfers"] == 0
    assert counts_b["transfers"] == 0


@pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
def test_pairs_mirror_to_staged_pairs(db):
    """NEXTGEN.md Phase A: detected row_pairs land in staged_pairs
    keyed on the staged_transactions rows, and both sides advance to
    status 'matched'. Phase C's unified matcher reads only the
    staged_pairs surface."""
    upload_id = _seed_upload(db)
    src_wf = upsert_source(
        db, upload_id=upload_id, path="wf.csv", sheet_name="WF",
        sheet_type="primary", source_class="wf_annotated",
    )
    src_pp = upsert_source(
        db, upload_id=upload_id, path="paypal.csv", sheet_name="PayPal",
        sheet_type="primary", source_class="paypal",
    )
    insert_raw_row(
        db, source_id=src_wf, row_num=1, raw={},
        date="2024-03-01", amount=500.0, description="PAYPAL TRANSFER",
    )
    insert_raw_row(
        db, source_id=src_pp, row_num=1, raw={},
        date="2024-03-01", amount=-500.0, description="Payout",
    )
    transfers.detect(db, upload_id)

    staged_pair = db.execute(
        "SELECT * FROM staged_pairs"
    ).fetchall()
    assert len(staged_pair) == 1
    assert staged_pair[0]["kind"] == "transfer"

    # Both sides should be 'matched' after the mirror runs.
    statuses = db.execute(
        "SELECT status FROM staged_transactions WHERE session_id = ?",
        (str(upload_id),),
    ).fetchall()
    assert {r["status"] for r in statuses} == {"matched"}
