# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import asyncio

from lamella.features.import_._db import insert_raw_row, upsert_source
from lamella.features.import_.categorize import categorize_import


def _seed(db, source_class="wf_annotated"):
    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES ('x', 'ee', '/tmp/x', 'ingested')"
    )
    upload_id = cur.lastrowid
    src_id = upsert_source(
        db, upload_id=upload_id, path="x.csv", sheet_name="(csv)",
        sheet_type="primary", source_class=source_class, entity="Acme",
    )
    return upload_id, src_id


def test_annotated_row_wins(db):
    upload_id, src_id = _seed(db)
    insert_raw_row(
        db, source_id=src_id, row_num=1, raw={},
        date="2024-05-01", amount=-42.17,
        payee="Hardware Store", description="Screws",
        ann_business_expense="Yes", ann_business="Acme",
        ann_expense_category="Supplies",
    )
    # Seed a payee_rule that would otherwise win — annotation must override.
    db.execute(
        "INSERT INTO payee_rules (pattern, account, reason) "
        "VALUES ('%hardware store%', 'Expenses:Personal:Hardware', 'rule')"
    )
    result = asyncio.run(categorize_import(db, import_id=upload_id))
    assert result.annotated == 1
    cat = db.execute("SELECT * FROM categorizations").fetchone()
    assert cat["confidence"] == "annotated"
    assert cat["account"].startswith("Expenses:Acme:")


def test_payee_rule_wins_over_classification_rules(db):
    upload_id, src_id = _seed(db)
    insert_raw_row(
        db, source_id=src_id, row_num=1, raw={},
        date="2024-05-02", amount=-15.50,
        payee="Fast Casual", description="Lunch",
    )
    db.execute(
        "INSERT INTO payee_rules (pattern, account, reason) "
        "VALUES ('%casual%', 'Expenses:Food:Dining', 'payee rule')"
    )
    db.execute(
        "INSERT INTO classification_rules "
        "(pattern_type, pattern_value, target_account, confidence, created_by, hit_count) "
        "VALUES ('merchant_contains', 'Fast Casual', 'Expenses:Other', 1.0, 'user', 0)"
    )
    result = asyncio.run(categorize_import(db, import_id=upload_id))
    assert result.by_rule == 1
    cat = db.execute("SELECT * FROM categorizations").fetchone()
    assert cat["account"] == "Expenses:Food:Dining"


def test_no_match_flags_needs_review(db):
    upload_id, src_id = _seed(db)
    insert_raw_row(
        db, source_id=src_id, row_num=1, raw={},
        date="2024-05-03", amount=-9.99,
        payee="Some Unknown Merchant", description="Mystery charge",
    )
    result = asyncio.run(categorize_import(db, import_id=upload_id, ai=None))
    assert result.needs_review == 1
    cat = db.execute("SELECT * FROM categorizations").fetchone()
    assert cat["needs_review"] == 1
    assert cat["account"] == "Expenses:Uncategorized"


def test_zero_amount_rows_skipped(db):
    upload_id, src_id = _seed(db)
    insert_raw_row(
        db, source_id=src_id, row_num=1, raw={},
        date="2024-05-04", amount=0.0, payee="Noise",
    )
    result = asyncio.run(categorize_import(db, import_id=upload_id, ai=None))
    assert result.categorized == 0
    cls = db.execute("SELECT status FROM classifications").fetchone()
    assert cls["status"] == "zero"


# ---------------------------------------------------------------------------
# Migration 055 — categorizations.lamella_txn_id bridge from categorize → emit
# ---------------------------------------------------------------------------

def test_lineage_uuid_minted_on_first_categorize(db):
    """Every categorization row gets a lamella_txn_id stamped on it
    so emit.render_transaction can use it as the on-disk lamella-txn-id
    AND the AI input_ref (when AI runs) matches the eventual entry's
    lineage."""
    from lamella.core.transform.normalize_txn_identity import _looks_like_uuid

    upload_id, src_id = _seed(db)
    insert_raw_row(
        db, source_id=src_id, row_num=1, raw={},
        date="2024-05-01", amount=-42.17,
        payee="Hardware Store", description="Screws",
        ann_business_expense="Yes", ann_business="Acme",
        ann_expense_category="Supplies",
    )
    asyncio.run(categorize_import(db, import_id=upload_id, ai=None))
    cat = db.execute(
        "SELECT lamella_txn_id FROM categorizations"
    ).fetchone()
    assert cat["lamella_txn_id"] is not None
    assert _looks_like_uuid(cat["lamella_txn_id"])


def test_lineage_stable_across_recategorize(db):
    """Re-running categorize on the same raw_row must NOT mint a new
    lineage. AI decisions logged under the original lineage must stay
    matchable; the COALESCE in the upsert preserves the existing value.
    Without this guard, a re-categorize would silently orphan every
    pre-existing AI decision for that row."""
    upload_id, src_id = _seed(db)
    insert_raw_row(
        db, source_id=src_id, row_num=1, raw={},
        date="2024-05-01", amount=-42.17,
        payee="Hardware Store", description="Screws",
        ann_business_expense="Yes", ann_business="Acme",
        ann_expense_category="Supplies",
    )
    asyncio.run(categorize_import(db, import_id=upload_id, ai=None))
    first = db.execute(
        "SELECT lamella_txn_id FROM categorizations"
    ).fetchone()["lamella_txn_id"]

    # Second categorize pass on the same upload.
    asyncio.run(categorize_import(db, import_id=upload_id, ai=None))
    second = db.execute(
        "SELECT lamella_txn_id FROM categorizations"
    ).fetchone()["lamella_txn_id"]
    assert first == second


def test_emit_uses_categorize_minted_lineage(db, tmp_path):
    """End-to-end: categorize mints a lineage, emit reads it from
    cat.lamella_txn_id and writes it as the entry's lamella-txn-id.
    The two values must match — that's the whole point of the bridge."""
    from lamella.features.import_.emit import _render_chunk

    upload_id, src_id = _seed(db)
    insert_raw_row(
        db, source_id=src_id, row_num=1, raw={},
        date="2024-05-01", amount=-42.17,
        payee="Hardware Store", description="Screws",
        ann_business_expense="Yes", ann_business="Acme",
        ann_expense_category="Supplies",
    )
    asyncio.run(categorize_import(db, import_id=upload_id, ai=None))
    minted_lineage = db.execute(
        "SELECT lamella_txn_id FROM categorizations"
    ).fetchone()["lamella_txn_id"]
    assert minted_lineage

    by_year = _render_chunk(db, upload_id)
    body = "\n".join(
        line for lines in by_year.values() for line in lines
    )
    # The on-disk entry's lineage MUST be the same value the
    # categorize stage minted (and would have used as AI input_ref
    # if AI had run). Same identifier, end to end.
    assert f'lamella-txn-id: "{minted_lineage}"' in body
