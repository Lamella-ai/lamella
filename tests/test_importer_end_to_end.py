# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io.reader import LedgerReader
from lamella.features.import_.service import ImportService


FIXTURES = Path(__file__).parent / "fixtures" / "imports"


def test_wf_end_to_end(db, settings, monkeypatch):
    # Skip bean-check shell out in emit.
    monkeypatch.setattr(
        "lamella.features.import_.emit.run_bean_check", lambda main_bean: None
    )
    reader = LedgerReader(settings.ledger_main)
    # Pass ai=None so the cascade falls through deterministically:
    # annotated rows → annotated, the rest → needs_review.
    svc = ImportService(conn=db, settings=settings, ai=None, reader=reader)

    body = (FIXTURES / "wf_2024_sample.csv").read_bytes()
    outcome = svc.register_upload(filename="wf_2024_sample.csv", body=body)
    assert outcome.was_new
    import_id = outcome.record.id

    svc.classify(import_id)
    # WF primary sheet — no mapping step needed.
    assert svc.mark_classify_complete(import_id) is True

    summary = svc.ingest(import_id)
    assert summary.total_rows == 3

    asyncio.run(svc.categorize(import_id))
    # The Hardware Store row has annotations — it ends up 'annotated'; the others
    # are uncategorized and marked needs_review.
    reviewed = db.execute(
        "SELECT COUNT(*) AS n FROM categorizations WHERE needs_review = 1"
    ).fetchone()
    assert reviewed["n"] >= 1

    result = svc.commit(import_id)
    assert sum(result.per_year.values()) >= 1
    record = svc.get(import_id)
    assert record.status == "committed"
    out_file = settings.import_ledger_output_dir_resolved / "2024.bean"
    assert out_file.exists()


def test_idempotent_reupload_returns_existing(db, settings, monkeypatch):
    monkeypatch.setattr(
        "lamella.features.import_.emit.run_bean_check", lambda main_bean: None
    )
    reader = LedgerReader(settings.ledger_main)
    ai = AIService(settings=settings, conn=db)
    svc = ImportService(conn=db, settings=settings, ai=ai, reader=reader)
    body = (FIXTURES / "wf_2024_sample.csv").read_bytes()
    first = svc.register_upload(filename="wf.csv", body=body)
    second = svc.register_upload(filename="wf_rename.csv", body=body)
    assert second.was_new is False
    assert second.record.id == first.record.id


@pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
def test_importer_mirrors_to_staged_transactions(db, settings, monkeypatch):
    """NEXTGEN.md Phase A: every CSV import lands a parallel row in
    ``staged_transactions`` keyed by (source='csv', source_ref) so the
    unified pipeline (matcher, review UI) sees importer rows alongside
    SimpleFIN + paste + reboot rows. This test asserts the mirror
    populates for a real upload."""
    monkeypatch.setattr(
        "lamella.features.import_.emit.run_bean_check", lambda main_bean: None
    )
    reader = LedgerReader(settings.ledger_main)
    ai = AIService(settings=settings, conn=db)
    svc = ImportService(conn=db, settings=settings, ai=ai, reader=reader)

    body = (FIXTURES / "wf_2024_sample.csv").read_bytes()
    outcome = svc.register_upload(filename="wf_2024_sample.csv", body=body)
    import_id = outcome.record.id
    svc.classify(import_id)
    svc.mark_classify_complete(import_id)
    svc.ingest(import_id)

    # Every raw_row for this upload has a staged_transactions mirror
    # tagged source='csv'.
    raw_rows = db.execute(
        "SELECT r.id AS raw_id FROM raw_rows r "
        "JOIN sources s ON s.id = r.source_id "
        "WHERE s.upload_id = ? AND r.date IS NOT NULL AND r.amount IS NOT NULL",
        (import_id,),
    ).fetchall()
    assert raw_rows, "fixture should produce at least one dated/amounted raw row"

    staged = db.execute(
        "SELECT COUNT(*) AS n FROM staged_transactions "
        "WHERE source = 'csv' AND session_id = ?",
        (str(import_id),),
    ).fetchone()
    assert staged["n"] == len(raw_rows), (
        f"expected {len(raw_rows)} staged rows, got {staged['n']}"
    )

    # Each staged row carries the originating raw_row_id in its
    # source_ref so downstream pipelines can trace back to the
    # importer's own tables.
    import json as _json
    one = db.execute(
        "SELECT source_ref FROM staged_transactions "
        "WHERE source = 'csv' AND session_id = ? LIMIT 1",
        (str(import_id),),
    ).fetchone()
    ref = _json.loads(one["source_ref"])
    assert "raw_row_id" in ref
    assert ref["upload_id"] == import_id


@pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
def test_importer_mirrors_decisions_and_pairs_to_staging(
    db, settings, monkeypatch
):
    """NEXTGEN.md Phase A: categorizations land as staged_decisions
    and row_pairs land as staged_pairs so Phase C's unified matcher
    and the cross-source review UI can see importer-produced signals
    without reading the importer-specific tables."""
    monkeypatch.setattr(
        "lamella.features.import_.emit.run_bean_check", lambda main_bean: None
    )
    reader = LedgerReader(settings.ledger_main)
    ai = AIService(settings=settings, conn=db)
    svc = ImportService(conn=db, settings=settings, ai=ai, reader=reader)

    body = (FIXTURES / "wf_2024_sample.csv").read_bytes()
    outcome = svc.register_upload(filename="wf_2024_sample.csv", body=body)
    import_id = outcome.record.id
    svc.classify(import_id)
    svc.mark_classify_complete(import_id)
    svc.ingest(import_id)
    asyncio.run(svc.categorize(import_id))

    # Every categorizations row for this import must have a matching
    # staged_decisions row on the paired staged_transactions.id.
    paired = db.execute(
        """
        SELECT COUNT(*) AS n
          FROM categorizations cat
          JOIN raw_rows r ON r.id = cat.raw_row_id
          JOIN sources  s ON s.id = r.source_id
          JOIN staged_transactions st
                 ON st.source = 'csv'
                AND json_extract(st.source_ref, '$.raw_row_id') = r.id
          JOIN staged_decisions d ON d.staged_id = st.id
         WHERE s.upload_id = ?
        """,
        (import_id,),
    ).fetchone()
    total_cats = db.execute(
        """
        SELECT COUNT(*) AS n
          FROM categorizations cat
          JOIN raw_rows r ON r.id = cat.raw_row_id
          JOIN sources  s ON s.id = r.source_id
         WHERE s.upload_id = ?
        """,
        (import_id,),
    ).fetchone()
    assert paired["n"] == total_cats["n"], (
        f"decision mirror missed rows: {paired['n']}/{total_cats['n']}"
    )

    # needs_review semantics carry through: any categorization with
    # needs_review=1 produces a staged_decision with needs_review=1.
    review_mirror = db.execute(
        """
        SELECT COUNT(*) AS n
          FROM categorizations cat
          JOIN raw_rows r ON r.id = cat.raw_row_id
          JOIN sources  s ON s.id = r.source_id
          JOIN staged_transactions st
                 ON st.source = 'csv'
                AND json_extract(st.source_ref, '$.raw_row_id') = r.id
          JOIN staged_decisions d ON d.staged_id = st.id
         WHERE s.upload_id = ?
           AND cat.needs_review = 1
           AND d.needs_review = 1
        """,
        (import_id,),
    ).fetchone()
    review_total = db.execute(
        """
        SELECT COUNT(*) AS n
          FROM categorizations cat
          JOIN raw_rows r ON r.id = cat.raw_row_id
          JOIN sources  s ON s.id = r.source_id
         WHERE s.upload_id = ?
           AND cat.needs_review = 1
        """,
        (import_id,),
    ).fetchone()
    assert review_mirror["n"] == review_total["n"]
