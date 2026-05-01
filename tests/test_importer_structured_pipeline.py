# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""End-to-end pipeline checks for OFX/QIF/IIF.

The structured-format detector lives in three places — content sniff,
preview synthesis, and classify dispatch. These tests verify they
agree: sniff → preview → classify_source → INGESTERS lookup all
return the right ``source_class`` for each format.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.features.import_.classify import KNOWN_SOURCE_CLASSES, classify_source
from lamella.features.import_.preview import preview_workbook
from lamella.features.import_.sources import INGESTERS

pytestmark = pytest.mark.xfail(
    reason="OFX/QIF/IIF preview synthesis not implemented; see preview.py:74; tracking issue TODO",
    strict=False,
)

# Reuse the same fixture strings from the per-format tests so we don't
# drift on what "valid OFX/QIF/IIF" means.
from tests.test_importer_sources_ofx import (
    OFX_1X_BANK,
    OFX_2X_XML,
    QFX_INTU_BID,
)
from tests.test_importer_sources_qif import QIF_BANK_ISO
from tests.test_importer_sources_iif import IIF_BASIC


def test_known_source_classes_includes_all_structured():
    for name in ("ofx", "qfx", "qbo", "qif", "iif"):
        assert name in KNOWN_SOURCE_CLASSES
        assert name in INGESTERS, f"INGESTERS missing {name!r}"


def test_preview_ofx_returns_synthetic_sheet(tmp_path: Path):
    p = tmp_path / "stmt.ofx"
    p.write_text(OFX_1X_BANK)
    previews = preview_workbook(p)
    assert len(previews) == 1
    pv = previews[0]
    assert pv.sheet_name == "(ofx)"
    assert "Date" in pv.columns
    assert "FITID" in pv.columns
    assert pv.row_count == 3


def test_preview_ofx_2x_xml_handled(tmp_path: Path):
    p = tmp_path / "stmt.ofx"
    p.write_text(OFX_2X_XML)
    previews = preview_workbook(p)
    assert previews[0].sheet_name == "(ofx)"
    assert previews[0].row_count == 1


def test_preview_qfx_extension_qbo_routes_to_qbo(tmp_path: Path):
    # Same body as a QFX file but with a .qbo extension. Preview's
    # detect step should refine the format to 'qbo'.
    p = tmp_path / "intuit.qbo"
    p.write_text(QFX_INTU_BID)
    previews = preview_workbook(p)
    assert previews[0].sheet_name == "(qbo)"


def test_preview_qif(tmp_path: Path):
    p = tmp_path / "stmt.qif"
    p.write_text(QIF_BANK_ISO)
    pv = preview_workbook(p)[0]
    assert pv.sheet_name == "(qif)"
    assert pv.row_count == 3
    assert pv.columns[0] == "Date"


def test_preview_iif(tmp_path: Path):
    p = tmp_path / "qb.iif"
    p.write_text(IIF_BASIC)
    pv = preview_workbook(p)[0]
    assert pv.sheet_name == "(iif)"
    assert pv.row_count == 1


def test_classify_recognizes_structured_sheet_markers():
    # The classifier short-circuits on the sheet-name marker the
    # preview step sets, ignoring filename-based rules.
    sc, stype, entity, _ = classify_source(
        "renamed.txt",  # extension lies — filename rule wouldn't fire
        "(ofx)",
        ["Date", "Amount", "Payee", "Memo", "Type", "FITID"],
        row_count=5,
    )
    assert sc == "ofx"
    assert stype == "primary"

    for marker, expected in [
        ("(qfx)", "qfx"),
        ("(qbo)", "qbo"),
        ("(qif)", "qif"),
        ("(iif)", "iif"),
    ]:
        sc, stype, _, _ = classify_source("file", marker, [], row_count=0)
        assert sc == expected
        assert stype == "primary"


def test_full_pipeline_ofx(db, tmp_path: Path):
    """Drive a file through preview → classify → INGESTERS lookup
    and confirm it reaches the OFX ingester producing rows.
    """
    from lamella.features.import_._db import upsert_source

    p = tmp_path / "renamed.dat"
    p.write_text(OFX_1X_BANK)
    pv = preview_workbook(p)[0]
    sc, stype, _, _ = classify_source(
        p.name, pv.sheet_name, pv.columns, pv.row_count
    )
    assert sc == "ofx"
    ingest_fn = INGESTERS[sc]

    cur = db.execute(
        "INSERT INTO imports (filename, content_sha256, stored_path, status) "
        "VALUES (?, ?, ?, 'classified')",
        (p.name, "ab" * 32, str(p)),
    )
    import_id = cur.lastrowid
    src_id = upsert_source(
        db,
        upload_id=import_id,
        path=p.name,
        sheet_name=pv.sheet_name,
        sheet_type=stype,
        source_class=sc,
    )
    n = ingest_fn(db, src_id, p, pv.sheet_name)
    assert n == 3
