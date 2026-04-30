# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.features.reports._pdf import PDFRenderingUnavailable
from lamella.features.reports.audit_portfolio import (
    collect_transactions,
    render_audit_html,
    render_audit_pdf,
)
from lamella.features.reports.line_map import load_line_map


CONFIG = Path(__file__).resolve().parents[1] / "config" / "schedule_c_lines.yml"


def _ledger(tmp_path: Path) -> LedgerReader:
    main = tmp_path / "main.bean"
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Liabilities:Acme:Card USD\n"
        "2023-01-01 open Expenses:Acme:Supplies USD\n"
        "2023-01-01 open Expenses:Acme:Shipping USD\n",
        encoding="utf-8",
    )
    main.write_text(
        'option "title" "x"\noption "operating_currency" "USD"\n'
        'include "accounts.bean"\n\n'
        '2025-02-15 * "Acme" "supplies"\n'
        "  Liabilities:Acme:Card -120.00 USD\n"
        "  Expenses:Acme:Supplies 120.00 USD\n\n"
        '2025-03-15 * "USPS" "Shipping"\n'
        "  Liabilities:Acme:Card -42.00 USD\n"
        "  Expenses:Acme:Shipping 42.00 USD\n",
        encoding="utf-8",
    )
    return LedgerReader(main)


def test_collect_picks_up_classified_postings(tmp_path: Path, db):
    line_map = load_line_map(CONFIG)
    reader = _ledger(tmp_path)
    rows = collect_transactions(
        entity="Acme", year=2025, entries=reader.load().entries,
        line_map=line_map, conn=db,
    )
    assert len(rows) == 2
    accounts = {r.account for r in rows}
    assert "Expenses:Acme:Supplies" in accounts


def test_collect_includes_attached_note_and_receipt(tmp_path: Path, db):
    line_map = load_line_map(CONFIG)
    reader = _ledger(tmp_path)
    # Find the txn hash for the supplies row.
    from beancount.core.data import Transaction

    supplies_txn = next(
        e for e in reader.load().entries
        if isinstance(e, Transaction) and "supplies" in (e.narration or "").lower()
    )
    h = txn_hash(supplies_txn)
    db.execute(
        """
        INSERT INTO receipt_links (paperless_id, txn_hash, txn_date, txn_amount, match_method, match_confidence)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (101, h, "2025-02-15", 120.0, "exact", 1.0),
    )
    db.execute(
        "INSERT INTO notes (body, resolved_txn) VALUES (?, ?)",
        ("Bought screws for the rebuild", h),
    )
    rows = collect_transactions(
        entity="Acme", year=2025, entries=reader.load().entries,
        line_map=line_map, conn=db,
    )
    supplies_row = next(r for r in rows if "Supplies" in r.account)
    assert supplies_row.paperless_id == 101
    assert "screws" in (supplies_row.note_body or "").lower()


def test_audit_html_has_cover_and_pages(tmp_path: Path, db):
    line_map = load_line_map(CONFIG)
    reader = _ledger(tmp_path)
    rows = collect_transactions(
        entity="Acme", year=2025, entries=reader.load().entries,
        line_map=line_map, conn=db,
    )
    html = render_audit_html(
        entity="Acme", year=2025, entries=reader.load().entries,
        line_map=line_map, rows=rows,
    )
    assert "Audit portfolio" in html
    assert html.count('class="audit-page"') == len(rows)


async def test_audit_pdf_render_or_skip(tmp_path: Path, db):
    from tests._weasy import require_weasyprint

    require_weasyprint()
    line_map = load_line_map(CONFIG)
    reader = _ledger(tmp_path)
    try:
        pdf = await render_audit_pdf(
            entity="Acme", year=2025, entries=reader.load().entries,
            line_map=line_map, conn=db, paperless_client=None,
            max_receipt_bytes=10_000_000,
        )
    except PDFRenderingUnavailable as exc:
        pytest.skip(f"WeasyPrint native libs unavailable: {exc}")
    assert pdf.startswith(b"%PDF-")
