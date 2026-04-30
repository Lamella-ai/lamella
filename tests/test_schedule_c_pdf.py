# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.beancount_io import LedgerReader
from lamella.features.reports._pdf import PDFRenderingUnavailable
from lamella.features.reports.line_map import load_line_map
from lamella.features.reports.schedule_c_pdf import (
    build_context,
    render_schedule_c_html,
    render_schedule_c_pdf,
)


CONFIG = Path(__file__).resolve().parents[1] / "config" / "schedule_c_lines.yml"


def _ledger_with_supplies(tmp_path: Path) -> LedgerReader:
    main = tmp_path / "main.bean"
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Assets:Acme:Checking USD\n"
        "2023-01-01 open Income:Acme:Sales USD\n"
        "2023-01-01 open Expenses:Acme:Supplies USD\n"
        "2023-01-01 open Expenses:Acme:Shipping USD\n"
        "2023-01-01 open Equity:Acme:Opening-Balances USD\n",
        encoding="utf-8",
    )
    main.write_text(
        'option "title" "x"\noption "operating_currency" "USD"\n'
        'include "accounts.bean"\n\n'
        '2025-02-15 * "Acme" "supplies"\n'
        "  Assets:Acme:Checking -120.00 USD\n"
        "  Expenses:Acme:Supplies 120.00 USD\n\n"
        '2025-03-15 * "Customer" "Sale"\n'
        "  Assets:Acme:Checking 1500.00 USD\n"
        "  Income:Acme:Sales -1500.00 USD\n\n"
        '2025-06-10 * "USPS" "Shipping"\n'
        "  Assets:Acme:Checking -42.00 USD\n"
        "  Expenses:Acme:Shipping 42.00 USD\n",
        encoding="utf-8",
    )
    return LedgerReader(main)


def test_html_render_includes_summary_lines(tmp_path: Path):
    line_map = load_line_map(CONFIG)
    reader = _ledger_with_supplies(tmp_path)
    ctx = build_context(
        entity="Acme", year=2025, entries=reader.load().entries, line_map=line_map,
    )
    html = render_schedule_c_html(ctx)
    assert "Schedule C" in html
    assert "Supplies" in html
    assert "1,500.00" in html  # gross receipts formatted
    assert "120.00" in html


def test_pdf_render_or_skip(tmp_path: Path):
    from tests._weasy import require_weasyprint

    require_weasyprint()
    line_map = load_line_map(CONFIG)
    reader = _ledger_with_supplies(tmp_path)
    ctx = build_context(
        entity="Acme", year=2025, entries=reader.load().entries, line_map=line_map,
    )
    try:
        pdf = render_schedule_c_pdf(ctx)
    except PDFRenderingUnavailable as exc:
        pytest.skip(f"WeasyPrint native libs unavailable: {exc}")
    assert pdf.startswith(b"%PDF-")
    # Cross-check totals with text extraction.
    pdfminer = pytest.importorskip("pdfminer.high_level")
    import io

    text = pdfminer.extract_text(io.BytesIO(pdf))
    assert "Schedule C" in text
    assert "Supplies" in text
    # CSV report total for line 22 (Supplies) should appear.
    assert "120.00" in text
