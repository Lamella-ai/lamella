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
from lamella.features.reports.schedule_f_pdf import (
    build_context,
    render_schedule_f_html,
    render_schedule_f_pdf,
)


CONFIG = Path(__file__).resolve().parents[1] / "config" / "schedule_f_lines.yml"


def _farm_ledger(tmp_path: Path) -> LedgerReader:
    main = tmp_path / "main.bean"
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Assets:Farm:Checking USD\n"
        "2023-01-01 open Income:Farm:Sales USD\n"
        "2023-01-01 open Expenses:Farm:Feed USD\n"
        "2023-01-01 open Equity:Farm:Opening-Balances USD\n",
        encoding="utf-8",
    )
    main.write_text(
        'option "title" "x"\noption "operating_currency" "USD"\n'
        'include "accounts.bean"\n\n'
        '2025-04-15 * "Coop" "Feed"\n'
        "  Assets:Farm:Checking -300.00 USD\n"
        "  Expenses:Farm:Feed 300.00 USD\n\n"
        '2025-05-01 * "Buyer" "Cattle sale"\n'
        "  Assets:Farm:Checking 4000.00 USD\n"
        "  Income:Farm:Sales -4000.00 USD\n",
        encoding="utf-8",
    )
    return LedgerReader(main)


def test_schedule_f_html_basic(tmp_path: Path):
    line_map = load_line_map(CONFIG)
    reader = _farm_ledger(tmp_path)
    ctx = build_context(
        entity="Farm", year=2025, entries=reader.load().entries, line_map=line_map,
    )
    html = render_schedule_f_html(ctx)
    assert "Schedule F" in html
    assert "Income:Farm:Sales" in html


def test_schedule_f_pdf_render_or_skip(tmp_path: Path):
    from tests._weasy import require_weasyprint

    require_weasyprint()
    line_map = load_line_map(CONFIG)
    reader = _farm_ledger(tmp_path)
    ctx = build_context(
        entity="Farm", year=2025, entries=reader.load().entries, line_map=line_map,
    )
    try:
        pdf = render_schedule_f_pdf(ctx)
    except PDFRenderingUnavailable as exc:
        pytest.skip(f"WeasyPrint native libs unavailable: {exc}")
    assert pdf.startswith(b"%PDF-")
