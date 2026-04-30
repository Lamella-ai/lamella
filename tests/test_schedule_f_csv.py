# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from decimal import Decimal

from lamella.core.beancount_io import LedgerReader
from lamella.features.reports.line_map import load_line_map
from lamella.features.reports.schedule_f import build_schedule_f


_APPEND = """
2024-01-01 open Expenses:FarmCo:Feed USD
2024-01-01 open Expenses:FarmCo:Seeds USD

2026-04-01 * "Tractor Supply" "Feed purchase"
  Liabilities:Acme:Card:CardA1234   -200.00 USD
  Expenses:FarmCo:Feed             200.00 USD

2026-05-10 * "Johnny's Seeds" "Spring planting"
  Liabilities:Acme:Card:CardA1234    -40.00 USD
  Expenses:FarmCo:Seeds             40.00 USD
"""


def test_schedule_f_uses_its_own_line_map(ledger_dir, tmp_path):
    p = ledger_dir / "simplefin_transactions.bean"
    p.write_text(p.read_text(encoding="utf-8") + _APPEND, encoding="utf-8")

    reader = LedgerReader(ledger_dir / "main.bean")
    entries = reader.load().entries

    config_dir = tmp_path / "cfg"
    config_dir.mkdir()
    (config_dir / "schedule_f.yml").write_text(
        "- line: 16\n"
        "  description: Feed\n"
        "  account_patterns:\n"
        "    - \"Expenses:[^:]+:Feed($|:)\"\n"
        "- line: 26\n"
        "  description: Seeds and plants\n"
        "  account_patterns:\n"
        "    - \"Expenses:[^:]+:Seeds($|:)\"\n",
        encoding="utf-8",
    )
    line_map = load_line_map(config_dir / "schedule_f.yml")

    report = build_schedule_f(
        entity="FarmCo", year=2026, entries=entries, line_map=line_map
    )
    totals = {t.line: t.amount for t in report.summary}
    assert totals[16] == Decimal("200.00")
    assert totals[26] == Decimal("40.00")
