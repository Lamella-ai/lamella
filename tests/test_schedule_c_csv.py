# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import csv
import io
from decimal import Decimal

from lamella.core.beancount_io import LedgerReader
from lamella.features.reports.line_map import load_line_map
from lamella.features.reports.schedule_c import (
    build_schedule_c,
    stream_detail_csv,
    stream_summary_csv,
)


_APPEND = """
2024-01-01 open Expenses:Acme:Advertising USD

2026-03-05 * "Google Ads" "Q1 ads"
  Liabilities:Acme:Card:CardA1234   -120.00 USD
  Expenses:Acme:Advertising          120.00 USD

2026-06-02 * "Facebook Ads"
  Liabilities:Acme:Card:CardA1234    -80.00 USD
  Expenses:Acme:Advertising           80.00 USD

2025-09-10 * "Stale ads last year — should not appear in 2026"
  Liabilities:Acme:Card:CardA1234    -30.00 USD
  Expenses:Acme:Advertising           30.00 USD
"""


def _write(ledger_dir, append: str):
    p = ledger_dir / "simplefin_transactions.bean"
    p.write_text(p.read_text(encoding="utf-8") + append, encoding="utf-8")


def _consume(stream) -> str:
    return "".join(stream)


def test_schedule_c_summary_and_detail_reconcile(ledger_dir, tmp_path):
    _write(ledger_dir, _APPEND)
    reader = LedgerReader(ledger_dir / "main.bean")
    entries = reader.load().entries

    config_dir = tmp_path / "cfg"
    config_dir.mkdir()
    (config_dir / "schedule_c.yml").write_text(
        "- line: 8\n"
        "  description: Advertising\n"
        "  account_patterns:\n"
        "    - \"Expenses:[^:]+:Advertising($|:)\"\n"
        "    - \"Expenses:[^:]+:Supplies($|:)\"\n"
        "- line: 22\n"
        "  description: Supplies\n"
        "  account_patterns: []\n",
        encoding="utf-8",
    )
    line_map = load_line_map(config_dir / "schedule_c.yml")

    report = build_schedule_c(
        entity="Acme", year=2026, entries=entries, line_map=line_map
    )

    # Supplies entries from fixture (42.17) + Advertising from appends (200)
    # map to line 8 under our broad test pattern; only the 2026-dated ones count.
    totals = {t.line: t.amount for t in report.summary}
    # 120 + 80 (advertising 2026) + 42.17 (supplies 2026 fixture) = 242.17
    assert totals[8] == Decimal("242.17")

    # Stale 2025 ads must NOT appear.
    assert all(row.date.year == 2026 for row in report.detail)

    # Detail sum per line equals summary amount.
    line_8_detail_sum = sum((d.amount for d in report.detail if d.line == 8), Decimal("0"))
    assert line_8_detail_sum == totals[8]

    # CSV streaming shape.
    summary_csv = _consume(stream_summary_csv(report))
    rdr = csv.reader(io.StringIO(summary_csv))
    header = next(rdr)
    assert header == ["line_number", "description", "amount", "txn_count"]
    rows = list(rdr)
    # Only non-zero lines are emitted.
    assert len(rows) == len([t for t in report.summary if t.amount != 0])

    detail_csv = _consume(stream_detail_csv(report))
    drdr = csv.reader(io.StringIO(detail_csv))
    dheader = next(drdr)
    assert dheader == ["date", "narration", "account", "amount", "line_number"]
    drows = list(drdr)
    assert len(drows) == len(report.detail)


def test_schedule_c_ignores_foreign_entity(ledger_dir, tmp_path):
    _write(ledger_dir, _APPEND)
    reader = LedgerReader(ledger_dir / "main.bean")
    entries = reader.load().entries

    config_dir = tmp_path / "cfg"
    config_dir.mkdir()
    (config_dir / "schedule_c.yml").write_text(
        "- line: 22\n"
        "  description: Supplies\n"
        "  account_patterns:\n"
        "    - \"Expenses:[^:]+:Supplies($|:)\"\n",
        encoding="utf-8",
    )
    line_map = load_line_map(config_dir / "schedule_c.yml")

    report = build_schedule_c(entity="Personal", year=2026, entries=entries, line_map=line_map)
    # Personal has no Supplies in the fixture — totals are empty.
    assert report.summary == ()
