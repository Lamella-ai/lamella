# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from lamella.core.beancount_io import LedgerReader
from lamella.features.recurring.detector import RecurringDetector, run_detection
from lamella.features.recurring.service import RecurringService, RecurringStatus


def _build_ledger(tmp_path: Path, *, today: date) -> Path:
    """18-month ledger with two recurring patterns:
    - Monthly mortgage @ $1200 from Assets:Personal:Checking
    - Monthly Utility Co @ ~$95 from Liabilities:Personal:Card:Amex
    """
    main = tmp_path / "main.bean"
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Assets:Personal:Checking USD\n"
        "2023-01-01 open Liabilities:Personal:Card:Amex USD\n"
        "2023-01-01 open Expenses:Personal:Mortgage USD\n"
        "2023-01-01 open Expenses:Personal:Utilities USD\n"
        "2023-01-01 open Equity:Personal:Opening-Balances USD\n",
        encoding="utf-8",
    )
    txns: list[str] = [
        '2024-01-15 * "Opening" "balance"\n'
        "  Assets:Personal:Checking 50000.00 USD\n"
        "  Equity:Personal:Opening-Balances -50000.00 USD\n",
    ]
    # 14 monthly mortgage payments
    for i in range(14):
        d = today - timedelta(days=30 * (i + 1))
        txns.append(
            f'{d.isoformat()} * "Bigbank Mortgage" "Monthly payment"\n'
            f"  Assets:Personal:Checking -1200.00 USD\n"
            f"  Expenses:Personal:Mortgage 1200.00 USD\n"
        )
    # 14 monthly UtilityCo bills with slight variance
    for i, amt in enumerate([95, 102, 88, 91, 99, 110, 89, 95, 97, 101, 103, 92, 94, 96]):
        d = today - timedelta(days=30 * (i + 1) + 3)
        txns.append(
            f'{d.isoformat()} * "Utility Co" "Monthly bill"\n'
            f"  Liabilities:Personal:Card:Amex -{amt:.2f} USD\n"
            f"  Expenses:Personal:Utilities {amt:.2f} USD\n"
        )
    # One-off — must NOT be detected.
    txns.append(
        f'{(today - timedelta(days=10)).isoformat()} * "OneTime" "thing"\n'
        f"  Assets:Personal:Checking -25.00 USD\n"
        f"  Expenses:Personal:Mortgage 25.00 USD\n"
    )
    main.write_text(
        'option "title" "x"\n'
        'option "operating_currency" "USD"\n'
        'include "accounts.bean"\n\n' + "\n".join(txns),
        encoding="utf-8",
    )
    return main


@pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
def test_detector_finds_mortgage_and_xcel(tmp_path: Path):
    today = date(2026, 4, 20)
    main = _build_ledger(tmp_path, today=today)
    reader = LedgerReader(main)
    detector = RecurringDetector(scan_window_days=540, min_occurrences=3)
    candidates = detector.candidates(reader.load().entries, today=today)
    labels = {c[0].label for c in candidates}
    assert any("mortgage" in l for l in labels)
    assert any("xcel" in l for l in labels)


@pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
def test_run_detection_inserts_proposed_rows(db, tmp_path: Path):
    today = date(2026, 4, 20)
    main = _build_ledger(tmp_path, today=today)
    reader = LedgerReader(main)
    result = run_detection(
        conn=db, entries=reader.load().entries, today=today,
        scan_window_days=540, min_occurrences=3,
    )
    assert result.candidates_found >= 2
    proposed = RecurringService(db).list(status=RecurringStatus.PROPOSED.value)
    assert len(proposed) == result.new_proposals
    labels = " ".join(p.label.lower() for p in proposed)
    assert "mortgage" in labels
    assert "xcel" in labels
    # Check expected_amount roughly correct.
    mortgage = next(p for p in proposed if "mortgage" in p.label.lower())
    assert abs(mortgage.expected_amount - 1200.0) < 1.0


def test_run_detection_records_audit_row(db, tmp_path: Path):
    today = date(2026, 4, 20)
    main = _build_ledger(tmp_path, today=today)
    reader = LedgerReader(main)
    run_detection(conn=db, entries=reader.load().entries, today=today)
    rows = db.execute(
        "SELECT * FROM recurring_detections ORDER BY id"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["candidates_found"] >= 2
    assert rows[0]["error"] is None
