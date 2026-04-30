# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from lamella.features.mileage.beancount_writer import (
    EQUITY_ACCOUNT,
    MileageBeancountWriter,
    MileageSummaryError,
)
from lamella.features.mileage.service import YearlySummaryRow


FIXTURES = Path(__file__).parent / "fixtures" / "mileage"


@pytest.fixture
def ledger_with_equity(ledger_dir: Path) -> Path:
    """Ledger fixture extended with the Equity:MileageDeductions open."""
    accounts = ledger_dir / "accounts.bean"
    text = accounts.read_text(encoding="utf-8")
    if EQUITY_ACCOUNT not in text:
        accounts.write_text(
            text + f"\n2023-01-01 open {EQUITY_ACCOUNT} USD\n", encoding="utf-8"
        )
    # Open the per-entity Mileage accounts the writer uses.
    accounts.write_text(
        accounts.read_text(encoding="utf-8")
        + "\n2023-01-01 open Expenses:Acme:Mileage USD\n"
        + "2023-01-01 open Expenses:Personal:Mileage USD\n",
        encoding="utf-8",
    )
    return ledger_dir


def _stub_check(monkeypatch):
    monkeypatch.setattr(
        "lamella.features.mileage.beancount_writer.run_bean_check",
        lambda main: None,
    )


def _rows_two_pairs() -> list[YearlySummaryRow]:
    return [
        YearlySummaryRow(vehicle="SuvA", entity="Acme", miles=140.0, deduction_usd=93.80),
        YearlySummaryRow(vehicle="TruckB", entity="Personal", miles=55.0, deduction_usd=36.85),
    ]


def test_write_year_first_time(ledger_with_equity: Path, monkeypatch):
    _stub_check(monkeypatch)
    summary_path = ledger_with_equity / "mileage_summary.bean"
    writer = MileageBeancountWriter(
        main_bean=ledger_with_equity / "main.bean",
        summary_path=summary_path,
    )
    res = writer.write_year(year=2026, rows=_rows_two_pairs(), rate_per_mile=0.67)
    assert res.replaced is False
    assert res.rows_written == 2
    assert summary_path.exists()
    text = summary_path.read_text(encoding="utf-8")
    assert ";; BEGIN year=2026" in text
    assert ";; END year=2026" in text
    assert "Expenses:Acme:Mileage" in text
    assert "Expenses:Personal:Mileage" in text
    main_text = (ledger_with_equity / "main.bean").read_text(encoding="utf-8")
    assert 'include "mileage_summary.bean"' in main_text


def test_write_year_idempotent_replaces_block(ledger_with_equity: Path, monkeypatch):
    _stub_check(monkeypatch)
    summary_path = ledger_with_equity / "mileage_summary.bean"
    writer = MileageBeancountWriter(
        main_bean=ledger_with_equity / "main.bean",
        summary_path=summary_path,
    )
    writer.write_year(year=2026, rows=_rows_two_pairs(), rate_per_mile=0.67)
    text_after_first = summary_path.read_text(encoding="utf-8")
    res = writer.write_year(year=2026, rows=_rows_two_pairs(), rate_per_mile=0.67)
    assert res.replaced is True
    text_after_second = summary_path.read_text(encoding="utf-8")
    # Same content shape (one BEGIN/END pair for 2026), not duplicated.
    assert text_after_first.count(";; BEGIN year=2026") == 1
    assert text_after_second.count(";; BEGIN year=2026") == 1


def test_write_year_revert_on_bean_check_fail(ledger_with_equity: Path, monkeypatch):
    from lamella.core.ledger_writer import BeanCheckError

    def _fail(_main):
        raise BeanCheckError("boom")

    monkeypatch.setattr(
        "lamella.features.mileage.beancount_writer.run_bean_check", _fail
    )
    summary_path = ledger_with_equity / "mileage_summary.bean"
    writer = MileageBeancountWriter(
        main_bean=ledger_with_equity / "main.bean",
        summary_path=summary_path,
    )
    main_before = (ledger_with_equity / "main.bean").read_bytes()
    summary_before = summary_path.read_bytes() if summary_path.exists() else None
    with pytest.raises(BeanCheckError):
        writer.write_year(year=2026, rows=_rows_two_pairs(), rate_per_mile=0.67)
    assert (ledger_with_equity / "main.bean").read_bytes() == main_before
    if summary_before is None:
        # The header file was created but should match a clean header-only
        # file after revert.
        assert not summary_path.exists() or summary_path.read_bytes() == summary_before
    else:
        assert summary_path.read_bytes() == summary_before


def test_write_year_requires_equity_open(ledger_dir: Path, monkeypatch):
    _stub_check(monkeypatch)
    summary_path = ledger_dir / "mileage_summary.bean"
    writer = MileageBeancountWriter(
        main_bean=ledger_dir / "main.bean",
        summary_path=summary_path,
    )
    with pytest.raises(MileageSummaryError, match="not opened"):
        writer.write_year(year=2026, rows=_rows_two_pairs(), rate_per_mile=0.67)


def test_write_year_no_rows_raises(ledger_with_equity: Path, monkeypatch):
    _stub_check(monkeypatch)
    summary_path = ledger_with_equity / "mileage_summary.bean"
    writer = MileageBeancountWriter(
        main_bean=ledger_with_equity / "main.bean",
        summary_path=summary_path,
    )
    with pytest.raises(MileageSummaryError, match="no mileage rows"):
        writer.write_year(year=2026, rows=[], rate_per_mile=0.67)
