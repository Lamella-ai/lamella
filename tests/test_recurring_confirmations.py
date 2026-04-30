# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from lamella.core.beancount_io import LedgerReader
from lamella.features.recurring.detector import run_detection
from lamella.features.recurring.service import (
    RecurringService,
    RecurringStatus,
    RecurringValidationError,
)


def _build_ledger(tmp_path: Path, *, today: date) -> Path:
    main = tmp_path / "main.bean"
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Assets:Personal:Checking USD\n"
        "2023-01-01 open Expenses:Personal:Mortgage USD\n"
        "2023-01-01 open Equity:Personal:Opening-Balances USD\n",
        encoding="utf-8",
    )
    txns: list[str] = [
        '2024-01-15 * "Opening" "balance"\n'
        "  Assets:Personal:Checking 50000.00 USD\n"
        "  Equity:Personal:Opening-Balances -50000.00 USD\n",
    ]
    for i in range(6):
        d = today - timedelta(days=30 * (i + 1))
        txns.append(
            f'{d.isoformat()} * "Bigbank Mortgage" "Monthly payment"\n'
            f"  Assets:Personal:Checking -1200.00 USD\n"
            f"  Expenses:Personal:Mortgage 1200.00 USD\n"
        )
    main.write_text(
        'option "title" "x"\n'
        'option "operating_currency" "USD"\n'
        'include "accounts.bean"\n\n' + "\n".join(txns),
        encoding="utf-8",
    )
    return main


def _detect(db, tmp_path: Path) -> int:
    today = date(2026, 4, 20)
    reader = LedgerReader(_build_ledger(tmp_path, today=today))
    run_detection(conn=db, entries=reader.load().entries, today=today)
    proposals = RecurringService(db).list(status=RecurringStatus.PROPOSED.value)
    assert len(proposals) >= 1
    return proposals[0].id


def test_confirm_promotes_status(db, tmp_path: Path):
    rid = _detect(db, tmp_path)
    service = RecurringService(db)
    confirmed = service.confirm(
        rid, label="Mortgage", expected_day=15,
        source_account="Assets:Personal:Checking",
        open_accounts={"Assets:Personal:Checking"},
    )
    assert confirmed.status == RecurringStatus.CONFIRMED
    assert confirmed.label == "Mortgage"


def test_confirm_rejects_unknown_account(db, tmp_path: Path):
    rid = _detect(db, tmp_path)
    service = RecurringService(db)
    with pytest.raises(RecurringValidationError, match="not opened"):
        service.confirm(
            rid, source_account="Assets:Nonexistent",
            open_accounts={"Assets:Personal:Checking"},
        )


def test_ignore_quarantines_for_90_days(db, tmp_path: Path):
    rid = _detect(db, tmp_path)
    service = RecurringService(db)
    service.ignore(rid)
    expense = service.get(rid)
    assert expense.status == RecurringStatus.IGNORED
    assert service.in_quarantine(
        expense.merchant_pattern, expense.source_account,
        now=datetime.now(UTC) + timedelta(days=10),
    )


def test_quarantine_expires_after_90_days(db, tmp_path: Path):
    rid = _detect(db, tmp_path)
    service = RecurringService(db)
    service.ignore(rid)
    expense = service.get(rid)
    assert not service.in_quarantine(
        expense.merchant_pattern, expense.source_account,
        now=datetime.now(UTC) + timedelta(days=91),
    )


def test_re_detection_skips_quarantined(db, tmp_path: Path):
    rid = _detect(db, tmp_path)
    service = RecurringService(db)
    service.ignore(rid)
    today = date(2026, 4, 20)
    reader = LedgerReader(_build_ledger(tmp_path, today=today))
    result = run_detection(conn=db, entries=reader.load().entries, today=today)
    # candidates still found, but the proposal is skipped (left ignored).
    assert result.candidates_found >= 1
    assert RecurringService(db).get(rid).status == RecurringStatus.IGNORED


def test_update_proposal_only_for_proposed(db, tmp_path: Path):
    rid = _detect(db, tmp_path)
    service = RecurringService(db)
    service.update_proposal(rid, label="renamed", expected_day=10)
    fresh = service.get(rid)
    assert fresh.label == "renamed"
    service.confirm(rid, source_account=fresh.source_account, open_accounts={fresh.source_account})
    with pytest.raises(RecurringValidationError, match="proposed"):
        service.update_proposal(rid, label="oops")
