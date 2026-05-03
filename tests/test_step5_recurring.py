# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from beancount import loader

from lamella.features.recurring.writer import (
    append_recurring_confirmed,
    append_recurring_ignored,
    append_recurring_revoke,
    read_recurring_from_entries,
)


def _load(main_bean: Path) -> list:
    entries, _errors, _ = loader.load_file(str(main_bean))
    return list(entries)


def _prepare_opens(ledger_dir: Path, *accounts: str) -> None:
    accounts_bean = ledger_dir / "accounts.bean"
    body = accounts_bean.read_text(encoding="utf-8")
    for a in accounts:
        body += f"2020-01-01 open {a} USD\n"
    accounts_bean.write_text(body, encoding="utf-8")


def test_confirm_round_trips(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    rules_path = ledger_dir / "connector_rules.bean"
    _prepare_opens(
        ledger_dir,
        "Expenses:Personal:Subscriptions:Streaming",
        "Liabilities:Personal:BankOne:VisaSignature",
    )
    block = append_recurring_confirmed(
        connector_rules=rules_path, main_bean=main_bean,
        label="Streaming", entity="Personal",
        source_account="Liabilities:Personal:BankOne:VisaSignature",
        target_account="Expenses:Personal:Subscriptions:Streaming",
        merchant_pattern="streaming",
        cadence="monthly",
        expected_amount=Decimal("14.99"),
        expected_day=15,
        run_check=False,
    )
    assert 'custom "recurring-confirmed" "Streaming"' in block
    assert "lamella-target-account: Expenses:Personal:Subscriptions:Streaming" in block
    assert 'lamella-cadence: "monthly"' in block
    assert "lamella-amount-hint: 14.99 USD" in block
    assert "lamella-expected-day: 15" in block
    entries, errors, _ = loader.load_file(str(main_bean))
    assert errors == []

    rows = read_recurring_from_entries(_load(main_bean))
    assert len(rows) == 1
    assert rows[0]["status"] == "confirmed"
    assert rows[0]["target_account"] == "Expenses:Personal:Subscriptions:Streaming"


def test_ignore_records_status(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    rules_path = ledger_dir / "connector_rules.bean"
    _prepare_opens(ledger_dir, "Liabilities:Personal:BankOne:VisaSignature")
    append_recurring_ignored(
        connector_rules=rules_path, main_bean=main_bean,
        label="NoiseVendor",
        source_account="Liabilities:Personal:BankOne:VisaSignature",
        merchant_pattern="noisevendor",
        run_check=False,
    )
    rows = read_recurring_from_entries(_load(main_bean))
    assert len(rows) == 1
    assert rows[0]["status"] == "ignored"


def test_revoke_removes_recurring(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    rules_path = ledger_dir / "connector_rules.bean"
    _prepare_opens(
        ledger_dir,
        "Expenses:Personal:Subscriptions:Streaming",
        "Liabilities:Personal:BankOne:VisaSignature",
    )
    append_recurring_confirmed(
        connector_rules=rules_path, main_bean=main_bean,
        label="Streaming", entity="Personal",
        source_account="Liabilities:Personal:BankOne:VisaSignature",
        target_account="Expenses:Personal:Subscriptions:Streaming",
        merchant_pattern="streaming", cadence="monthly",
        expected_amount=Decimal("14.99"), run_check=False,
    )
    append_recurring_revoke(
        connector_rules=rules_path, main_bean=main_bean,
        label="Streaming",
        source_account="Liabilities:Personal:BankOne:VisaSignature",
        merchant_pattern="streaming", run_check=False,
    )
    rows = read_recurring_from_entries(_load(main_bean))
    assert not any(r["merchant_pattern"] == "streaming" for r in rows)


@pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
def test_reconstruct_rebuilds(ledger_dir: Path, tmp_path):
    from lamella.core.db import connect, migrate

    main_bean = ledger_dir / "main.bean"
    rules_path = ledger_dir / "connector_rules.bean"
    _prepare_opens(
        ledger_dir,
        "Expenses:Personal:Subscriptions:Streaming",
        "Liabilities:Personal:BankOne:VisaSignature",
    )
    append_recurring_confirmed(
        connector_rules=rules_path, main_bean=main_bean,
        label="Streaming", entity="Personal",
        source_account="Liabilities:Personal:BankOne:VisaSignature",
        target_account="Expenses:Personal:Subscriptions:Streaming",
        merchant_pattern="streaming", cadence="monthly",
        expected_amount=Decimal("14.99"), expected_day=15, run_check=False,
    )

    db = connect(tmp_path / "rc.sqlite")
    migrate(db)

    import lamella.core.transform.steps.step5_recurring_confirmations  # noqa: F401
    from lamella.core.transform.reconstruct import run_all

    reports = run_all(db, _load(main_bean))
    assert any(r.pass_name == "step5:recurring-confirmations" for r in reports)

    rows = db.execute(
        "SELECT label, status, merchant_pattern, cadence FROM recurring_expenses "
        "ORDER BY label"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["label"] == "Streaming"
    assert rows[0]["status"] == "confirmed"
    assert rows[0]["merchant_pattern"] == "streaming"
