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

from lamella.features.budgets.writer import (
    append_budget,
    append_budget_revoke,
    read_budgets_from_entries,
)


def _load(main_bean: Path) -> list:
    entries, _errors, _ = loader.load_file(str(main_bean))
    return list(entries)


def test_append_budget_writes_valid_directive(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    budgets_path = ledger_dir / "connector_budgets.bean"
    block = append_budget(
        connector_budgets=budgets_path,
        main_bean=main_bean,
        label="Groceries",
        entity="Personal",
        account_pattern=r"^Expenses:Personal:Food",
        period="monthly",
        amount=Decimal("600.00"),
        alert_threshold=0.85,
        run_check=False,
    )
    assert 'custom "budget" "Groceries" 600.00 USD' in block
    assert "lamella-period: \"monthly\"" in block
    assert 'lamella-alert-threshold: "0.85"' in block
    entries, errors, _ = loader.load_file(str(main_bean))
    assert errors == []


def test_revoke_removes_budget(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    budgets_path = ledger_dir / "connector_budgets.bean"
    append_budget(
        connector_budgets=budgets_path, main_bean=main_bean,
        label="Groceries", entity="Personal",
        account_pattern=r"^Expenses:Personal:Food",
        period="monthly", amount=Decimal("600"), run_check=False,
    )
    append_budget_revoke(
        connector_budgets=budgets_path, main_bean=main_bean,
        label="Groceries", entity="Personal",
        account_pattern=r"^Expenses:Personal:Food",
        period="monthly", run_check=False,
    )
    rows = read_budgets_from_entries(_load(main_bean))
    assert not any(r["label"] == "Groceries" for r in rows)


@pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
def test_reconstruct_rebuilds_budgets(ledger_dir: Path, tmp_path):
    from lamella.core.db import connect, migrate

    main_bean = ledger_dir / "main.bean"
    budgets_path = ledger_dir / "connector_budgets.bean"
    append_budget(
        connector_budgets=budgets_path, main_bean=main_bean,
        label="Groceries", entity="Personal",
        account_pattern=r"^Expenses:Personal:Food",
        period="monthly", amount=Decimal("600"), run_check=False,
    )
    append_budget(
        connector_budgets=budgets_path, main_bean=main_bean,
        label="Supplies", entity="Acme",
        account_pattern=r"^Expenses:Acme:Supplies",
        period="quarterly", amount=Decimal("2500"),
        alert_threshold=0.9, run_check=False,
    )

    db = connect(tmp_path / "rc.sqlite")
    migrate(db)

    import lamella.core.transform.steps.step3_budgets  # noqa: F401
    from lamella.core.transform.reconstruct import run_all

    reports = run_all(db, _load(main_bean))
    assert any(r.pass_name == "step3:budgets" for r in reports)

    rows = db.execute(
        "SELECT label, period, amount FROM budgets ORDER BY label"
    ).fetchall()
    labels = [(r["label"], r["period"], float(r["amount"])) for r in rows]
    assert labels == [
        ("Groceries", "monthly", 600.0),
        ("Supplies", "quarterly", 2500.0),
    ]


def test_budget_supports_user_chosen_period(ledger_dir: Path):
    """lamella-period carries the user-chosen period. Values outside the
    pre-defined enum (rolling-30, yearly, etc.) round-trip too — the
    read path doesn't gatekeep."""
    main_bean = ledger_dir / "main.bean"
    budgets_path = ledger_dir / "connector_budgets.bean"
    append_budget(
        connector_budgets=budgets_path, main_bean=main_bean,
        label="RollingGroceries", entity="Personal",
        account_pattern=r"^Expenses:Personal:Food",
        period="rolling-30", amount=Decimal("600"),
        run_check=False,
    )
    rows = read_budgets_from_entries(_load(main_bean))
    assert any(r["period"] == "rolling-30" for r in rows)
