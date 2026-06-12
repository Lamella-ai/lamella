# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

from beancount import loader

from lamella.features.rules.rule_writer import (
    append_rule,
    append_rule_revoke,
    read_rules_from_entries,
)


def _load(main_bean: Path) -> list:
    entries, _errors, _ = loader.load_file(str(main_bean))
    return list(entries)


def _prepare_open(ledger_dir: Path, account: str) -> None:
    accounts = ledger_dir / "accounts.bean"
    accounts.write_text(
        accounts.read_text(encoding="utf-8")
        + f"2020-01-01 open {account} USD\n",
        encoding="utf-8",
    )


def test_append_rule_writes_valid_directive(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    rules_path = ledger_dir / "connector_rules.bean"
    _prepare_open(ledger_dir, "Expenses:Personal:Food:Groceries")
    block = append_rule(
        connector_rules=rules_path,
        main_bean=main_bean,
        pattern_type="merchant_contains",
        pattern_value="grocer",
        target_account="Expenses:Personal:Food:Groceries",
        run_check=False,
    )
    assert 'custom "classification-rule" "grocer"' in block
    assert "lamella-target-account: Expenses:Personal:Food:Groceries" in block
    assert 'lamella-pattern-type: "merchant_contains"' in block
    assert 'lamella-created-by: "user"' in block
    entries, errors, _ = loader.load_file(str(main_bean))
    assert errors == []


def test_revoke_removes_rule(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    rules_path = ledger_dir / "connector_rules.bean"
    _prepare_open(ledger_dir, "Expenses:Personal:Food:Groceries")
    append_rule(
        connector_rules=rules_path, main_bean=main_bean,
        pattern_type="merchant_contains", pattern_value="grocer",
        target_account="Expenses:Personal:Food:Groceries",
        run_check=False,
    )
    append_rule_revoke(
        connector_rules=rules_path, main_bean=main_bean,
        pattern_type="merchant_contains", pattern_value="grocer",
        target_account="Expenses:Personal:Food:Groceries",
        run_check=False,
    )
    rows = read_rules_from_entries(_load(main_bean))
    assert not any(
        r["pattern_value"] == "grocer" and r["target_account"].endswith("Groceries")
        for r in rows
    )


def test_rule_identity_includes_card_account(ledger_dir: Path):
    main_bean = ledger_dir / "main.bean"
    rules_path = ledger_dir / "connector_rules.bean"
    _prepare_open(ledger_dir, "Expenses:Personal:Food:Groceries")
    _prepare_open(ledger_dir, "Liabilities:Personal:BankOne:VisaSignature")

    # Two rules with same pattern/target but different card scope.
    append_rule(
        connector_rules=rules_path, main_bean=main_bean,
        pattern_type="merchant_contains", pattern_value="grocer",
        target_account="Expenses:Personal:Food:Groceries",
        card_account="Liabilities:Personal:BankOne:VisaSignature",
        run_check=False,
    )
    append_rule(
        connector_rules=rules_path, main_bean=main_bean,
        pattern_type="merchant_contains", pattern_value="grocer",
        target_account="Expenses:Personal:Food:Groceries",
        run_check=False,
    )
    rows = read_rules_from_entries(_load(main_bean))
    # Two distinct rules.
    assert len(rows) == 2


def test_reconstruct_rebuilds_rules(ledger_dir: Path, tmp_path):
    from lamella.core.db import connect, migrate

    main_bean = ledger_dir / "main.bean"
    rules_path = ledger_dir / "connector_rules.bean"
    _prepare_open(ledger_dir, "Expenses:Personal:Food:Groceries")
    _prepare_open(ledger_dir, "Expenses:Acme:Supplies")
    append_rule(
        connector_rules=rules_path, main_bean=main_bean,
        pattern_type="merchant_contains", pattern_value="grocer",
        target_account="Expenses:Personal:Food:Groceries",
        run_check=False,
    )
    append_rule(
        connector_rules=rules_path, main_bean=main_bean,
        pattern_type="merchant_exact", pattern_value="Amazon Marketplace",
        target_account="Expenses:Acme:Supplies",
        run_check=False,
    )

    db = connect(tmp_path / "rc.sqlite")
    migrate(db)

    import lamella.core.transform.steps.step2_classification_rules  # noqa: F401
    from lamella.core.transform.reconstruct import run_all

    entries = _load(main_bean)
    reports = run_all(db, entries)
    assert any(r.pass_name == "step2:classification-rules" for r in reports)

    rows = db.execute(
        "SELECT pattern_value, target_account FROM classification_rules "
        "ORDER BY pattern_value"
    ).fetchall()
    values = {r["pattern_value"] for r in rows}
    assert values == {"grocer", "Amazon Marketplace"}
