# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""End-to-end test of the reconstruct pipeline across all six state
types: write a representative ledger, delete the DB, run reconstruct
against a fresh DB, assert the state came back."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from beancount import loader

from lamella.features.budgets.writer import append_budget
from lamella.core.db import connect, migrate
from lamella.features.paperless_bridge.field_map_writer import append_field_mapping
from lamella.features.receipts.dismissals_writer import append_dismissal
from lamella.features.recurring.writer import append_recurring_confirmed
from lamella.features.rules.rule_writer import append_rule
from lamella.core.settings.writer import append_setting
from lamella.core.transform.reconstruct import _import_all_steps, run_all


def _prepare_opens(ledger_dir: Path, *accounts: str) -> None:
    accounts_bean = ledger_dir / "accounts.bean"
    body = accounts_bean.read_text(encoding="utf-8")
    for a in accounts:
        body += f"2020-01-01 open {a} USD\n"
    accounts_bean.write_text(body, encoding="utf-8")


@pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
def test_all_steps_round_trip_end_to_end(ledger_dir: Path, tmp_path):
    """Write one of each state type, then run reconstruct into a
    fresh SQLite and verify every table gets repopulated."""
    main_bean = ledger_dir / "main.bean"
    _prepare_opens(
        ledger_dir,
        "Expenses:Personal:Food:Groceries",
        "Liabilities:Personal:BankOne:VisaSignature",
        "Expenses:Personal:Subscriptions:Streaming",
    )

    # Step 1: receipt dismissal.
    append_dismissal(
        connector_links=ledger_dir / "connector_links.bean",
        main_bean=main_bean,
        txn_hash="e2e-dismiss-1",
        reason="parking meter",
        run_check=False,
    )
    # Step 2: classification rule.
    append_rule(
        connector_rules=ledger_dir / "connector_rules.bean",
        main_bean=main_bean,
        pattern_type="merchant_contains",
        pattern_value="grocer",
        target_account="Expenses:Personal:Food:Groceries",
        run_check=False,
    )
    # Step 3: budget.
    append_budget(
        connector_budgets=ledger_dir / "connector_budgets.bean",
        main_bean=main_bean,
        label="E2E Groceries",
        entity="Personal",
        account_pattern=r"^Expenses:Personal:Food",
        period="monthly",
        amount=Decimal("600"),
        run_check=False,
    )
    # Step 4: paperless field mapping.
    append_field_mapping(
        connector_config=ledger_dir / "connector_config.bean",
        main_bean=main_bean,
        paperless_field_id=99,
        paperless_field_name="E2E Total",
        canonical_role="total",
        run_check=False,
    )
    # Step 5: recurring confirmation.
    append_recurring_confirmed(
        connector_rules=ledger_dir / "connector_rules.bean",
        main_bean=main_bean,
        label="E2E Streaming",
        entity="Personal",
        source_account="Liabilities:Personal:BankOne:VisaSignature",
        target_account="Expenses:Personal:Subscriptions:Streaming",
        merchant_pattern="streaming",
        cadence="monthly",
        expected_amount=Decimal("14.99"),
        run_check=False,
    )
    # Step 6: non-secret setting.
    append_setting(
        connector_config=ledger_dir / "connector_config.bean",
        main_bean=main_bean,
        key="mileage_rate",
        value="0.67",
        run_check=False,
    )

    entries, errors, _ = loader.load_file(str(main_bean))
    assert errors == [], f"ledger has errors: {errors}"

    # Fresh SQLite — simulate "user deleted the DB."
    db = connect(tmp_path / "e2e.sqlite")
    migrate(db)

    _import_all_steps()
    reports = run_all(db, list(entries))

    pass_names = {r.pass_name for r in reports}
    assert "step1:receipt-dismissals" in pass_names
    assert "step2:classification-rules" in pass_names
    assert "step3:budgets" in pass_names
    assert "step4:paperless-fields" in pass_names
    assert "step5:recurring-confirmations" in pass_names
    assert "step6:settings-overrides" in pass_names

    # Spot-check each table populated.
    assert db.execute(
        "SELECT COUNT(*) AS n FROM receipt_dismissals"
    ).fetchone()["n"] == 1
    assert db.execute(
        "SELECT COUNT(*) AS n FROM classification_rules"
    ).fetchone()["n"] == 1
    assert db.execute(
        "SELECT COUNT(*) AS n FROM budgets"
    ).fetchone()["n"] == 1
    assert db.execute(
        "SELECT COUNT(*) AS n FROM paperless_field_map"
    ).fetchone()["n"] == 1
    assert db.execute(
        "SELECT COUNT(*) AS n FROM recurring_expenses"
    ).fetchone()["n"] == 1
    assert db.execute(
        "SELECT value FROM app_settings WHERE key = 'mileage_rate'"
    ).fetchone()["value"] == "0.67"


def test_reconstruct_refuses_non_empty_db_without_force(ledger_dir: Path, tmp_path):
    """Safety rail: don't silently rebuild on top of existing state."""
    main_bean = ledger_dir / "main.bean"
    _prepare_opens(ledger_dir, "Expenses:Personal:Food:Groceries")
    append_rule(
        connector_rules=ledger_dir / "connector_rules.bean",
        main_bean=main_bean,
        pattern_type="merchant_contains",
        pattern_value="grocer",
        target_account="Expenses:Personal:Food:Groceries",
        run_check=False,
    )

    db = connect(tmp_path / "populated.sqlite")
    migrate(db)
    # Pre-populate a state table.
    db.execute(
        "INSERT INTO receipt_dismissals (txn_hash, reason) VALUES (?, ?)",
        ("existing", "pre-existing"),
    )

    entries, _, _ = loader.load_file(str(main_bean))

    _import_all_steps()
    import pytest
    with pytest.raises(RuntimeError, match="refused"):
        run_all(db, list(entries), force=False)


def test_reconstruct_force_wipes_state_only(ledger_dir: Path, tmp_path):
    """With --force, state tables are wiped before rebuilding. Cache
    tables (paperless_doc_index, receipt_links, etc.) are preserved.
    """
    main_bean = ledger_dir / "main.bean"
    _prepare_opens(ledger_dir, "Expenses:Personal:Food:Groceries")
    append_rule(
        connector_rules=ledger_dir / "connector_rules.bean",
        main_bean=main_bean,
        pattern_type="merchant_contains",
        pattern_value="grocer",
        target_account="Expenses:Personal:Food:Groceries",
        run_check=False,
    )

    db = connect(tmp_path / "force.sqlite")
    migrate(db)
    # State row that should be wiped.
    db.execute(
        "INSERT INTO receipt_dismissals (txn_hash) VALUES (?)",
        ("will-be-wiped",),
    )
    # Cache row that should survive.
    db.execute(
        "INSERT INTO receipt_links (paperless_id, txn_hash, txn_date, txn_amount) "
        "VALUES (?, ?, ?, ?)",
        (1, "cached-link", "2026-01-01", 10.0),
    )

    entries, _, _ = loader.load_file(str(main_bean))
    _import_all_steps()
    run_all(db, list(entries), force=True)

    # receipt_dismissals was wiped (no directive for "will-be-wiped").
    assert not db.execute(
        "SELECT 1 FROM receipt_dismissals WHERE txn_hash = ?",
        ("will-be-wiped",),
    ).fetchone()
    # receipt_links survived (cache table, not wiped).
    assert db.execute(
        "SELECT 1 FROM receipt_links WHERE txn_hash = ?",
        ("cached-link",),
    ).fetchone()
