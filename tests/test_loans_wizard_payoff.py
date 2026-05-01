# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP10 — Payoff flow.

Covers:
  - Validation: loan must exist + be active, payoff date/amount/source
    required and well-formed
  - Linear next_step
  - write_plan shape: 3 PlannedWrites in the right order
  - Commit happy path: anchor written, loan re-emitted with
    is_active=False, SQLite row flipped
  - **Named test**: bean-check failure rolls back BOTH the SQLite
    is_active flip AND the ledger writes — the SQLite/ledger
    atomicity property the user called out as worth proving for
    payoff specifically (since the cross-boundary commit is the
    failure mode that would be worst if rollback discipline broke)
"""
from __future__ import annotations

import sqlite3
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from lamella.features.loans.wizard._base import (
    PlannedLoanBalanceAnchor,
    PlannedLoanWrite,
    PlannedSqliteRowUpdate,
    WizardCommitError,
)
from lamella.features.loans.wizard.payoff import PayoffFlow


def _conn_with_loan():
    from lamella.core.db import migrate, connect
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = Path(f.name)
    conn = connect(path)
    migrate(conn)
    conn.execute(
        """
        INSERT INTO loans (slug, display_name, loan_type,
                           original_principal, funded_date,
                           term_months, interest_rate_apr,
                           liability_account_path, interest_account_path,
                           is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("PaidLoan", "Loan to pay off", "auto",
         "30000", "2022-01-01", 60, "5.0",
         "Liabilities:Personal:Bank:PaidLoan",
         "Expenses:Personal:PaidLoan:Interest", 1),
    )
    return conn


def _settings_for(tmp_path):
    main = tmp_path / "main.bean"
    overrides = tmp_path / "connector_overrides.bean"
    accounts = tmp_path / "connector_accounts.bean"
    config = tmp_path / "connector_config.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        'plugin "beancount.plugins.auto_accounts"\n'
        'include "connector_overrides.bean"\n'
        'include "connector_accounts.bean"\n'
        'include "connector_config.bean"\n',
        encoding="utf-8",
    )
    overrides.write_text("", encoding="utf-8")
    accounts.write_text("", encoding="utf-8")
    config.write_text("", encoding="utf-8")
    return SimpleNamespace(
        ledger_main=main,
        connector_overrides_path=overrides,
        connector_accounts_path=accounts,
        connector_config_path=config,
    )


def _payoff_params(**overrides):
    base = {
        "loan_slug": "PaidLoan",
        "payoff_date": "2025-08-01",
        "payoff_amount": "12345.67",
        "payoff_source": "cash",
        "payoff_notes": "Paid off early via lump sum",
    }
    base.update(overrides)
    return base


# --------------------------------------------------------------- validate


class TestValidate:
    def test_loan_required(self):
        flow = PayoffFlow()
        errs = flow.validate("select_loan", {}, None)
        assert any(e.field == "loan_slug" for e in errs)

    def test_loan_must_exist(self):
        flow = PayoffFlow()
        conn = _conn_with_loan()
        errs = flow.validate(
            "select_loan", {"loan_slug": "Nonexistent"}, conn,
        )
        assert any("not in the registry" in e.message for e in errs)

    def test_loan_must_be_active(self):
        flow = PayoffFlow()
        conn = _conn_with_loan()
        conn.execute(
            "UPDATE loans SET is_active = 0 WHERE slug = ?",
            ("PaidLoan",),
        )
        errs = flow.validate(
            "select_loan", {"loan_slug": "PaidLoan"}, conn,
        )
        assert any("already inactive" in e.message for e in errs)

    def test_payoff_date_required(self):
        flow = PayoffFlow()
        conn = _conn_with_loan()
        params = _payoff_params(payoff_date="")
        errs = flow.validate("payoff_details", params, conn)
        assert any(e.field == "payoff_date" for e in errs)

    def test_payoff_amount_must_be_nonneg(self):
        flow = PayoffFlow()
        conn = _conn_with_loan()
        params = _payoff_params(payoff_amount="-1")
        errs = flow.validate("payoff_details", params, conn)
        assert any(e.field == "payoff_amount" for e in errs)

    def test_zero_amount_accepted_for_writeoff(self):
        # The validation explicitly allows 0 (loan forgiven /
        # written off). Make sure that's not flagged.
        flow = PayoffFlow()
        conn = _conn_with_loan()
        params = _payoff_params(payoff_amount="0")
        errs = flow.validate("payoff_details", params, conn)
        assert all(e.field != "payoff_amount" for e in errs)

    def test_payoff_source_must_be_known(self):
        flow = PayoffFlow()
        conn = _conn_with_loan()
        params = _payoff_params(payoff_source="garbage")
        errs = flow.validate("payoff_details", params, conn)
        assert any(e.field == "payoff_source" for e in errs)


# --------------------------------------------------------------- next_step


class TestNextStep:
    def test_linear(self):
        flow = PayoffFlow()
        assert flow.next_step("select_loan", {}, None) == "payoff_details"
        assert flow.next_step("payoff_details", {}, None) is None  # → preview


# --------------------------------------------------------------- write_plan


class TestWritePlan:
    def test_plan_shape(self):
        flow = PayoffFlow()
        conn = _conn_with_loan()
        plan = flow.write_plan(_payoff_params(), conn)
        kinds = [type(p).__name__ for p in plan]
        assert kinds == [
            "PlannedLoanBalanceAnchor",  # 1. final anchor
            "PlannedLoanWrite",           # 2. directive re-emit
            "PlannedSqliteRowUpdate",     # 3. SQLite is_active flip
        ]

    def test_anchor_carries_payoff_meta(self):
        flow = PayoffFlow()
        conn = _conn_with_loan()
        plan = flow.write_plan(_payoff_params(), conn)
        anchor = plan[0]
        assert isinstance(anchor, PlannedLoanBalanceAnchor)
        assert anchor.loan_slug == "PaidLoan"
        assert anchor.as_of_date == "2025-08-01"
        assert anchor.balance == "12345.67"
        assert anchor.source == "cash"
        assert "lump sum" in (anchor.notes or "")

    def test_sqlite_update_marks_inactive(self):
        flow = PayoffFlow()
        conn = _conn_with_loan()
        plan = flow.write_plan(_payoff_params(), conn)
        update = plan[2]
        assert isinstance(update, PlannedSqliteRowUpdate)
        cols = dict(update.set_columns)
        assert cols["is_active"] == 0
        assert cols["payoff_date"] == "2025-08-01"
        assert cols["payoff_amount"] == "12345.67"
        assert update.where_values == ("PaidLoan",)


# --------------------------------------------------------------- commit


class TestCommit:
    def test_commit_happy_path(self, tmp_path):
        flow = PayoffFlow()
        conn = _conn_with_loan()
        settings = _settings_for(tmp_path)
        params = _payoff_params()
        for step_name in flow.steps():
            errs = flow.validate(step_name, params, conn)
            assert errs == [], (step_name, errs)
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(settings.ledger_main)
        result = flow.commit(params, settings, conn, reader)
        assert result.redirect_to == "/settings/loans/PaidLoan"
        assert result.saved_message == "loan-paid-off"

        # SQLite row flipped.
        row = conn.execute(
            "SELECT is_active, payoff_date, payoff_amount FROM loans "
            "WHERE slug = ?", ("PaidLoan",),
        ).fetchone()
        assert row["is_active"] == 0
        assert row["payoff_date"] == "2025-08-01"
        assert row["payoff_amount"] == "12345.67"

        # Ledger has the anchor + the (re-emitted) loan directive.
        config_text = settings.connector_config_path.read_text(encoding="utf-8")
        assert 'custom "loan-balance-anchor" "PaidLoan"' in config_text
        assert 'custom "loan" "PaidLoan"' in config_text

    @pytest.mark.xfail(
        reason="bean-check not on test PATH; rollback isn't triggered. "
        "Pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md.",
        strict=False,
    )
    def test_sqlite_ledger_atomicity_on_bean_check_failure(self, tmp_path):
        """LOAD-BEARING TEST for payoff:
        if the directive write fails bean-check, the SQLite is_active
        UPDATE must also roll back. Otherwise the user would see the
        loan as inactive in the UI but the ledger would show no
        payoff record — silent state divergence.

        Force a malformed loan-type so the directive's bean-check
        fails (unknown plugin metadata isn't really a syntax error
        in Beancount; instead we invalidate via a malformed account
        path through the loan re-emit's existing-paths). Cleanest:
        force the SQLite update to happen first, then a later
        PlannedWrite to fail.

        Simpler: force the loan re-emit with a malformed account
        path that the PlannedLoanWrite passes through to append_loan.
        Bean-check rejects, WizardCommitTxn rolls back files +
        SAVEPOINT, OLD loan stays is_active=1.
        """
        flow = PayoffFlow()
        conn = _conn_with_loan()
        settings = _settings_for(tmp_path)
        # Inject a syntactically-invalid liability path into the
        # loan row so the re-emitted directive fails bean-check.
        conn.execute(
            "UPDATE loans SET liability_account_path = ? WHERE slug = ?",
            ("Liabilities:badleaf", "PaidLoan"),
        )
        params = _payoff_params()
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(settings.ledger_main)

        # Snapshot pre-commit state.
        config_before = settings.connector_config_path.read_bytes()
        accounts_before = settings.connector_accounts_path.read_bytes()
        overrides_before = settings.connector_overrides_path.read_bytes()
        main_before = settings.ledger_main.read_bytes()
        active_before = conn.execute(
            "SELECT is_active FROM loans WHERE slug = ?", ("PaidLoan",),
        ).fetchone()["is_active"]
        assert active_before == 1

        with pytest.raises(WizardCommitError):
            flow.commit(params, settings, conn, reader)

        # All four ledger files restored.
        assert settings.connector_config_path.read_bytes() == config_before
        assert settings.connector_accounts_path.read_bytes() == accounts_before
        assert (
            settings.connector_overrides_path.read_bytes() == overrides_before
        )
        assert settings.ledger_main.read_bytes() == main_before

        # SQLite is_active reverted by SAVEPOINT — this is the
        # property under test. Without SAVEPOINT discipline, the
        # PlannedSqliteRowUpdate would have stuck at is_active=0.
        active_after = conn.execute(
            "SELECT is_active FROM loans WHERE slug = ?", ("PaidLoan",),
        ).fetchone()["is_active"]
        assert active_after == 1, (
            "SQLite is_active should have rolled back to match the "
            "ledger's pre-commit state — atomicity broken"
        )
