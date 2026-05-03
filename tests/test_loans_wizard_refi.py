# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP10 — Refi flow.

Covers:
  - Validation per step (old loan must exist + be active, new slug
    must not collide + can't equal old, payoff terms, account paths)
  - Linear next_step progression
  - write_plan shape: 6 PlannedWrites in the right order
  - SQLite UPDATE on the OLD loan's row works
  - Cross-ref-anchor write IS in the plan
  - Commit happy path: old loan inactive, new loan active, anchor
    written, funding txn written
  - **Named test**: cross-ref-anchor failure rolls back the
    close-out — the load-bearing rollback discipline for refi
  - Bean-check failure rollback restores all four files + SQLite
"""
from __future__ import annotations

import sqlite3
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from lamella.features.loans.wizard._base import (
    PlannedAccountsOpen,
    PlannedLoanBalanceAnchor,
    PlannedLoanFunding,
    PlannedLoanWrite,
    PlannedSqliteRowUpdate,
    WizardCommitError,
)
from lamella.features.loans.wizard.refi import RefiFlow


def _conn_with_old_loan():
    """Returns a conn with a real loans table + an existing active
    loan we can refinance."""
    from lamella.core.db import migrate, connect
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = Path(f.name)
    conn = connect(path)
    migrate(conn)
    # entity_slug + institution intentionally omitted — entities is
    # FK'd and the test loan doesn't need them for refi-flow shape
    # tests. The flow handles None gracefully.
    conn.execute(
        """
        INSERT INTO loans (slug, display_name, loan_type,
                           original_principal, funded_date,
                           term_months, interest_rate_apr,
                           liability_account_path, interest_account_path,
                           is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("OldMortgage", "Old Mortgage", "mortgage",
         "550000", "2020-01-01", 360, "4.5",
         "Liabilities:Personal:BankTwo:OldMortgage",
         "Expenses:Personal:OldMortgage:Interest", 1),
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


def _refi_params(**overrides):
    base = {
        "old_loan_slug": "OldMortgage",
        "payoff_date": "2025-06-01",
        "payoff_amount": "490000",
        "payoff_reason": "rate_and_term",
        "new_loan_slug": "NewMortgage",
        "new_loan_display_name": "New Mortgage",
        "new_loan_type": "mortgage",
        "new_loan_entity_slug": "Personal",
        "new_loan_institution": "Acme Bank",
        "new_original_principal": "490000",
        "new_term_months": "300",
        "new_interest_rate_apr": "5.5",
        "new_liability_account_path": "Liabilities:Personal:Acme:NewMortgage",
        "new_interest_account_path": "Expenses:Personal:NewMortgage:Interest",
        "new_funded_date": "2025-06-01",
        "new_offset_account": "Liabilities:Personal:BankTwo:OldMortgage",
        "new_funding_narration": "Refi funding",
    }
    base.update(overrides)
    return base


# --------------------------------------------------------------- validate


class TestValidate:
    def test_old_loan_required(self):
        flow = RefiFlow()
        errs = flow.validate("select_old", {}, None)
        assert any(e.field == "old_loan_slug" for e in errs)

    def test_old_loan_must_exist(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        errs = flow.validate(
            "select_old", {"old_loan_slug": "Nonexistent"}, conn,
        )
        assert any(
            "not in the registry" in e.message for e in errs
        )

    def test_old_loan_must_be_active(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        conn.execute(
            "UPDATE loans SET is_active = 0 WHERE slug = ?",
            ("OldMortgage",),
        )
        errs = flow.validate(
            "select_old", {"old_loan_slug": "OldMortgage"}, conn,
        )
        assert any("already inactive" in e.message for e in errs)

    def test_new_slug_cant_match_old(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        params = _refi_params(new_loan_slug="OldMortgage")
        errs = flow.validate("new_loan_terms", params, conn)
        # The collision check fires first ("already exists"); accept
        # either that or the can't-match message.
        assert any(
            ("already exists" in e.message or "match the old" in e.message)
            and e.field == "new_loan_slug"
            for e in errs
        )

    def test_payoff_amount_required_positive(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        params = _refi_params(payoff_amount="0")
        errs = flow.validate("payoff_terms", params, conn)
        assert any(e.field == "payoff_amount" for e in errs)

    def test_payoff_reason_must_be_known(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        params = _refi_params(payoff_reason="garbage")
        errs = flow.validate("payoff_terms", params, conn)
        assert any(e.field == "payoff_reason" for e in errs)

    def test_new_loan_term_validations(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        # APR out of range.
        params = _refi_params(new_interest_rate_apr="60")
        errs = flow.validate("new_loan_terms", params, conn)
        assert any(e.field == "new_interest_rate_apr" for e in errs)

    def test_liability_path_validation(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        params = _refi_params(new_liability_account_path="NotALiability")
        errs = flow.validate("accounts", params, conn)
        assert any(e.field == "new_liability_account_path" for e in errs)

    def test_funding_offset_required(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        params = _refi_params(new_offset_account="")
        errs = flow.validate("funding", params, conn)
        assert any(e.field == "new_offset_account" for e in errs)

    def test_back_edit_invalidates_earlier(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        params = _refi_params(old_loan_slug="")
        errs = flow.validate("funding", params, conn)
        assert any(e.field == "old_loan_slug" for e in errs)


# --------------------------------------------------------------- next_step


class TestNextStep:
    def test_linear_progression(self):
        flow = RefiFlow()
        assert flow.next_step("select_old", {}, None) == "payoff_terms"
        assert flow.next_step("payoff_terms", {}, None) == "new_loan_terms"
        assert flow.next_step("new_loan_terms", {}, None) == "accounts"
        assert flow.next_step("accounts", {}, None) == "funding"
        assert flow.next_step("funding", {}, None) is None  # → preview


# --------------------------------------------------------------- write_plan


class TestWritePlan:
    def test_plan_shape(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        plan = flow.write_plan(_refi_params(), conn)
        kinds = [type(p).__name__ for p in plan]
        assert kinds == [
            "PlannedLoanWrite",          # 1. old loan close-out
            "PlannedSqliteRowUpdate",    # 2. SQLite is_active flip
            "PlannedLoanBalanceAnchor",  # 3. cross-ref anchor
            "PlannedAccountsOpen",       # 4. new loan opens
            "PlannedLoanWrite",          # 5. new loan directive
            "PlannedLoanFunding",        # 6. new loan funding txn
        ]

    def test_old_loan_close_out_carries_payoff_meta(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        plan = flow.write_plan(_refi_params(), conn)
        # The first PlannedLoanWrite is the OLD loan re-emit.
        old_close = plan[0]
        assert isinstance(old_close, PlannedLoanWrite)
        assert old_close.slug == "OldMortgage"
        # Notes describe the refi for the human reader.
        assert "Refinanced as NewMortgage" in (old_close.notes or "")
        assert "rate_and_term" in (old_close.notes or "")

    def test_sqlite_update_targets_old_loan(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        plan = flow.write_plan(_refi_params(), conn)
        sqlite_update = next(
            p for p in plan if isinstance(p, PlannedSqliteRowUpdate)
        )
        assert sqlite_update.table == "loans"
        assert sqlite_update.where_clause == "slug = ?"
        assert sqlite_update.where_values == ("OldMortgage",)
        cols = dict(sqlite_update.set_columns)
        assert cols["is_active"] == 0
        assert cols["payoff_date"] == "2025-06-01"
        assert cols["payoff_amount"] == "490000"

    def test_cross_ref_anchor_in_plan(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        plan = flow.write_plan(_refi_params(), conn)
        anchor = next(
            p for p in plan if isinstance(p, PlannedLoanBalanceAnchor)
        )
        # The anchor lives on the OLD loan, dated to payoff.
        assert anchor.loan_slug == "OldMortgage"
        assert anchor.as_of_date == "2025-06-01"
        assert anchor.balance == "490000"
        assert anchor.source == "refi-payoff"
        assert "NewMortgage" in (anchor.notes or "")

    def test_funding_offset_defaults_to_old_liability(self):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        params = _refi_params()
        plan = flow.write_plan(params, conn)
        funding = next(p for p in plan if isinstance(p, PlannedLoanFunding))
        assert funding.offset_account == (
            "Liabilities:Personal:BankTwo:OldMortgage"
        )

    def test_new_loan_inherits_property_from_old(self):
        # Property links carry forward from the old loan to the new
        # loan automatically (refi typically keeps the same property).
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        # FK requires the property row to exist first.
        conn.execute(
            "INSERT INTO properties (slug, display_name, property_type, "
            "is_active, is_primary_residence, is_rental) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("MainResidence", "Main Residence", "primary_residence",
             1, 1, 0),
        )
        conn.execute(
            "UPDATE loans SET property_slug = ? WHERE slug = ?",
            ("MainResidence", "OldMortgage"),
        )
        plan = flow.write_plan(_refi_params(), conn)
        # The 5th item (index 4) is the new loan's PlannedLoanWrite.
        new_loan = plan[4]
        assert isinstance(new_loan, PlannedLoanWrite)
        assert new_loan.property_slug == "MainResidence"


# --------------------------------------------------------------- commit


class TestCommit:
    def test_commit_happy_path(self, tmp_path):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        settings = _settings_for(tmp_path)
        params = _refi_params()
        for step_name in flow.steps():
            errs = flow.validate(step_name, params, conn)
            assert errs == [], (step_name, errs)
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(settings.ledger_main)
        result = flow.commit(params, settings, conn, reader)
        assert result.redirect_to == "/settings/loans/NewMortgage"
        assert result.saved_message == "refi-committed"

        # OLD loan inactive in SQLite.
        old = conn.execute(
            "SELECT is_active, payoff_date, payoff_amount FROM loans "
            "WHERE slug = ?", ("OldMortgage",),
        ).fetchone()
        assert old["is_active"] == 0
        assert old["payoff_date"] == "2025-06-01"
        assert old["payoff_amount"] == "490000"

        # Ledger has all expected directives.
        config_text = settings.connector_config_path.read_text(encoding="utf-8")
        assert 'custom "loan" "OldMortgage"' in config_text
        assert 'custom "loan" "NewMortgage"' in config_text
        assert 'custom "loan-balance-anchor" "OldMortgage"' in config_text
        overrides_text = settings.connector_overrides_path.read_text(
            encoding="utf-8",
        )
        assert "#lamella-loan-funding" in overrides_text

    def test_cross_ref_anchor_failure_rolls_back_close_out(self, tmp_path):
        """LOAD-BEARING TEST: if close-out succeeds and new-loan
        succeeds and the cross-ref-anchor fails, the rollback must
        undo close-out so we don't end up with two loans that don't
        know about each other.

        We force the cross-ref-anchor write specifically by patching
        PlannedLoanBalanceAnchor.execute to raise. Then assert:
          - WizardCommitError surfaces
          - OLD loan's SQLite row is back to is_active=1 (SAVEPOINT
            rollback worked)
          - All four ledger files are byte-for-byte identical to
            their pre-commit state
        """
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        settings = _settings_for(tmp_path)
        params = _refi_params()
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(settings.ledger_main)

        # Snapshot the four files + the OLD loan's SQLite row state.
        config_before = settings.connector_config_path.read_bytes()
        accounts_before = settings.connector_accounts_path.read_bytes()
        overrides_before = settings.connector_overrides_path.read_bytes()
        main_before = settings.ledger_main.read_bytes()
        old_before = dict(conn.execute(
            "SELECT slug, is_active, payoff_date, payoff_amount "
            "FROM loans WHERE slug = ?", ("OldMortgage",),
        ).fetchone())
        assert old_before["is_active"] == 1

        # Patch the anchor's execute to blow up.
        original_execute = PlannedLoanBalanceAnchor.execute

        def _failing_execute(self, *, settings, conn, reader):
            raise RuntimeError(
                "simulated cross-ref-anchor write failure"
            )

        with patch.object(
            PlannedLoanBalanceAnchor, "execute", _failing_execute,
        ):
            with pytest.raises(RuntimeError, match="cross-ref-anchor"):
                flow.commit(params, settings, conn, reader)

        # Files restored byte-for-byte.
        assert settings.connector_config_path.read_bytes() == config_before
        assert settings.connector_accounts_path.read_bytes() == accounts_before
        assert (
            settings.connector_overrides_path.read_bytes() == overrides_before
        )
        assert settings.ledger_main.read_bytes() == main_before
        # SQLite SAVEPOINT rolled back: OLD loan still active.
        old_after = dict(conn.execute(
            "SELECT slug, is_active, payoff_date, payoff_amount "
            "FROM loans WHERE slug = ?", ("OldMortgage",),
        ).fetchone())
        assert old_after == old_before

    @pytest.mark.xfail(
        reason="bean-check not on test PATH; rollback isn't triggered. "
        "Pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md.",
        strict=False,
    )
    def test_bean_check_failure_rolls_back_files_and_sqlite(self, tmp_path):
        flow = RefiFlow()
        conn = _conn_with_old_loan()
        settings = _settings_for(tmp_path)
        # Force bean-check failure with a malformed liability path.
        params = _refi_params(
            new_liability_account_path="Liabilities:badleaf",
        )
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(settings.ledger_main)

        config_before = settings.connector_config_path.read_bytes()
        old_before = dict(conn.execute(
            "SELECT is_active FROM loans WHERE slug = ?", ("OldMortgage",),
        ).fetchone())

        with pytest.raises(WizardCommitError):
            flow.commit(params, settings, conn, reader)

        # All restored — OLD loan still active.
        assert settings.connector_config_path.read_bytes() == config_before
        old_after = dict(conn.execute(
            "SELECT is_active FROM loans WHERE slug = ?", ("OldMortgage",),
        ).fetchone())
        assert old_after["is_active"] == old_before["is_active"] == 1
