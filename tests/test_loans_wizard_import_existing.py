# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP10 — Import-existing flow.

Covers:
  - terms_source 'full' vs 'statement' branching in next_step
  - _resolve_terms() derives sensible original_principal from
    statement balance (sanity-check against a known schedule)
  - Both branches produce identical write_plan output (modulo
    user-provided text fields like display_name)
  - backfill_choice 'opt-in' redirects to /backfill, 'skip' lands
    on the loan detail page
  - Anchor write IS in the plan; backfill is NEVER in the plan
  - Validation per step (full + statement branches both)
  - End-to-end commit happy path with rollback on bean-check failure
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
    PlannedAccountsOpen,
    PlannedLoanBalanceAnchor,
    PlannedLoanWrite,
    PlannedWrite,
    WizardCommitError,
)
from lamella.features.loans.wizard.import_existing import (
    ImportExistingFlow,
    _derive_original_principal_from_statement,
    _resolve_terms,
)


def _conn():
    from lamella.core.db import migrate, connect
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = Path(f.name)
    conn = connect(path)
    migrate(conn)
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


def _full_terms_params(**overrides):
    base = {
        "terms_source": "full",
        "loan_slug": "Mortgage1",
        "loan_display_name": "Mortgage 1",
        "loan_type": "mortgage",
        "loan_entity_slug": "Personal",
        "loan_institution": "Bank Two",
        "original_principal": "550000",
        "funded_date": "2020-01-01",
        "term_months": "360",
        "interest_rate_apr": "4.5",
        "liability_account_path": "Liabilities:Personal:BankTwo:Mortgage1",
        "interest_account_path": "Expenses:Personal:Mortgage1:Interest",
        "anchor_date": "2025-01-01",
        "anchor_balance": "470000",
        "anchor_source": "statement",
        "backfill_choice": "skip",
    }
    base.update(overrides)
    return base


def _statement_params(**overrides):
    base = {
        "terms_source": "statement",
        "loan_slug": "Mortgage2",
        "loan_display_name": "Mortgage 2",
        "loan_type": "mortgage",
        "loan_entity_slug": "Personal",
        "loan_institution": "Acme Bank",
        "statement_balance": "470000",
        "statement_date": "2025-01-01",
        "months_remaining": "300",
        "months_elapsed": "60",
        "interest_rate_apr": "4.5",
        "liability_account_path": "Liabilities:Personal:Acme:Mortgage2",
        "interest_account_path": "Expenses:Personal:Mortgage2:Interest",
        "anchor_date": "2025-01-01",
        "anchor_balance": "470000",
        "anchor_source": "statement",
        "backfill_choice": "skip",
    }
    base.update(overrides)
    return base


# --------------------------------------------------------------- derivation


class TestDerivation:
    def test_zero_apr_linear_payoff(self):
        # 100k principal, 0% APR, 12 month total, 6 months remaining,
        # 6 months elapsed → balance should be 50k.
        # Reverse: balance=50k, remaining=6, elapsed=6, apr=0
        # → derived = 50k * 12/6 = 100k.
        derived = _derive_original_principal_from_statement(
            statement_balance=Decimal("50000"),
            apr=Decimal("0"),
            months_remaining=6, months_elapsed=6,
        )
        assert derived == Decimal("100000.00")

    def test_standard_amortization(self):
        # 550k @ 4.5% / 360mo: actual balance after 60 payments is
        # ~$501,337. Reverse-amortizing that with the right
        # (elapsed=60, remaining=300, apr=4.5) inputs should land
        # back near $550k within $50 (rounding noise from per-month
        # quantize ops).
        derived = _derive_original_principal_from_statement(
            statement_balance=Decimal("501337"),
            apr=Decimal("4.5"),
            months_remaining=300, months_elapsed=60,
        )
        assert derived is not None
        assert abs(derived - Decimal("550000")) < Decimal("100")

    def test_degenerate_inputs_return_none(self):
        assert _derive_original_principal_from_statement(
            Decimal("0"), Decimal("4.5"), 300, 60,
        ) is None
        assert _derive_original_principal_from_statement(
            Decimal("100000"), Decimal("4.5"), 0, 60,
        ) is None
        assert _derive_original_principal_from_statement(
            Decimal("100000"), Decimal("4.5"), 300, -1,
        ) is None


class TestResolveTerms:
    def test_full_branch_passes_through(self):
        params = _full_terms_params()
        resolved = _resolve_terms(params)
        assert resolved["_resolved_principal"] == Decimal("550000")
        assert resolved["_resolved_funded_date"] == "2020-01-01"
        assert resolved["_resolved_term_months"] == 360

    def test_statement_branch_derives(self):
        params = _statement_params(
            statement_balance="491000",
            months_remaining="300", months_elapsed="60",
        )
        resolved = _resolve_terms(params)
        # Derived from the statement-balance reverse-amortization.
        assert resolved["_resolved_principal"] > Decimal("500000")
        assert resolved["_resolved_principal"] < Decimal("600000")
        # Term = elapsed + remaining
        assert resolved["_resolved_term_months"] == 360
        # Funded date = statement_date - elapsed_months
        assert resolved["_resolved_funded_date"] == "2020-01-01"


# --------------------------------------------------------------- next_step


class TestNextStep:
    def test_full_branches_to_terms_full(self):
        flow = ImportExistingFlow()
        nxt = flow.next_step(
            "terms_source", {"terms_source": "full"}, None,
        )
        assert nxt == "terms_full"

    def test_statement_branches_to_statement(self):
        flow = ImportExistingFlow()
        nxt = flow.next_step(
            "terms_source", {"terms_source": "statement"}, None,
        )
        assert nxt == "terms_from_statement"

    def test_both_branches_converge_at_accounts(self):
        flow = ImportExistingFlow()
        # Doesn't matter what params says — both intermediate steps
        # converge.
        assert flow.next_step("terms_full", {}, None) == "accounts"
        assert flow.next_step("terms_from_statement", {}, None) == "accounts"

    def test_linear_to_backfill_choice(self):
        flow = ImportExistingFlow()
        assert flow.next_step("accounts", {}, None) == "anchor"
        assert flow.next_step("anchor", {}, None) == "backfill_choice"
        assert flow.next_step("backfill_choice", {}, None) is None  # → preview


# --------------------------------------------------------------- write_plan


class TestWritePlan:
    def test_full_branch_plan_shape(self):
        flow = ImportExistingFlow()
        plan = flow.write_plan(_full_terms_params(), None)
        kinds = [type(p).__name__ for p in plan]
        # Three writes: opens, loan, anchor. NO funding txn, NO
        # backfill writes.
        assert kinds == [
            "PlannedAccountsOpen",
            "PlannedLoanWrite",
            "PlannedLoanBalanceAnchor",
        ]

    def test_statement_branch_plan_same_shape(self):
        flow = ImportExistingFlow()
        plan = flow.write_plan(_statement_params(), None)
        kinds = [type(p).__name__ for p in plan]
        # Same three writes — branch only affects what user saw, not
        # what gets written.
        assert kinds == [
            "PlannedAccountsOpen",
            "PlannedLoanWrite",
            "PlannedLoanBalanceAnchor",
        ]

    def test_loan_directive_matches_resolved_terms(self):
        flow = ImportExistingFlow()
        plan = flow.write_plan(_statement_params(
            statement_balance="491000",
            months_remaining="300", months_elapsed="60",
        ), None)
        loan_planned = next(p for p in plan if isinstance(p, PlannedLoanWrite))
        # Derived principal lands in the directive.
        assert Decimal(loan_planned.original_principal) > Decimal("500000")
        # Term = 60 + 300 = 360.
        assert loan_planned.term_months == 360
        # Funded date derived from statement_date - elapsed.
        assert loan_planned.funded_date == "2020-01-01"

    def test_anchor_in_plan(self):
        flow = ImportExistingFlow()
        plan = flow.write_plan(_full_terms_params(), None)
        anchor = next(
            p for p in plan if isinstance(p, PlannedLoanBalanceAnchor)
        )
        assert anchor.loan_slug == "Mortgage1"
        assert anchor.as_of_date == "2025-01-01"
        assert anchor.balance == "470000"
        assert anchor.source == "statement"

    def test_backfill_never_in_plan(self):
        # opt-in vs skip should both produce the same plan; the
        # difference is only in commit's redirect target.
        flow = ImportExistingFlow()
        plan_optin = flow.write_plan(
            _full_terms_params(backfill_choice="opt-in"), None,
        )
        plan_skip = flow.write_plan(
            _full_terms_params(backfill_choice="skip"), None,
        )
        # Neither plan mentions backfill in any class name.
        for plan in (plan_optin, plan_skip):
            for p in plan:
                assert "Backfill" not in type(p).__name__


# --------------------------------------------------------------- validate


class TestValidate:
    def test_terms_source_required(self):
        flow = ImportExistingFlow()
        errs = flow.validate("terms_source", {}, None)
        assert any(e.field == "terms_source" for e in errs)

    def test_full_branch_requires_principal(self):
        flow = ImportExistingFlow()
        params = _full_terms_params(original_principal="0")
        errs = flow.validate("terms_full", params, None)
        assert any(e.field == "original_principal" for e in errs)

    def test_statement_branch_requires_balance(self):
        flow = ImportExistingFlow()
        params = _statement_params(statement_balance="")
        errs = flow.validate("terms_from_statement", params, None)
        assert any(e.field == "statement_balance" for e in errs)

    def test_statement_branch_requires_remaining_positive(self):
        flow = ImportExistingFlow()
        params = _statement_params(months_remaining="0")
        errs = flow.validate("terms_from_statement", params, None)
        assert any(e.field == "months_remaining" for e in errs)

    def test_anchor_required(self):
        flow = ImportExistingFlow()
        params = _full_terms_params(anchor_date="")
        errs = flow.validate("anchor", params, None)
        assert any(e.field == "anchor_date" for e in errs)

    def test_anchor_balance_must_be_nonneg(self):
        flow = ImportExistingFlow()
        params = _full_terms_params(anchor_balance="-100")
        errs = flow.validate("anchor", params, None)
        assert any(e.field == "anchor_balance" for e in errs)

    def test_backfill_choice_required(self):
        flow = ImportExistingFlow()
        params = _full_terms_params(backfill_choice="")
        errs = flow.validate("backfill_choice", params, None)
        assert any(e.field == "backfill_choice" for e in errs)

    def test_loan_slug_collision(self):
        flow = ImportExistingFlow()
        conn = _conn()
        conn.execute(
            "INSERT INTO loans (slug, loan_type, original_principal, "
            "funded_date, is_active) VALUES (?, ?, ?, ?, ?)",
            ("Mortgage1", "mortgage", "100000", "2020-01-01", 1),
        )
        params = _full_terms_params()
        errs = flow.validate("terms_full", params, conn)
        assert any(
            "already exists" in e.message and e.field == "loan_slug"
            for e in errs
        )

    def test_back_edit_invalidates_earlier(self):
        # User on backfill_choice but corrupted terms_source.
        flow = ImportExistingFlow()
        params = _full_terms_params(terms_source="garbage")
        errs = flow.validate("backfill_choice", params, None)
        assert any(e.field == "terms_source" for e in errs)


# --------------------------------------------------------------- commit


class TestCommit:
    def test_commit_skip_redirects_to_detail(self, tmp_path):
        flow = ImportExistingFlow()
        conn = _conn()
        settings = _settings_for(tmp_path)
        params = _full_terms_params(backfill_choice="skip")
        for step_name in flow.steps():
            errs = flow.validate(step_name, params, conn)
            assert errs == [], (step_name, errs)
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(settings.ledger_main)
        result = flow.commit(params, settings, conn, reader)
        assert result.redirect_to == "/settings/loans/Mortgage1"
        assert result.saved_message == "loan-imported"

    def test_commit_opt_in_redirects_to_backfill(self, tmp_path):
        flow = ImportExistingFlow()
        conn = _conn()
        settings = _settings_for(tmp_path)
        params = _full_terms_params(
            loan_slug="Mortgage42", backfill_choice="opt-in",
        )
        for step_name in flow.steps():
            errs = flow.validate(step_name, params, conn)
            assert errs == [], (step_name, errs)
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(settings.ledger_main)
        result = flow.commit(params, settings, conn, reader)
        assert result.redirect_to == "/settings/loans/Mortgage42/backfill"
        assert result.saved_message == "loan-imported-go-backfill"

    def test_commit_writes_loan_and_anchor(self, tmp_path):
        flow = ImportExistingFlow()
        conn = _conn()
        settings = _settings_for(tmp_path)
        params = _full_terms_params()
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(settings.ledger_main)
        flow.commit(params, settings, conn, reader)

        config_text = settings.connector_config_path.read_text(encoding="utf-8")
        assert 'custom "loan" "Mortgage1"' in config_text
        assert 'custom "loan-balance-anchor" "Mortgage1"' in config_text
        # NO funding transaction — that's purchase's territory.
        overrides_text = settings.connector_overrides_path.read_text(
            encoding="utf-8",
        )
        assert "#lamella-loan-funding" not in overrides_text

    @pytest.mark.xfail(
        reason="bean-check not on test PATH; rollback isn't triggered. "
        "Pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md.",
        strict=False,
    )
    def test_commit_rolls_back_on_bean_check_failure(self, tmp_path):
        flow = ImportExistingFlow()
        conn = _conn()
        settings = _settings_for(tmp_path)
        params = _full_terms_params(
            liability_account_path="Liabilities:badleaf",  # lowercase
        )
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(settings.ledger_main)
        config_before = settings.connector_config_path.read_bytes()
        accounts_before = settings.connector_accounts_path.read_bytes()
        with pytest.raises(WizardCommitError):
            flow.commit(params, settings, conn, reader)
        assert settings.connector_config_path.read_bytes() == config_before
        assert settings.connector_accounts_path.read_bytes() == accounts_before
