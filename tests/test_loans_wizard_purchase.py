# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP10 — Purchase flow.

Covers:
  - Validation: each step's rules fire when expected
  - next_step branching: 'new' → new_property_details, 'existing' → loan_terms
  - write_plan shape: produces the expected sequence of PlannedWrites
    with the property/no-property distinction
  - Defensive validation on round-tripped fields
  - End-to-end commit happy path
  - Bean-check rejection rolls everything back
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
    PlannedLoanFunding,
    PlannedLoanWrite,
    PlannedPropertyWrite,
    WizardCommitError,
)
from lamella.features.loans.wizard.purchase import PurchaseFlow


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


def _new_property_params(**overrides):
    base = {
        "property_choice": "new",
        "new_property_slug": "MainResidence",
        "new_property_display_name": "Main Residence",
        "new_property_type": "primary_residence",
        "new_property_entity_slug": "Personal",
        "new_property_address": "123 Main St",
        "loan_slug": "MainMortgage",
        "loan_display_name": "Main Mortgage",
        "loan_type": "mortgage",
        "loan_entity_slug": "Personal",
        "loan_institution": "Bank Two",
        "original_principal": "550000",
        "term_months": "360",
        "interest_rate_apr": "6.625",
        "first_payment_date": "2025-03-01",
        "payment_due_day": "1",
        "liability_account_path": "Liabilities:Personal:BankTwo:MainMortgage",
        "interest_account_path": "Expenses:Personal:MainMortgage:Interest",
        "escrow_account_path": "",
        "funded_date": "2025-02-01",
        "offset_account": "Assets:Personal:MainResidence:CostBasis",
        "funding_narration": "Mortgage funding",
    }
    base.update(overrides)
    return base


# --------------------------------------------------------------- validation


class TestValidate:
    def test_initial_step_requires_choice(self):
        flow = PurchaseFlow()
        errs = flow.validate("choose_property", {}, None)
        assert any(e.field == "property_choice" for e in errs)

    def test_existing_choice_requires_slug(self):
        flow = PurchaseFlow()
        errs = flow.validate(
            "choose_property",
            {"property_choice": "existing"}, None,
        )
        assert any(e.field == "existing_property_slug" for e in errs)

    def test_existing_choice_validates_against_registry(self):
        flow = PurchaseFlow()
        conn = _conn()
        errs = flow.validate(
            "choose_property",
            {"property_choice": "existing",
             "existing_property_slug": "DoesNotExist"},
            conn,
        )
        assert any(
            "not in the registry" in e.message for e in errs
        )

    def test_new_property_slug_validation(self):
        flow = PurchaseFlow()
        # bad slug — too short
        errs = flow.validate(
            "new_property_details",
            {"property_choice": "new", "new_property_slug": "ab"},
            None,
        )
        assert any(e.field == "new_property_slug" for e in errs)

    def test_loan_slug_collision_detected(self):
        flow = PurchaseFlow()
        conn = _conn()
        conn.execute(
            "INSERT INTO loans (slug, loan_type, original_principal, "
            "funded_date, is_active) VALUES (?, ?, ?, ?, ?)",
            ("ExistingLoan", "mortgage", "100000", "2024-01-01", 1),
        )
        params = _new_property_params(loan_slug="ExistingLoan")
        errs = flow.validate("loan_terms", params, conn)
        assert any(
            "already exists" in e.message and e.field == "loan_slug"
            for e in errs
        )

    def test_principal_must_be_positive(self):
        flow = PurchaseFlow()
        params = _new_property_params(original_principal="-100")
        errs = flow.validate("loan_terms", params, None)
        assert any(e.field == "original_principal" for e in errs)

    def test_apr_in_range(self):
        flow = PurchaseFlow()
        params = _new_property_params(interest_rate_apr="500")
        errs = flow.validate("loan_terms", params, None)
        assert any(e.field == "interest_rate_apr" for e in errs)

    def test_liability_path_must_be_proper_prefix(self):
        flow = PurchaseFlow()
        params = _new_property_params(
            liability_account_path="Expenses:Wrong",
        )
        errs = flow.validate("accounts", params, None)
        assert any(e.field == "liability_account_path" for e in errs)

    def test_funded_date_required(self):
        flow = PurchaseFlow()
        params = _new_property_params(funded_date="")
        errs = flow.validate("funding", params, None)
        assert any(e.field == "funded_date" for e in errs)

    def test_back_edit_invalidates_earlier_step(self):
        # User on step 5 (funding) but has corrupted step 1's choice.
        flow = PurchaseFlow()
        params = _new_property_params(property_choice="garbage")
        errs = flow.validate("funding", params, None)
        # Step-1 error surfaces even when the user is on step 5.
        assert any(e.field == "property_choice" for e in errs)

    def test_defensive_against_corrupted_hidden_field(self):
        # Hidden field looks valid syntactically but encodes garbage.
        flow = PurchaseFlow()
        params = _new_property_params(term_months="not-a-number")
        errs = flow.validate("loan_terms", params, None)
        assert any(e.field == "term_months" for e in errs)


# --------------------------------------------------------------- next_step


class TestNextStep:
    def test_choose_new_branches_to_new_property_details(self):
        flow = PurchaseFlow()
        nxt = flow.next_step(
            "choose_property",
            {"property_choice": "new"}, None,
        )
        assert nxt == "new_property_details"

    def test_choose_existing_skips_to_loan_terms(self):
        flow = PurchaseFlow()
        nxt = flow.next_step(
            "choose_property",
            {"property_choice": "existing"}, None,
        )
        assert nxt == "loan_terms"

    def test_linear_progression_to_funding(self):
        flow = PurchaseFlow()
        assert flow.next_step("new_property_details", {}, None) == "loan_terms"
        assert flow.next_step("loan_terms", {}, None) == "accounts"
        assert flow.next_step("accounts", {}, None) == "funding"
        assert flow.next_step("funding", {}, None) is None  # → preview


# --------------------------------------------------------------- write_plan


class TestWritePlan:
    def test_new_property_plan_contains_property_write(self):
        flow = PurchaseFlow()
        plan = flow.write_plan(_new_property_params(), None)
        kinds = [type(p).__name__ for p in plan]
        assert "PlannedPropertyWrite" in kinds
        assert "PlannedLoanWrite" in kinds
        assert "PlannedLoanFunding" in kinds
        assert "PlannedAccountsOpen" in kinds

    def test_existing_property_plan_skips_property_write(self):
        flow = PurchaseFlow()
        # Need an existing property in the registry; for write_plan
        # itself we don't validate against the DB so we just pass
        # the choice + slug.
        params = _new_property_params(
            property_choice="existing",
            existing_property_slug="ExistingMain",
        )
        plan = flow.write_plan(params, None)
        kinds = [type(p).__name__ for p in plan]
        assert "PlannedPropertyWrite" not in kinds
        # The loan still references the existing property slug.
        loan_planned = next(p for p in plan if isinstance(p, PlannedLoanWrite))
        assert loan_planned.property_slug == "ExistingMain"

    def test_plan_account_paths_dedup(self):
        flow = PurchaseFlow()
        # Liability and offset are different accounts; interest left
        # default; escrow blank — plan should have liability + interest
        # + offset, no duplicates.
        plan = flow.write_plan(_new_property_params(), None)
        opens = next(p for p in plan if isinstance(p, PlannedAccountsOpen))
        assert "Liabilities:Personal:BankTwo:MainMortgage" in opens.paths
        assert "Expenses:Personal:MainMortgage:Interest" in opens.paths
        assert "Assets:Personal:MainResidence:CostBasis" in opens.paths
        # No duplicates.
        assert len(set(opens.paths)) == len(opens.paths)

    def test_plan_property_carries_purchase_meta(self):
        flow = PurchaseFlow()
        plan = flow.write_plan(_new_property_params(), None)
        prop = next(p for p in plan if isinstance(p, PlannedPropertyWrite))
        assert prop.purchase_date == "2025-02-01"
        assert prop.purchase_price == "550000"
        assert prop.is_primary_residence is True


# --------------------------------------------------------------- template_context


class TestTemplateContext:
    def test_existing_properties_listed_on_choose_property(self):
        flow = PurchaseFlow()
        conn = _conn()
        # Insert two properties — one active, one inactive.
        conn.execute(
            "INSERT INTO properties (slug, display_name, property_type, "
            "is_active, is_primary_residence, is_rental) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("MainResidence", "Main Residence", "primary_residence", 1, 1, 0),
        )
        conn.execute(
            "INSERT INTO properties (slug, display_name, property_type, "
            "is_active, is_primary_residence, is_rental) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("OldRental", "Old Rental", "rental", 0, 0, 1),
        )
        ctx = flow.template_context("choose_property", {}, conn)
        slugs = [p["slug"] for p in ctx["existing_properties"]]
        assert "MainResidence" in slugs
        assert "OldRental" not in slugs  # inactive filtered out

    def test_other_steps_no_extra_context(self):
        flow = PurchaseFlow()
        ctx = flow.template_context("loan_terms", {}, None)
        assert ctx == {}


# --------------------------------------------------------------- commit


class TestCommit:
    def test_commit_happy_path(self, tmp_path):
        flow = PurchaseFlow()
        conn = _conn()
        settings = _settings_for(tmp_path)
        params = _new_property_params(
            funded_date="2025-02-01",
        )
        # Sanity: validate first.
        for step_name in flow.steps():
            errs = flow.validate(step_name, params, conn)
            assert errs == [], (step_name, errs)
        # Use a fake reader since the writers we exercise don't need one.
        # AccountsOpen actually reads ledger entries — we provide a
        # cheap one.
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(settings.ledger_main)
        result = flow.commit(params, settings, conn, reader)
        assert result.redirect_to == "/settings/loans/MainMortgage"

        # Verify ledger files contain expected directives.
        config_text = settings.connector_config_path.read_text(encoding="utf-8")
        accounts_text = settings.connector_accounts_path.read_text(encoding="utf-8")
        overrides_text = settings.connector_overrides_path.read_text(encoding="utf-8")

        assert 'custom "property" "MainResidence"' in config_text
        assert 'custom "loan" "MainMortgage"' in config_text
        assert "Liabilities:Personal:BankTwo:MainMortgage" in accounts_text
        assert "#lamella-loan-funding" in overrides_text

    @pytest.mark.xfail(
        reason="bean-check not on test PATH; rollback isn't triggered. "
        "Pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md.",
        strict=False,
    )
    def test_commit_rolls_back_on_bean_check_failure(self, tmp_path):
        flow = PurchaseFlow()
        conn = _conn()
        settings = _settings_for(tmp_path)
        # Force a bean-check failure: liability path uses an invalid
        # currency-suffixed account name (Beancount rejects accounts
        # with leading lowercase).
        params = _new_property_params(
            liability_account_path="Liabilities:badleaf",  # starts lowercase
        )
        # Skip flow.validate (which doesn't enforce Beancount account
        # syntax) so we exercise the txn rollback path.
        from lamella.core.beancount_io import LedgerReader
        reader = LedgerReader(settings.ledger_main)

        # Snapshot before.
        config_before = settings.connector_config_path.read_bytes()
        accounts_before = settings.connector_accounts_path.read_bytes()
        overrides_before = settings.connector_overrides_path.read_bytes()
        main_before = settings.ledger_main.read_bytes()

        with pytest.raises(WizardCommitError):
            flow.commit(params, settings, conn, reader)

        # Verify all four files restored byte-for-byte.
        assert settings.connector_config_path.read_bytes() == config_before
        assert settings.connector_accounts_path.read_bytes() == accounts_before
        assert settings.connector_overrides_path.read_bytes() == overrides_before
        assert settings.ledger_main.read_bytes() == main_before
