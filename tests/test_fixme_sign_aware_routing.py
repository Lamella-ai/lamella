# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Sign-aware FIXME placeholder routing.

The user-reported bug: a $15 mobile deposit on BankOne checking
got classified by the AI as BankOne Fees because the FIXME
placeholder was hardcoded to Expenses:{entity}:FIXME regardless of
sign. The AI's whitelist is scoped per-root, so a deposit landing
on Expenses:* gives it nothing to pick except expense leaves.

Fix: route by sign — positive amount goes to Income:{entity}:FIXME,
non-positive stays at Expenses:{entity}:FIXME.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from lamella.features.bank_sync.ingest import _fixme_for_entity


class TestSignAware:
    def test_positive_amount_routes_to_income(self):
        # $15 mobile deposit — positive on the bank account
        assert _fixme_for_entity(
            "Personal", Decimal("15.00"),
        ) == "Income:Personal:FIXME"

    def test_negative_amount_routes_to_expenses(self):
        # -$42.17 hardware-store charge — negative on the bank account
        assert _fixme_for_entity(
            "Personal", Decimal("-42.17"),
        ) == "Expenses:Personal:FIXME"

    def test_exact_zero_routes_to_expenses(self):
        # Edge case: an exact-zero txn (fee waiver) defaults to
        # Expenses since fees are the more common zero-or-near-zero
        # case in the wild.
        assert _fixme_for_entity(
            "Acme", Decimal("0"),
        ) == "Expenses:Acme:FIXME"

    def test_legacy_call_without_amount_keeps_expenses(self):
        # Back-compat: callers that don't pass amount yet keep the
        # legacy Expenses-only routing. No regression for existing
        # call sites that only know the entity.
        assert _fixme_for_entity(
            "Personal",
        ) == "Expenses:Personal:FIXME"

    def test_no_entity_positive_amount(self):
        # Positive amount with unknown entity still routes to Income.
        assert _fixme_for_entity(
            None, Decimal("100.00"),
        ) == "Income:FIXME"

    def test_no_entity_negative_amount(self):
        assert _fixme_for_entity(
            None, Decimal("-50.00"),
        ) == "Expenses:FIXME"


class TestUserReportedScenario:
    """The actual user bug: $15 Mobile Deposit getting classified as
    a fee. Verify the placeholder lands on Income with the entity."""

    def test_mobile_deposit_routes_to_income(self):
        # BankOne Personal checking, mobile deposit of $15
        # SimpleFIN reports this as +15.00 (credit to account)
        assert _fixme_for_entity(
            "Personal", Decimal("15.00"),
        ).startswith("Income:")

    def test_credit_card_charge_routes_to_expenses(self):
        # BankOne Personal Visa, $42.17 hardware-store charge
        # SimpleFIN reports this as -42.17 (debit to account)
        assert _fixme_for_entity(
            "Personal", Decimal("-42.17"),
        ).startswith("Expenses:")


class TestLiabilitySourceUniversalConvention:
    """The FIXME placeholder builder uses the *universal* sign
    convention regardless of source type. Verified against actual
    ledger writes (writer.py:166-172 + real entries):

      Liabilities:CC -9.49  ← charge (money OUT to user)
      Liabilities:CC +20.95 ← refund (money IN to user)

    So negative on a liability = Expense (charge), positive = Income
    (refund/paydown). Same rule as for assets — no inversion. Earlier
    "liability-aware inversion" code assumed SimpleFIN delivered card
    charges as POSITIVE amounts; the actual ledger shape proved the
    opposite, and the inversion silently routed every CC charge to
    Income:*:FIXME (wrong whitelist) and every refund to
    Expenses:*:FIXME (wrong whitelist)."""

    def test_credit_card_charge_routes_to_expenses(self):
        # -$42.17 charge on a Personal Visa (Liabilities:*).
        assert _fixme_for_entity(
            "Personal",
            Decimal("-42.17"),
            source_account="Liabilities:Personal:Card:CardA1234",
        ) == "Expenses:Personal:FIXME"

    def test_credit_card_refund_routes_to_income(self):
        # +$20.95 refund on a credit card (positive = money IN).
        assert _fixme_for_entity(
            "Personal",
            Decimal("20.95"),
            source_account="Liabilities:Personal:Card:CardA1234",
        ) == "Income:Personal:FIXME"

    def test_loan_disbursement_routes_to_income(self):
        # A loan disbursement (e.g., new mortgage / LOC draw):
        # positive on the liability = money IN to the user.
        assert _fixme_for_entity(
            "Acme",
            Decimal("50000.00"),
            source_account="Liabilities:Acme:Loan:Mortgage",
        ) == "Income:Acme:FIXME"

    def test_asset_source_keeps_universal_convention(self):
        assert _fixme_for_entity(
            "Personal",
            Decimal("15.00"),
            source_account="Assets:Personal:Checking",
        ) == "Income:Personal:FIXME"
        assert _fixme_for_entity(
            "Personal",
            Decimal("-42.17"),
            source_account="Assets:Personal:Checking",
        ) == "Expenses:Personal:FIXME"

    def test_unknown_source_uses_universal_convention(self):
        assert _fixme_for_entity(
            "Personal", Decimal("15.00"), source_account=None,
        ) == "Income:Personal:FIXME"
        assert _fixme_for_entity(
            "Personal", Decimal("-42.17"), source_account=None,
        ) == "Expenses:Personal:FIXME"

    def test_equity_or_other_source_uses_universal_convention(self):
        assert _fixme_for_entity(
            "Acme",
            Decimal("100.00"),
            source_account="Equity:OpeningBalances",
        ) == "Income:Acme:FIXME"

    def test_zero_amount_with_liability_source_stays_expense(self):
        # The exact-zero edge case (fee waiver, etc.) keeps the
        # Expenses default regardless of source kind — symmetric with
        # the existing legacy contract.
        assert _fixme_for_entity(
            "Acme",
            Decimal("0"),
            source_account="Liabilities:Acme:Card:CardA1234",
        ) == "Expenses:Acme:FIXME"


class TestCallersThreadSourceAccount:
    """Source-level guard that BOTH _classify and _maybe_ai_classify
    in ingest.py pass source_account through to _fixme_for_entity.
    Otherwise the routing fix is dead code — the call sites would
    keep falling through to the asset-convention default."""

    def test_classify_passes_source_account(self):
        import inspect
        from lamella.features.bank_sync import ingest

        src = inspect.getsource(ingest.SimpleFINIngest._classify)
        assert "_fixme_for_entity(" in src
        assert "source_account=source_account" in src, (
            "ingest.SimpleFINIngest._classify must pass "
            "source_account=source_account to _fixme_for_entity so "
            "credit-card charges land on Expenses:*:FIXME, not "
            "Income:*:FIXME. This is Gate 3 of the AI-rejection "
            "regression."
        )

    def test_maybe_ai_classify_passes_source_account(self):
        import inspect
        from lamella.features.bank_sync import ingest

        src = inspect.getsource(
            ingest.SimpleFINIngest._maybe_ai_classify,
        )
        assert "_fixme_for_entity(" in src
        assert "source_account=source_account" in src, (
            "ingest.SimpleFINIngest._maybe_ai_classify must pass "
            "source_account=source_account to _fixme_for_entity so "
            "the prompt's FIXME placeholder matches the whitelist "
            "root computed from the same source-account hint."
        )
