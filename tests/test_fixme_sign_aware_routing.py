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
