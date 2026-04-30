# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Unit tests for validate_entity_first_path (ADR-0042) and its integration
into AccountsWriter.write_opens via the known_entity_slugs guard."""
from __future__ import annotations

import pytest

from lamella.core.registry.service import (
    InvalidAccountSegmentError,
    validate_entity_first_path,
)

# ---------------------------------------------------------------------------
# Canonical entity slugs used across tests
# ---------------------------------------------------------------------------

_ENTITIES = frozenset({"Personal", "Acme", "WidgetCo", "ExampleLLC"})


# ---------------------------------------------------------------------------
# validate_entity_first_path — happy paths
# ---------------------------------------------------------------------------


class TestValidateEntityFirstPathHappy:
    def test_asset_with_known_entity(self):
        result = validate_entity_first_path(
            "Assets:Personal:BankOne:Checking", _ENTITIES
        )
        assert result == "Assets:Personal:BankOne:Checking"

    def test_liability_with_known_entity(self):
        result = validate_entity_first_path(
            "Liabilities:Acme:Chase:CreditCard", _ENTITIES
        )
        assert result == "Liabilities:Acme:Chase:CreditCard"

    def test_expenses_with_known_entity(self):
        result = validate_entity_first_path(
            "Expenses:ExampleLLC:Vehicle:VAcmeVan1:Fuel", _ENTITIES
        )
        assert result == "Expenses:ExampleLLC:Vehicle:VAcmeVan1:Fuel"

    def test_income_with_known_entity(self):
        result = validate_entity_first_path(
            "Income:Personal:Interest:BankOne", _ENTITIES
        )
        assert result == "Income:Personal:Interest:BankOne"

    def test_equity_exempt_from_entity_check(self):
        # Equity:OpeningBalances is a system path — the segment after
        # Equity is a system label, not an entity slug.
        result = validate_entity_first_path(
            "Equity:OpeningBalances:Personal:BankOne:Checking", _ENTITIES
        )
        assert result == "Equity:OpeningBalances:Personal:BankOne:Checking"

    def test_equity_retained_exempt(self):
        result = validate_entity_first_path(
            "Equity:Retained:Acme", _ENTITIES
        )
        assert result == "Equity:Retained:Acme"

    def test_no_entity_slugs_provided_passes_any_second_segment(self):
        # When known_entity_slugs is None, only structural checks apply.
        result = validate_entity_first_path(
            "Assets:Vehicles:VAcmeVan2", None
        )
        assert result == "Assets:Vehicles:VAcmeVan2"

    def test_property_under_correct_entity(self):
        # Assets:Personal:Property:TestProperty1 is CORRECT — Personal is
        # the entity, Property is the institution-like container.
        result = validate_entity_first_path(
            "Assets:Personal:Property:TestProperty1", _ENTITIES
        )
        assert result == "Assets:Personal:Property:TestProperty1"

    def test_vehicle_under_correct_entity(self):
        # Assets:Personal:Vehicle:VAcmeVan3 — Personal is entity.
        result = validate_entity_first_path(
            "Assets:Personal:Vehicle:VAcmeVan3", _ENTITIES
        )
        assert result == "Assets:Personal:Vehicle:VAcmeVan3"


# ---------------------------------------------------------------------------
# validate_entity_first_path — violation paths
# ---------------------------------------------------------------------------


class TestValidateEntityFirstPathViolations:
    def test_vehicles_as_second_segment_assets(self):
        # Assets:Vehicles:VAcmeVan2 — "Vehicles" is not an entity slug
        with pytest.raises(InvalidAccountSegmentError) as exc_info:
            validate_entity_first_path(
                "Assets:Vehicles:VAcmeVan2", _ENTITIES
            )
        err = exc_info.value
        assert err.path == "Assets:Vehicles:VAcmeVan2"
        assert "Vehicles" in err.bad_segments

    def test_vehicles_as_second_segment_expenses(self):
        with pytest.raises(InvalidAccountSegmentError) as exc_info:
            validate_entity_first_path(
                "Expenses:Vehicles:VAcmeVan2:Fuel", _ENTITIES
            )
        err = exc_info.value
        assert "Vehicles" in err.bad_segments

    def test_category_label_as_second_segment(self):
        # Expenses:Custom:Something — "Custom" is not a registered entity
        with pytest.raises(InvalidAccountSegmentError) as exc_info:
            validate_entity_first_path(
                "Expenses:Custom:Something", _ENTITIES
            )
        assert "Custom" in exc_info.value.bad_segments

    def test_error_carries_adr_reference(self):
        with pytest.raises(InvalidAccountSegmentError) as exc_info:
            validate_entity_first_path(
                "Assets:Vehicles:VAcmeVan2", _ENTITIES
            )
        assert "ADR-0042" in str(exc_info.value)

    def test_unknown_entity_with_set_provided(self):
        # "UnknownCo" is not in the known slugs
        with pytest.raises(InvalidAccountSegmentError) as exc_info:
            validate_entity_first_path(
                "Assets:UnknownCo:BankOne:Checking", _ENTITIES
            )
        assert "UnknownCo" in exc_info.value.bad_segments

    def test_empty_path_raises(self):
        with pytest.raises(InvalidAccountSegmentError):
            validate_entity_first_path("", _ENTITIES)


# ---------------------------------------------------------------------------
# AccountsWriter.write_opens integration — known_entity_slugs guard
# ---------------------------------------------------------------------------


class TestWriteOpensEntityGuard:
    """AccountsWriter.write_opens must reject non-conforming entity paths
    when known_entity_slugs is provided, before any disk write."""

    def _writer(self, tmp_path):
        """Build a minimal AccountsWriter with a throwaway ledger."""
        from lamella.core.registry.accounts_writer import AccountsWriter

        main = tmp_path / "main.bean"
        connector = tmp_path / "connector_accounts.bean"
        # A minimal main.bean so bean-check can parse it.
        main.write_text(
            'option "title" "Test"\n'
            'option "operating_currency" "USD"\n',
            encoding="utf-8",
        )
        return AccountsWriter(
            main_bean=main,
            connector_accounts=connector,
            run_check=False,  # avoid real bean-check in unit tests
        )

    def test_valid_entity_path_accepted(self, tmp_path):
        writer = self._writer(tmp_path)
        # Should not raise
        writer.write_opens(
            ["Assets:Personal:BankOne:Checking"],
            known_entity_slugs=frozenset({"Personal"}),
        )

    def test_nonconforming_entity_path_rejected(self, tmp_path):
        writer = self._writer(tmp_path)
        with pytest.raises(InvalidAccountSegmentError) as exc_info:
            writer.write_opens(
                ["Assets:Vehicles:VAcmeVan2"],
                known_entity_slugs=frozenset({"Personal", "ExampleLLC"}),
            )
        assert "Vehicles" in exc_info.value.bad_segments

    def test_mixed_batch_rejects_if_any_bad(self, tmp_path):
        writer = self._writer(tmp_path)
        with pytest.raises(InvalidAccountSegmentError):
            writer.write_opens(
                [
                    "Assets:Personal:BankOne:Checking",
                    "Assets:Vehicles:VAcmeVan2",  # bad
                ],
                known_entity_slugs=frozenset({"Personal"}),
            )

    def test_no_entity_slugs_skips_entity_check(self, tmp_path):
        writer = self._writer(tmp_path)
        # Without known_entity_slugs, the entity check is skipped —
        # only ADR-0045 character rules apply.  "Vehicles" is a valid
        # slug character-wise so this must not raise.
        writer.write_opens(
            ["Assets:Vehicles:VAcmeVan2"],
            known_entity_slugs=None,
        )

    def test_equity_paths_exempt_even_with_entity_slugs(self, tmp_path):
        writer = self._writer(tmp_path)
        # Equity:OpeningBalances is a system path; second segment is not
        # an entity slug and must not be rejected.
        writer.write_opens(
            ["Equity:OpeningBalances:Personal:BankOne:Checking"],
            known_entity_slugs=frozenset({"Personal"}),
        )
