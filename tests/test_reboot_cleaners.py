# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0057 §2 — composable RebootCleaner pipeline tests.

The cleaner is the "transform" half of the round-trip ETL —
takes the typed envelope from extract, applies a normalization,
returns the envelope (or a DropDecision). Cleaners compose; each
sees the previous one's output."""
from __future__ import annotations

import pytest

from lamella.features.import_.staging.cleaners import (
    CleanedEnvelope,
    DropDecision,
    account_path_normalize,
    compose,
)


def _envelope(*postings):
    return {
        "flag": "*",
        "tags": [],
        "links": [],
        "txn_meta": [],
        "postings": list(postings),
    }


def _post(account, amount="-12.50", currency="USD"):
    return {
        "account": account,
        "amount": amount,
        "currency": currency,
        "cost": None,
        "price": None,
        "flag": None,
        "meta": [],
    }


# --- account_path_normalize --------------------------------------


class TestAccountPathNormalize:
    """Rewrites legacy category-first paths into entity-first form
    when a known entity slug appears at position 3."""

    def test_rewrites_legacy_vehicles_path(self):
        cleaner = account_path_normalize(
            known_entity_slugs=frozenset({"Acme", "Personal"}),
        )
        env = _envelope(
            _post("Expenses:Vehicles:Acme:Fuel", amount="-30.00"),
            _post("Liabilities:Card", amount="30.00"),
        )
        result = cleaner(CleanedEnvelope(envelope=env))
        assert isinstance(result, CleanedEnvelope)
        accounts = [p["account"] for p in result.envelope["postings"]]
        assert "Expenses:Acme:Vehicles:Fuel" in accounts
        # Liabilities posting (no entity at pos 3) untouched.
        assert "Liabilities:Card" in accounts
        # Change recorded for the diff UI.
        assert len(result.changes) == 1
        assert (
            result.changes[0]["before"] == "Expenses:Vehicles:Acme:Fuel"
        )
        assert (
            result.changes[0]["after"] == "Expenses:Acme:Vehicles:Fuel"
        )

    def test_skips_already_entity_first(self):
        cleaner = account_path_normalize(
            known_entity_slugs=frozenset({"Acme", "Personal"}),
        )
        env = _envelope(
            _post("Expenses:Acme:Vehicles:Fuel", amount="-30.00"),
            _post("Liabilities:Card", amount="30.00"),
        )
        result = cleaner(CleanedEnvelope(envelope=env))
        assert isinstance(result, CleanedEnvelope)
        # No rewrite applied → cleaner returns the input unchanged.
        assert result.envelope == env
        assert result.changes == []

    def test_skips_when_pos3_is_not_a_known_entity(self):
        """Legacy category at pos 2 but pos 3 isn't a known entity
        → can't safely rewrite; leave it for a later pass / human."""
        cleaner = account_path_normalize(
            known_entity_slugs=frozenset({"Acme"}),
        )
        env = _envelope(
            _post(
                "Expenses:Vehicles:UnknownEntity:Fuel",
                amount="-30.00",
            ),
        )
        result = cleaner(CleanedEnvelope(envelope=env))
        assert isinstance(result, CleanedEnvelope)
        # No change.
        accounts = [p["account"] for p in result.envelope["postings"]]
        assert accounts == ["Expenses:Vehicles:UnknownEntity:Fuel"]
        assert result.changes == []

    def test_short_paths_left_alone(self):
        cleaner = account_path_normalize(
            known_entity_slugs=frozenset({"Acme"}),
        )
        env = _envelope(
            _post("Equity:Opening", amount="-100.00"),
            _post("Cash", amount="100.00"),
        )
        result = cleaner(CleanedEnvelope(envelope=env))
        assert result.envelope == env

    def test_records_legacy_token_in_reason(self):
        cleaner = account_path_normalize(
            known_entity_slugs=frozenset({"Acme"}),
        )
        env = _envelope(
            _post("Expenses:Property:Acme:Repairs", amount="-100.00"),
        )
        result = cleaner(CleanedEnvelope(envelope=env))
        assert isinstance(result, CleanedEnvelope)
        assert len(result.changes) == 1
        assert "Property" in result.changes[0]["reason"]


# --- compose ------------------------------------------------------


class TestCompose:
    """compose() chains cleaners; DropDecision short-circuits."""

    def test_runs_in_order(self):
        cleaner1 = account_path_normalize(
            known_entity_slugs=frozenset({"Acme"}),
        )

        def append_note(input_: CleanedEnvelope) -> CleanedEnvelope:
            return CleanedEnvelope(
                envelope=input_.envelope,
                changes=[],
                notes=["second cleaner ran"],
            )

        composed = compose(cleaner1, append_note)
        env = _envelope(
            _post("Expenses:Vehicles:Acme:Fuel", amount="-30.00"),
        )
        result = composed(CleanedEnvelope(envelope=env))
        assert isinstance(result, CleanedEnvelope)
        accounts = [p["account"] for p in result.envelope["postings"]]
        assert "Expenses:Acme:Vehicles:Fuel" in accounts
        assert "second cleaner ran" in result.notes

    def test_drop_decision_short_circuits(self):
        def drop(_input):
            return DropDecision(rationale="duplicate of #42")

        def never_runs(input_):
            raise AssertionError(
                "cleaner after a DropDecision must not run"
            )

        composed = compose(drop, never_runs)
        result = composed(CleanedEnvelope(envelope=_envelope()))
        assert isinstance(result, DropDecision)
        assert "duplicate" in result.rationale

    def test_changes_accumulate_across_passes(self):
        """Each cleaner contributes its own changes; compose merges
        them so the per-file diff UI sees the full audit trail."""

        def add_change_a(input_):
            return CleanedEnvelope(
                envelope=input_.envelope,
                changes=[{
                    "field": "x", "before": "old", "after": "new",
                    "reason": "first",
                }],
            )

        def add_change_b(input_):
            return CleanedEnvelope(
                envelope=input_.envelope,
                changes=[{
                    "field": "y", "before": "old", "after": "new",
                    "reason": "second",
                }],
            )

        composed = compose(add_change_a, add_change_b)
        result = composed(CleanedEnvelope(envelope=_envelope()))
        assert isinstance(result, CleanedEnvelope)
        assert len(result.changes) == 2
        assert result.changes[0]["reason"] == "first"
        assert result.changes[1]["reason"] == "second"
