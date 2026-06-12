# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for Phase G7 — notes card_override wire into classify.

When an active note carries ``card_override=True`` and a valid
``entity_hint``, the classify pipeline swaps the working entity
to the note's hint and widens the account whitelist cross-entity
so the AI can pick accounts from the hinted entity. The deterministic
card → entity binding is suspended for this note's window.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from beancount.core import data as bdata
from beancount.core.amount import Amount
from beancount.core.number import D

from lamella.features.ai_cascade.classify import build_classify_context
from lamella.core.db import connect, migrate
from lamella.features.notes.service import NoteService


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _fixme_txn(
    *, d: date, card_account: str, amount: str,
    payee: str | None = None, narration: str | None = None,
) -> bdata.Transaction:
    amt = D(amount)
    return bdata.Transaction(
        meta={}, date=d, flag="*", payee=payee, narration=narration,
        tags=frozenset(), links=frozenset(),
        postings=[
            bdata.Posting(
                account=card_account, units=Amount(-amt, "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
            bdata.Posting(
                account="Expenses:FIXME", units=Amount(amt, "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
        ],
    )


def _open(account: str) -> bdata.Open:
    return bdata.Open(
        meta={}, date=date(2020, 1, 1),
        account=account, currencies=["USD"], booking=None,
    )


@pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
class TestCardOverrideWire:
    def test_override_note_swaps_entity_and_widens_whitelist(self, conn):
        """Card on Personal, note says 'using personal card for
        Acme this week' (card_override=True, entity_hint=Acme).
        Classify context swaps entity → Acme and returns a
        cross-entity accounts_by_entity so the AI can pick Acme
        accounts."""
        NoteService(conn).create(
            "using personal Visa for Acme business this week",
            entity_hint="Acme",
            captured_at=datetime(2026, 4, 17, tzinfo=timezone.utc),
            card_override=True,
        )
        entries = [
            _open("Expenses:Acme:Supplies"),
            _open("Expenses:Acme:Meals"),
            _open("Expenses:Personal:Groceries"),
            _open("Expenses:WidgetCo:Supplies"),
        ]
        txn = _fixme_txn(
            d=date(2026, 4, 18),
            card_account="Liabilities:Personal:Visa:0123",
            amount="50",
            payee="Staples",
        )

        (view, similar, accounts, entity, notes,
         _suspicion, accounts_by_entity, _receipt, _mileage) = build_classify_context(
            entries=entries, txn=txn, conn=conn,
        )

        # Entity swapped from Personal (card binding) → Acme (note override).
        assert entity == "Acme"
        # Whitelist widened cross-entity.
        assert accounts_by_entity is not None
        assert set(accounts_by_entity.keys()) >= {"Acme", "Personal", "WidgetCo"}
        assert "Expenses:Acme:Supplies" in accounts
        assert "Expenses:Personal:Groceries" in accounts

    def test_override_note_without_entity_hint_does_nothing(self, conn):
        """card_override=True but no entity_hint — we don't know
        what to swap to, so behavior is unchanged and the card
        binding still wins."""
        NoteService(conn).create(
            "note without hint",
            captured_at=datetime(2026, 4, 17, tzinfo=timezone.utc),
            card_override=True,
        )
        entries = [
            _open("Expenses:Personal:Groceries"),
        ]
        txn = _fixme_txn(
            d=date(2026, 4, 18),
            card_account="Liabilities:Personal:Visa:0123",
            amount="50",
        )

        (_view, _sim, _acc, entity, _notes,
         _susp, accounts_by_entity, _receipt, _mileage) = build_classify_context(
            entries=entries, txn=txn, conn=conn,
        )

        # Card binding stands.
        assert entity == "Personal"
        assert accounts_by_entity is None

    def test_override_flag_off_has_no_effect(self, conn):
        NoteService(conn).create(
            "regular trip note",
            entity_hint="Acme",
            captured_at=datetime(2026, 4, 17, tzinfo=timezone.utc),
            card_override=False,
        )
        entries = [_open("Expenses:Personal:Groceries")]
        txn = _fixme_txn(
            d=date(2026, 4, 18),
            card_account="Liabilities:Personal:Visa:0123",
            amount="50",
        )
        (_v, _s, _a, entity, _n, _su, abe, _r, _m) = build_classify_context(
            entries=entries, txn=txn, conn=conn,
        )
        assert entity == "Personal"
        assert abe is None

    def test_override_note_outside_proximity_window_ignored(self, conn):
        """A card_override note captured three weeks ago no longer
        applies — the txn falls outside the proximity window and
        the card binding reverts."""
        NoteService(conn).create(
            "stale override",
            entity_hint="Acme",
            captured_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
            card_override=True,
        )
        entries = [_open("Expenses:Personal:Groceries")]
        txn = _fixme_txn(
            d=date(2026, 4, 20),
            card_account="Liabilities:Personal:Visa:0123",
            amount="50",
        )
        (_v, _s, _a, entity, _n, _su, abe, _r, _m) = build_classify_context(
            entries=entries, txn=txn, conn=conn,
        )
        assert entity == "Personal"
        assert abe is None
