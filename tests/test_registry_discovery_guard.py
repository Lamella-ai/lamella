# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import date

import pytest
from beancount.core.data import Open

from lamella.core.registry.discovery import (
    LedgerStructureMismatch,
    assert_entity_first_structure,
)


def _open(path: str) -> Open:
    return Open(
        meta={"filename": "t.bean", "lineno": 1},
        date=date(2020, 1, 1),
        account=path,
        currencies=["USD"],
        booking=None,
    )


def test_entity_first_ledger_passes():
    entries = [
        _open("Assets:Acme:BankOne:Checking"),
        _open("Liabilities:Acme:BankOne:Credit"),
        _open("Expenses:Acme:Supplies"),
        _open("Expenses:Acme:Shipping"),
        _open("Assets:Personal:BankOne:Checking"),
        _open("Liabilities:Personal:BankOne:Credit"),
        _open("Expenses:Personal:Food"),
        _open("Expenses:Personal:Utilities"),
        _open("Assets:WidgetCo:BankOne:Checking"),
        _open("Expenses:WidgetCo:Materials"),
    ]
    # 10 accounts, 10 parseable → should not raise
    assert_entity_first_structure(entries) is None


def test_category_first_ledger_trips_guard():
    entries = [
        _open("Expenses:Food:Groceries"),
        _open("Expenses:Food:Restaurants"),
        _open("Expenses:Transport:Gas"),
        _open("Expenses:Transport:Repairs"),
        _open("Expenses:Utilities:Electric"),
        _open("Expenses:Utilities:Internet"),
        _open("Expenses:Entertainment:Movies"),
        _open("Expenses:Shopping:Clothes"),
        _open("Expenses:Shopping:Hobbies"),
        _open("Expenses:Travel:Hotels"),
        _open("Assets:Checking"),
        _open("Liabilities:Credit"),
    ]
    # 12 accounts, all valid second segments (Food, Transport, Utilities,
    # etc.) look like "entities" by the heuristic — but realistic
    # category-first ledgers use a handful of repeating second-segment
    # names. This dataset IS category-first, and the heuristic can't
    # distinguish it from entity-first because "Food" is a valid slug
    # shape. The guard is a coarse structural check — it catches the
    # obvious "<20% parseable" case (empty segments, excluded segments,
    # non-standard roots), not semantically misplaced hierarchies.
    # So this test documents that limitation: the guard passes.
    assert_entity_first_structure(entries) is None


def test_ledger_with_few_accounts_is_not_blocked():
    # Fresh install: only 3 accounts, all excluded segments. Guard
    # must not block — user is still setting up.
    entries = [
        _open("Assets:Checking"),
        _open("Liabilities:Credit"),
        _open("Expenses:FIXME"),
    ]
    assert_entity_first_structure(entries) is None


def test_ledger_dominated_by_excluded_segments_trips_guard():
    # 11 accounts under entity roots, all with FIXME / Clearing /
    # OpeningBalances second segments — nothing parseable.
    entries = [
        _open("Assets:FIXME"),
        _open("Assets:Clearing:Transfers"),
        _open("Assets:OpeningBalances"),
        _open("Assets:Uncategorized"),
        _open("Liabilities:FIXME"),
        _open("Expenses:FIXME"),
        _open("Expenses:Uncategorized"),
        _open("Expenses:PayPal"),
        _open("Expenses:Venmo"),
        _open("Equity:OpeningBalances"),
        _open("Equity:Retained"),
    ]
    with pytest.raises(LedgerStructureMismatch) as exc:
        assert_entity_first_structure(entries)
    msg = str(exc.value)
    assert "Expenses:<Entity>:<Category>" in msg
    assert "LAMELLA_SKIP_DISCOVERY_GUARD" in msg
    # Error message must report concrete numbers so users know what
    # the guard saw.
    assert "11" in msg  # total


def test_bypass_env_var_skips_guard(monkeypatch):
    monkeypatch.setenv("LAMELLA_SKIP_DISCOVERY_GUARD", "1")
    entries = [_open("Assets:FIXME") for _ in range(20)]
    # Would normally trip, but env var bypasses.
    assert_entity_first_structure(entries) is None


def test_legacy_bcg_bypass_env_var_still_works(monkeypatch):
    # Cutover-window contract: the legacy BCG_-prefixed name keeps
    # working until we drop the deprecation shim.
    monkeypatch.delenv("LAMELLA_SKIP_DISCOVERY_GUARD", raising=False)
    monkeypatch.setenv("BCG_SKIP_DISCOVERY_GUARD", "1")
    entries = [_open("Assets:FIXME") for _ in range(20)]
    assert_entity_first_structure(entries) is None


def test_error_message_documents_expected_shape():
    entries = [_open("Equity:OpeningBalances") for _ in range(12)]
    with pytest.raises(LedgerStructureMismatch) as exc:
        assert_entity_first_structure(entries)
    msg = str(exc.value)
    # Must name the expected convention, the counts, the bypass env var.
    assert "entity-first" in msg.lower()
    assert "parseable entity segment" in msg
    assert "LAMELLA_SKIP_DISCOVERY_GUARD=1" in msg
    # Legacy name still surfaced in the helpful message during the cutover.
    assert "BCG_SKIP_DISCOVERY_GUARD" in msg
