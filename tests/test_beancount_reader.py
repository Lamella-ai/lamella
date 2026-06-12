# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from decimal import Decimal

from beancount.core.data import Transaction

from lamella.core.beancount_io import LedgerReader, entity_balances, txn_hash


def test_reader_loads_fixture(ledger_dir):
    reader = LedgerReader(ledger_dir / "main.bean")
    loaded = reader.load()
    assert loaded.errors == []
    txns = list(loaded.transactions())
    assert len(txns) >= 5


def test_reader_caches_until_mtime_changes(ledger_dir):
    reader = LedgerReader(ledger_dir / "main.bean")
    first = reader.load()
    again = reader.load()
    assert first is again  # same cached object


def test_entity_balances_by_entity(ledger_dir):
    reader = LedgerReader(ledger_dir / "main.bean")
    loaded = reader.load()
    balances = {b.entity: b for b in entity_balances(loaded.entries)}
    assert "Acme" in balances
    assert "Personal" in balances

    acme = balances["Acme"]
    # Opening 5000, then +150 sale, no asset decreases
    assert acme.assets == Decimal("5150.00")
    # Two card charges summing to -57.67
    assert acme.liabilities == Decimal("-57.67")
    # Expenses: 42.17 + 15.50 = 57.67
    assert acme.expenses == Decimal("57.67")
    # Net worth = 5150 - (-57.67) = 5207.67
    assert acme.net_worth == Decimal("5207.67")


def test_txn_hash_is_stable(ledger_dir):
    reader = LedgerReader(ledger_dir / "main.bean")
    loaded = reader.load()
    hashes = [txn_hash(e) for e in loaded.entries if isinstance(e, Transaction)]
    assert len(set(hashes)) == len(hashes)  # all distinct

    # Recomputing from a reload returns identical values.
    reader2 = LedgerReader(ledger_dir / "main.bean")
    loaded2 = reader2.load()
    hashes2 = [txn_hash(e) for e in loaded2.entries if isinstance(e, Transaction)]
    assert hashes == hashes2
