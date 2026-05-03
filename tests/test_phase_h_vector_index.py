# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for Phase H — vector index over resolved transactions.

Uses a deterministic fake embedder so tests don't require
sentence-transformers + torch in the test environment. Each
text gets a small, stable vector based on the set of tokens
it contains. That's enough to verify:
  * corrections get weighted higher
  * the build is idempotent
  * stale-signature triggers a rebuild
  * recency decay applies
  * results round-trip into SimilarTxn shape
"""
from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Sequence

import pytest
from beancount.core import data as bdata
from beancount.core.amount import Amount
from beancount.core.number import D

from lamella.features.ai_cascade.decisions import DecisionsLog
from lamella.features.ai_cascade.vector_index import (
    DEFAULT_CORRECTION_WEIGHT,
    VectorIndex,
    similar_transactions_via_vector,
)
from lamella.core.db import connect, migrate


# ------------------------------------------------------------------
# Fake embedder — maps each lowercased token to a one-hot dim, so
# two texts with overlapping tokens get non-zero cosine sim and
# identical texts get 1.0.
# ------------------------------------------------------------------


_VOCAB: dict[str, int] = {}
_DIM = 64


def _embed_fake(texts: Sequence[str]) -> list[list[float]]:
    import math

    out: list[list[float]] = []
    for text in texts:
        vec = [0.0] * _DIM
        for tok in text.lower().split():
            idx = _VOCAB.setdefault(tok, len(_VOCAB) % _DIM)
            vec[idx] += 1.0
        # L2 normalize so cosine sim is well-defined.
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        out.append([v / norm for v in vec])
    return out


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _txn(
    *, d: date, payee: str, narration: str,
    card: str = "Liabilities:Acme:Card:0123",
    target: str = "Expenses:Acme:Supplies",
    amount: str = "10",
    filename: str = "main.bean", lineno: int = 10,
) -> bdata.Transaction:
    amt = D(amount)
    return bdata.Transaction(
        meta={"filename": filename, "lineno": lineno},
        date=d, flag="*", payee=payee, narration=narration,
        tags=frozenset(), links=frozenset(),
        postings=[
            bdata.Posting(
                account=card, units=Amount(-amt, "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
            bdata.Posting(
                account=target, units=Amount(amt, "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
        ],
    )


# ------------------------------------------------------------------


class TestBuild:
    def test_indexes_resolved_ledger_txns(self, conn):
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        entries = [
            _txn(d=date(2026, 1, 10), payee="Hardware Store",
                 narration="lumber", target="Expenses:Acme:Supplies"),
            _txn(d=date(2026, 2, 5), payee="Coffee Shop",
                 narration="coffee", target="Expenses:Personal:Meals"),
        ]
        stats = idx.build(entries=entries, ledger_signature="sig-1")
        assert stats["ledger_added"] == 2
        n = conn.execute(
            "SELECT COUNT(*) FROM txn_embeddings WHERE source='ledger'"
        ).fetchone()[0]
        assert n == 2

    def test_fixme_txns_excluded(self, conn):
        """Unresolved (FIXME) transactions are not part of the corpus."""
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        entries = [
            _txn(d=date(2026, 1, 10), payee="X", narration="y",
                 target="Expenses:Acme:FIXME"),
        ]
        stats = idx.build(entries=entries, ledger_signature="sig-1")
        assert stats["ledger_added"] == 0

    def test_synthetic_plugin_entries_skipped(self, conn):
        """Entries whose filename starts with '<' come from plugins
        like auto_accounts — they aren't real source lines."""
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        entries = [
            _txn(d=date(2026, 1, 10), payee="X", narration="y",
                 filename="<auto_insert_open>", lineno=0),
        ]
        stats = idx.build(entries=entries, ledger_signature="sig-1")
        assert stats["ledger_added"] == 0

    def test_matching_signature_skips_rebuild(self, conn):
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        entries = [
            _txn(d=date(2026, 1, 10), payee="Target", narration="x"),
        ]
        first = idx.build(entries=entries, ledger_signature="sig-A")
        assert first["ledger_added"] == 1
        # Re-build with the same signature — nothing new.
        second = idx.build(entries=entries, ledger_signature="sig-A")
        assert second["ledger_added"] == 0

    def test_new_signature_triggers_rebuild(self, conn):
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        entries = [
            _txn(d=date(2026, 1, 10), payee="Target", narration="x"),
        ]
        idx.build(entries=entries, ledger_signature="sig-A")
        # Signature changes — rebuild happens.
        new_entries = entries + [
            _txn(d=date(2026, 2, 5), payee="Warehouse Club", narration="bulk",
                 lineno=20),
        ]
        second = idx.build(entries=new_entries, ledger_signature="sig-B")
        assert second["ledger_added"] == 2  # both re-upserted


class TestCorrectionsWeighting:
    def test_correction_rows_get_higher_weight(self, conn):
        """When ai_decisions has a user_corrected entry, its embedding
        row lands with weight=DEFAULT_CORRECTION_WEIGHT, outranking
        a plain ledger row at retrieval time."""
        txn = _txn(
            d=date(2026, 1, 10), payee="Amazon", narration="order",
            target="Expenses:Acme:Supplies",
        )
        from lamella.core.beancount_io.txn_hash import txn_hash
        th = txn_hash(txn)
        # Log an AI decision + correction against this txn.
        log = DecisionsLog(conn)
        log.log(
            decision_type="classify_txn",
            input_ref=th,
            model="test",
            result={"target_account": "Expenses:Acme:Supplies", "confidence": 0.9},
        )
        log.mark_correction(1, user_correction="auto_accepted→Expenses:Personal:Groceries")

        idx = VectorIndex(conn, embed_fn=_embed_fake)
        stats = idx.build(
            entries=[txn], ai_decisions=log, ledger_signature="sig-1",
        )
        assert stats["ledger_added"] == 1
        assert stats["corrections_added"] == 1

        # The correction row has weight > 1.
        row = conn.execute(
            "SELECT weight, target_account FROM txn_embeddings "
            "WHERE source='correction'"
        ).fetchone()
        assert row["weight"] == DEFAULT_CORRECTION_WEIGHT
        # And it carries the CORRECTED account, not the original AI guess.
        assert row["target_account"] == "Expenses:Personal:Groceries"

    def test_correction_outranks_original_at_query(self, conn):
        """When both a ledger row and a correction row exist for the
        same merchant, the correction should appear first in the
        ranked query results (weight > 1 multiplies the score)."""
        txn = _txn(
            d=date(2026, 4, 20), payee="Amazon", narration="order",
            target="Expenses:Acme:Supplies",
        )
        from lamella.core.beancount_io.txn_hash import txn_hash
        th = txn_hash(txn)
        log = DecisionsLog(conn)
        log.log(
            decision_type="classify_txn", input_ref=th, model="test",
            result={"target_account": "Expenses:Acme:Supplies"},
        )
        log.mark_correction(
            1, user_correction="auto_accepted→Expenses:Personal:Groceries",
        )
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        idx.build(entries=[txn], ai_decisions=log, ledger_signature="sig-1")

        matches = idx.query(
            needle="Amazon order", reference_date=date(2026, 4, 21),
            min_similarity=0.0,
        )
        # Both the ledger and correction rows match. The correction
        # should come first because its weight is 2x.
        assert len(matches) >= 2
        assert matches[0].source == "correction"
        assert matches[0].target_account == "Expenses:Personal:Groceries"


class TestQuery:
    def test_empty_needle_returns_empty(self, conn):
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        assert idx.query(needle="", reference_date=date(2026, 4, 20)) == []
        assert idx.query(needle="   ", reference_date=date(2026, 4, 20)) == []

    def test_token_overlap_ranks_similar_merchants(self, conn):
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        idx.build(
            entries=[
                _txn(d=date(2026, 1, 10), payee="Hardware Store",
                     narration="lumber", target="Expenses:Acme:Supplies",
                     lineno=10),
                _txn(d=date(2026, 2, 5), payee="Coffee Shop",
                     narration="coffee",
                     target="Expenses:Personal:Meals", lineno=20),
                _txn(d=date(2026, 3, 1), payee="Hardware Store",
                     narration="paint",
                     target="Expenses:Acme:Supplies", lineno=30),
            ],
            ledger_signature="sig-1",
        )
        matches = idx.query(
            needle="Hardware Store lumber",
            reference_date=date(2026, 4, 20),
            min_similarity=0.0,
        )
        # Hardware Store matches should rank above Coffee Shop.
        top_merchants = [m.merchant_text for m in matches[:2]]
        assert all("Hardware Store" in m for m in top_merchants)

    def test_recency_decay_preferes_recent(self, conn):
        """Two identical-text entries at different dates — the more
        recent one scores higher due to recency decay."""
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        idx.build(
            entries=[
                _txn(d=date(2020, 1, 10), payee="Target", narration="x",
                     target="Expenses:Acme:Supplies", lineno=10),
                _txn(d=date(2026, 1, 10), payee="Target", narration="x",
                     target="Expenses:Acme:Supplies", lineno=20),
            ],
            ledger_signature="sig-1",
        )
        matches = idx.query(
            needle="Target x",
            reference_date=date(2026, 4, 20),
            min_similarity=0.0,
            half_life_days=365,
        )
        assert len(matches) == 2
        # Recent first.
        assert matches[0].posting_date == date(2026, 1, 10)
        assert matches[0].score > matches[1].score

    def test_long_ago_still_retrievable(self, conn):
        """A 3-year-old annual tax-preparer charge should still be
        returned — recency decays it, doesn't eliminate it. This is
        the specific failure mode the substring window misses."""
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        idx.build(
            entries=[
                _txn(d=date(2023, 4, 15), payee="H and R Block",
                     narration="tax prep 2022",
                     target="Expenses:Personal:Professional:Tax",
                     lineno=10),
            ],
            ledger_signature="sig-1",
        )
        matches = idx.query(
            needle="H and R Block tax prep",
            reference_date=date(2026, 4, 20),
            min_similarity=0.0,
            half_life_days=365,
        )
        assert len(matches) == 1
        assert "tax" in matches[0].merchant_text.lower()


class TestDefaultOn:
    def test_missing_app_setting_means_enabled(self, conn):
        """No row in app_settings for ai_vector_search_enabled should
        mean ON — matches Settings.ai_vector_search_enabled defaulting
        to True."""
        from lamella.features.ai_cascade.classify import _vector_search_enabled
        assert _vector_search_enabled(conn) is True

    def test_explicit_false_disables(self, conn):
        from lamella.features.ai_cascade.classify import _vector_search_enabled
        conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value) "
            "VALUES ('ai_vector_search_enabled', 'false')"
        )
        assert _vector_search_enabled(conn) is False

    def test_explicit_zero_disables(self, conn):
        from lamella.features.ai_cascade.classify import _vector_search_enabled
        conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value) "
            "VALUES ('ai_vector_search_enabled', '0')"
        )
        assert _vector_search_enabled(conn) is False

    def test_explicit_true_enables(self, conn):
        from lamella.features.ai_cascade.classify import _vector_search_enabled
        conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value) "
            "VALUES ('ai_vector_search_enabled', 'true')"
        )
        assert _vector_search_enabled(conn) is True

    def test_settings_default_is_true(self, monkeypatch):
        """Settings.ai_vector_search_enabled must default True so a
        fresh install gets vector search without configuration."""
        # conftest sets AI_VECTOR_SEARCH_ENABLED=0 process-wide; this
        # test asserts the field default, not the env-derived value.
        monkeypatch.delenv("AI_VECTOR_SEARCH_ENABLED", raising=False)
        from lamella.core.config import Settings
        assert Settings().ai_vector_search_enabled is True


def _txn_two_leg(
    *, d: date, payee: str, narration: str,
    leg_a: tuple[str, str],  # (account, amount_str — signed)
    leg_b: tuple[str, str],
    filename: str = "main.bean", lineno: int = 10,
) -> bdata.Transaction:
    """Two-posting txn with explicit per-leg amounts so tests can
    exercise non-expense targets (income deposits, CC payments,
    loan splits, transfers) where the default _txn fixture's
    "card + expense" shape doesn't apply."""
    return bdata.Transaction(
        meta={"filename": filename, "lineno": lineno},
        date=d, flag="*", payee=payee, narration=narration,
        tags=frozenset(), links=frozenset(),
        postings=[
            bdata.Posting(
                account=leg_a[0], units=Amount(D(leg_a[1]), "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
            bdata.Posting(
                account=leg_b[0], units=Amount(D(leg_b[1]), "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
        ],
    )


class TestTargetResolver:
    """Phase 1 of AI-AGENT.md: _first_resolved_target now picks the
    most-classification-relevant posting across every root, not
    just Expenses."""

    def test_picks_expenses_first(self, conn):
        from lamella.features.ai_cascade.vector_index import _first_resolved_target
        t = _txn(d=date(2026, 2, 1), payee="Hardware Store", narration="lumber",
                 target="Expenses:Acme:Supplies")
        assert _first_resolved_target(t) == "Expenses:Acme:Supplies"

    def test_income_attribution(self, conn):
        from lamella.features.ai_cascade.vector_index import _first_resolved_target
        t = _txn_two_leg(
            d=date(2026, 4, 14), payee="ATM Deposit", narration="cash deposit",
            leg_a=("Assets:Personal:Checking", "800"),
            leg_b=("Income:Acme:Sales", "-800"),
        )
        assert _first_resolved_target(t) == "Income:Acme:Sales"

    def test_cc_payment_picks_liability(self, conn):
        from lamella.features.ai_cascade.vector_index import _first_resolved_target
        t = _txn_two_leg(
            d=date(2026, 4, 15), payee="Chase",
            narration="PAYMENT THANK YOU",
            leg_a=("Assets:Personal:Checking", "-500"),
            leg_b=("Liabilities:CreditCard:Chase4422", "500"),
        )
        assert _first_resolved_target(t) == "Liabilities:CreditCard:Chase4422"

    def test_asset_transfer_picks_destination(self, conn):
        from lamella.features.ai_cascade.vector_index import _first_resolved_target
        # Transfer from checking to savings — pick the destination
        # (positive amount) so monthly transfers cluster by destination.
        t = _txn_two_leg(
            d=date(2026, 4, 1), payee=None, narration="Transfer to Savings",
            leg_a=("Assets:Personal:Checking", "-1000"),
            leg_b=("Assets:Personal:Savings:Ally", "1000"),
        )
        assert _first_resolved_target(t) == "Assets:Personal:Savings:Ally"

    def test_fixme_skipped_everywhere(self, conn):
        """FIXME leaves skipped across every root, not just Expenses."""
        from lamella.features.ai_cascade.vector_index import _first_resolved_target
        t = _txn_two_leg(
            d=date(2026, 4, 1), payee="?", narration="?",
            leg_a=("Assets:Personal:Checking", "500"),
            leg_b=("Income:FIXME", "-500"),
        )
        # Income:FIXME is skipped → falls back to the Assets leg.
        assert _first_resolved_target(t) == "Assets:Personal:Checking"

    def test_no_classifiable_posting_returns_none(self, conn):
        from lamella.features.ai_cascade.vector_index import _first_resolved_target
        t = _txn_two_leg(
            d=date(2026, 4, 1), payee="?", narration="?",
            leg_a=("Expenses:FIXME", "10"),
            leg_b=("Liabilities:FIXME", "-10"),
        )
        assert _first_resolved_target(t) is None


class TestScopeFilter:
    """Phase 1 mandates a query-time target_roots filter so the
    widened index doesn't pollute expense lookups with CC-payment
    or transfer neighbors."""

    def test_expenses_scope_filters_out_liability_rows(self, conn):
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        entries = [
            _txn(d=date(2026, 3, 1), payee="Fast Food", narration="fries",
                 target="Expenses:Personal:Meals"),
            _txn_two_leg(
                d=date(2026, 3, 15), payee="Chase",
                narration="PAYMENT THANK YOU CHASE",
                leg_a=("Assets:Personal:Checking", "-500"),
                leg_b=("Liabilities:CreditCard:Chase4422", "500"),
            ),
        ]
        idx.build(entries=entries, ledger_signature="sig-scope")

        # Scoped to Expenses: only the Fast Food row should come back,
        # even though "CHASE" overlaps the needle tokens.
        matches = idx.query(
            needle="FAST FOOD",
            reference_date=date(2026, 4, 20),
            target_roots=("Expenses",),
        )
        accts = [m.target_account for m in matches]
        assert "Expenses:Personal:Meals" in accts
        assert not any(a.startswith("Liabilities:") for a in accts)

    def test_none_target_roots_returns_all(self, conn):
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        entries = [
            _txn_two_leg(
                d=date(2026, 3, 15), payee="Chase",
                narration="PAYMENT THANK YOU CHASE",
                leg_a=("Assets:Personal:Checking", "-500"),
                leg_b=("Liabilities:CreditCard:Chase4422", "500"),
            ),
        ]
        idx.build(entries=entries, ledger_signature="sig-all")
        matches = idx.query(
            needle="PAYMENT CHASE",
            reference_date=date(2026, 4, 20),
            target_roots=None,
        )
        assert any(
            m.target_account == "Liabilities:CreditCard:Chase4422"
            for m in matches
        )

    def test_multi_root_scope_query(self, conn):
        idx = VectorIndex(conn, embed_fn=_embed_fake)
        entries = [
            _txn(d=date(2026, 3, 1), payee="Fast Food", narration="fries",
                 target="Expenses:Personal:Meals"),
            _txn_two_leg(
                d=date(2026, 4, 14), payee="ATM Deposit",
                narration="cash deposit ATM",
                leg_a=("Assets:Personal:Checking", "800"),
                leg_b=("Income:Acme:Sales", "-800"),
            ),
        ]
        idx.build(entries=entries, ledger_signature="sig-multi")
        matches = idx.query(
            needle="cash deposit ATM",
            reference_date=date(2026, 4, 20),
            target_roots=("Expenses", "Income"),
        )
        accts = [m.target_account for m in matches]
        assert "Income:Acme:Sales" in accts


class TestSeam:
    def test_similar_transactions_via_vector_returns_SimilarTxn(self, conn):
        """The seam used by classify context returns the existing
        SimilarTxn dataclass shape, so downstream prompt rendering
        doesn't need to care which backend produced the matches."""
        entries = [
            _txn(d=date(2026, 1, 10), payee="Target", narration="x",
                 target="Expenses:Acme:Supplies", amount="12.34",
                 lineno=10),
        ]
        results = similar_transactions_via_vector(
            conn, entries,
            needle="Target x",
            reference_date=date(2026, 4, 20),
            embed_fn=_embed_fake,
            ledger_signature="sig-1",
        )
        assert len(results) == 1
        from lamella.features.ai_cascade.context import SimilarTxn
        assert isinstance(results[0], SimilarTxn)
        assert results[0].target_account == "Expenses:Acme:Supplies"
        assert results[0].narration == "Target x"
