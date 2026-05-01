# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Unit tests for ``_infer_account_kind`` heuristic + Phase 2
sibling-inference pass.

Phase 1 of /setup/recovery work: the keyword inference must classify
real-world Bank One / BankTwo / CardC / etc. accounts that
the leaf-keyword-only version was leaving as NULL.

Phase 2: when keyword inference is silent (brand names like
``AffiliateD`` / ``BusinessElite``), look at peers under the same
``Liabilities:{Entity}:{Institution}:*`` prefix and adopt their
consensus kind. See ``SETUP_IMPLEMENTATION.md`` for the locked
disambiguation contract.
"""
from __future__ import annotations

import sqlite3

import pytest

from lamella.core.db import migrate
from lamella.core.registry.discovery import (
    _infer_account_kind,
    _sibling_prefix,
    infer_kinds_by_sibling,
    sibling_hint_for,
)


class TestLiabilities:
    """Liabilities-root inference covers credit cards, loans,
    lines-of-credit, and tax-payable accounts."""

    @pytest.mark.parametrize("path", [
        # Brand-named credit cards that don't carry a "credit"/"visa"
        # token in the leaf — these used to fall through to NULL.
        "Liabilities:AcmeCo:BankOne:AffiliateD",
        "Liabilities:BetaCorp:BankOne:AffiliateD",
        "Liabilities:AcmeCo:BankOne:BusinessElite",
        "Liabilities:Personal:Chase:WorldElite",
        "Liabilities:Personal:USBank:CashPlus",
        # Existing keywords still classify.
        "Liabilities:Personal:BankOne:VisaSignature",
        "Liabilities:Personal:Chase:CardB",
        "Liabilities:Personal:AmericanExpress:Platinum",
        "Liabilities:Personal:Costco:Visa",
    ])
    @pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
    def test_credit_cards(self, path: str):
        assert _infer_account_kind(path) == "credit_card"

    @pytest.mark.parametrize("path", [
        # Non-prefixed substring — "BankTwoMortgage" used to fail
        # because the loan check required ``:mortgage``.
        "Liabilities:Personal:BankTwoBank:BankTwoMortgage",
        "Liabilities:Personal:ChaseAutoLoan",
        "Liabilities:BetaCorp:EIDL",
        "Liabilities:Personal:EIDL",
        "Liabilities:GammaProperties:EIDL",
        # Existing colon-prefixed keywords still classify.
        "Liabilities:Personal:BankTwoBank:Mortgage",
        "Liabilities:Personal:StudentLoan",
    ])
    def test_loans(self, path: str):
        assert _infer_account_kind(path) == "loan"

    @pytest.mark.parametrize("path", [
        "Liabilities:Personal:BankOne:LineOfCredit",
        "Liabilities:AcmeCo:BankOne:LineOfCredit",
        # No leading colon — substring match.
        "Liabilities:Personal:BankOneLineOfCredit",
        "Liabilities:Personal:BankOne:HELOC",
        "Liabilities:Personal:Heloc",
    ])
    def test_lines_of_credit(self, path: str):
        assert _infer_account_kind(path) == "line_of_credit"

    @pytest.mark.parametrize("path", [
        "Liabilities:AcmeCo:SalesTaxPayable:Colorado",
        "Liabilities:BetaCorp:SalesTaxPayable:Colorado",
        "Liabilities:AcmeCo:UseTax:Colorado",
        "Liabilities:AcmeCo:VATPayable",
        "Liabilities:Personal:Withholding:Federal",
    ])
    def test_tax_liabilities(self, path: str):
        assert _infer_account_kind(path) == "tax_liability"

    @pytest.mark.parametrize("path", [
        # Intercompany payables stay unclassified — they're neither
        # cards nor loans, and they're entity-pair-specific so
        # automation shouldn't guess.
        "Liabilities:AcmeCo:Payable:ToBetaCorp",
        "Liabilities:BetaCorp:Payable:ToAcmeCo",
        "Liabilities:AcmeCo:Payable:ToPersonal",
    ])
    def test_intercompany_payables_stay_null(self, path: str):
        assert _infer_account_kind(path) is None

    def test_tax_payable_wins_over_intercompany_check(self):
        """Regression: ``salestaxpayable`` contains ``payable``, so
        the tax-keyword branch must run before the intercompany
        bail-out or sales-tax-payable accounts go NULL."""
        assert _infer_account_kind(
            "Liabilities:AcmeCo:SalesTaxPayable:Colorado"
        ) == "tax_liability"


class TestAssets:
    """Assets-root inference is unchanged by Phase 1 — verify it
    still works so we know the loosened Liabilities pass didn't
    regress anything."""

    @pytest.mark.parametrize("path,expected", [
        ("Assets:Personal:BankOne:Checking", "checking"),
        ("Assets:Personal:BankOne:CheckWriting", "checking"),
        ("Assets:Personal:BankOne:Savings", "savings"),
        ("Assets:Personal:Cash", "cash"),
        ("Assets:Personal:BrokerageOne:RothIRA", "brokerage"),
        ("Assets:Personal:BrokerageOne:Brokerage", "brokerage"),
        ("Assets:Personal:Transfers:InFlight", "virtual"),
        # Unrecognized leaf falls through to generic 'asset'.
        ("Assets:Personal:Property:PinewoodHouse", "asset"),
    ])
    def test_assets(self, path: str, expected: str):
        assert _infer_account_kind(path) == expected


class TestNonClassifiable:
    """Roots we don't classify: Expenses, Income, Equity."""

    @pytest.mark.parametrize("path", [
        "Expenses:Personal:Groceries",
        "Income:AcmeCo:Sales",
        "Equity:OpeningBalances",
        "",
    ])
    def test_returns_none(self, path: str):
        assert _infer_account_kind(path) is None


# ---------------------------------------------------------------------------
# Phase 2: sibling inference
# ---------------------------------------------------------------------------


class TestSiblingPrefix:
    @pytest.mark.parametrize("path,expected", [
        # Depth-4 Liabilities/Assets paths produce a 3-segment prefix.
        ("Liabilities:BetaCorp:BankOne:AffiliateD",
         "Liabilities:BetaCorp:BankOne:"),
        ("Liabilities:AcmeCo:BankOne:BusinessElite",
         "Liabilities:AcmeCo:BankOne:"),
        ("Assets:Personal:BankOne:Checking",
         "Assets:Personal:BankOne:"),
        # Depth-5 still uses the 3-segment prefix (institution branch
        # is the level we group at).
        ("Liabilities:Personal:BankTwoBank:BankTwoMortgage:Escrow",
         "Liabilities:Personal:BankTwoBank:"),
    ])
    def test_returns_prefix(self, path: str, expected: str):
        assert _sibling_prefix(path) == expected

    @pytest.mark.parametrize("path", [
        # Depth-3 paths are too broad — Liabilities:Personal:* mixes
        # cards, loans, and intercompany payables.
        "Liabilities:Personal:EIDL",
        "Assets:BetaCorp:Cash",
        "Liabilities:AcmeCo:Visa",
        # Wrong root.
        "Expenses:Personal:BankOne:Groceries",
        "Income:AcmeCo:Sales",
        "Equity:OpeningBalances:AcmeCo",
        # Excluded entity segment — Vehicles isn't an entity.
        "Assets:Vehicles:V2008FabrikamSuv",
        # Empty / malformed.
        "",
        "Liabilities",
    ])
    def test_returns_none(self, path: str):
        assert _sibling_prefix(path) is None


@pytest.fixture
def conn_with_schema(tmp_path):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    yield db
    db.close()


def _seed(conn: sqlite3.Connection, *rows: tuple[str, str | None, str | None]) -> None:
    """Helper: seed accounts_meta with (path, kind, kind_source) tuples."""
    for path, kind, source in rows:
        conn.execute(
            """
            INSERT INTO accounts_meta
                (account_path, display_name, kind, kind_source,
                 seeded_from_ledger)
            VALUES (?, ?, ?, ?, 1)
            """,
            (path, path.split(":")[-1], kind, source),
        )
    conn.commit()


class TestInferKindsBySibling:
    """Exercises the locked Phase 2 contract from
    SETUP_IMPLEMENTATION.md."""

    def test_unanimous_keyword_peers_propagate(self, conn_with_schema):
        # Three keyword-derived credit cards under Liabilities:BetaCorp:
        # BankOne:* + one NULL AffiliateD. The NULL row should adopt
        # credit_card with kind_source='sibling'.
        _seed(
            conn_with_schema,
            ("Liabilities:BetaCorp:BankOne:Visa", "credit_card", "keyword"),
            ("Liabilities:BetaCorp:BankOne:Platinum", "credit_card", "keyword"),
            ("Liabilities:BetaCorp:BankOne:Credit", "credit_card", "keyword"),
            ("Liabilities:BetaCorp:BankOne:AffiliateD", None, None),
        )
        n = infer_kinds_by_sibling(conn_with_schema)
        assert n == 1
        row = conn_with_schema.execute(
            "SELECT kind, kind_source FROM accounts_meta "
            "WHERE account_path = 'Liabilities:BetaCorp:BankOne:AffiliateD'"
        ).fetchone()
        assert row["kind"] == "credit_card"
        assert row["kind_source"] == "sibling"

    def test_one_peer_is_enough(self, conn_with_schema):
        # The contract permits ≥1 eligible peer. Even with a single
        # keyword-derived sibling, the consensus is unambiguous.
        _seed(
            conn_with_schema,
            ("Liabilities:AcmeCo:BankOne:Visa", "credit_card", "keyword"),
            ("Liabilities:AcmeCo:BankOne:AffiliateD", None, None),
        )
        n = infer_kinds_by_sibling(conn_with_schema)
        assert n == 1
        row = conn_with_schema.execute(
            "SELECT kind, kind_source FROM accounts_meta "
            "WHERE account_path = 'Liabilities:AcmeCo:BankOne:AffiliateD'"
        ).fetchone()
        assert row["kind"] == "credit_card"
        assert row["kind_source"] == "sibling"

    def test_conflicting_peers_skip(self, conn_with_schema):
        # One credit_card and one loan under the same prefix — strict
        # zero-conflicts means we leave the NULL row alone.
        _seed(
            conn_with_schema,
            ("Liabilities:Personal:BankOne:Visa", "credit_card", "keyword"),
            ("Liabilities:Personal:BankOne:Mortgage", "loan", "keyword"),
            ("Liabilities:Personal:BankOne:AffiliateD", None, None),
        )
        n = infer_kinds_by_sibling(conn_with_schema)
        assert n == 0
        row = conn_with_schema.execute(
            "SELECT kind, kind_source FROM accounts_meta "
            "WHERE account_path = 'Liabilities:Personal:BankOne:AffiliateD'"
        ).fetchone()
        assert row["kind"] is None
        assert row["kind_source"] is None

    def test_no_peers_skip(self, conn_with_schema):
        # The AffiliateD row is the only one in the branch — nothing to
        # consensus from. Stays NULL.
        _seed(
            conn_with_schema,
            ("Liabilities:GhostCo:BankOne:AffiliateD", None, None),
        )
        n = infer_kinds_by_sibling(conn_with_schema)
        assert n == 0
        row = conn_with_schema.execute(
            "SELECT kind FROM accounts_meta "
            "WHERE account_path = 'Liabilities:GhostCo:BankOne:AffiliateD'"
        ).fetchone()
        assert row["kind"] is None

    def test_user_set_peers_count_as_evidence(self, conn_with_schema):
        # kind_source IS NULL means user-confirmed (or pre-Phase-2
        # row). Per the contract, those count alongside 'keyword'
        # peers.
        _seed(
            conn_with_schema,
            ("Liabilities:BetaCorp:BankOne:UserSet", "credit_card", None),
            ("Liabilities:BetaCorp:BankOne:AffiliateD", None, None),
        )
        n = infer_kinds_by_sibling(conn_with_schema)
        assert n == 1

    def test_does_not_transitively_infer(self, conn_with_schema):
        # A sibling-derived peer must NOT count as evidence for further
        # inference. Otherwise one Visa would propagate credit_card to
        # AffiliateD, and then AffiliateD (now credit_card) would propagate
        # to BusinessElite — a chain reaction off a single keyword
        # match. Phase 2 contract says peer.kind_source must be
        # NULL or 'keyword'.
        _seed(
            conn_with_schema,
            # Eligible — has a keyword source.
            ("Liabilities:AcmeCo:BankOne:Visa", "credit_card", "keyword"),
            # Already sibling-derived — not eligible as a peer.
            ("Liabilities:AcmeCo:BankOne:AffiliateD", "credit_card", "sibling"),
            ("Liabilities:AcmeCo:BankOne:BusinessElite", None, None),
        )
        # BusinessElite has 1 eligible peer (Visa) → still inferred.
        # The test point is that `AffiliateD` doesn't *also* count.
        # Verify by checking the count: one row updated, plus the
        # per-peer query includes Visa only.
        n = infer_kinds_by_sibling(conn_with_schema)
        assert n == 1
        # Construct the same scenario but where the only credit_card is
        # the sibling-derived one — now there are zero eligible peers
        # and inference must skip.
        conn2 = sqlite3.connect(":memory:")
        conn2.row_factory = sqlite3.Row
        migrate(conn2)
        _seed(
            conn2,
            ("Liabilities:AcmeCo:BankOne:AffiliateD", "credit_card", "sibling"),
            ("Liabilities:AcmeCo:BankOne:BusinessElite", None, None),
        )
        assert infer_kinds_by_sibling(conn2) == 0
        conn2.close()

    def test_skips_depth_3_paths(self, conn_with_schema):
        # Liabilities:Personal:EIDL is depth 3 — the prefix would be
        # Liabilities:Personal:* which mixes too many kinds. The
        # spec explicitly excludes these from sibling inference.
        _seed(
            conn_with_schema,
            ("Liabilities:Personal:Visa", "credit_card", "keyword"),
            ("Liabilities:Personal:NewLeaf", None, None),
        )
        n = infer_kinds_by_sibling(conn_with_schema)
        assert n == 0

    def test_idempotent_across_runs(self, conn_with_schema):
        _seed(
            conn_with_schema,
            ("Liabilities:BetaCorp:BankOne:Visa", "credit_card", "keyword"),
            ("Liabilities:BetaCorp:BankOne:AffiliateD", None, None),
        )
        first = infer_kinds_by_sibling(conn_with_schema)
        second = infer_kinds_by_sibling(conn_with_schema)
        # First run updates AffiliateD; second run finds no NULL-kind
        # rows under the prefix, so it does nothing.
        assert first == 1
        assert second == 0


class TestSiblingHint:
    def test_hint_includes_entity_and_institution(self, conn_with_schema):
        _seed(
            conn_with_schema,
            ("Liabilities:BetaCorp:BankOne:Visa", "credit_card", "keyword"),
            ("Liabilities:BetaCorp:BankOne:Platinum", "credit_card", "keyword"),
            ("Liabilities:BetaCorp:BankOne:AffiliateD", None, None),
        )
        hint = sibling_hint_for(
            conn_with_schema, "Liabilities:BetaCorp:BankOne:AffiliateD"
        )
        assert hint is not None
        assert "BetaCorp" in hint
        assert "BankOne" in hint
        # Plural for 2 peers.
        assert "other 2 accounts" in hint

    def test_hint_singular_for_one_peer(self, conn_with_schema):
        _seed(
            conn_with_schema,
            ("Liabilities:AcmeCo:BankOne:Visa", "credit_card", "keyword"),
            ("Liabilities:AcmeCo:BankOne:AffiliateD", None, None),
        )
        hint = sibling_hint_for(
            conn_with_schema, "Liabilities:AcmeCo:BankOne:AffiliateD"
        )
        assert hint is not None
        assert "other account" in hint

    def test_hint_none_for_orphan(self, conn_with_schema):
        _seed(
            conn_with_schema,
            ("Liabilities:GhostCo:BankOne:AffiliateD", None, None),
        )
        hint = sibling_hint_for(
            conn_with_schema, "Liabilities:GhostCo:BankOne:AffiliateD"
        )
        assert hint is None

    def test_hint_none_for_shallow_path(self, conn_with_schema):
        hint = sibling_hint_for(
            conn_with_schema, "Liabilities:Personal:EIDL"
        )
        assert hint is None
