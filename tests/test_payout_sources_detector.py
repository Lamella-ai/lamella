# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the payout-source detector.

Confirms:
  * Pattern matching is lowercase substring on payee + narration
    (matches the rule engine's behavior).
  * Amazon Seller patterns match seller-payout text only — generic
    "amazon" does NOT trigger a candidate (so retail Amazon
    spending doesn't get suggested as a payout).
  * Direction threshold suppresses processors the user mostly pays
    rather than receives from.
  * Frequency threshold suppresses one-off payments.
  * Multi-entity: same pattern hitting two different entities'
    checking accounts produces two candidates, one per entity.
  * Already-scaffolded accounts get flagged so the UI can hide the
    suggestion.
"""
from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.bank_sync.payout_sources import (
    PAYOUT_PATTERNS,
    PayoutCandidate,
    detect_payout_sources,
    match_payout_pattern,
    suggested_account_path,
)
from lamella.features.import_.staging import StagingService


@pytest.fixture()
def conn() -> sqlite3.Connection:
    c = connect(Path(":memory:"))
    migrate(c)
    return c


@pytest.fixture()
def svc(conn: sqlite3.Connection) -> StagingService:
    return StagingService(conn)


def _seed_account(
    conn: sqlite3.Connection, *,
    account_path: str,
    entity_slug: str,
    simplefin_account_id: str | None = None,
    kind: str | None = "checking",
) -> None:
    """Insert a row into accounts_meta. Needed so the staged-row
    scanner can resolve simplefin_account_id → account_path. The
    entities row is seeded too because accounts_meta.entity_slug
    has a FK to entities.slug."""
    if entity_slug:
        conn.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name) VALUES (?, ?)",
            (entity_slug, entity_slug),
        )
    conn.execute(
        "INSERT INTO accounts_meta "
        "    (account_path, display_name, entity_slug, "
        "     simplefin_account_id, kind, is_active, seeded_from_ledger) "
        " VALUES (?, ?, ?, ?, ?, 1, 0)",
        (account_path, account_path.split(":")[-1],
         entity_slug or None, simplefin_account_id, kind),
    )
    conn.commit()


def _stage_inflow(
    svc: StagingService, *,
    sf_account_id: str, posting_date: str, amount: str,
    payee: str, description: str = "",
) -> int:
    return svc.stage(
        source="simplefin",
        source_ref={"account_id": sf_account_id, "txn_id": payee + posting_date},
        posting_date=posting_date,
        amount=amount,
        payee=payee,
        description=description,
    ).id


# --- pattern matching ------------------------------------------------------


class TestPatternMatching:
    @pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
    def test_ebay_payments_string_matches(self):
        # The exact shape the user's bank uses for eBay payouts:
        # "eBay ComQ3PNNNED PAYMENTS 260404 6C1AYSMSLMCHP6H NTE*ZZZ*…"
        pat = match_payout_pattern("eBay ComQ3PNNNED PAYMENTS 260404")
        assert pat is not None
        assert pat.id == "ebay"
        assert pat.leaf == "eBay"

    def test_paypal_matches(self):
        assert match_payout_pattern("PAYPAL TRANSFER").id == "paypal"
        assert match_payout_pattern("PayPal *MERCHANT").id == "paypal"

    def test_stripe_matches(self):
        assert match_payout_pattern("STRIPE PAYOUT").id == "stripe"

    def test_square_only_matches_brand_tokens(self):
        # The English word "square" alone shouldn't match — too noisy.
        assert match_payout_pattern("square footage of building") is None
        # Real Square deposits include the brand token.
        assert match_payout_pattern("SQUAREUP *DEPOSIT").id == "square"
        assert match_payout_pattern("SQ *MERCHANT NAME").id == "square"

    def test_amazon_seller_matches_seller_text_only(self):
        # Generic "AMAZON.COM" (retail purchase) must NOT match —
        # otherwise we'd suggest scaffolding a payout for every
        # retail Amazon outflow.
        assert match_payout_pattern("AMAZON.COM ORDER #123") is None
        assert match_payout_pattern("AMAZON MARKETPLACE") is None
        # Seller-payout patterns DO match.
        assert match_payout_pattern("AMAZON MKTPL PAYMENT").id == "amazon_seller"
        assert match_payout_pattern("AMZN PMTS DISBURSEMENT").id == "amazon_seller"
        assert match_payout_pattern("Amazon.com Services LLC").id == "amazon_seller"

    def test_no_match_returns_none(self):
        assert match_payout_pattern(None) is None
        assert match_payout_pattern("") is None
        assert match_payout_pattern("Local Coffee Shop") is None
        assert match_payout_pattern("ACH DEPOSIT EMPLOYER") is None

    def test_suggested_path_simple(self):
        assert suggested_account_path("Acme", "eBay") == "Assets:Acme:eBay"

    def test_suggested_path_nested_for_collisions(self):
        # The nested "Amazon:Seller" leaf produces a sub-level path,
        # matching the user's preference for hierarchy over verbose
        # leaves when the brand spans multiple roles.
        assert (
            suggested_account_path("Acme", "Amazon:Seller")
            == "Assets:Acme:Amazon:Seller"
        )


# --- detection across staged data -----------------------------------------


class TestDetectionFromStaging:
    @pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
    def test_three_ebay_payouts_to_business_checking_creates_candidate(
        self, conn, svc,
    ):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        for d in ("2026-03-24", "2026-04-06", "2026-04-20"):
            _stage_inflow(
                svc,
                sf_account_id="acme-chk",
                posting_date=d, amount="150.00",
                payee="eBay",
                description="eBay Com PAYMENTS 260404",
            )
        cands = detect_payout_sources(conn, entries=[])
        assert len(cands) == 1
        c = cands[0]
        assert c.pattern_id == "ebay"
        assert c.entity == "Acme"
        assert c.suggested_path == "Assets:Acme:eBay"
        assert c.hits == 3
        assert c.inbound_count == 3
        assert c.outbound_count == 0
        assert c.inbound_share == pytest.approx(1.0)
        assert c.already_scaffolded is False

    def test_below_min_hits_excluded(self, conn, svc):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        # Two hits — below the default min_hits=3.
        for d in ("2026-04-06", "2026-04-20"):
            _stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="150.00",
                payee="eBay PAYMENTS",
            )
        assert detect_payout_sources(conn, entries=[]) == []

    def test_below_inbound_share_excluded(self, conn, svc):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        # Three inflows + two outflows from PayPal = 60% inbound,
        # below the default 80% threshold. Treat as a regular
        # processor the user pays AND receives from, not a payout
        # source the system should suggest scaffolding for.
        for d in ("2026-04-01", "2026-04-08", "2026-04-15"):
            _stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="50.00",
                payee="PayPal",
            )
        for d in ("2026-04-04", "2026-04-11"):
            _stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="-25.00",
                payee="PayPal",
            )
        assert detect_payout_sources(conn, entries=[]) == []

    def test_split_across_two_entities_yields_two_candidates(
        self, conn, svc,
    ):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        _seed_account(
            conn,
            account_path="Assets:Personal:Checking",
            entity_slug="Personal",
            simplefin_account_id="personal-chk",
        )
        for d in ("2026-03-24", "2026-04-06", "2026-04-20"):
            _stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="150.00",
                payee="PayPal",
            )
        for d in ("2026-03-28", "2026-04-08", "2026-04-22"):
            _stage_inflow(
                svc, sf_account_id="personal-chk",
                posting_date=d, amount="40.00",
                payee="PayPal",
            )
        cands = detect_payout_sources(conn, entries=[])
        entities = sorted(c.entity for c in cands)
        assert entities == ["Acme", "Personal"]
        suggested = sorted(c.suggested_path for c in cands)
        assert suggested == [
            "Assets:Acme:PayPal",
            "Assets:Personal:PayPal",
        ]

    def test_amazon_retail_purchases_do_not_create_candidate(self, conn, svc):
        """If 5 outflows hit AMAZON.COM but there's no seller-side
        text, we must not suggest scaffolding an Amazon payout
        account."""
        _seed_account(
            conn,
            account_path="Assets:Personal:Checking",
            entity_slug="Personal",
            simplefin_account_id="personal-chk",
        )
        # Mix of inflows (refunds!) and outflows on retail Amazon —
        # the detector shouldn't pick this up at all because the
        # text doesn't match any pattern.
        for d in ("2026-04-01", "2026-04-08", "2026-04-15"):
            _stage_inflow(
                svc, sf_account_id="personal-chk",
                posting_date=d, amount="-25.00",
                payee="AMAZON.COM",
                description="ORDER #ABC123",
            )
        for d in ("2026-04-04", "2026-04-11"):
            _stage_inflow(
                svc, sf_account_id="personal-chk",
                posting_date=d, amount="25.00",
                payee="AMAZON.COM REFUND",
            )
        assert detect_payout_sources(conn, entries=[]) == []

    def test_amazon_seller_payouts_create_candidate(self, conn, svc):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        for d, descr in [
            ("2026-03-24", "AMZN MKTP PMT 260324"),
            ("2026-04-06", "AMAZON MKTPL PAYMENT"),
            ("2026-04-20", "Amazon.com Services LLC DISB"),
        ]:
            _stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="800.00",
                payee="Amazon",
                description=descr,
            )
        cands = detect_payout_sources(conn, entries=[])
        assert len(cands) == 1
        assert cands[0].pattern_id == "amazon_seller"
        assert cands[0].suggested_leaf == "Amazon:Seller"
        assert cands[0].suggested_path == "Assets:Acme:Amazon:Seller"

    @pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
    def test_already_scaffolded_flag_set_when_account_exists(
        self, conn, svc,
    ):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        # Pre-existing payout-source account.
        _seed_account(
            conn,
            account_path="Assets:Acme:eBay",
            entity_slug="Acme",
            kind="payout",
        )
        for d in ("2026-03-24", "2026-04-06", "2026-04-20"):
            _stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="150.00",
                payee="eBay PAYMENTS",
            )
        cands = detect_payout_sources(conn, entries=[])
        assert len(cands) == 1
        assert cands[0].already_scaffolded is True

    def test_non_entity_first_account_skipped(self, conn, svc):
        """Pre-Phase-G ledgers may have a flat ``Assets:Checking``
        path. We can't infer the entity from that, so we don't
        emit a candidate (we'd have nothing to suggest)."""
        _seed_account(
            conn,
            account_path="Assets:Checking",
            entity_slug="",
            simplefin_account_id="flat-chk",
        )
        for d in ("2026-03-24", "2026-04-06", "2026-04-20"):
            _stage_inflow(
                svc, sf_account_id="flat-chk",
                posting_date=d, amount="150.00",
                payee="eBay PAYMENTS",
            )
        assert detect_payout_sources(conn, entries=[]) == []


# --- ledger-side detection -------------------------------------------------


class TestDetectionFromLedger:
    """Once a payout has been classified into the ledger (e.g. as a
    transfer to ``Assets:Acme:eBay``), it should still count toward
    the histogram so the candidate's hits reflect total observed
    activity. This exercises the entry-scan path."""

    @pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
    def test_ledger_entries_contribute_to_hits(self, conn):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
        )
        # Hand-build minimal Beancount Transactions.
        from beancount.core.amount import Amount
        from beancount.core.data import Posting, Transaction
        from beancount.core.position import CostSpec

        def _txn(d: str, amount: str, payee: str) -> Transaction:
            return Transaction(
                meta={"filename": "x", "lineno": 1},
                date=date.fromisoformat(d),
                flag="*",
                payee=payee,
                narration="",
                tags=set(),
                links=set(),
                postings=[
                    Posting(
                        account="Assets:Acme:Checking",
                        units=Amount(Decimal(amount), "USD"),
                        cost=None, price=None, flag=None, meta={},
                    ),
                    Posting(
                        account="Income:Acme:eBay",
                        units=Amount(Decimal(amount) * -1, "USD"),
                        cost=None, price=None, flag=None, meta={},
                    ),
                ],
            )
        entries = [
            _txn("2026-03-24", "150.00", "eBay PAYMENTS"),
            _txn("2026-04-06", "150.00", "eBay PAYMENTS"),
            _txn("2026-04-20", "150.00", "eBay PAYMENTS"),
        ]
        cands = detect_payout_sources(conn, entries=entries)
        assert len(cands) == 1
        assert cands[0].pattern_id == "ebay"
        assert cands[0].hits == 3
        assert cands[0].suggested_path == "Assets:Acme:eBay"
