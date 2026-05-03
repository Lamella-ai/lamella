# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the SuggestionCard registry — the primitive that
feeds the dashboard / /review / /card suggestion strip.

Confirms:
  * Global context emits one card per detected payout candidate.
  * Already-scaffolded candidates are skipped (no churn on accepted
    suggestions).
  * Row context emits a card matching THIS row's pattern + entity,
    bypassing the frequency threshold.
  * Row context skips when the row's payee doesn't match any
    pattern, when its receiving account isn't entity-first, or
    when the suggested account already exists.
  * Cards are filtered to their declared context — a global-only
    card never escapes into a row slot and vice versa.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.import_.staging import StagingService
from lamella.features.review_queue.suggestions import SuggestionCard, build_suggestion_cards


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


# --- global context --------------------------------------------------------


class TestGlobalContext:
    def test_emits_card_for_detected_payout_source(self, conn, svc):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        for d in ("2026-03-24", "2026-04-06", "2026-04-20"):
            _stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="150.00",
                payee="eBay PAYMENTS",
            )
        cards = build_suggestion_cards(conn, [], context="global")
        assert len(cards) == 1
        c = cards[0]
        assert isinstance(c, SuggestionCard)
        assert c.kind == "payout_source"
        assert c.id == "payout_source:ebay:Acme"
        assert c.cta_action == "/settings/payout-sources/scaffold"
        assert c.cta_form_data["pattern_id"] == "ebay"
        assert c.cta_form_data["entity"] == "Acme"
        # Per ADR-0045, on-ledger account segments must start with [A-Z];
        # the eBay payout pattern's leaf is "Ebay" (display name keeps
        # the canonical "eBay" branding — see PAYOUT_PATTERNS).
        assert c.cta_form_data["suggested_path"] == "Assets:Acme:Ebay"
        assert "global" in c.contexts

    def test_skips_already_scaffolded(self, conn, svc):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        # Per ADR-0045 the eBay leaf is "Ebay" on-ledger.
        _seed_account(
            conn,
            account_path="Assets:Acme:Ebay",
            entity_slug="Acme",
            kind="payout",
        )
        for d in ("2026-03-24", "2026-04-06", "2026-04-20"):
            _stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="150.00",
                payee="eBay PAYMENTS",
            )
        # The detector still finds the candidate but flags it
        # already_scaffolded; the registry uses that flag to
        # suppress the card entirely.
        cards = build_suggestion_cards(conn, [], context="global")
        assert cards == []

    def test_no_data_no_cards(self, conn):
        assert build_suggestion_cards(conn, [], context="global") == []


# --- row context -----------------------------------------------------------


class TestRowContext:
    def test_emits_card_when_row_matches_pattern(self, conn):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
        )
        cards = build_suggestion_cards(
            conn, [],
            context="row",
            row_payee_text="eBay ComQ3PNNNED PAYMENTS 260404",
            row_account_path="Assets:Acme:Checking",
        )
        assert len(cards) == 1
        c = cards[0]
        assert c.kind == "payout_source"
        assert c.id.endswith(":row")
        # Per ADR-0045 the eBay leaf is "Ebay" on-ledger.
        assert c.cta_form_data["suggested_path"] == "Assets:Acme:Ebay"
        assert "row" in c.contexts
        # And NOT global — row cards must not pop up on the dashboard.
        assert "global" not in c.contexts

    def test_row_with_no_pattern_match_emits_nothing(self, conn):
        _seed_account(
            conn,
            account_path="Assets:Personal:Checking",
            entity_slug="Personal",
        )
        cards = build_suggestion_cards(
            conn, [],
            context="row",
            row_payee_text="Local Coffee Shop",
            row_account_path="Assets:Personal:Checking",
        )
        assert cards == []

    def test_row_without_entity_first_path_emits_nothing(self, conn):
        # Pre-Phase-G ledger shape — we have no entity to suggest
        # the new account under.
        cards = build_suggestion_cards(
            conn, [],
            context="row",
            row_payee_text="eBay PAYMENTS",
            row_account_path="Assets:Checking",
        )
        assert cards == []

    def test_row_with_already_scaffolded_account_emits_nothing(self, conn):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
        )
        # Per ADR-0045 the eBay leaf is "Ebay" on-ledger.
        _seed_account(
            conn,
            account_path="Assets:Acme:Ebay",
            entity_slug="Acme",
            kind="payout",
        )
        cards = build_suggestion_cards(
            conn, [],
            context="row",
            row_payee_text="eBay PAYMENTS",
            row_account_path="Assets:Acme:Checking",
        )
        assert cards == []

    def test_row_context_ignores_global_only_candidates(self, conn, svc):
        """A frequency-threshold-clearing candidate would normally
        produce a global card; in row context with NO row payload
        we should get nothing back. (Defense in depth — the host
        page asks for the right context, and the registry must
        respect it.)"""
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
            simplefin_account_id="acme-chk",
        )
        for d in ("2026-03-24", "2026-04-06", "2026-04-20"):
            _stage_inflow(
                svc, sf_account_id="acme-chk",
                posting_date=d, amount="150.00",
                payee="eBay PAYMENTS",
            )
        cards = build_suggestion_cards(conn, [], context="row")
        assert cards == []


# --- amazon disambiguation flows through to the cards --------------------


class TestAmazonDisambiguation:
    def test_retail_amazon_row_does_not_suggest_payout_card(self, conn):
        _seed_account(
            conn,
            account_path="Assets:Personal:Checking",
            entity_slug="Personal",
        )
        cards = build_suggestion_cards(
            conn, [],
            context="row",
            row_payee_text="AMAZON.COM ORDER #ABC",
            row_account_path="Assets:Personal:Checking",
        )
        assert cards == []

    def test_amazon_seller_row_suggests_nested_path(self, conn):
        _seed_account(
            conn,
            account_path="Assets:Acme:Checking",
            entity_slug="Acme",
        )
        cards = build_suggestion_cards(
            conn, [],
            context="row",
            row_payee_text="Amazon.com Services LLC DISBURSEMENT",
            row_account_path="Assets:Acme:Checking",
        )
        assert len(cards) == 1
        assert (
            cards[0].cta_form_data["suggested_path"]
            == "Assets:Acme:Amazon:Seller"
        )
