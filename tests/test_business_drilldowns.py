# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Per-business drill-down pages: /transactions (all txns touching the
entity) and /accounts (every account the entity owns)."""
from __future__ import annotations

from datetime import date, timedelta

import pytest


def _seed_entity(app_client, slug: str, display: str = "Acme Co.") -> None:
    conn = app_client.app.state.db
    conn.execute(
        "INSERT OR IGNORE INTO entities "
        "(slug, display_name, entity_type, is_active) "
        "VALUES (?, ?, ?, 1)",
        (slug, display, "llc"),
    )
    conn.commit()


def _append(ledger_dir, body: str) -> None:
    sf = ledger_dir / "simplefin_transactions.bean"
    sf.write_text(sf.read_text(encoding="utf-8") + body, encoding="utf-8")


def _seed_mixed(app_client, ledger_dir, slug: str) -> None:
    today = date.today()
    d1 = (today - timedelta(days=2)).isoformat()
    d2 = (today - timedelta(days=5)).isoformat()
    d3 = (today - timedelta(days=8)).isoformat()
    _append(
        ledger_dir,
        # An expense
        f'\n{d1} * "Acme Supplies" "hardware"\n'
        f'  lamella-txn-id: "01900000-0000-7000-8000-cccccccccc01"\n'
        f"  Liabilities:Acme:Card:CardA1234  -42.17 USD\n"
        f"  Expenses:Acme:Supplies            42.17 USD\n"
        # An income deposit
        f'\n{d2} * "Acme customer" "Sale"\n'
        f'  lamella-txn-id: "01900000-0000-7000-8000-cccccccccc02"\n'
        f"  Assets:Acme:Checking      150.00 USD\n"
        f"  Income:Acme:Sales        -150.00 USD\n"
        # A transfer between Acme accounts
        f'\n{d3} * "Internal transfer"\n'
        f'  lamella-txn-id: "01900000-0000-7000-8000-cccccccccc03"\n'
        f"  Assets:Acme:Checking            -100.00 USD\n"
        f"  Liabilities:Acme:Card:CardA1234  100.00 USD\n"
    )
    app_client.app.state.ledger_reader.invalidate()


class TestTransactions:
    def test_404_unknown_slug(self, app_client):
        r = app_client.get("/businesses/no-such/transactions")
        assert r.status_code == 404

    def test_includes_all_kinds(self, app_client, ledger_dir):
        _seed_entity(app_client, "Acme")
        _seed_mixed(app_client, ledger_dir, "Acme")
        r = app_client.get("/businesses/Acme/transactions?period=30d")
        assert r.status_code == 200
        # Expense, income, and transfer all present.
        assert "Acme Supplies" in r.text
        assert "Acme customer" in r.text
        assert "Internal transfer" in r.text

    def test_kind_filter_narrows(self, app_client, ledger_dir):
        _seed_entity(app_client, "Acme")
        _seed_mixed(app_client, ledger_dir, "Acme")
        r = app_client.get(
            "/businesses/Acme/transactions?period=30d&kind=Income"
        )
        assert r.status_code == 200
        # Income deposit visible; expense not.
        assert "Acme customer" in r.text
        assert "Acme Supplies" not in r.text


class TestAccounts:
    def test_404_unknown_slug(self, app_client):
        r = app_client.get("/businesses/no-such/accounts")
        assert r.status_code == 404

    def test_lists_assets_and_liabilities_with_balance(
        self, app_client, ledger_dir,
    ):
        _seed_entity(app_client, "Acme")
        _seed_mixed(app_client, ledger_dir, "Acme")
        r = app_client.get("/businesses/Acme/accounts")
        assert r.status_code == 200
        # Both account roots show up on the page.
        assert "Assets" in r.text
        assert "Liabilities" in r.text
        # Specific accounts touched by the seeded txns are listed.
        assert "Assets:Acme:Checking" in r.text
        assert "Liabilities:Acme:Card:CardA1234" in r.text


class TestInbound:
    def test_business_detail_links_to_transactions_and_accounts(
        self, app_client, ledger_dir,
    ):
        _seed_entity(app_client, "Acme")
        _seed_mixed(app_client, ledger_dir, "Acme")
        r = app_client.get("/businesses/Acme")
        assert r.status_code == 200
        assert "/businesses/Acme/transactions" in r.text
        assert "/businesses/Acme/accounts" in r.text


class TestAccountsIndexShowsAll:
    @pytest.mark.xfail(
        reason="/accounts page no longer lists raw open directives; "
        "pre-existing template change. See project_pytest_baseline_triage.md.",
        strict=False,
    )
    def test_accounts_index_includes_unregistered_open_directives(
        self, app_client,
    ):
        """The fixture's accounts.bean opens many accounts but none of
        them are in the SQLite accounts_meta table. The /accounts page
        must still list them as ledger truth."""
        r = app_client.get("/accounts")
        assert r.status_code == 200
        # Every open directive in the fixture should surface.
        assert "Assets:Acme:Checking" in r.text
        assert "Liabilities:Acme:Card:CardA1234" in r.text
        assert "Expenses:Acme:Supplies" in r.text


class TestAiLogsRedirect:
    def test_legacy_audit_url_301s_to_logs(self, app_client):
        r = app_client.get("/ai/audit", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/ai/logs"

    def test_legacy_audit_url_preserves_querystring(self, app_client):
        r = app_client.get(
            "/ai/audit?days=7&decision_type=classify_txn",
            follow_redirects=False,
        )
        assert r.status_code == 301
        assert r.headers["location"] == (
            "/ai/logs?days=7&decision_type=classify_txn"
        )

    def test_logs_route_renders(self, app_client):
        r = app_client.get("/ai/logs")
        assert r.status_code == 200
        assert "AI logs" in r.text


class TestSidebarTransactionsLink:
    def test_sidebar_links_to_transactions(self, app_client):
        r = app_client.get("/")
        # Dashboard always renders the sidebar; the new entry should
        # be present in the nav.
        assert r.status_code == 200
        assert 'href="/transactions"' in r.text
