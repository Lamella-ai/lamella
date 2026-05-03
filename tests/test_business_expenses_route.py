# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Per-business expense list at /businesses/{slug}/expenses.

Drill-down counterpart to the read-only /businesses/{slug} dashboard:
every Expenses:{slug}:* leg in the selected period, filterable by
text search / category / FIXME-only, paginated, with each row linking
to the immutable /txn/{lamella_txn_id} URL.
"""
from __future__ import annotations


def _seed_entity(app_client, slug: str, display_name: str = "Acme Co.") -> None:
    conn = app_client.app.state.db
    conn.execute(
        "INSERT OR IGNORE INTO entities "
        "(slug, display_name, entity_type, is_active) "
        "VALUES (?, ?, ?, 1)",
        (slug, display_name, "llc"),
    )
    conn.commit()


def _append_txns(ledger_dir, body: str) -> None:
    sf = ledger_dir / "simplefin_transactions.bean"
    sf.write_text(sf.read_text(encoding="utf-8") + body, encoding="utf-8")


def _txn(
    *, date_iso: str, payee: str, narration: str, lamella_txn_id: str,
    expense_acct: str, amount: str, source_acct: str, fixme: bool = False,
) -> str:
    expense_block = (
        f"  Expenses:FIXME            {amount} USD\n"
        if fixme
        else f"  {expense_acct:<40s}  {amount} USD\n"
    )
    return (
        "\n"
        + f'{date_iso} * "{payee}" "{narration}"\n'
        + f'  lamella-txn-id: "{lamella_txn_id}"\n'
        + f"  {source_acct:<40s}  -{amount} USD\n"
        + expense_block
    )


def _seed_two_expenses(app_client, ledger_dir, slug: str) -> None:
    """Two clean expense txns + one FIXME row, all in the last 30 days
    so the default 1mo / 30d period catches them."""
    from datetime import date, timedelta
    today = date.today()
    d1 = (today - timedelta(days=2)).isoformat()
    d2 = (today - timedelta(days=10)).isoformat()
    d3 = (today - timedelta(days=20)).isoformat()
    # Fixture already opens Liabilities:Acme:Card:CardA1234,
    # Assets:Acme:Checking, Expenses:Acme:Supplies, Expenses:Acme:Shipping
    # — reuse those rather than declaring new ones (duplicate Open
    # directives are bean-check errors).
    _append_txns(
        ledger_dir,
        _txn(
            date_iso=d1, payee="Acme Supplies", narration="hardware",
            lamella_txn_id="01900000-0000-7000-8000-bbbbbbbbbb01",
            expense_acct=f"Expenses:{slug}:Supplies", amount="42.17",
            source_acct=f"Liabilities:{slug}:Card:CardA1234",
        )
        + _txn(
            date_iso=d2, payee="Office Rent Co.", narration="april rent",
            lamella_txn_id="01900000-0000-7000-8000-bbbbbbbbbb02",
            expense_acct=f"Expenses:{slug}:Shipping", amount="1500.00",
            source_acct=f"Assets:{slug}:Checking",
        )
        + _txn(
            date_iso=d3, payee="Acme Insurance", narration="needs review",
            lamella_txn_id="01900000-0000-7000-8000-bbbbbbbbbb03",
            expense_acct="x", amount="89.99",
            source_acct=f"Liabilities:{slug}:Card:CardA1234",
            fixme=True,
        )
    )
    # Open Expenses:FIXME for the seed-FIXME row.
    accts = ledger_dir / "accounts.bean"
    accts.write_text(
        accts.read_text(encoding="utf-8")
        + "\n2020-01-01 open Expenses:FIXME USD\n",
        encoding="utf-8",
    )
    app_client.app.state.ledger_reader.invalidate()


def _ensure_open(ledger_dir, slug: str) -> None:
    """No-op for slug='Acme' (fixture already opens the accounts);
    here as a placeholder if other slugs are tested later."""
    pass


class TestBasic:
    def test_route_404_for_unknown_slug(self, app_client):
        r = app_client.get("/businesses/no-such-entity/expenses")
        assert r.status_code == 404

    def test_renders_lists_each_expense_leg(self, app_client, ledger_dir):
        slug = "Acme"
        _seed_entity(app_client, slug)
        _ensure_open(ledger_dir, slug)
        _seed_two_expenses(app_client, ledger_dir, slug)
        r = app_client.get(f"/businesses/{slug}/expenses?period=30d")
        assert r.status_code == 200, r.text
        assert "Office Rent Co." in r.text
        assert "Acme Supplies" in r.text
        # Default view does NOT hide FIXME rows.
        assert "Acme Insurance" in r.text
        # Each row links via the immutable token, not the legacy hex.
        assert "/txn/01900000-0000-7000-8000-bbbbbbbbbb01" in r.text
        assert "/txn/01900000-0000-7000-8000-bbbbbbbbbb02" in r.text


class TestFilters:
    def test_text_search_narrows_payee(self, app_client, ledger_dir):
        slug = "Acme"
        _seed_entity(app_client, slug)
        _ensure_open(ledger_dir, slug)
        _seed_two_expenses(app_client, ledger_dir, slug)
        r = app_client.get(f"/businesses/{slug}/expenses?period=30d&q=rent")
        assert r.status_code == 200
        assert "Office Rent Co." in r.text
        assert "Acme Supplies" not in r.text

    def test_category_filter(self, app_client, ledger_dir):
        slug = "Acme"
        _seed_entity(app_client, slug)
        _ensure_open(ledger_dir, slug)
        _seed_two_expenses(app_client, ledger_dir, slug)
        r = app_client.get(
            f"/businesses/{slug}/expenses?period=30d"
            f"&category=Expenses:{slug}:Shipping"
        )
        assert r.status_code == 200
        assert "Office Rent Co." in r.text
        assert "Acme Supplies" not in r.text

    def test_fixme_only(self, app_client, ledger_dir):
        slug = "Acme"
        _seed_entity(app_client, slug)
        _ensure_open(ledger_dir, slug)
        _seed_two_expenses(app_client, ledger_dir, slug)
        r = app_client.get(f"/businesses/{slug}/expenses?period=30d&fixme=1")
        assert r.status_code == 200
        assert "Acme Insurance" in r.text
        # Clean-categorized rows drop out under fixme=1.
        assert "Office Rent Co." not in r.text


class TestEmptyState:
    def test_no_expenses_in_period_renders_empty_state(self, app_client):
        slug = "Empty"
        _seed_entity(app_client, slug, display_name="Empty Co.")
        r = app_client.get(f"/businesses/{slug}/expenses?period=30d")
        assert r.status_code == 200
        assert "No expense transactions" in r.text


class TestInboundLink:
    def test_business_detail_links_to_expenses(self, app_client, ledger_dir):
        slug = "Acme"
        _seed_entity(app_client, slug)
        _ensure_open(ledger_dir, slug)
        _seed_two_expenses(app_client, ledger_dir, slug)
        r = app_client.get(f"/businesses/{slug}")
        assert r.status_code == 200
        assert f"/businesses/{slug}/expenses" in r.text
