# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0047 + IA polish: /loans is a card grid matching /entities,
/vehicles, /properties — not a table-with-giant-add-form. New loans
go through /settings/loans/wizard/{flow}; the dashboard surfaces
existing loans and ledger candidates."""
from __future__ import annotations


def _seed_loan(conn, slug: str, **kwargs) -> None:
    conn.execute(
        "INSERT INTO loans "
        "  (slug, display_name, loan_type, entity_slug, "
        "   institution, original_principal, is_active, is_revolving, "
        "   liability_account_path, funded_date) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(slug) DO UPDATE SET "
        "  display_name = excluded.display_name, "
        "  loan_type = excluded.loan_type, "
        "  entity_slug = excluded.entity_slug, "
        "  institution = excluded.institution, "
        "  original_principal = excluded.original_principal, "
        "  is_active = excluded.is_active, "
        "  is_revolving = excluded.is_revolving",
        (
            slug,
            kwargs.get("display_name", slug),
            kwargs.get("loan_type", "mortgage"),
            kwargs.get("entity_slug", "Personal"),
            kwargs.get("institution", "Bank"),
            kwargs.get("original_principal", "100000"),
            kwargs.get("is_active", 1),
            kwargs.get("is_revolving", 0),
            kwargs.get("liability_account_path",
                       f"Liabilities:Personal:Mortgage:{slug}"),
            kwargs.get("funded_date", "2024-01-01"),
        ),
    )
    conn.commit()


class TestLoansDashboardCardGrid:
    def test_renders_card_grid_when_loans_exist(self, app_client):
        conn = app_client.app.state.db
        _seed_loan(conn, "MainHouse", display_name="Main House Mortgage")
        r = app_client.get("/loans")
        assert r.status_code == 200
        # Card grid wrapper present with grid id.
        assert 'id="loans-grid"' in r.text
        # Card text shows up.
        assert "Main House Mortgage" in r.text
        # Card links to detail.
        assert "/settings/loans/MainHouse" in r.text

    def test_page_head_has_wizard_entry_buttons(self, app_client):
        r = app_client.get("/loans")
        assert r.status_code == 200
        # The four wizard flows are exposed as primary CTAs from the
        # page head (no inline "Add loan" form on the dashboard).
        assert "/settings/loans/wizard/purchase" in r.text
        assert "/settings/loans/wizard/import_existing" in r.text
        assert "/settings/loans/wizard/refi" in r.text
        assert "/settings/loans/wizard/payoff" in r.text

    def test_no_inline_add_form_when_no_prefill(self, app_client):
        r = app_client.get("/loans")
        assert r.status_code == 200
        # Without prefill, the bottom Add-Loan form is absent — the
        # page is purely dashboard.
        assert "promote-form" not in r.text
        # Promote section anchor is also absent.
        assert 'id="promote"' not in r.text

    def test_prefill_query_params_show_promote_form(self, app_client):
        r = app_client.get(
            "/loans?prefill_slug=NewLoan"
            "&prefill_account_path=Liabilities:Personal:Mortgage:NewLoan"
            "&prefill_type=mortgage"
        )
        assert r.status_code == 200
        # When prefill is set (candidate-promotion flow), the promote
        # form appears with the values pre-loaded.
        assert "promote-form" in r.text
        assert 'value="NewLoan"' in r.text
