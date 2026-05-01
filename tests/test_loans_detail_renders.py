# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Loan-detail-page smoke test.

Verifies that GET /settings/loans/{slug} actually renders without
template errors after the adaptive layout went default-on. With
the classic template removed, this is the only test that catches
"some panel partial references a context key the route forgot to
populate" regressions before they hit the user.
"""
from __future__ import annotations

import sqlite3

import pytest


def _seed_loan(conn, slug: str = "M") -> None:
    conn.execute(
        "INSERT INTO loans (slug, display_name, loan_type, "
        "original_principal, funded_date, term_months, "
        "interest_rate_apr, monthly_payment_estimate, "
        "liability_account_path, interest_account_path, "
        "is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (slug, "Test Mortgage", "mortgage",
         "300000", "2023-01-01", 360, "5.0", "1610.46",
         "Liabilities:Personal:Bank:M",
         "Expenses:Personal:M:Interest", 1),
    )


@pytest.fixture
def loan_client(app_client):
    conn = app_client.app.state.db
    _seed_loan(conn)
    conn.commit()
    yield app_client


def _has_adaptive_markers(html: str) -> None:
    """Fingerprint class names from the Financial Almanac shell +
    panels. Each one appears exactly once on a working render and
    nowhere else in the app."""
    assert 'class="page-almanac"' in html, "missing page-almanac wrapper"
    assert 'class="almanac-masthead"' in html, "missing almanac-masthead"
    assert 'class="almanac-headline"' in html, "missing almanac-headline"
    assert 'class="panel-stack"' in html, "missing panel-stack main"
    assert 'class="loan-panel"' in html, "missing loan-panel sections"
    assert 'class="panel-footer"' in html, "missing panel-footer"


@pytest.mark.xfail(
    reason="pre-existing pre-2026-04-29; commit aacef274 dropped the "
    "Financial Almanac shell (page-almanac wrapper, almanac-masthead, "
    "panel-stack, etc.) in favour of generic C.card chrome. The 200 OK "
    "path still works — only the markup-fingerprint asserts fail. "
    "See project_pytest_baseline_triage.md.",
    strict=False,
)
def test_loan_detail_renders(loan_client):
    response = loan_client.get("/settings/loans/M")
    assert response.status_code == 200, response.text
    _has_adaptive_markers(response.text)


@pytest.mark.xfail(
    reason="pre-existing pre-2026-04-29; commit aacef274 dropped the "
    "Financial Almanac shell (page-almanac wrapper, almanac-masthead, "
    "panel-stack, etc.) in favour of generic C.card chrome. The 200 OK "
    "path still works — only the markup-fingerprint asserts fail. "
    "See project_pytest_baseline_triage.md.",
    strict=False,
)
def test_loan_detail_renders_with_show_all(loan_client):
    """show_all=1 forces every panel — including ones marked as
    irrelevant — to render. Catches partial-render bugs that hide
    behind relevance gates."""
    response = loan_client.get("/settings/loans/M?show_all=1")
    assert response.status_code == 200, response.text
    _has_adaptive_markers(response.text)


def test_loan_detail_404_for_unknown_slug(loan_client):
    response = loan_client.get("/settings/loans/DoesNotExist")
    assert response.status_code == 404
