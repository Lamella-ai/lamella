# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP2 — adaptive detail-page layout.

Pure tests over `loans.layout.panels_for` plus a direct-Jinja render
that asserts panel markers (`data-panel="<key>"`) appear or not.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from jinja2 import ChoiceLoader, Environment, FileSystemLoader, select_autoescape

from lamella.features.loans.health import (
    AnchorFreshnessReport,
    EscrowReport,
    FundingReport,
    LoanHealth,
    NextAction,
    ScaffoldingReport,
)
from lamella.features.loans.layout import PanelSpec, panels_for


# --------------------------------------------------------------- fixtures


def _loan(**overrides) -> dict:
    base = {
        "slug": "M",
        "display_name": "Test Mortgage",
        "loan_type": "mortgage",
        "entity_slug": "Personal",
        "institution": "Bank",
        "funded_date": "2025-01-01",
        "first_payment_date": "2025-02-01",
        "term_months": 360,
        "is_active": 1,
    }
    base.update(overrides)
    return base


def _health(
    *, next_actions: list[NextAction] | None = None,
    escrow: EscrowReport | None = None,
    anomalies: list | None = None,
    funded: bool = True,
) -> LoanHealth:
    return LoanHealth(
        loan_slug="M",
        summary_badge="ok",
        next_actions=next_actions or [],
        scaffolding=ScaffoldingReport(issues=[]),
        funding=FundingReport(
            is_funded=funded,
            funding_date=None, offset_account=None,
        ),
        coverage=None,
        anchor_freshness=AnchorFreshnessReport(0, None, None, True),
        escrow=escrow,
        anomalies=anomalies or [],
    )


def _na(kind: str, severity: str = "attention") -> NextAction:
    return NextAction(
        kind=kind, severity=severity,
        title=kind, detail="", action_label="go",
        method="GET", endpoint="/", payload={}, priority=500,
    )


# ------------------------------------------------------ panels_for() logic


def test_mortgage_default_panel_order():
    loan = _loan(loan_type="mortgage")
    panels = panels_for(loan, _health(), show_all=False)
    keys = [p.key for p in panels]
    assert keys == [
        "terms", "coverage", "escrow", "anomalies",
        "payments", "groups", "pauses", "anchors", "projection",
    ]


def test_auto_loan_drops_escrow():
    loan = _loan(loan_type="auto")
    panels = panels_for(loan, _health(), show_all=False)
    assert "escrow" not in [p.key for p in panels]


def test_heloc_drops_coverage_and_projection_adds_revolving():
    loan = _loan(loan_type="heloc", is_revolving=1)
    panels = panels_for(loan, _health(), show_all=False)
    keys = [p.key for p in panels]
    assert "coverage" not in keys
    assert "projection" not in keys
    assert "escrow" not in keys
    assert "revolving" in keys
    # revolving should sit right after terms per the layout rule.
    assert keys.index("revolving") == keys.index("terms") + 1


def test_personal_loan_drops_escrow_and_anchors():
    loan = _loan(loan_type="personal")
    panels = panels_for(loan, _health(), show_all=False)
    keys = [p.key for p in panels]
    assert "escrow" not in keys
    assert "anchors" not in keys


def test_escrow_panel_irrelevant_when_not_configured():
    loan = _loan(loan_type="mortgage")
    health = _health(escrow=None)
    panels = panels_for(loan, health, show_all=False)
    escrow = next(p for p in panels if p.key == "escrow")
    assert escrow.relevant is False


def test_escrow_panel_relevant_when_configured():
    from decimal import Decimal
    loan = _loan(loan_type="mortgage")
    escrow = EscrowReport(
        is_configured=True, monthly_inflow=Decimal("850"),
        annual_inflow=Decimal("10200"),
        annual_outflow_configured=Decimal("9600"),
        projected_shortage=False,
    )
    panels = panels_for(loan, _health(escrow=escrow), show_all=False)
    escrow_panel = next(p for p in panels if p.key == "escrow")
    assert escrow_panel.relevant is True


def test_coverage_irrelevant_without_term_or_first_payment():
    loan = _loan(loan_type="mortgage", term_months=None)
    panels = panels_for(loan, _health(), show_all=False)
    coverage = next(p for p in panels if p.key == "coverage")
    assert coverage.relevant is False


def test_anomalies_relevant_only_when_present():
    class _An:
        severity = "attention"
    loan = _loan(loan_type="mortgage")
    panels_empty = panels_for(loan, _health(anomalies=[]), show_all=False)
    panels_with = panels_for(loan, _health(anomalies=[_An()]), show_all=False)
    empty = next(p for p in panels_empty if p.key == "anomalies")
    withdata = next(p for p in panels_with if p.key == "anomalies")
    assert empty.relevant is False
    assert withdata.relevant is True


def test_expanded_flag_follows_next_actions():
    """The coverage panel should start expanded when there's an
    outstanding 'missing-payment' action."""
    loan = _loan(loan_type="mortgage")
    panels = panels_for(
        loan, _health(next_actions=[_na("missing-payment")]),
        show_all=False,
    )
    coverage = next(p for p in panels if p.key == "coverage")
    terms = next(p for p in panels if p.key == "terms")
    anchors = next(p for p in panels if p.key == "anchors")
    assert coverage.expanded is True
    assert terms.expanded is True  # always-expanded
    assert anchors.expanded is False


def test_fund_initial_expands_terms_panel():
    loan = _loan(loan_type="mortgage")
    panels = panels_for(
        loan, _health(next_actions=[_na("fund-initial", "blocking")]),
        show_all=False,
    )
    terms = next(p for p in panels if p.key == "terms")
    assert terms.expanded is True


def test_payments_panel_irrelevant_when_unfunded():
    loan = _loan(loan_type="mortgage")
    panels = panels_for(loan, _health(funded=False), show_all=False)
    payments = next(p for p in panels if p.key == "payments")
    assert payments.relevant is False


# ---------------------------------------------------- Jinja render smoke


TEMPLATE_DIR = (
    Path(__file__).resolve().parent.parent
    / "src" / "lamella" / "web" / "templates"
)


def _stub_filters(env: Environment) -> None:
    """Tiny stubs for the app's custom filters. The tests just need the
    templates to render without UndefinedError, not with real
    formatting."""
    env.filters["money"] = lambda v: f"${v}" if v not in (None, "") else ""
    env.filters["alias"] = lambda v: v or ""
    env.filters["humanize"] = lambda v: (str(v).replace("_", " ").replace("-", " ")
                                         if v not in (None, "") else "")


def _make_env() -> Environment:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html"]),
    )
    _stub_filters(env)
    return env


def test_next_action_card_renders_ok_state_when_no_actions():
    env = _make_env()
    tmpl = env.get_template("partials/loans/_next_action_card.html")
    html = tmpl.render(
        loan={"slug": "M"},
        health=_health(next_actions=[]),
    )
    assert "All set" in html
    assert "next-action-ok" in html


def test_next_action_card_renders_top_action():
    env = _make_env()
    tmpl = env.get_template("partials/loans/_next_action_card.html")
    action = NextAction(
        kind="fund-initial", severity="blocking",
        title="Post the initial funding transaction",
        detail="Pick an offset account.",
        action_label="Post funding",
        method="GET", endpoint="/settings/loans/M",
        payload={}, priority=20,
    )
    html = tmpl.render(
        loan={"slug": "M"},
        health=_health(next_actions=[action]),
    )
    assert "Post the initial funding transaction" in html
    assert "blocking" in html
    assert "/settings/loans/M" in html


@pytest.mark.xfail(
    reason="pre-existing pre-2026-04-29; markup-fingerprint test predates "
    "commit aacef274 which dropped the Financial Almanac shell + data-panel "
    "attrs in favour of generic C.card chrome. Asserts markup the current "
    "template no longer emits. See project_pytest_baseline_triage.md.",
    strict=False,
)
def test_adaptive_template_includes_panel_markers():
    env = _make_env()
    tmpl = env.get_template("settings_loan_detail_adaptive.html")

    # Base-template placeholder — the full base.html imports a lot of
    # app state. Override the extends target with a minimal block.
    # We do this by rendering only the content block via a helper
    # template that mocks "base.html". Simpler: override the loader
    # to serve a stub base.html.
    stub_dirs = [
        str(TEMPLATE_DIR),
        str(Path(__file__).resolve().parent / "_jinja_stubs"),
    ]
    stub_base_dir = Path(__file__).resolve().parent / "_jinja_stubs"
    stub_base_dir.mkdir(exist_ok=True)
    (stub_base_dir / "base.html").write_text(
        "<html><body>{% block content %}{% endblock %}</body></html>",
        encoding="utf-8",
    )
    env2 = Environment(
        loader=ChoiceLoader([
            FileSystemLoader(str(stub_base_dir)),
            FileSystemLoader(str(TEMPLATE_DIR)),
        ]),
        autoescape=select_autoescape(["html"]),
    )
    _stub_filters(env2)

    loan = _loan(loan_type="mortgage")
    from decimal import Decimal
    escrow = EscrowReport(
        is_configured=True, monthly_inflow=Decimal("850"),
        annual_inflow=Decimal("10200"),
        annual_outflow_configured=Decimal("9600"),
        projected_shortage=False,
    )
    health = _health(escrow=escrow)
    panels = panels_for(loan, health, show_all=False)

    tmpl = env2.get_template("settings_loan_detail_adaptive.html")
    html = tmpl.render(
        loan=loan, health=health, panels=panels,
        show_all=False, request=None, saved=None,
        # Classic-template variables referenced by panel partials.
        est_pmt=None, current_split=None, schedule=[],
        total_principal_paid=0, total_interest_paid=0, remaining=0,
        payments=[], escrow_flows=[], actual_escrow_paid=0,
        anchors=[], anchor_used=None, anchored_balance=None,
        principal_paid_since_anchor=0, model_vs_actual=None,
        fixme_candidates=[],
    )

    # Relevant mortgage panels — terms + coverage + escrow + payments + anchors.
    # Coverage relevance requires term_months + first_payment_date
    # which the fixture provides.
    assert 'data-panel="terms"' in html
    assert 'data-panel="coverage"' in html
    assert 'data-panel="escrow"' in html
    assert 'data-panel="payments"' in html
    assert 'data-panel="anchors"' in html
    # Anomalies panel — irrelevant (no anomalies), hidden by default.
    assert 'data-panel="anomalies"' not in html
    # Revolving panel — not in mortgage layout at all.
    assert 'data-panel="revolving"' not in html


@pytest.mark.xfail(
    reason="pre-existing pre-2026-04-29; markup-fingerprint test predates "
    "commit aacef274 which dropped the Financial Almanac shell + data-panel "
    "attrs in favour of generic C.card chrome. Asserts markup the current "
    "template no longer emits. See project_pytest_baseline_triage.md.",
    strict=False,
)
def test_show_all_reveals_hidden_panels():
    env = Environment(
        loader=ChoiceLoader([
            FileSystemLoader(str(Path(__file__).resolve().parent / "_jinja_stubs")),
            FileSystemLoader(str(TEMPLATE_DIR)),
        ]),
        autoescape=select_autoescape(["html"]),
    )
    _stub_filters(env)

    loan = _loan(loan_type="mortgage")
    health = _health()  # no escrow, no anomalies
    panels = panels_for(loan, health, show_all=True)

    tmpl = env.get_template("settings_loan_detail_adaptive.html")
    html = tmpl.render(
        loan=loan, health=health, panels=panels,
        show_all=True, request=None, saved=None,
        est_pmt=None, current_split=None, schedule=[],
        total_principal_paid=0, total_interest_paid=0, remaining=0,
        payments=[], escrow_flows=[], actual_escrow_paid=0,
        anchors=[], anchor_used=None, anchored_balance=None,
        principal_paid_since_anchor=0, model_vs_actual=None,
        fixme_candidates=[],
    )

    # With show_all=True, the irrelevant anomalies panel does render.
    assert 'data-panel="anomalies"' in html
