# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Adaptive detail-page layout.

`panels_for(loan, health, show_all=False)` returns a typed list of
`PanelSpec` records the template iterates over. Panel order, per-type
relevance, and start-expanded state all derive from the loan and its
`LoanHealth` — the template itself contains no state logic.

Panels for WPs that haven't shipped yet (coverage, escrow dashboard,
anomalies, projection, revolving) are present in the spec list with
their own partial names; those partials exist as placeholders until
the corresponding WP fills them in.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PanelSpec:
    key: str           # "coverage", "escrow", "payments", …
    template: str      # Jinja partial path relative to templates/
    expanded: bool     # start-expanded flag (priority stack)
    relevant: bool     # False → only render when show_all=True


# ----------------------------------------------------------- per-type layouts


# Canonical panel order, inherited by every loan type with type-specific
# drops / additions applied below. Each entry is the panel `key`.
_BASE_ORDER: tuple[str, ...] = (
    "terms",
    "coverage",
    "escrow",
    "anomalies",
    "payments",
    "groups",      # WP5 — proposed / confirmed multi-leg payment groups
    "pauses",      # WP12 — forbearance windows
    "anchors",
    "projection",
)


# Panels dropped from each loan type's default order. HELOC also
# adds a "revolving" panel near the top (WP13); until that panel
# ships, dropping coverage + projection is enough to hide
# amortization-only content.
_DROP_BY_TYPE: dict[str, tuple[str, ...]] = {
    "mortgage": (),
    "auto":     ("escrow",),
    "student":  ("escrow", "anchors"),
    "personal": ("escrow", "anchors"),
    "heloc":    ("coverage", "projection", "escrow"),
    "other":    (),
}


# HELOC-specific additions. The key is inserted after "terms".
_ADD_BY_TYPE: dict[str, tuple[str, ...]] = {
    "heloc": ("revolving",),
}


# Map a next-action kind to the panel that expanding-will-help-with-it.
# Action kinds not in this map don't expand any panel (they live in
# the next-actions strip at the top).
PANEL_FOR_ACTION_KIND: dict[str, str] = {
    "fund-initial":                       "terms",
    "scaffolding-open-missing":           "terms",
    "scaffolding-open-date-too-late":     "terms",
    "scaffolding-escrow-path-missing":    "escrow",
    "scaffolding-tax-path-missing":       "escrow",
    "scaffolding-insurance-path-missing": "escrow",
    "record-payment":                     "coverage",
    "missing-payment":                    "coverage",
    "add-anchor":                         "anchors",
    "stale-anchor":                       "anchors",
    "escrow-shortage-projected":          "escrow",
    "anomaly":                            "anomalies",
    "sustained-overflow":                 "anomalies",
    "long-payment-gap":                   "coverage",
}


# Partial path per panel key. Centralized so tests can assert the
# correct template is selected without Jinja rendering.
PANEL_TEMPLATE: dict[str, str] = {
    "terms":      "partials/loans/_panel_terms.html",
    "coverage":   "partials/loans/_panel_coverage.html",
    "escrow":     "partials/loans/_panel_escrow.html",
    "anomalies":  "partials/loans/_panel_anomalies.html",
    "payments":   "partials/loans/_panel_payments.html",
    "groups":     "partials/loans/_panel_groups.html",
    "pauses":     "partials/loans/_panel_pauses.html",
    "anchors":    "partials/loans/_panel_anchors.html",
    "projection": "partials/loans/_panel_projection.html",
    "revolving":  "partials/loans/_panel_revolving.html",
}


# --------------------------------------------------------------- relevance


def _is_panel_relevant(panel_key: str, loan: dict, health: Any) -> bool:
    """A panel is relevant when it has data to render. Irrelevant
    panels are hidden unless `show_all=True`."""
    if panel_key == "terms":
        return True  # always relevant

    if panel_key == "coverage":
        # Needs term + first_payment_date to compute an expected schedule.
        return bool(loan.get("term_months")) and bool(
            loan.get("first_payment_date"))

    if panel_key == "escrow":
        return getattr(health, "escrow", None) is not None

    if panel_key == "anomalies":
        return bool(getattr(health, "anomalies", None))

    if panel_key == "payments":
        # Always relevant once the loan is funded; WP1's funding report
        # tells us.
        funding = getattr(health, "funding", None)
        return bool(funding and funding.is_funded)

    if panel_key == "anchors":
        # Relevant whether or not anchors exist — adding the first
        # anchor is itself a next-action.
        return True

    if panel_key == "projection":
        return bool(loan.get("term_months"))

    if panel_key == "revolving":
        return bool(loan.get("is_revolving"))

    if panel_key == "groups":
        # WP5 — relevant when either a proposal or a confirmed group
        # exists. No-data loans hide the panel unless show_all.
        groups = getattr(health, "payment_groups", None)
        return bool(groups and (
            groups.get("proposed") or groups.get("confirmed")
        ))

    if panel_key == "pauses":
        # WP12 — relevant when at least one pause row exists.
        pauses = getattr(health, "pauses", None)
        return bool(pauses)

    return True


# --------------------------------------------------------------- entry point


def panels_for(
    loan: dict, health: Any, *, show_all: bool = False,
) -> list[PanelSpec]:
    """Produce the ordered panel list for this loan.

    The template iterates the result and renders `panel.template` for
    each where `panel.relevant or show_all`. No loan-state logic lives
    in Jinja; the template is a mechanical loop.
    """
    loan_type = (loan.get("loan_type") or "other").lower()
    drops = set(_DROP_BY_TYPE.get(loan_type, ()))
    adds = _ADD_BY_TYPE.get(loan_type, ())

    order: list[str] = []
    for key in _BASE_ORDER:
        if key in drops:
            continue
        order.append(key)
        # Insert type-specific additions right after "terms" so the
        # revolving panel shows first-after-header on HELOCs.
        if key == "terms" and adds:
            order.extend(adds)

    # Dedupe (defensive) while preserving order.
    seen: set[str] = set()
    final_order: list[str] = []
    for k in order:
        if k in seen:
            continue
        seen.add(k)
        final_order.append(k)

    # Which panels should start expanded? Panels that back any
    # next-action; fall back to "always-expanded" for `terms`.
    next_actions = getattr(health, "next_actions", []) or []
    target_panels: set[str] = {"terms"}
    for action in next_actions:
        kind = getattr(action, "kind", "")
        panel = PANEL_FOR_ACTION_KIND.get(kind)
        if panel:
            target_panels.add(panel)

    specs: list[PanelSpec] = []
    for key in final_order:
        specs.append(PanelSpec(
            key=key,
            template=PANEL_TEMPLATE.get(key, f"partials/loans/_panel_{key}.html"),
            expanded=(key in target_panels),
            relevant=_is_panel_relevant(key, loan, health),
        ))
    return specs
