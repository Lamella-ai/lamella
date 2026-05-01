# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Cross-page anomaly detection framework.

Shared primitives for the pattern established on the mortgage detail
page: detect something wrong, render a banner at top of page with a
proposed fix the user can one-click. Vehicle / property / account
detail pages each register their own detectors and feed findings
into the shared banner partial.

Design:

* ``AnomalyFinding`` — a single issue one page wants to surface.
  Severity-colored, carries a title + description, optional
  per-fix CTAs the user can click without leaving the page.

* ``detect_for_mortgage(...)`` / ``detect_for_vehicle(...)`` /
  ``detect_for_property(...)`` / ``detect_for_account(...)`` —
  page-specific detectors that scan the ledger + DB + page
  context and return a list of findings. New pages add new
  detectors here; shared logic (banner template, severity
  colors, etc.) stays in one place so every specialized page
  looks and behaves the same when something's wrong.

* ``partials/_anomaly_banner.html`` — renders a list of
  findings. Pages `{% include %}` it at the top of their
  content block.

Philosophy per user: "software should handle bad data
proactively, not force the user to find things manually." The
anomaly framework is how that shows up on every specialized
page.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


@dataclass(frozen=True)
class AnomalyAction:
    """One click the user can take to resolve an anomaly inline.

    ``label`` shows on the button. ``href`` is the href (for a link)
    OR the POST target when ``method=='post'``. ``method`` defaults
    to ``'get'`` (a plain link); set to ``'post'`` for a destructive
    action.
    """
    label: str
    href: str
    method: str = "get"
    primary: bool = False


@dataclass(frozen=True)
class AnomalyFinding:
    """One issue surfaced on a specialized page.

    ``severity``:
      - ``"info"``: informational, blue
      - ``"warn"``: needs attention, yellow — the usual case
      - ``"error"``: something is demonstrably wrong, red

    ``title`` is a one-liner; ``description`` is prose detail.
    ``actions`` are optional per-fix buttons/links rendered inline.
    ``code`` is a stable machine identifier (for dismissal storage).
    """
    code: str
    severity: str
    title: str
    description: str = ""
    actions: tuple[AnomalyAction, ...] = ()


def collect(
    findings: Iterable[AnomalyFinding | None],
) -> list[AnomalyFinding]:
    """Filter None (so detectors can return optionals) + sort by
    severity (error → warn → info) so the most-urgent anomalies
    render at the top of the banner."""
    order = {"error": 0, "warn": 1, "info": 2}
    out = [f for f in findings if f is not None]
    out.sort(key=lambda f: (order.get(f.severity, 9), f.code))
    return out


__all__ = ["AnomalyAction", "AnomalyFinding", "collect"]
