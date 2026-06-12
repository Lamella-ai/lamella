# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Canonical ledger templates for the scaffolder and import flow.

These constants and helpers are the authoritative in-code
representation of the file shapes specified in
``docs/specs/LEDGER_LAYOUT.md`` §2.1, §2.2, and §3.2. Any change here
that diverges from the spec is a spec bug; any change to the spec
requires a matching change here.

Per §12.3 of the spec, changes that alter existing templates are
major-version bumps; adding a new template is a minor bump.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date as _date
from importlib.metadata import PackageNotFoundError, version as _pkg_version

__all__ = [
    "CanonicalFile",
    "CANONICAL_FILES",
    "get_package_version",
    "render_connector_header",
    "render_main_bean",
    "render_user_header",
]


def get_package_version(default: str = "0.0.0-dev") -> str:
    """Return the installed package version string, with a dev fallback.

    The dev fallback kicks in during out-of-tree development where
    the distribution isn't installed (pytest on a bare source tree,
    for example). In a container build it always resolves.
    """
    try:
        return _pkg_version("lamella")
    except PackageNotFoundError:
        return default


@dataclass(frozen=True)
class CanonicalFile:
    """One file in the canonical ledger layout (excluding main.bean)."""

    name: str
    owner: str  # "user" or "lamella"


# Files the scaffolder creates *besides* main.bean. Order matters:
# every file in this tuple is created before main.bean so that the
# moment main.bean lands, every `include` it references resolves.
# Within each ownership class, alphabetical for predictability.
CANONICAL_FILES: tuple[CanonicalFile, ...] = (
    # User-authored (empty-with-header).
    CanonicalFile("accounts.bean", "user"),
    CanonicalFile("commodities.bean", "user"),
    CanonicalFile("events.bean", "user"),
    CanonicalFile("manual_transactions.bean", "user"),
    CanonicalFile("prices.bean", "user"),
    # Connector-owned (empty-with-header; app writers populate over time).
    CanonicalFile("connector_accounts.bean", "lamella"),
    CanonicalFile("connector_budgets.bean", "lamella"),
    CanonicalFile("connector_config.bean", "lamella"),
    CanonicalFile("connector_links.bean", "lamella"),
    CanonicalFile("connector_overrides.bean", "lamella"),
    CanonicalFile("connector_rules.bean", "lamella"),
    CanonicalFile("connector_transfers.bean", "lamella"),
    CanonicalFile("simplefin_transactions.bean", "lamella"),
)


_USER_HEADER = """\
;; ---------------------------------------------------------------
;; User-authored file. Edit freely.
;;
;; Lamella reads this file every parse but never rewrites it.
;; Changes made in the app's editor land here only if you
;; explicitly save them from the editor. Manual edits via SSH /
;; nano / etc. are always fine.
;;
;; File:      {filename}
;; Owner:     user
;; Generated: {date} by Lamella v{version} (scaffolder only)
;; ---------------------------------------------------------------
"""


_CONNECTOR_HEADER = """\
;; ---------------------------------------------------------------
;; Managed by Lamella. Do not edit by hand.
;;
;; This file is regenerated from user actions in the web UI.
;; Manual edits may be reverted silently on the next write. To
;; modify behavior, use the app. To inspect, read freely.
;;
;; File:      {filename}
;; Owner:     Lamella
;; Schema:    lamella-ledger-version=4
;; Generated: {date} by Lamella v{version}
;; ---------------------------------------------------------------
"""


_MAIN_BEAN = """\
;; ---------------------------------------------------------------
;; Lamella — ledger root
;;
;; This file is user-authored. The scaffolder creates it on first
;; run; the import flow normalizes an existing main.bean to this
;; shape. You can edit freely — Lamella reads this file every
;; parse but never rewrites it.
;;
;; File:      main.bean
;; Owner:     user
;; Schema:    lamella-ledger-version=4
;; Generated: {date} by Lamella v{version} (scaffolder only)
;; ---------------------------------------------------------------

option "title"              "Lamella Ledger"
option "operating_currency" "USD"

;; Schema version marker — updated only by Lamella migration
;; passes. Do not edit.
2026-01-01 custom "lamella-ledger-version" "4"

;; Plugin set — fixed by the Lamella install. Adding plugins
;; here that are not installed in the image will cause the app to
;; refuse to start. See docs/specs/LEDGER_LAYOUT.md §5.
plugin "beancount_lazy_plugins.auto_accounts"

;; User-authored includes (load before Connector-owned so user
;; precedence wins on directive collisions — see §3.1).
include "accounts.bean"
include "commodities.bean"
include "prices.bean"
include "events.bean"
include "manual_transactions.bean"

;; Connector-owned includes. These files are managed by the app.
include "connector_accounts.bean"
include "connector_links.bean"
include "connector_overrides.bean"
include "connector_rules.bean"
include "connector_budgets.bean"
include "connector_config.bean"
include "connector_transfers.bean"
include "simplefin_transactions.bean"

;; Optional — uncomment when applicable.
;; include "mileage_summary.bean"
;; include "connector_imports/_all.bean"
;; include "historical_2024.bean"
"""


def render_user_header(filename: str, *, on: _date | None = None) -> str:
    """Render the user-authored file header with filename + date + version."""
    return _USER_HEADER.format(
        filename=filename,
        date=(on or _date.today()).isoformat(),
        version=get_package_version(),
    )


def render_connector_header(filename: str, *, on: _date | None = None) -> str:
    """Render the Connector-owned file header with filename + date + version."""
    return _CONNECTOR_HEADER.format(
        filename=filename,
        date=(on or _date.today()).isoformat(),
        version=get_package_version(),
    )


def render_main_bean(*, on: _date | None = None) -> str:
    """Render the canonical ``main.bean`` per LEDGER_LAYOUT.md §3.2."""
    return _MAIN_BEAN.format(
        date=(on or _date.today()).isoformat(),
        version=get_package_version(),
    )
