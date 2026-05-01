# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for `custom "balance-anchor"` directives.

Anchors a known balance on an account at a specific date. The audit
surface computes drift between consecutive anchors vs. what the ledger
postings added up to in the segment — so the user gets a visible
reconciliation view instead of a bean-check error (we deliberately
don't emit Beancount's native `balance` assertion because the user
wants to SEE drift, not have it break writes).

Directive shape:
    2026-02-01 custom "balance-anchor" Assets:Personal:BankOne:Checking 15234.56
      lamella-anchor-currency: "USD"
      lamella-anchor-source: "statement"
      lamella-anchor-notes: "Feb statement"
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from lamella.core.transform.custom_directive import (
    Account,
    append_custom_directive,
)

log = logging.getLogger(__name__)


CONNECTOR_CONFIG_HEADER = (
    "; connector_config.bean — configuration state written by Lamella.\n"
    "; Paperless field-role mappings and UI-persisted settings live here.\n"
    "; Do not hand-edit; use the /settings pages.\n"
)


def _as_date(value: str | date | None) -> date:
    if value is None or value == "":
        return datetime.now(timezone.utc).date()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def append_balance_anchor(
    *,
    connector_config: Path,
    main_bean: Path,
    account_path: str,
    as_of_date: str | date,
    balance: str,
    currency: str = "USD",
    source: str | None = None,
    notes: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {"lamella-anchor-currency": currency}
    if source:
        meta["lamella-anchor-source"] = source
    if notes:
        meta["lamella-anchor-notes"] = notes
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_as_date(as_of_date),
        directive_type="balance-anchor",
        args=[Account(account_path), str(balance)],
        meta=meta,
        run_check=run_check,
    )


def append_balance_anchor_revoked(
    *,
    connector_config: Path,
    main_bean: Path,
    account_path: str,
    as_of_date: str | date,
    run_check: bool = True,
) -> str:
    """Tombstone a previous anchor so reconstruct skips it. Keyed by
    (account_path, as_of_date) — same UNIQUE as the DB row."""
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_as_date(as_of_date),
        directive_type="balance-anchor-revoked",
        args=[Account(account_path)],
        meta=None,
        run_check=run_check,
    )
