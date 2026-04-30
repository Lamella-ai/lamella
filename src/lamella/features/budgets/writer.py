# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for ``custom "budget"`` directives in ``connector_budgets.bean``.

Budgets are user configuration (state), not derivable. They must survive
a DB delete. One custom directive per budget. Rule identity for
reconstruct = (label, entity, account_pattern, period).

The period is stamped as metadata (``lamella-period``) rather than positional
so user-chosen alternatives (``monthly`` / ``quarterly`` / ``yearly`` /
``rolling-30``) can be added without reshuffling argument order.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    Amount,
    append_custom_directive,
    custom_arg,
    custom_meta,
)

log = logging.getLogger(__name__)


CONNECTOR_BUDGETS_HEADER = (
    "; connector_budgets.bean — budgets written by Lamella.\n"
    "; One custom \"budget\" directive per budget. Do not hand-edit;\n"
    "; use the /budgets UI.\n"
)


def append_budget(
    *,
    connector_budgets: Path,
    main_bean: Path,
    label: str,
    entity: str,
    account_pattern: str,
    period: str,
    amount: Decimal,
    alert_threshold: float = 0.8,
    currency: str = "USD",
    created_at: datetime | None = None,
    backfilled: bool = False,
    run_check: bool = True,
) -> str:
    ts = created_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta: dict = {
        "lamella-entity": entity,
        "lamella-account-pattern": account_pattern,
        "lamella-period": period,
        "lamella-alert-threshold": str(alert_threshold),
        "lamella-created-at": ts,
    }
    if backfilled:
        meta["lamella-backfilled"] = True
    return append_custom_directive(
        target=connector_budgets,
        main_bean=main_bean,
        header=CONNECTOR_BUDGETS_HEADER,
        directive_date=ts.date(),
        directive_type="budget",
        args=[label, Amount(Decimal(amount), currency)],
        meta=meta,
        run_check=run_check,
    )


def append_budget_revoke(
    *,
    connector_budgets: Path,
    main_bean: Path,
    label: str,
    entity: str,
    account_pattern: str,
    period: str,
    revoked_at: datetime | None = None,
    run_check: bool = True,
) -> str:
    ts = revoked_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta: dict = {
        "lamella-entity": entity,
        "lamella-account-pattern": account_pattern,
        "lamella-period": period,
        "lamella-revoked-at": ts,
    }
    return append_custom_directive(
        target=connector_budgets,
        main_bean=main_bean,
        header=CONNECTOR_BUDGETS_HEADER,
        directive_date=ts.date(),
        directive_type="budget-revoked",
        args=[label],
        meta=meta,
        run_check=run_check,
    )


def _to_decimal_amount(value) -> tuple[Decimal, str] | None:
    """Beancount parses an Amount literal (``600.00 USD``) as a tuple-like
    object with .number and .currency. Accept either shape."""
    if value is None:
        return None
    if hasattr(value, "number") and hasattr(value, "currency"):
        try:
            return Decimal(str(value.number)), str(value.currency)
        except Exception:
            return None
    if isinstance(value, (tuple, list)) and len(value) == 2:
        try:
            return Decimal(str(value[0])), str(value[1])
        except Exception:
            return None
    return None


def read_budgets_from_entries(entries) -> list[dict]:
    """Return active budgets from the ledger, filtering revoked ones.
    Identity = (label, entity, account_pattern, period)."""
    state: dict[tuple, dict | None] = {}
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type not in ("budget", "budget-revoked"):
            continue
        label = custom_arg(entry, 0)
        if not isinstance(label, str) or not label:
            continue
        entity = custom_meta(entry, "lamella-entity")
        if not isinstance(entity, str) or not entity:
            continue
        account_pattern = custom_meta(entry, "lamella-account-pattern")
        if not isinstance(account_pattern, str) or not account_pattern:
            continue
        period = custom_meta(entry, "lamella-period")
        if not isinstance(period, str) or not period:
            continue
        key = (label, entity, account_pattern, period)
        if entry.type == "budget-revoked":
            state[key] = None
            continue
        amount_val = custom_arg(entry, 1)
        amt_tuple = _to_decimal_amount(amount_val)
        if amt_tuple is None:
            continue
        amount, currency = amt_tuple
        raw_threshold = custom_meta(entry, "lamella-alert-threshold")
        try:
            threshold = float(raw_threshold) if raw_threshold is not None else 0.8
        except (TypeError, ValueError):
            threshold = 0.8
        state[key] = {
            "label": label,
            "entity": entity,
            "account_pattern": account_pattern,
            "period": period,
            "amount": amount,
            "currency": currency,
            "alert_threshold": threshold,
        }
    return [row for row in state.values() if row is not None]
