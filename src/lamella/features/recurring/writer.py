# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for ``custom "recurring-confirmed"`` / ``recurring-ignored``
directives.

State goes to the ledger: the user's decision to confirm a detected
pattern (with the canonical target_account chosen) or to permanently
ignore one. Re-detected proposals are cache.

Canonical-target handling: a pattern that historically matched
transactions across multiple accounts is stamped with ONE target —
the most-frequent historical target over recent occurrences. Picking
it is the caller's job (at confirmation time); this writer just
serializes whichever target was chosen.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    Account,
    Amount,
    append_custom_directive,
    custom_arg,
    custom_meta,
)

log = logging.getLogger(__name__)


CONNECTOR_RULES_HEADER = (
    "; connector_rules.bean — classification rules + recurring confirmations.\n"
    "; Do not hand-edit; use the /rules and /recurring UIs.\n"
)


def append_recurring_confirmed(
    *,
    connector_rules: Path,
    main_bean: Path,
    label: str,
    entity: str,
    source_account: str,
    target_account: str | None,
    merchant_pattern: str,
    cadence: str,
    expected_amount: Decimal,
    expected_day: int | None = None,
    currency: str = "USD",
    confirmed_at: datetime | None = None,
    backfilled: bool = False,
    run_check: bool = True,
) -> str:
    ts = confirmed_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta: dict = {
        "lamella-entity": entity,
        "lamella-source-account": Account(source_account),
        "lamella-merchant-pattern": merchant_pattern,
        "lamella-cadence": cadence,
        "lamella-amount-hint": Amount(Decimal(expected_amount), currency),
        "lamella-confirmed-at": ts,
    }
    if target_account:
        meta["lamella-target-account"] = Account(target_account)
    if expected_day is not None:
        meta["lamella-expected-day"] = int(expected_day)
    if backfilled:
        meta["lamella-backfilled"] = True
    return append_custom_directive(
        target=connector_rules,
        main_bean=main_bean,
        header=CONNECTOR_RULES_HEADER,
        directive_date=ts.date(),
        directive_type="recurring-confirmed",
        args=[label],
        meta=meta,
        run_check=run_check,
    )


def append_recurring_ignored(
    *,
    connector_rules: Path,
    main_bean: Path,
    label: str,
    source_account: str,
    merchant_pattern: str,
    ignored_at: datetime | None = None,
    run_check: bool = True,
) -> str:
    ts = ignored_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta: dict = {
        "lamella-source-account": Account(source_account),
        "lamella-merchant-pattern": merchant_pattern,
        "lamella-ignored-at": ts,
    }
    return append_custom_directive(
        target=connector_rules,
        main_bean=main_bean,
        header=CONNECTOR_RULES_HEADER,
        directive_date=ts.date(),
        directive_type="recurring-ignored",
        args=[label],
        meta=meta,
        run_check=run_check,
    )


def append_recurring_revoke(
    *,
    connector_rules: Path,
    main_bean: Path,
    label: str,
    source_account: str,
    merchant_pattern: str,
    revoked_at: datetime | None = None,
    run_check: bool = True,
) -> str:
    """Un-confirm / un-ignore a pattern (append-only revoke). The
    proposal goes back to 'proposed' on next detection scan."""
    ts = revoked_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta: dict = {
        "lamella-source-account": Account(source_account),
        "lamella-merchant-pattern": merchant_pattern,
        "lamella-revoked-at": ts,
    }
    return append_custom_directive(
        target=connector_rules,
        main_bean=main_bean,
        header=CONNECTOR_RULES_HEADER,
        directive_date=ts.date(),
        directive_type="recurring-revoked",
        args=[label],
        meta=meta,
        run_check=run_check,
    )


def _account_to_str(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return ":".join(str(v) for v in value)
    return str(value)


def _amount_pair(value) -> tuple[Decimal, str] | None:
    if value is None:
        return None
    if hasattr(value, "number") and hasattr(value, "currency"):
        try:
            return Decimal(str(value.number)), str(value.currency)
        except Exception:
            return None
    return None


def read_recurring_from_entries(entries) -> list[dict]:
    """Return active confirmed/ignored recurring rows. Identity =
    (source_account, merchant_pattern) — matches the UNIQUE index on
    ``recurring_expenses``.
    """
    state: dict[tuple, dict | None] = {}
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type not in (
            "recurring-confirmed",
            "recurring-ignored",
            "recurring-revoked",
        ):
            continue
        source = _account_to_str(custom_meta(entry, "lamella-source-account"))
        pattern = custom_meta(entry, "lamella-merchant-pattern")
        if not source or not isinstance(pattern, str) or not pattern:
            continue
        key = (source, pattern)
        if entry.type == "recurring-revoked":
            state[key] = None
            continue
        label = custom_arg(entry, 0)
        if not isinstance(label, str) or not label:
            continue
        row = {
            "label": label,
            "source_account": source,
            "merchant_pattern": pattern,
        }
        if entry.type == "recurring-confirmed":
            entity = custom_meta(entry, "lamella-entity") or ""
            target = _account_to_str(custom_meta(entry, "lamella-target-account"))
            cadence = custom_meta(entry, "lamella-cadence") or "monthly"
            amt_pair = _amount_pair(custom_meta(entry, "lamella-amount-hint"))
            expected_day = custom_meta(entry, "lamella-expected-day")
            row.update(
                {
                    "status": "confirmed",
                    "entity": str(entity),
                    "target_account": target,
                    "cadence": str(cadence),
                    "expected_amount": amt_pair[0] if amt_pair else Decimal("0"),
                    "expected_day": int(expected_day) if isinstance(expected_day, (int, float)) else None,
                }
            )
        else:  # recurring-ignored
            row.update(
                {
                    "status": "ignored",
                    "entity": "",
                    "target_account": None,
                    "cadence": "monthly",
                    "expected_amount": Decimal("0"),
                    "expected_day": None,
                }
            )
        state[key] = row
    return [row for row in state.values() if row is not None]
