# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Reader for `custom "balance-anchor"` directives."""
from __future__ import annotations

from typing import Any, Iterable

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    custom_arg,
    custom_meta,
)


def _str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def read_balance_anchors(entries: Iterable[Any]) -> list[dict[str, Any]]:
    """Yield one dict per (account_path, as_of_date). Later directives
    supersede earlier ones at the same key. `balance-anchor-revoked`
    drops the anchor from the result set."""
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    revoked: set[tuple[str, str]] = set()
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type == "balance-anchor-revoked":
            account = _str(custom_arg(entry, 0))
            if account:
                key = (account, entry.date.isoformat())
                revoked.add(key)
                rows.pop(key, None)
            continue
        if entry.type != "balance-anchor":
            continue
        account = _str(custom_arg(entry, 0))
        balance = _str(custom_arg(entry, 1))
        if not account or balance is None:
            continue
        key = (account, entry.date.isoformat())
        if key in revoked:
            continue
        rows[key] = {
            "account_path": account,
            "as_of_date": entry.date.isoformat(),
            "balance": balance,
            "currency": _str(custom_meta(entry, "lamella-anchor-currency")) or "USD",
            "source": _str(custom_meta(entry, "lamella-anchor-source")),
            "notes": _str(custom_meta(entry, "lamella-anchor-notes")),
        }
    return list(rows.values())
