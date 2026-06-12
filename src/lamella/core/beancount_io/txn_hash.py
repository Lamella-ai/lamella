# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import hashlib
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction
from beancount.core.number import MISSING


def _is_missing(value) -> bool:
    """Beancount's MISSING sentinel is a class (not a value), so the
    cheap ``is None`` check does not catch it. Treat both None and
    MISSING as 'no value present'."""
    return value is None or value is MISSING


def _posting_key(posting) -> tuple[str, str, str]:
    units = posting.units
    if units is None or _is_missing(units):
        number = ""
        currency = ""
    else:
        number = (
            "" if _is_missing(units.number)
            else format(Decimal(units.number), "f")
        )
        currency = (
            "" if _is_missing(units.currency) else str(units.currency)
        )
    return (str(posting.account), number, currency)


def txn_hash(txn: Transaction) -> str:
    """Stable SHA-1 over (date, narration, sorted((account, number, currency))).

    The sort means posting order doesn't matter; two reorderings of the same
    transaction produce the same hash. Missing numbers/currencies hash to
    empty strings so interpolated postings collapse consistently.
    """
    parts: list[str] = [txn.date.isoformat(), txn.narration or ""]
    keys: Iterable[tuple[str, str, str]] = sorted(_posting_key(p) for p in txn.postings)
    for account, number, currency in keys:
        parts.append(f"{account}|{number}|{currency}")
    joined = "\n".join(parts).encode("utf-8")
    return hashlib.sha1(joined).hexdigest()
