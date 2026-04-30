# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Readers for loan state. Parallel to ``writer.py``.

``read_loans`` returns dict rows ready for ``INSERT INTO loans``.
``read_loan_balance_anchors`` returns rows for ``loan_balance_anchors``.
"""
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


def _int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def _bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def read_loans(entries: Iterable[Any]) -> list[dict[str, Any]]:
    """Yield one row per loan slug, with the LAST-seen directive
    winning. Loans tombstoned by ``loan-deleted`` drop out of the result.
    """
    rows: dict[str, dict[str, Any]] = {}
    deleted: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type == "loan-deleted":
            slug = _str(custom_arg(entry, 0))
            if slug:
                deleted.add(slug)
                rows.pop(slug, None)
            continue
        if entry.type != "loan":
            continue
        slug = _str(custom_arg(entry, 0))
        if not slug or slug in deleted:
            continue
        rows[slug] = {
            "slug": slug,
            "display_name": _str(custom_meta(entry, "lamella-loan-display-name")),
            "loan_type": _str(custom_meta(entry, "lamella-loan-type")) or "other",
            "entity_slug": _str(custom_meta(entry, "lamella-loan-entity-slug")),
            "institution": _str(custom_meta(entry, "lamella-loan-institution")),
            "original_principal": _str(custom_meta(entry, "lamella-loan-original-principal")) or "0",
            "funded_date": _str(custom_meta(entry, "lamella-loan-funded-date")),
            "first_payment_date": _str(custom_meta(entry, "lamella-loan-first-payment-date")),
            "payment_due_day": _int(custom_meta(entry, "lamella-loan-payment-due-day")),
            "term_months": _int(custom_meta(entry, "lamella-loan-term-months")),
            "interest_rate_apr": _str(custom_meta(entry, "lamella-loan-interest-rate-apr")),
            "monthly_payment_estimate": _str(custom_meta(entry, "lamella-loan-monthly-payment")),
            "escrow_monthly": _str(custom_meta(entry, "lamella-loan-escrow-monthly")),
            "property_tax_monthly": _str(custom_meta(entry, "lamella-loan-property-tax-monthly")),
            "insurance_monthly": _str(custom_meta(entry, "lamella-loan-insurance-monthly")),
            "liability_account_path": _str(custom_meta(entry, "lamella-loan-liability-account")),
            "interest_account_path": _str(custom_meta(entry, "lamella-loan-interest-account")),
            "escrow_account_path": _str(custom_meta(entry, "lamella-loan-escrow-account")),
            "simplefin_account_id": _str(custom_meta(entry, "lamella-loan-simplefin-account-id")),
            "property_slug": _str(custom_meta(entry, "lamella-loan-property-slug")),
            "payoff_date": _str(custom_meta(entry, "lamella-loan-payoff-date")),
            "payoff_amount": _str(custom_meta(entry, "lamella-loan-payoff-amount")),
            "is_active": _bool(custom_meta(entry, "lamella-loan-is-active")),
            "is_revolving": _bool(custom_meta(entry, "lamella-loan-is-revolving")),
            "credit_limit": _str(custom_meta(entry, "lamella-loan-credit-limit")),
            "notes": _str(custom_meta(entry, "lamella-loan-notes")),
        }
    return list(rows.values())


def read_loan_directive_history(
    entries: Iterable[Any], slug: str,
) -> list[dict[str, Any]]:
    """Every `custom "loan"` directive for a given slug, in chronological
    order (earliest first).

    Unlike `read_loans()` which collapses to one row per slug
    (last-seen wins), this surfaces every version the directive has
    gone through so consumers like the WP8 drift detector can find
    the most recent config-change point and reset their rolling
    baseline accordingly.

    Each dict includes a `directive_date` field carrying the
    directive's own date (the day the user saved the edit).
    """
    tombstoned = False
    history: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type == "loan-deleted":
            slug_arg = _str(custom_arg(entry, 0))
            if slug_arg == slug:
                tombstoned = True
                history.clear()
            continue
        if entry.type != "loan":
            continue
        slug_arg = _str(custom_arg(entry, 0))
        if slug_arg != slug or tombstoned:
            continue
        history.append({
            "directive_date": entry.date,
            "slug": slug,
            "display_name": _str(custom_meta(entry, "lamella-loan-display-name")),
            "loan_type": _str(custom_meta(entry, "lamella-loan-type")),
            "entity_slug": _str(custom_meta(entry, "lamella-loan-entity-slug")),
            "institution": _str(custom_meta(entry, "lamella-loan-institution")),
            "original_principal": _str(custom_meta(entry, "lamella-loan-original-principal")),
            "funded_date": _str(custom_meta(entry, "lamella-loan-funded-date")),
            "first_payment_date": _str(custom_meta(entry, "lamella-loan-first-payment-date")),
            "payment_due_day": _int(custom_meta(entry, "lamella-loan-payment-due-day")),
            "term_months": _int(custom_meta(entry, "lamella-loan-term-months")),
            "interest_rate_apr": _str(custom_meta(entry, "lamella-loan-interest-rate-apr")),
            "monthly_payment_estimate": _str(custom_meta(entry, "lamella-loan-monthly-payment")),
            "escrow_monthly": _str(custom_meta(entry, "lamella-loan-escrow-monthly")),
            "property_tax_monthly": _str(custom_meta(entry, "lamella-loan-property-tax-monthly")),
            "insurance_monthly": _str(custom_meta(entry, "lamella-loan-insurance-monthly")),
        })
    return history


def read_loan_pauses(entries: Iterable[Any]) -> list[dict[str, Any]]:
    """Yield one row per (loan_slug, start_date) pause window.

    Tombstone semantics mirror ``read_loans``: a ``loan-pause-revoked``
    directive carrying ``lamella-pause-start`` matching an earlier pause's
    start_date drops that pause from the result. Last-seen-wins on
    duplicates.
    """
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    revoked: set[tuple[str, str]] = set()
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type == "loan-pause-revoked":
            slug = _str(custom_arg(entry, 0))
            start = _str(custom_meta(entry, "lamella-pause-start"))
            if slug and start:
                revoked.add((slug, start))
                rows.pop((slug, start), None)
            continue
        if entry.type != "loan-pause":
            continue
        slug = _str(custom_arg(entry, 0))
        if not slug:
            continue
        start_date = entry.date.isoformat()
        if (slug, start_date) in revoked:
            continue
        rows[(slug, start_date)] = {
            "loan_slug": slug,
            "start_date": start_date,
            "end_date": _str(custom_meta(entry, "lamella-pause-end")),
            "reason": _str(custom_meta(entry, "lamella-pause-reason")),
            "notes": _str(custom_meta(entry, "lamella-pause-notes")),
            "accrued_interest": _str(
                custom_meta(entry, "lamella-pause-accrued-interest")
            ),
        }
    return list(rows.values())


def read_loan_balance_anchors(entries: Iterable[Any]) -> list[dict[str, Any]]:
    """Yield one row per (loan_slug, as_of_date). Later directives
    overwrite earlier ones at the same key."""
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "loan-balance-anchor":
            continue
        slug = _str(custom_arg(entry, 0))
        balance = _str(custom_arg(entry, 1))
        as_of_date = entry.date.isoformat()
        if not slug or balance is None:
            continue
        rows[(slug, as_of_date)] = {
            "loan_slug": slug,
            "as_of_date": as_of_date,
            "balance": balance,
            "source": _str(custom_meta(entry, "lamella-anchor-source")),
            "notes": _str(custom_meta(entry, "lamella-anchor-notes")),
        }
    return list(rows.values())
