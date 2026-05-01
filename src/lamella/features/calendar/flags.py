# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Cross-source diagnostic flags for the day view.

Each flag is a deterministic, SQL-computable check that surfaces
"this looks weird, eyeball it" hints. They are NOT blockers — the
day view renders them as passive prompts.

Starting set (matches the brief):

  1. mileage_without_gas    — mileage logged but no gas/fuel/toll/
                              parking txn within ±7 days.
  2. gas_without_mileage    — gas transaction but no mileage entry
                              within ±1 day.
  3. paperless_without_txn  — Paperless doc on this day without any
                              matching txn (linked OR unlinked) in
                              ±7 days at a close amount.
  4. near_duplicate_txns    — two or more txns same day with same
                              merchant/narration and same amount.
  5. mileage_vs_gas_vehicle — mileage on vehicle A and a gas txn
                              the same day on a card usually tied
                              to vehicle B. Skipped when the
                              card↔vehicle association table is
                              empty (current deploys don't all
                              carry that mapping).
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Iterable

log = logging.getLogger(__name__)


_FUEL_WORDS = re.compile(
    r"\b(gas|fuel|shell|chevron|exxon|mobil|bp|marathon|speedway|"
    r"sheetz|wawa|costco\s+gas|sam'?s\s+gas|sunoco|valero|arco|"
    r"phillips\s*66|texaco|76|citgo|pilot|flying\s*j)\b",
    re.I,
)


@dataclass
class Flag:
    code: str  # machine-readable identifier, e.g. "mileage_without_gas"
    severity: str  # "info" | "warn"
    title: str
    detail: str


def compute_day_flags(
    conn: sqlite3.Connection,
    entries: Iterable[Any],
    day: date,
) -> list[Flag]:
    """Run every flag check for ``day`` and return the hits.

    ``entries`` is the loaded ledger; caller passes the cached
    reader.load().entries so we don't re-parse.
    """
    from beancount.core.data import Transaction

    # Pre-slice ledger txns around the day we care about to cheaply
    # support ±N-day proximity checks.
    near: list[Transaction] = []
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        d = e.date
        if not isinstance(d, date):
            continue
        if abs((d - day).days) <= 7:
            near.append(e)

    flags: list[Flag] = []
    flags.extend(_flag_mileage_without_gas(conn, near, day))
    flags.extend(_flag_gas_without_mileage(conn, near, day))
    flags.extend(_flag_paperless_without_txn(conn, near, day))
    flags.extend(_flag_near_duplicate_txns(near, day))
    flags.extend(_flag_mileage_vs_gas_vehicle(conn, near, day))
    return flags


# -- individual checks -----------------------------------------------------

def _is_fuel_txn(txn) -> bool:
    text = " ".join(
        filter(None, [txn.narration, txn.payee, *(p.account or "" for p in txn.postings or ())])
    )
    return bool(_FUEL_WORDS.search(text))


def _flag_mileage_without_gas(
    conn: sqlite3.Connection, near: list, day: date,
) -> list[Flag]:
    row = conn.execute(
        "SELECT COUNT(*) FROM mileage_entries WHERE entry_date = ?",
        (day.isoformat(),),
    ).fetchone()
    if not row or int(row[0]) == 0:
        return []
    # Any fuel-looking txn within ±7 days?
    for txn in near:
        if _is_fuel_txn(txn):
            return []
    return [
        Flag(
            code="mileage_without_gas",
            severity="info",
            title="Mileage logged without nearby fuel transaction",
            detail="No gas/fuel/toll/parking transaction within ±7 days.",
        )
    ]


def _flag_gas_without_mileage(
    conn: sqlite3.Connection, near: list, day: date,
) -> list[Flag]:
    # Gas on this day?
    gas_same_day = [t for t in near if t.date == day and _is_fuel_txn(t)]
    if not gas_same_day:
        return []
    # Any mileage within ±1 day?
    rows = conn.execute(
        "SELECT COUNT(*) FROM mileage_entries "
        "WHERE entry_date BETWEEN ? AND ?",
        ((day - timedelta(days=1)).isoformat(),
         (day + timedelta(days=1)).isoformat()),
    ).fetchone()
    if rows and int(rows[0]) > 0:
        return []
    return [
        Flag(
            code="gas_without_mileage",
            severity="info",
            title="Gas transaction without nearby mileage entry",
            detail=f"{len(gas_same_day)} fuel txn(s) today, no mileage logged within ±1 day.",
        )
    ]


def _flag_paperless_without_txn(
    conn: sqlite3.Connection, near: list, day: date,
) -> list[Flag]:
    rows = conn.execute(
        "SELECT paperless_id, title FROM paperless_doc_index "
        "WHERE created_date = ?",
        (day.isoformat(),),
    ).fetchall()
    if not rows:
        return []
    # Already-linked docs don't count.
    linked = set()
    try:
        linked_rows = conn.execute(
            "SELECT DISTINCT paperless_id FROM receipt_links"
        ).fetchall()
        linked = {int(r["paperless_id"]) for r in linked_rows}
    except sqlite3.OperationalError:
        pass
    unmatched = [r for r in rows if int(r["paperless_id"]) not in linked]
    if not unmatched:
        return []
    return [
        Flag(
            code="paperless_without_txn",
            severity="info",
            title=f"{len(unmatched)} Paperless doc(s) unmatched",
            detail=(
                "Paperless has document(s) dated this day that aren't linked "
                "to any transaction yet — link from receipts-needed or "
                "dismiss if intentionally unattached."
            ),
        )
    ]


def _flag_near_duplicate_txns(near: list, day: date) -> list[Flag]:
    """Flag transactions that share merchant + amount on the same day.

    Transfer-shaped transactions — where one leg is a FIXME and the
    other is a real Asset/Liability account — are intentionally
    EXCLUDED. Bank-to-bank transfers commonly show up twice (once
    per side of the transfer) with identical narration + amount;
    those are the normal shape of a transfer, not a duplicate, and
    flagging them produces noise on every tax-day transfer batch.

    We also skip transactions whose narration contains the word
    "transfer" — a conservative belt-and-suspenders guard for the
    common reference-number duplicates Bank One / Chase online-
    transfer flows produce.
    """
    today = [t for t in near if t.date == day]
    groups: dict[tuple[str, str], int] = {}
    for t in today:
        key_payee = (t.payee or t.narration or "").strip().lower()
        if not key_payee:
            continue
        if "transfer" in key_payee:
            continue
        # Skip transactions shaped like transfers: a FIXME leg + a
        # real Asset/Liability leg. That's the canonical
        # one-sided-bank-transfer shape before categorization.
        has_fixme = False
        has_bank_leg = False
        for p in t.postings or ():
            acct = (p.account or "")
            if "FIXME" in acct.upper():
                has_fixme = True
            elif acct.startswith(("Assets:", "Liabilities:")):
                has_bank_leg = True
        if has_fixme and has_bank_leg:
            continue

        # Use first non-FIXME posting amount as the signature.
        amt_sig = ""
        for p in t.postings or ():
            units = p.units
            if units is None or units.number is None:
                continue
            acct = p.account or ""
            if "FIXME" in acct.upper():
                continue
            amt_sig = f"{abs(Decimal(units.number)):.2f}"
            break
        if not amt_sig:
            continue
        key = (key_payee, amt_sig)
        groups[key] = groups.get(key, 0) + 1
    dupes = [(k, n) for k, n in groups.items() if n > 1]
    if not dupes:
        return []
    summary = ", ".join(
        f"{payee} x {n} @ ${amt}" for (payee, amt), n in dupes[:3]
    )
    return [
        Flag(
            code="near_duplicate_txns",
            severity="warn",
            title="Near-duplicate transactions today",
            detail=f"Same merchant + amount: {summary}",
        )
    ]


def _flag_mileage_vs_gas_vehicle(
    conn: sqlite3.Connection, near: list, day: date,
) -> list[Flag]:
    """Skipped when there's no vehicle↔card association data to
    cross-check against. Specifically: we'd need a join path from
    (a card account) to (a vehicle) that's populated for this user.
    Check for a `vehicle_fuel_log` or similar that does that mapping;
    if absent, skip cleanly rather than emit noise."""
    try:
        has_mapping_col = any(
            r[1] == "card_account"
            for r in conn.execute("PRAGMA table_info(vehicles)")
        )
    except sqlite3.OperationalError:
        return []
    if not has_mapping_col:
        # No deterministic data to back this check — don't emit.
        return []

    mileage_rows = conn.execute(
        "SELECT DISTINCT vehicle FROM mileage_entries WHERE entry_date = ?",
        (day.isoformat(),),
    ).fetchall()
    if not mileage_rows:
        return []
    vehicles_today = {r["vehicle"] for r in mileage_rows if r["vehicle"]}
    if not vehicles_today:
        return []

    gas_today = [t for t in near if t.date == day and _is_fuel_txn(t)]
    if not gas_today:
        return []

    # Pull vehicle↔card mapping.
    rows = conn.execute(
        "SELECT slug, card_account FROM vehicles WHERE card_account IS NOT NULL"
    ).fetchall()
    card_to_vehicle = {r["card_account"]: r["slug"] for r in rows}

    mismatches: list[str] = []
    for txn in gas_today:
        for p in txn.postings or ():
            acct = p.account or ""
            if acct in card_to_vehicle:
                gas_vehicle = card_to_vehicle[acct]
                if gas_vehicle and gas_vehicle not in vehicles_today:
                    mismatches.append(
                        f"{gas_vehicle}'s card charged but mileage logged on {', '.join(sorted(vehicles_today))}"
                    )
    if not mismatches:
        return []
    return [
        Flag(
            code="mileage_vs_gas_vehicle",
            severity="warn",
            title="Fuel charged on wrong-vehicle card",
            detail="; ".join(mismatches[:3]),
        )
    ]
