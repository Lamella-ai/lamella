# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Companion-account scaffolding: given a registered bank/card/brokerage
account, the system is responsible for ensuring every related Expense,
Income, and Equity account it will realistically need exists and is
opened against Schedule-C-compatible paths.

The user's ask (summarized): "when I register a Mercury credit card,
the app should know there'll be fees, there'll be cashback, there'll
be interest, and pre-open the right accounts under the right Schedule
C lines — so classification isn't dumping things into a made-up path."

Path conventions (Schedule C):
  - Line 16 ("Interest")  → `Expenses:{entity}:Interest:{institution}`
  - Line 27a ("Other")    → `Expenses:{entity}:Bank:{institution}:*`
    (The `:Bank(:` segment matches the 27a regex in
    config/schedule_c_lines.yml, so anything under it rolls up to
    "Other expenses" at tax time.)
  - Opening balance       → `Equity:OpeningBalances:{entity}:{institution}:{name}`
  - Interest earned       → `Income:{entity}:Interest:{institution}`

Everything is derived from four inputs — the account path, its
entity_slug, its kind, and its institution — so re-running is safe
(idempotent: AccountsWriter skips paths already open in the ledger).
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date
from typing import Iterable

from beancount.core.data import Open

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.registry.accounts_writer import AccountsWriter

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CompanionPath:
    path: str
    purpose: str  # short human string: "bank fees", "cashback", …

    def __str__(self) -> str:
        return self.path


def _segment_after_entity(account_path: str, entity_slug: str | None) -> tuple[str, str]:
    """Split an account path into (institution_segment, leaf_name).

    Typical paths:
      Assets:ZetaGen:Mercury:Checking  → ("Mercury", "Checking")
      Liabilities:Personal:Chase:SapphireReserve → ("Chase", "SapphireReserve")

    Returns ("", "") when the path doesn't follow the
    {Root}:{Entity}:{Institution}:{Name} convention — caller skips
    scaffolding in that case so malformed paths don't leak into
    connector_accounts.bean.
    """
    parts = account_path.split(":")
    if len(parts) < 4:
        return "", ""
    # Entity segment should be at index 1 for Assets/Liabilities.
    if entity_slug and parts[1] != entity_slug:
        return "", ""
    institution = parts[2]
    name = ":".join(parts[3:])
    return institution, name


def companion_paths_for(
    *,
    account_path: str,
    kind: str | None,
    entity_slug: str | None,
    institution: str | None = None,
) -> list[CompanionPath]:
    """Return the list of companion accounts that should exist for the
    given account. Pure — does no I/O, no DB lookups — so it can be
    unit-tested without a ledger fixture.

    ``institution`` is usually the same as parts[2] of the path, but
    callers can pass it explicitly (accounts_meta.institution) when
    the path segment is a slug and the user has a prettier
    institution name on the meta row. When they differ, the path
    segment wins — it has to match the account's own Beancount path.
    """
    if not entity_slug:
        # Can't anchor anything under an entity-less account. Bail
        # clean so callers don't emit uncategorized Expenses:Bank:… paths.
        return []
    inst_seg, name_seg = _segment_after_entity(account_path, entity_slug)
    if not inst_seg or not name_seg:
        return []

    out: list[CompanionPath] = []
    # Opening balance companion — same for every asset/liability.
    if account_path.startswith(("Assets:", "Liabilities:")):
        out.append(
            CompanionPath(
                path=f"Equity:OpeningBalances:{entity_slug}:{inst_seg}:{name_seg}",
                purpose="opening balance",
            )
        )
    # Entity-scoped transfer clearing account — used when the two
    # sides of a transfer hit the ledger on different dates. Every
    # entity gets one (and only one); AccountsWriter dedupes against
    # existing opens so this only actually writes the first time an
    # account under this entity is registered.
    if account_path.startswith(("Assets:", "Liabilities:")):
        out.append(
            CompanionPath(
                path=f"Assets:{entity_slug}:Transfers:InFlight",
                purpose=(
                    "holding account for cross-date transfers. "
                    "Same-entity pairs net to zero here; cross-entity "
                    "pairs leave a balance until reconciled"
                ),
            )
        )

    kind_key = (kind or "").strip().lower()
    if kind_key == "credit_card":
        out.append(
            CompanionPath(
                path=f"Expenses:{entity_slug}:Interest:{inst_seg}",
                purpose="interest charged on this card (Schedule C line 16)",
            )
        )
        out.append(
            CompanionPath(
                path=f"Expenses:{entity_slug}:Bank:{inst_seg}:Fees",
                purpose="card fees: annual, late, foreign (Schedule C line 27a)",
            )
        )
        out.append(
            CompanionPath(
                path=f"Expenses:{entity_slug}:Bank:{inst_seg}:Cashback",
                purpose="cashback / rewards — booked as a contra-expense (Schedule C line 27a)",
            )
        )
    elif kind_key == "line_of_credit":
        out.append(
            CompanionPath(
                path=f"Expenses:{entity_slug}:Interest:{inst_seg}",
                purpose="LOC interest (Schedule C line 16)",
            )
        )
        out.append(
            CompanionPath(
                path=f"Expenses:{entity_slug}:Bank:{inst_seg}:Fees",
                purpose="LOC fees (Schedule C line 27a)",
            )
        )
    elif kind_key == "loan":
        # Loan-specific companions are handled by loans/step9 — it
        # writes its own Interest + Escrow accounts for line 16 /
        # appropriate lines. Keep this bucket quiet so we don't
        # double-open them.
        pass
    elif kind_key in ("checking", "savings", "cash"):
        # Any deposit account can hit an overdraft / wire / maintenance
        # fee. Pre-opening the bucket avoids the "where do I post this
        # $5 fee" moment later.
        out.append(
            CompanionPath(
                path=f"Expenses:{entity_slug}:Bank:{inst_seg}:Fees",
                purpose="maintenance / overdraft / wire fees (Schedule C line 27a)",
            )
        )
        if kind_key in ("savings",):
            out.append(
                CompanionPath(
                    path=f"Income:{entity_slug}:Interest:{inst_seg}",
                    purpose="interest earned on savings",
                )
            )
    elif kind_key == "brokerage":
        out.append(
            CompanionPath(
                path=f"Expenses:{entity_slug}:Bank:{inst_seg}:Fees",
                purpose="brokerage fees / commissions (Schedule C line 27a)",
            )
        )
        out.append(
            CompanionPath(
                path=f"Income:{entity_slug}:Interest:{inst_seg}",
                purpose="interest / dividends earned",
            )
        )
    # "asset" and "virtual" kinds don't get companions — their
    # taxable flows come from elsewhere.

    return out


def ensure_companions(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    reader: LedgerReader,
    account_path: str,
) -> list[CompanionPath]:
    """Open every companion account that isn't already open in the
    ledger. Returns the list of paths that were actually newly opened
    (may be empty if everything was present or the account has no
    companions). Safe to call repeatedly.
    """
    row = conn.execute(
        "SELECT account_path, kind, entity_slug, institution "
        "FROM accounts_meta WHERE account_path = ?",
        (account_path,),
    ).fetchone()
    if row is None:
        return []
    desired = companion_paths_for(
        account_path=row["account_path"],
        kind=row["kind"],
        entity_slug=row["entity_slug"],
        institution=row["institution"],
    )
    if not desired:
        return []
    # Snapshot existing opens to avoid "duplicate open" bean-check
    # failures. write_opens also filters against existing_paths, so
    # both layers are belt-and-suspenders.
    existing: set[str] = set()
    for entry in reader.load().entries:
        acct = getattr(entry, "account", None)
        if isinstance(acct, str):
            existing.add(acct)

    needed = [cp for cp in desired if cp.path not in existing]
    if not needed:
        return []

    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        writer.write_opens(
            [cp.path for cp in needed],
            comment=f"Companion accounts for {account_path} (kind={row['kind']})",
            existing_paths=existing,
        )
    except BeanCheckError:
        # Re-raise so callers can surface a useful error; ledger
        # is already reverted by write_opens.
        raise
    reader.invalidate()
    return needed
