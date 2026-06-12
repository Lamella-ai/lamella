# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer + reader for `custom "account-kind"` directives.

accounts_meta.kind is normally inferred by discovery (see
registry/discovery.py::_infer_account_kind) but a user can override
it via /settings/accounts. That override needs to persist through a
DB wipe, so every UI save emits a `custom "account-kind"` directive
that reconstruct (or even plain startup) reads back.

Directive shape:
    2026-04-23 custom "account-kind" Assets:Personal:BankOne:Checking "cash"

Later directives for the same account supersede earlier ones. A
``lamella-account-kind-cleared`` directive (with just the account arg)
clears the override, reverting the row to the heuristic-inferred kind.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    Account,
    append_custom_directive,
    custom_arg,
)

log = logging.getLogger(__name__)


CONNECTOR_CONFIG_HEADER = (
    "; connector_config.bean — configuration state written by Lamella.\n"
    "; Paperless field-role mappings and UI-persisted settings live here.\n"
    "; Do not hand-edit; use the /settings pages.\n"
)


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def append_account_kind(
    *,
    connector_config: Path,
    main_bean: Path,
    account_path: str,
    kind: str,
    run_check: bool = True,
) -> str:
    """Stamp a user-chosen kind for an account."""
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="account-kind",
        args=[Account(account_path), kind],
        run_check=run_check,
    )


def append_account_kind_cleared(
    *,
    connector_config: Path,
    main_bean: Path,
    account_path: str,
    run_check: bool = True,
) -> str:
    """Clear a user override so discovery's inferred kind wins again."""
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="account-kind-cleared",
        args=[Account(account_path)],
        meta=None,
        run_check=run_check,
    )


def read_kind_overrides(entries: Iterable[Any]) -> dict[str, str | None]:
    """Return {account_path: kind_or_None} from the ledger. ``None`` means
    the user cleared their override (treat as "no override"). Later
    directives supersede earlier ones at the same account."""
    out: dict[str, str | None] = {}
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type == "account-kind":
            account = _str(custom_arg(entry, 0))
            kind = _str(custom_arg(entry, 1))
            if account and kind:
                out[account] = kind
        elif entry.type == "account-kind-cleared":
            account = _str(custom_arg(entry, 0))
            if account:
                out[account] = None
    return out


def apply_kind_overrides(
    conn: sqlite3.Connection, entries: Iterable[Any],
) -> int:
    """UPDATE accounts_meta.kind for every override in the ledger. Returns
    count of rows touched. Safe to call on every boot — idempotent."""
    overrides = read_kind_overrides(entries)
    if not overrides:
        return 0
    touched = 0
    for path, kind in overrides.items():
        cursor = conn.execute(
            "UPDATE accounts_meta SET kind = ? WHERE account_path = ?",
            (kind, path),
        )
        if cursor.rowcount:
            touched += int(cursor.rowcount)
    return touched
