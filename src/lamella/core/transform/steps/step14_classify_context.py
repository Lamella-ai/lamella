# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 14: classify-context reconstruct.

Two directive types:
  - `custom "account-description" <account> "<text>"` — user-authored
    description fed into the classify-prompt sidebar for that account.
  - `custom "entity-context" <entity_slug> "<text>"` — entity-level
    context fed into classify for any txn attributed to that entity.

Both tables are content-only (no timestamps), so the last-seen value
per key wins.
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
    custom_meta,
)
from lamella.core.transform.reconstruct import ReconstructReport, register

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


# --- writers ---------------------------------------------------------


def append_account_description(
    *,
    connector_config: Path,
    main_bean: Path,
    account_path: str,
    description: str,
    run_check: bool = True,
) -> str:
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="account-description",
        args=[Account(account_path), description],
        run_check=run_check,
    )


def append_entity_context(
    *,
    connector_config: Path,
    main_bean: Path,
    entity_slug: str,
    context: str,
    run_check: bool = True,
) -> str:
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="entity-context",
        args=[entity_slug, context],
        run_check=run_check,
    )


# --- readers ---------------------------------------------------------


def _read_account_descriptions(entries: Iterable[Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "account-description":
            continue
        account = _str(custom_arg(entry, 0))
        description = _str(custom_arg(entry, 1))
        if account and description:
            out[account] = description
    return out


def _read_entity_contexts(entries: Iterable[Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "entity-context":
            continue
        slug = _str(custom_arg(entry, 0))
        context = _str(custom_arg(entry, 1))
        if slug and context:
            out[slug] = context
    return out


# --- reconstruct pass -------------------------------------------------


@register(
    "step14:classify_context",
    # entities table is also touched (classify_context column) but it's
    # owned by the registry; we only UPDATE an existing row, never
    # INSERT or DELETE. Listing it here keeps the verify policy honest.
    state_tables=["account_classify_context"],
)
def reconstruct_classify_context(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    written = 0
    for account, description in _read_account_descriptions(entries).items():
        conn.execute(
            """
            INSERT INTO account_classify_context (account_path, description)
            VALUES (?, ?)
            ON CONFLICT (account_path) DO UPDATE SET
                description = excluded.description,
                updated_at  = CURRENT_TIMESTAMP
            """,
            (account, description),
        )
        written += 1
    for slug, context in _read_entity_contexts(entries).items():
        conn.execute(
            "UPDATE entities SET classify_context = ? WHERE slug = ?",
            (context, slug),
        )
        if conn.execute("SELECT changes()").fetchone()[0]:
            written += 1
    return ReconstructReport(
        pass_name="step14:classify_context", rows_written=written,
        notes=[f"rebuilt {written} descriptions/contexts"] if written else [],
    )
