# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 15: audit_dismissals reconstruct.

Directive: `custom "audit-dismissed" <fingerprint>` keyed by audit
item fingerprint. Keeps already-dismissed items from reappearing
after a reconstruct.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
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


def append_audit_dismissed(
    *,
    connector_config: Path,
    main_bean: Path,
    fingerprint: str,
    reason: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {}
    if reason:
        meta["lamella-audit-dismiss-reason"] = reason
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="audit-dismissed",
        args=[fingerprint],
        meta=meta,
        run_check=run_check,
    )


def _read_dismissals(entries: Iterable[Any]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "audit-dismissed":
            continue
        fp = _str(custom_arg(entry, 0))
        if not fp:
            continue
        out[fp] = _str(custom_meta(entry, "lamella-audit-dismiss-reason"))
    return out


@register(
    "step15:audit_dismissals",
    state_tables=["audit_dismissals"],
)
def reconstruct_audit_dismissals(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    # Actual schema is (merchant_text, current_account, reason) — the
    # directive encodes the composite key as "merchant|account" in its
    # positional arg. Split and INSERT into the correct columns.
    cols = [r[1] for r in conn.execute("PRAGMA table_info(audit_dismissals)")]
    if not cols:
        return ReconstructReport(
            pass_name="step15:audit_dismissals", rows_written=0,
            notes=["audit_dismissals table not present — skip"],
        )
    written = 0
    for fp, reason in _read_dismissals(entries).items():
        if "merchant_text" in cols and "current_account" in cols:
            merchant, _, account = fp.partition("|")
            if not account:
                # Legacy/loose fingerprint without the split token — put
                # the whole thing in merchant_text so the row still
                # round-trips even if it won't match new audits.
                merchant, account = fp, ""
            conn.execute(
                "INSERT OR IGNORE INTO audit_dismissals "
                "(merchant_text, current_account, reason) VALUES (?, ?, ?)",
                (merchant, account, reason),
            )
        elif "fingerprint" in cols:
            conn.execute(
                "INSERT OR IGNORE INTO audit_dismissals (fingerprint, reason) "
                "VALUES (?, ?)",
                (fp, reason),
            )
        else:
            continue
        written += 1
    return ReconstructReport(
        pass_name="step15:audit_dismissals", rows_written=written,
        notes=[f"rebuilt {written} dismissals"] if written else [],
    )
