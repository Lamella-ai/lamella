# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for `custom "day-review"` directives.

Lands every day-review row in `connector_config.bean` (the catch-all
config file for low-volume user state — same home as
`paperless-field`, `setting`, `entity-context`). The file is
Connector-owned and already included in main.bean.

Called on every `/calendar/<date>` mark-reviewed, and by the
migrate_to_ledger one-shot for existing SQLite rows.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    append_custom_directive,
    custom_meta,
    custom_arg,
)

log = logging.getLogger(__name__)

HEADER = (
    "; connector_config.bean — Managed by Lamella.\n"
    "; Custom directives for low-volume user state. Do not hand-edit.\n"
)


def append_day_review(
    *,
    connector_config: Path,
    main_bean: Path,
    review_date: date,
    last_reviewed_at: datetime | str | None,
    ai_summary: str | None = None,
    ai_summary_at: datetime | str | None = None,
    ai_audit_result: str | None = None,
    ai_audit_result_at: datetime | str | None = None,
    run_check: bool = True,
) -> str:
    """Append a `custom "day-review"` directive. The directive is
    keyed by `lamella-day: YYYY-MM-DD` so reconstruct picks up the most
    recent stamp per day and ignores earlier ones.
    """
    meta: dict[str, Any] = {"lamella-day": review_date}
    if last_reviewed_at is not None:
        meta["lamella-last-reviewed-at"] = _as_str(last_reviewed_at)
    if ai_summary is not None:
        meta["lamella-ai-summary"] = ai_summary
    if ai_summary_at is not None:
        meta["lamella-ai-summary-at"] = _as_str(ai_summary_at)
    if ai_audit_result is not None:
        meta["lamella-ai-audit-result"] = ai_audit_result
    if ai_audit_result_at is not None:
        meta["lamella-ai-audit-result-at"] = _as_str(ai_audit_result_at)
    return append_custom_directive(
        target=connector_config,
        main_bean=main_bean,
        header=HEADER,
        directive_date=review_date,
        directive_type="day-review",
        args=(),
        meta=meta,
        run_check=run_check,
    )


def append_day_review_deleted(
    *,
    connector_config: Path,
    main_bean: Path,
    review_date: date,
    run_check: bool = True,
) -> str:
    """Tombstone — clears any prior `day-review` directive for
    ``review_date`` during reconstruct."""
    return append_custom_directive(
        target=connector_config,
        main_bean=main_bean,
        header=HEADER,
        directive_date=review_date,
        directive_type="day-review-deleted",
        args=(),
        meta={"lamella-day": review_date},
        run_check=run_check,
    )


def read_day_reviews_from_entries(
    entries: Iterable[Any],
) -> list[dict[str, Any]]:
    """Mirror of step18's reader, exposed here so migrate_to_ledger
    can diff against what's already on the ledger."""
    rows: dict[str, dict[str, Any]] = {}
    deleted: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type == "day-review-deleted":
            raw = custom_meta(entry, "lamella-day") or custom_arg(entry, 0)
            key = _key(raw)
            if key:
                deleted.add(key)
                rows.pop(key, None)
            continue
        if entry.type != "day-review":
            continue
        raw = custom_meta(entry, "lamella-day") or custom_arg(entry, 0)
        key = _key(raw)
        if not key or key in deleted:
            continue
        rows[key] = {
            "review_date": key,
            "last_reviewed_at": custom_meta(entry, "lamella-last-reviewed-at"),
            "ai_summary": custom_meta(entry, "lamella-ai-summary"),
            "ai_summary_at": custom_meta(entry, "lamella-ai-summary-at"),
            "ai_audit_result": custom_meta(entry, "lamella-ai-audit-result"),
            "ai_audit_result_at": custom_meta(entry, "lamella-ai-audit-result-at"),
        }
    return list(rows.values())


def _as_str(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _key(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    s = str(value).strip()
    return s or None
