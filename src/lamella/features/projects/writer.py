# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for `custom "project"` directives."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from lamella.core.transform.custom_directive import append_custom_directive

log = logging.getLogger(__name__)


CONNECTOR_CONFIG_HEADER = (
    "; connector_config.bean — configuration state written by Lamella.\n"
    "; Paperless field-role mappings and UI-persisted settings live here.\n"
    "; Do not hand-edit; use the /settings pages.\n"
)


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _as_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def append_project(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    display_name: str,
    start_date: str | date,
    entity_slug: str | None = None,
    property_slug: str | None = None,
    project_type: str | None = None,
    end_date: str | date | None = None,
    budget_amount: str | None = None,
    expected_merchants: Iterable[str] | None = None,
    previous_project_slug: str | None = None,
    is_active: bool = True,
    closed_at: str | datetime | None = None,
    description: str | None = None,
    notes: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {
        "lamella-project-display-name": display_name,
        "lamella-project-start-date": _as_date(start_date),
        "lamella-project-is-active": bool(is_active),
    }
    if entity_slug:
        meta["lamella-project-entity-slug"] = entity_slug
    if property_slug:
        meta["lamella-project-property-slug"] = property_slug
    if project_type:
        meta["lamella-project-type"] = project_type
    if end_date:
        meta["lamella-project-end-date"] = _as_date(end_date)
    if budget_amount:
        meta["lamella-project-budget-amount"] = str(budget_amount)
    merchants = list(expected_merchants or [])
    if merchants:
        # JSON-encode as a single string so the list shape survives
        # the round-trip through Beancount metadata.
        meta["lamella-project-expected-merchants"] = json.dumps(merchants)
    if previous_project_slug:
        meta["lamella-project-previous-slug"] = previous_project_slug
    if closed_at:
        if isinstance(closed_at, datetime):
            meta["lamella-project-closed-at"] = closed_at
        else:
            meta["lamella-project-closed-at"] = str(closed_at)
    if description:
        meta["lamella-project-description"] = description
    if notes:
        meta["lamella-project-notes"] = notes

    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="project",
        args=[slug],
        meta=meta,
        run_check=run_check,
    )


def append_project_deleted(
    *,
    connector_config: Path,
    main_bean: Path,
    slug: str,
    run_check: bool = True,
) -> str:
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_today(),
        directive_type="project-deleted",
        args=[slug],
        meta=None,
        run_check=run_check,
    )
