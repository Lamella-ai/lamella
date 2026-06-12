# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for ``custom "paperless-field"`` directives in
``connector_config.bean``.

Only user-edited rows (auto_assigned=0) are state — the keyword-guessed
rows (auto_assigned=1) are regeneratable by re-running ``sync_fields``.
We only stamp the user's explicit decisions so the ledger stays
compact and the reconstruct path knows which rows are "mine" vs.
"re-derivable."
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    append_custom_directive,
    custom_arg,
    custom_meta,
)

log = logging.getLogger(__name__)


CONNECTOR_CONFIG_HEADER = (
    "; connector_config.bean — configuration state written by Lamella.\n"
    "; Paperless field-role mappings and UI-persisted settings live here.\n"
    "; Do not hand-edit; use the /settings pages.\n"
)


def append_field_mapping(
    *,
    connector_config: Path,
    main_bean: Path,
    paperless_field_id: int,
    paperless_field_name: str,
    canonical_role: str,
    updated_at: datetime | None = None,
    run_check: bool = True,
) -> str:
    ts = updated_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta: dict = {
        "lamella-field-name": paperless_field_name,
        "lamella-auto-assigned": False,  # user explicit => always false here
        "lamella-updated-at": ts,
    }
    return append_custom_directive(
        target=connector_config,
        main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=ts.date(),
        directive_type="paperless-field",
        args=[int(paperless_field_id), canonical_role],
        meta=meta,
        run_check=run_check,
    )


def read_field_mappings_from_entries(entries) -> list[dict]:
    """Return user-edited Paperless field mappings. Last-write-wins
    per (paperless_field_id)."""
    state: dict[int, dict] = {}
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type != "paperless-field":
            continue
        field_id = custom_arg(entry, 0)
        try:
            fid = int(field_id)
        except (TypeError, ValueError):
            continue
        role = custom_arg(entry, 1)
        if not isinstance(role, str) or not role:
            continue
        name = custom_meta(entry, "lamella-field-name") or ""
        auto = custom_meta(entry, "lamella-auto-assigned")
        state[fid] = {
            "paperless_field_id": fid,
            "paperless_field_name": str(name),
            "canonical_role": role,
            "auto_assigned": 1 if auto is True else 0,
        }
    return list(state.values())
