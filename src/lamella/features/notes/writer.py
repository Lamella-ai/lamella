# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for `custom "note"` — user-captured notes.

Notes were previously ephemeral (step7_note_coverage marked them as
capture-only). We now persist them so a DB wipe doesn't lose history.

Directive shape:
    2026-04-23 custom "note" 17
      lamella-note-body: "bought solder at MicroCenter"
      lamella-note-merchant-hint: "MICRO CENTER"
      lamella-note-entity-hint: "Acme"
      lamella-note-active-from: 2026-04-23
      lamella-note-active-to: 2026-04-25
      lamella-note-keywords: "solder,prop builds"
      lamella-note-card-override: FALSE
      lamella-note-status: "open"

``id`` is the first positional arg so reconstruct can match on the
same numeric PK the app uses elsewhere.
"""
from __future__ import annotations

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


def _as_date(value: str | date | None) -> date:
    if value is None or value == "":
        return datetime.now(timezone.utc).date()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def append_note(
    *,
    connector_config: Path,
    main_bean: Path,
    note_id: int,
    body: str,
    captured_at: str | date | None = None,
    merchant_hint: str | None = None,
    entity_hint: str | None = None,
    active_from: str | date | None = None,
    active_to: str | date | None = None,
    keywords: Iterable[str] | None = None,
    card_override: bool | None = None,
    status: str | None = None,
    resolved_txn: str | None = None,
    resolved_receipt: int | None = None,
    txn_hash: str | None = None,
    run_check: bool = True,
) -> str:
    meta: dict[str, Any] = {
        "lamella-note-body": body,
    }
    if merchant_hint:
        meta["lamella-note-merchant-hint"] = merchant_hint
    if entity_hint:
        meta["lamella-note-entity-hint"] = entity_hint
    if active_from:
        meta["lamella-note-active-from"] = _as_date(active_from)
    if active_to:
        meta["lamella-note-active-to"] = _as_date(active_to)
    kw = [k for k in (keywords or []) if k]
    if kw:
        meta["lamella-note-keywords"] = ",".join(kw)
    if card_override is not None:
        meta["lamella-note-card-override"] = bool(card_override)
    if status:
        meta["lamella-note-status"] = status
    if resolved_txn:
        meta["lamella-note-resolved-txn"] = resolved_txn
    if resolved_receipt is not None:
        meta["lamella-note-resolved-receipt"] = int(resolved_receipt)
    if txn_hash:
        meta["lamella-note-txn-hash"] = txn_hash

    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=_as_date(captured_at),
        directive_type="note",
        args=[int(note_id)],
        meta=meta,
        run_check=run_check,
    )


def append_note_deleted(
    *,
    connector_config: Path,
    main_bean: Path,
    note_id: int,
    run_check: bool = True,
) -> str:
    return append_custom_directive(
        target=connector_config, main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=datetime.now(timezone.utc).date(),
        directive_type="note-deleted",
        args=[int(note_id)],
        meta=None,
        run_check=run_check,
    )
