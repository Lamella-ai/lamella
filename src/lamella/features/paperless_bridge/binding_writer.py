# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for ``custom "lamella-tag-binding"`` directives (ADR-0065).

User-defined tag → action bindings are state, not cache. They must
survive a DB rebuild (ADR-0001 / ADR-0015). The source of truth is
``connector_config.bean``; the DB cache (``tag_workflow_bindings``) is
rebuilt by ``step26_tag_bindings.py``.

The shape mirrors ``dismissals_writer.py`` exactly:

* ``append_binding`` — write a new binding (or update an existing one
  by appending a replacement; last-write-wins per tag_name).
* ``append_binding_revoke`` — write a revoke directive; the reconstruct
  reader skips bindings whose tag_name has a later revoke.
* ``read_bindings_from_entries`` — walk a loaded entry list and return
  the active bindings after applying revoke filtering. This is the
  pure-ledger half of the reconstruct contract.
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
from lamella.features.receipts.directive_types import (
    DIRECTIVE_TAG_BINDING_NEW,
    DIRECTIVE_TAG_BINDING_REVOKED_NEW,
    DIRECTIVE_TYPES_ALL_TAG_BINDING,
    DIRECTIVE_TYPES_TAG_BINDING_REVOKED,
)

log = logging.getLogger(__name__)


CONNECTOR_CONFIG_HEADER = "; Managed by Lamella. Do not hand-edit.\n"


def append_binding(
    *,
    connector_config: Path,
    main_bean: Path,
    tag_name: str,
    action_name: str,
    enabled: bool = True,
    config_json: str = "",
    created_at: datetime | None = None,
    run_check: bool = True,
) -> str:
    """Append a ``custom "lamella-tag-binding"`` directive to
    ``connector_config.bean``.

    Bindings are append-only; a later directive for the same ``tag_name``
    supersedes an earlier one (last-write-wins in the reconstruct reader).
    The caller does not need to revoke before re-binding.

    Returns the rendered block (useful for logging and tests).
    """
    if not tag_name or not tag_name.strip():
        raise ValueError("tag_name must be a non-empty string")
    if not action_name or not action_name.strip():
        raise ValueError("action_name must be a non-empty string")

    ts = created_at or datetime.now(timezone.utc).replace(tzinfo=None)
    directive_date = ts.date() if isinstance(ts, datetime) else date.today()

    meta: dict = {
        "lamella-enabled": enabled,
        "lamella-config-json": config_json if config_json else "",
        "lamella-created-at": ts.isoformat() if isinstance(ts, datetime) else str(ts),
    }

    return append_custom_directive(
        target=connector_config,
        main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=directive_date,
        directive_type=DIRECTIVE_TAG_BINDING_NEW,
        args=[tag_name, action_name],
        meta=meta,
        run_check=run_check,
    )


def append_binding_revoke(
    *,
    connector_config: Path,
    main_bean: Path,
    tag_name: str,
    revoked_at: datetime | None = None,
    run_check: bool = True,
) -> str:
    """Append a ``custom "lamella-tag-binding-revoked"`` directive.

    Revokes are append-only. The reconstruct reader considers a binding
    active iff the most recent directive for its ``tag_name`` is a
    binding (not a revoke). History is preserved.

    Returns the rendered block.
    """
    if not tag_name or not tag_name.strip():
        raise ValueError("tag_name must be a non-empty string")

    ts = revoked_at or datetime.now(timezone.utc).replace(tzinfo=None)
    directive_date = ts.date() if isinstance(ts, datetime) else date.today()

    return append_custom_directive(
        target=connector_config,
        main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=directive_date,
        directive_type=DIRECTIVE_TAG_BINDING_REVOKED_NEW,
        args=[tag_name],
        meta={"lamella-revoked-at": ts.isoformat() if isinstance(ts, datetime) else str(ts)},
        run_check=run_check,
    )


def read_bindings_from_entries(entries) -> list[dict]:
    """Parse every tag-binding directive in the loaded ledger entries
    and filter by revokes. A binding is active iff the most recent
    directive for its ``tag_name`` is a binding (not a revoke).

    Returns a list of row dicts with keys:
        tag_name, action_name, enabled, config_json, created_at

    Both ``lamella-tag-binding`` and ``lamella-tag-binding-revoked``
    are accepted. Order is load order (deterministic per Beancount
    parse). Last-write-wins per ``tag_name``.
    """
    # State: tag_name → row dict or None (revoked).
    state: dict[str, dict | None] = {}

    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type not in DIRECTIVE_TYPES_ALL_TAG_BINDING:
            continue

        tag_name = custom_arg(entry, 0)
        if not isinstance(tag_name, str) or not tag_name.strip():
            log.debug(
                "binding_writer: skipping malformed directive at %s "
                "(missing or non-string tag_name)",
                getattr(entry, "meta", {}).get("filename", "?"),
            )
            continue

        if entry.type in DIRECTIVE_TYPES_TAG_BINDING_REVOKED:
            state[tag_name] = None
            continue

        # It's a binding directive.
        action_name = custom_arg(entry, 1)
        if not isinstance(action_name, str) or not action_name.strip():
            log.debug(
                "binding_writer: skipping malformed binding for tag %r "
                "(missing or non-string action_name)",
                tag_name,
            )
            continue

        # enabled: TRUE/FALSE meta — beancount parses bare TRUE/FALSE as bool
        raw_enabled = custom_meta(entry, "lamella-enabled")
        if isinstance(raw_enabled, bool):
            enabled = raw_enabled
        elif isinstance(raw_enabled, str):
            enabled = raw_enabled.strip().upper() != "FALSE"
        else:
            enabled = True  # default when absent

        config_json = custom_meta(entry, "lamella-config-json") or ""
        if not isinstance(config_json, str):
            config_json = str(config_json)

        created_at_raw = custom_meta(entry, "lamella-created-at")
        if isinstance(created_at_raw, datetime):
            created_at = created_at_raw.isoformat()
        elif isinstance(created_at_raw, date):
            created_at = datetime.combine(
                created_at_raw, datetime.min.time()
            ).isoformat()
        elif isinstance(created_at_raw, str):
            created_at = created_at_raw
        else:
            # Fall back to the directive date.
            created_at = datetime.combine(
                entry.date, datetime.min.time()
            ).isoformat()

        state[tag_name] = {
            "tag_name": tag_name,
            "action_name": action_name.strip(),
            "enabled": bool(enabled),
            "config_json": config_json,
            "created_at": created_at,
        }

    return [row for row in state.values() if row is not None]
