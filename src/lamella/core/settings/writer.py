# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for ``custom "setting"`` directives in ``connector_config.bean``.

Secrets never round-trip. A key is considered secret when it matches
the ``_SECRET_PATTERN`` naming convention OR appears in the explicit
exclusion set. The convention-based rule is self-enforcing — future
settings added to the model don't need a per-key whitelist edit.
"""
from __future__ import annotations

import logging
import re
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


# Keys ending in any of these suffixes are treated as secrets and
# never get stamped into the ledger. Naming convention > exclusion list:
# future settings named e.g. ``webhook_secret`` are automatically
# excluded without us updating this file.
_SECRET_SUFFIX_PATTERN = re.compile(
    r"(?:_|\.)?(?:token|api_token|api_key|key|secret|password|credentials?)$",
    re.IGNORECASE,
)

# Keys whose VALUES contain embedded credentials even though the name
# doesn't scream "secret". Explicit list, kept tiny.
_SECRET_EXPLICIT_KEYS = frozenset({
    "simplefin_access_url",  # url of form user:pass@host
})


def is_secret_key(key: str) -> bool:
    """True if the setting should never round-trip through the ledger."""
    if not key:
        return False
    if key in _SECRET_EXPLICIT_KEYS:
        return True
    return bool(_SECRET_SUFFIX_PATTERN.search(key))


def append_setting(
    *,
    connector_config: Path,
    main_bean: Path,
    key: str,
    value: str,
    set_at: datetime | None = None,
    run_check: bool = True,
) -> str | None:
    """Append a ``custom "setting"`` directive. Returns ``None`` when
    the key is secret (no ledger write happens). Caller is expected to
    still update SQLite (cache) — secrets round-trip via env + cache
    only."""
    if is_secret_key(key):
        log.debug("setting %s is secret; not stamping to ledger", key)
        return None
    ts = set_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta = {"lamella-set-at": ts}
    return append_custom_directive(
        target=connector_config,
        main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=ts.date(),
        directive_type="setting",
        args=[key, value],
        meta=meta,
        run_check=run_check,
    )


def append_setting_unset(
    *,
    connector_config: Path,
    main_bean: Path,
    key: str,
    unset_at: datetime | None = None,
    run_check: bool = True,
) -> str | None:
    """Mark a setting as explicitly unset. Append-only; reconstruct
    filters out keys whose most-recent directive is an unset."""
    if is_secret_key(key):
        return None
    ts = unset_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta = {"lamella-unset-at": ts}
    return append_custom_directive(
        target=connector_config,
        main_bean=main_bean,
        header=CONNECTOR_CONFIG_HEADER,
        directive_date=ts.date(),
        directive_type="setting-unset",
        args=[key],
        meta=meta,
        run_check=run_check,
    )


def read_settings_from_entries(entries) -> dict[str, str]:
    """Return active non-secret settings from the ledger, filtering
    any that have a later unset directive. Last-write-wins per key."""
    state: dict[str, str | None] = {}
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type not in ("setting", "setting-unset"):
            continue
        key = custom_arg(entry, 0)
        if not isinstance(key, str) or not key:
            continue
        if is_secret_key(key):
            continue  # defensive — should never have been written
        if entry.type == "setting-unset":
            state[key] = None
            continue
        value = custom_arg(entry, 1)
        if value is None:
            continue
        state[key] = str(value)
    return {k: v for k, v in state.items() if v is not None}
