# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Detect + auto-heal settings that are in SQLite but not on the ledger.

The dual-write in ``AppSettingsStore.set()`` catches BeanCheckError and
logs-but-doesn't-raise so the /settings page never 500s on a transient
ledger error. That's the right UX choice, but it means a silent failure
can leave a setting permanently DB-only — which violates the
reconstruct guarantee because DB-delete would lose the setting.

This module closes the loop: on every boot, and on demand via the
verify CLI, it scans for non-secret keys present in ``app_settings``
that have no corresponding ledger directive, and restamps them. Any
stamp failures are surfaced at WARN level so operator-visible logs
don't hide the drift.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from lamella.core.beancount_io import LedgerReader
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.settings.writer import (
    append_setting,
    is_secret_key,
    read_settings_from_entries,
)

log = logging.getLogger(__name__)


def detect_missing_ledger_stamps(
    conn: sqlite3.Connection,
    reader: LedgerReader,
) -> list[tuple[str, str]]:
    """Return (key, value) pairs present in ``app_settings`` but not
    stamped on the ledger. Excludes secret keys (those intentionally
    don't round-trip)."""
    entries = list(reader.load().entries)
    on_ledger = read_settings_from_entries(entries)
    try:
        rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
    except sqlite3.OperationalError:
        return []
    missing: list[tuple[str, str]] = []
    for row in rows:
        key = row["key"]
        value = row["value"] or ""
        if is_secret_key(key):
            continue
        if key in on_ledger and on_ledger[key] == value:
            continue
        missing.append((key, value))
    return missing


def restamp_missing(
    conn: sqlite3.Connection,
    reader: LedgerReader,
    *,
    connector_config_path: Path,
    main_bean_path: Path,
) -> tuple[int, int]:
    """Restamp every non-secret setting whose ledger stamp is missing
    or stale. Returns (succeeded, failed). Failures are logged at WARN;
    the caller decides whether to escalate further."""
    missing = detect_missing_ledger_stamps(conn, reader)
    if not missing:
        return 0, 0
    succeeded = 0
    failed = 0
    log.info(
        "settings drift detected: %d key(s) need restamping onto ledger",
        len(missing),
    )
    for key, value in missing:
        try:
            append_setting(
                connector_config=connector_config_path,
                main_bean=main_bean_path,
                key=key,
                value=value,
            )
            succeeded += 1
        except BeanCheckError as exc:
            failed += 1
            log.warning(
                "settings restamp FAILED for %s (value left DB-only, "
                "will retry next boot): %s",
                key,
                exc,
            )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            log.warning("settings restamp error for %s: %s", key, exc)
    if failed:
        log.warning(
            "settings drift partially healed: %d stamped, %d still "
            "DB-only. Run `python -m lamella.core.transform.verify` "
            "for details.",
            succeeded,
            failed,
        )
    return succeeded, failed
