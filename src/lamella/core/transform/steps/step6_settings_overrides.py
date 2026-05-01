# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 6: user-editable settings persisted as custom directives.

Secrets never round-trip (naming-convention rule in
``settings_writer.is_secret_key``). Non-secret settings emit a
``custom "setting"`` directive; unset emits ``custom "setting-unset"``.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.core.settings.writer import (
    is_secret_key,
    read_settings_from_entries,
)
from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy

log = logging.getLogger(__name__)


@register("step6:settings-overrides", state_tables=["app_settings"])
def reconstruct_settings(
    conn: sqlite3.Connection, entries: list[Any]
) -> ReconstructReport:
    settings = read_settings_from_entries(entries)
    written = 0
    skipped_secret = 0
    for key, value in settings.items():
        if is_secret_key(key):
            skipped_secret += 1
            continue
        cursor = conn.execute(
            """
            INSERT INTO app_settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, value),
        )
        if cursor.rowcount:
            written += 1
    notes = [f"{len(settings)} settings stamped on ledger"]
    if skipped_secret:
        notes.append(f"{skipped_secret} secret-named keys skipped (defensive)")
    return ReconstructReport(
        pass_name="step6:settings-overrides",
        rows_written=written,
        notes=notes,
    )


def _drift_allowance(live_rows, rebuilt_rows):
    """Secrets live only in the live DB; rebuilt won't have them.
    A secret key present in live but missing from rebuilt is not drift
    — it's the expected outcome of the naming rule."""
    live_by_key = {r["key"]: r for r in live_rows}
    rebuilt_by_key = {r["key"]: r for r in rebuilt_rows}
    tolerated = []
    for key in live_by_key.keys() - rebuilt_by_key.keys():
        if is_secret_key(key):
            tolerated.append(f"secret key {key!r} correctly absent from rebuild")
    return tolerated


register_policy(
    TablePolicy(
        table="app_settings",
        kind="cache",
        primary_key=("key",),
        allow_drift=_drift_allowance,
    )
)
