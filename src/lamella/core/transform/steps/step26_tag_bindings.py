# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 26 of the reconstruct roadmap: tag-workflow bindings (ADR-0065).

The ``tag_workflow_bindings`` SQLite table is a cache that rebuilds from
``custom "lamella-tag-binding"`` / ``custom "lamella-tag-binding-revoked"``
directives in ``connector_config.bean``. An empty table after rebuild
means the user has no bindings — the scheduler tick is a no-op, which
is the correct default behavior per ADR-0065.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy
from lamella.features.paperless_bridge.binding_writer import (
    read_bindings_from_entries,
)

log = logging.getLogger(__name__)


@register("step26:tag-bindings", state_tables=["tag_workflow_bindings"])
def reconstruct_tag_bindings(
    conn: sqlite3.Connection, entries: list[Any]
) -> ReconstructReport:
    """Rebuild ``tag_workflow_bindings`` from ledger directives.

    Truncates then repopulates — idempotent per ADR-0015. Running
    twice on the same ledger yields the same DB state.
    """
    rows = read_bindings_from_entries(entries)
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    written = 0

    for row in rows:
        conn.execute(
            """
            INSERT INTO tag_workflow_bindings
                (tag_name, action_name, enabled, config_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(tag_name) DO UPDATE SET
                action_name = excluded.action_name,
                enabled     = excluded.enabled,
                config_json = excluded.config_json,
                created_at  = excluded.created_at,
                updated_at  = excluded.updated_at
            """,
            (
                row["tag_name"],
                row["action_name"],
                1 if row["enabled"] else 0,
                row["config_json"],
                row["created_at"],
                now_iso,
            ),
        )
        written += 1

    log.debug(
        "step26:tag-bindings: %d active binding(s) after revoke-filter",
        len(rows),
    )
    return ReconstructReport(
        pass_name="step26:tag-bindings",
        rows_written=written,
        notes=[f"{len(rows)} active binding(s) after revoke-filter"],
    )


register_policy(
    TablePolicy(
        table="tag_workflow_bindings",
        kind="state",
        primary_key=("tag_name",),
    )
)
