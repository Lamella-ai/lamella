# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 19: txn_classification_modified rebuild from override blocks.

Walks `connector_overrides.bean` entries (Transaction entries
carrying `lamella-override-of`) and populates the
`txn_classification_modified` cache used by the calendar's
dirty-since-reviewed query.

Source-of-truth is the ledger. For each override block:
  * `lamella-modified-at` metadata is the authoritative timestamp
    (stamped by `OverrideWriter.append*()` from migration 044
    forward).
  * If absent (pre-044 override blocks, hand-edits), fall back
    to the block's own transaction date at local midnight under
    ``APP_TZ``. This fallback is deliberately in the past so it
    does NOT flip freshly-reviewed days to dirty on first ship.

This is a *cache* table — wiped before each rebuild — but listed
as a state table in the reconstruct registry so the force-wipe
path clears it.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.calendar.classification_modified import rebuild_from_entries
from lamella.features.calendar.tz import app_tz
from lamella.core.config import get_settings
from lamella.core.transform.reconstruct import ReconstructReport, register

log = logging.getLogger(__name__)


@register(
    "step19:classification_modified",
    state_tables=["txn_classification_modified"],
)
def reconstruct_classification_modified(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(txn_classification_modified)")]
    if not cols:
        return ReconstructReport(
            pass_name="step19:classification_modified",
            rows_written=0,
            notes=["txn_classification_modified table missing — migration 044 not applied?"],
        )
    # The register-call wipe already cleared the table when force is set.
    # If invoked via `reconstruct` without --force, the outer gate refused
    # the pass anyway because day_reviews/txn_classification_modified would
    # count as populated. When we run this from the boot-time bootstrap
    # (see `main.py` lifespan), we wipe explicitly first.
    settings = get_settings()
    tz = app_tz(settings)
    written = rebuild_from_entries(conn, entries, tz_for_fallback=tz)
    return ReconstructReport(
        pass_name="step19:classification_modified",
        rows_written=written,
        notes=[f"rebuilt {written} classification-modified row(s)"] if written else [],
    )
