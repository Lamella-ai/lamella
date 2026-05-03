# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Recovery detectors. Pure functions ``(conn, entries) -> tuple[Finding, ...]``.

Each detector covers one drift category. The Phase 6 bulk-apply
orchestrator consumes :func:`detect_all`, which calls every
registered detector and concatenates their results. Per-detector
imports below register them in :data:`DETECTORS`; new categories
add a module here, an import in this file, and an entry in
``DETECTORS``.

Order in :data:`DETECTORS` determines the order findings appear
in the aggregated output. Stable across reboots so
:data:`setup_repair_state.findings` overlay remains
deterministic.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Callable, Iterable

from lamella.features.recovery.findings.legacy_paths import (
    detect_legacy_paths,
)
from lamella.features.recovery.findings.schema_drift import (
    detect_schema_drift,
)
from lamella.features.recovery.models import Finding


__all__ = [
    "DETECTORS",
    "detect_all",
    "detect_legacy_paths",
    "detect_schema_drift",
]


_LOG = logging.getLogger(__name__)


# Detector signature: ``(conn, entries) -> tuple[Finding, ...]``.
# Pure — no writes, no DB mutations. Same finding ids returned on
# repeated calls with the same input (the Phase 3 contract).
DetectorFn = Callable[[sqlite3.Connection, Iterable[Any]], tuple[Finding, ...]]


# Order matters for the aggregated output:
#
# 1. ``schema_drift`` first — blockers always render before warnings.
# 2. ``legacy_paths`` next — established Phase 3 cleanup category.
#
# New detectors append. Don't reorder existing entries; the order
# is part of the aggregator's contract because Phase 6.1.5's
# bulk-review UI renders findings in this order on initial draft
# composition (the user can reorder via the UI, but the default is
# this list).
DETECTORS: tuple[DetectorFn, ...] = (
    detect_schema_drift,
    detect_legacy_paths,
)


def detect_all(
    conn: sqlite3.Connection,
    entries: Iterable[Any],
) -> tuple[Finding, ...]:
    """Call every registered detector, concatenate their findings.

    Pure function — same input yields the same output (same
    Finding ids), per the Phase 3 detector contract. The Phase 6
    bulk-apply orchestrator calls this between groups for re-
    detection; the bulk-review UI calls it at page-render time.

    A detector that raises is logged and skipped — one broken
    detector should not poison the aggregated output. The error
    surfaces as zero findings from that category, which is the
    same shape as "category currently has nothing wrong"; ops
    sees the log message and the missing detector eventually
    gets fixed.

    Args:
        conn: live SQLite connection. Read-only from the
            detector's perspective; aggregator does not start
            its own transaction.
        entries: parsed Beancount entries (typically
            ``LedgerReader.load().entries`` materialized to a
            list — multiple detectors will iterate over them so
            consume-once iterables don't work).

    Returns:
        Flat tuple of Findings in detector-registration order.
        Within a single detector's output, ordering is the
        detector's own deterministic shape (typically alphabetic
        by target).
    """
    # Materialize entries up front so each detector iterates over
    # the same view. Iterables that exhaust on first read (e.g.
    # generators) would cause later detectors to see empty input.
    entries_list = list(entries)

    out: list[Finding] = []
    for detector in DETECTORS:
        try:
            findings = detector(conn, entries_list)
        except Exception as exc:  # noqa: BLE001 — defensive; logged
            _LOG.exception(
                "detector %s raised during detect_all; skipping",
                getattr(detector, "__name__", repr(detector)),
            )
            continue
        out.extend(findings)
    return tuple(out)
