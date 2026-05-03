# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from typing import Iterable

from lamella.features.reports.line_map import LineMap
from lamella.features.reports.schedule_c import ReportData, build_report


def build_schedule_f(
    *, entity: str, year: int, entries: Iterable, line_map: LineMap
) -> ReportData:
    # Schedule F is the same aggregation over a different line map — no
    # separate math path.
    return build_report(entity=entity, year=year, entries=entries, line_map=line_map)
