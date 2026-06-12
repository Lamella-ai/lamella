# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""First-N-row preview + header detection for the import UI.

Handles the three supported formats: CSV, XLSX/XLS, ODS. Also handles the
bundle's known wart (CONVENTIONS §10): ODS files with a title on row 1
and real headers on row 2+.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class SheetPreview:
    sheet_name: str
    columns: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    row_count: int = 0
    header_row_index: int = 0
    notes: str | None = None


def _pd():
    import pandas as pd
    return pd


def _header_candidate(series: list[Any]) -> bool:
    """Heuristic: treat a row as a header if it has >= 2 non-empty string
    cells and no obvious date/amount cells."""
    pd = _pd()
    non_empty = 0
    for v in series:
        try:
            if pd.isna(v):
                continue
        except Exception:
            pass
        if v is None or v == "":
            continue
        s = str(v).strip()
        if not s:
            continue
        non_empty += 1
        # If it parses as a float or date, probably a data row, not a header.
        try:
            float(s.replace(",", "").replace("$", ""))
            return False
        except ValueError:
            pass
    return non_empty >= 2


def _find_header_row(df) -> int:
    """Scan the first 5 rows for the most header-shaped row. Returns 0
    if the first row is fine."""
    n = min(5, len(df.index))
    for i in range(n):
        if _header_candidate(list(df.iloc[i].tolist())):
            return i
    return 0


def list_sheets(path: Path) -> list[str]:
    ext = path.suffix.lower()
    if ext == ".csv":
        return ["(csv)"]
    pd = _pd()
    try:
        if ext == ".ods":
            xl = pd.ExcelFile(path, engine="odf")
        else:
            xl = pd.ExcelFile(path, engine="openpyxl")
        return list(xl.sheet_names)
    except Exception as exc:
        log.warning("list_sheets failed for %s: %s", path, exc)
        return []


def preview_sheet(
    path: Path, sheet_name: str | None, *, n_rows: int = 10
) -> SheetPreview:
    pd = _pd()
    ext = path.suffix.lower()
    if ext == ".csv":
        # Read the first N+10 rows raw (no header) so we can find the header.
        raw = pd.read_csv(path, header=None, nrows=n_rows + 10, dtype=str).fillna("")
    elif ext == ".ods":
        raw = pd.read_excel(
            path, sheet_name=sheet_name, engine="odf", header=None,
            nrows=n_rows + 10, dtype=str
        ).fillna("")
    else:
        raw = pd.read_excel(
            path, sheet_name=sheet_name, engine="openpyxl", header=None,
            nrows=n_rows + 10, dtype=str
        ).fillna("")

    header_idx = _find_header_row(raw)
    header_row = [str(x).strip() for x in raw.iloc[header_idx].tolist()]
    # Collapse trailing empty columns.
    while header_row and header_row[-1] == "":
        header_row.pop()
    if not header_row:
        header_row = [f"col_{i}" for i in range(len(raw.columns))]

    data_rows: list[list[Any]] = []
    for i in range(header_idx + 1, min(header_idx + 1 + n_rows, len(raw.index))):
        row = raw.iloc[i].tolist()[: len(header_row)]
        data_rows.append([str(c) if c is not None else "" for c in row])

    # Estimate total row count — quick for CSV/XLSX/ODS by loading with header.
    try:
        if ext == ".csv":
            full = pd.read_csv(path)
        elif ext == ".ods":
            full = pd.read_excel(path, sheet_name=sheet_name, engine="odf")
        else:
            full = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
        row_count = int(len(full.index))
    except Exception:
        row_count = len(data_rows)

    notes = None
    if header_idx > 0:
        notes = f"Title row on line(s) 1..{header_idx}; header detected at row {header_idx + 1}"

    return SheetPreview(
        sheet_name=sheet_name or "(csv)",
        columns=header_row,
        rows=data_rows,
        row_count=row_count,
        header_row_index=header_idx,
        notes=notes,
    )


def preview_workbook(path: Path, *, n_rows: int = 10) -> list[SheetPreview]:
    previews: list[SheetPreview] = []
    sheets = list_sheets(path)
    for name in sheets:
        sn = name if name != "(csv)" else None
        try:
            previews.append(preview_sheet(path, sn, n_rows=n_rows))
        except Exception as exc:
            log.warning("preview failed for %s!%s: %s", path, name, exc)
            previews.append(
                SheetPreview(sheet_name=name, notes=f"preview failed: {exc}")
            )
    return previews
