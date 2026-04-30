# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Shared pandas-parsing helpers used by every per-source ingester.

All the NaN / NaT / pd.isna tap-dancing lives here so the per-source
ingesters stay one page each.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


def _pd():
    import pandas as pd  # local import so pandas isn't imported at app boot
    return pd


def parse_date(val: Any) -> str | None:
    pd = _pd()
    if val is None or val == "":
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    try:
        ts = pd.to_datetime(val, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date().isoformat()
    except Exception:
        return None


def safe_decimal(val: Any) -> Decimal | None:
    """Parse a money value from a CSV/XLSX cell into Decimal.

    ADR-0022: money MUST be Decimal, not float. We construct Decimals
    from the *string* form of numeric inputs to avoid the float-binary
    representation drift that would otherwise contaminate the value
    on entry into the system.
    """
    pd = _pd()
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    if isinstance(val, Decimal):
        return val
    if isinstance(val, (int, float)):
        try:
            return Decimal(str(val))
        except (InvalidOperation, ValueError):
            return None
    s = str(val).strip().replace("$", "").replace(",", "").replace("\u00a0", "")
    if not s or s.lower() in ("nan", "none", "-", "--", "---"):
        return None
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def clean_str(val: Any) -> str | None:
    pd = _pd()
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    s = str(val).strip()
    return s if s else None


def row_to_raw(row: dict) -> dict:
    pd = _pd()
    out: dict[str, Any] = {}
    for k, v in row.items():
        try:
            if pd.isna(v):
                out[k] = None
                continue
        except Exception:
            pass
        out[k] = v
    return out


def read_tabular(path: Path, sheet_name: str | None):
    """Read `path` (csv/xls/xlsx/ods) into a DataFrame. For csv, `sheet_name`
    is ignored and used only as a label by the caller."""
    pd = _pd()
    ext = path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path)
    if ext == ".ods":
        return pd.read_excel(path, sheet_name=sheet_name, engine="odf", header=0)
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl", header=0)
