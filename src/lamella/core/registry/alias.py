# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Humanize Beancount account paths for the UI.

Three entry points:

- ``alias_for(conn, path)`` — returns a single display string.
- ``account_label(conn, path)`` — returns (primary, secondary) tuple for
  two-line cards ("Amazon Prime Visa", "Chase · ****1234").
- ``format_money(amount, currency=None)`` — turns a Decimal-like value
  into "$247.83" for the UI. (Currency suffix reserved for non-USD.)

Lookup order for display strings:

1. ``accounts_meta.display_name`` if present.
2. Heuristic pretty-format of the raw path, with the entity segment
   swapped to ``entities.display_name`` when we have one.
3. Final fallback: raw path (should almost never happen in practice).
"""
from __future__ import annotations

import re
import sqlite3
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from typing import Any


_CAMEL_SPLIT_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_ROOTS_STRIPPED = ("Assets:", "Liabilities:", "Expenses:", "Income:", "Equity:")
_INSTITUTION_TAILS = ("BankOne", "Chase", "Citi", "AmericanExpress", "Mercury",
                      "BrokerageOne", "Schwab", "Bank", "Credit", "Visa", "Mastercard")


def _split_camel(segment: str) -> str:
    """PrimeChecking → "Prime Checking", 2009WorkSUV → "2009 Work SUV"."""
    if not segment:
        return segment
    # Insert a space before a digit that follows a letter (2009WorkSUV case).
    s = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", segment)
    # Insert a space before a capital letter that follows a lowercase/digit.
    s = _CAMEL_SPLIT_RE.sub(" ", s)
    return s.strip()


def _strip_root(path: str) -> str:
    for r in _ROOTS_STRIPPED:
        if path.startswith(r):
            return path[len(r):]
    return path


def _entity_display(conn: sqlite3.Connection, slug: str) -> str | None:
    if not slug:
        return None
    row = conn.execute(
        "SELECT display_name FROM entities WHERE slug = ?",
        (slug,),
    ).fetchone()
    if row is None:
        return None
    value = row["display_name"] if isinstance(row, sqlite3.Row) else row[0]
    return value if value else None


def entity_label(conn: sqlite3.Connection, slug: str | None) -> str:
    """Public: return the display_name for an entity slug, falling back
    to the slug itself when no display_name is set. Use anywhere a
    user-facing label needs the human name without forcing every
    caller to handle the None case."""
    if not slug:
        return ""
    return _entity_display(conn, slug) or slug


def _accounts_meta_display(conn: sqlite3.Connection, path: str) -> str | None:
    row = conn.execute(
        "SELECT display_name FROM accounts_meta WHERE account_path = ?",
        (path,),
    ).fetchone()
    if row is None:
        return None
    value = row["display_name"] if isinstance(row, sqlite3.Row) else row[0]
    return value if value else None


def _heuristic(conn: sqlite3.Connection, path: str) -> str:
    """Last-resort pretty-format for accounts not in accounts_meta.

    Example transformations:
      Assets:Personal:BankOne:PrimeChecking  →  "Personal · Prime Checking"
      Liabilities:Acme:BankOne:AffiliateD     →  "Acme · AffiliateD"
      Expenses:FIXME                            →  "Uncategorized"
      Expenses:Vehicles:2009WorkSUV:Fuel        →  "2009 Work SUV · Fuel"
    """
    if not path:
        return ""
    if path == "Expenses:FIXME" or path.endswith(":FIXME"):
        return "Uncategorized"

    stripped = _strip_root(path)
    parts = [p for p in stripped.split(":") if p]
    if not parts:
        return path

    # Entity segment → look up display name. Entity slugs are typically
    # proper nouns or abbreviations (Acme, WidgetCo) where camel-splitting
    # reads worse than the raw string; leave them intact in the fallback.
    # The user overrides with a nicer label via /settings/entities.
    entity_seg = parts[0]
    entity_pretty = _entity_display(conn, entity_seg) or entity_seg

    # Everything after the entity: drop institution names we consider
    # redundant ("BankOne" when we're going to suffix "Checking" etc.).
    remainder = [p for p in parts[1:] if p not in _INSTITUTION_TAILS]
    remainder_pretty = [_split_camel(p) for p in remainder]

    # Vehicles exception: Expenses:Vehicles:2009WorkSUV:Fuel
    # should render as "2009 Work SUV · Fuel", not "Vehicles · 2009 Work SUV · Fuel".
    if entity_seg == "Vehicles" and remainder_pretty:
        return " · ".join(remainder_pretty)

    if not remainder_pretty:
        return entity_pretty
    tail = " ".join(remainder_pretty)
    return f"{entity_pretty} · {tail}" if tail else entity_pretty


def alias_for(conn: sqlite3.Connection, path: str) -> str:
    """Return the human-readable display for an account path."""
    if not path:
        return ""
    explicit = _accounts_meta_display(conn, path)
    if explicit:
        return explicit
    return _heuristic(conn, path)


def account_label(conn: sqlite3.Connection, path: str) -> tuple[str, str]:
    """(primary, secondary) for two-line renders.

    Primary = display name; secondary = "{institution} · ****{last_four}"
    when known, else the heuristic context (e.g. the entity name).
    """
    if not path:
        return ("", "")
    row = conn.execute(
        """
        SELECT display_name, institution, last_four, entity_slug, kind
        FROM accounts_meta WHERE account_path = ?
        """,
        (path,),
    ).fetchone()
    if row is not None:
        primary = row["display_name"] or _heuristic(conn, path)
        parts: list[str] = []
        if row["institution"]:
            parts.append(row["institution"])
        if row["last_four"]:
            parts.append(f"****{row['last_four']}")
        if not parts and row["entity_slug"]:
            ent = _entity_display(conn, row["entity_slug"])
            if ent:
                parts.append(ent)
        secondary = " · ".join(parts)
        return (primary, secondary)
    # No meta row — derive everything from the heuristic.
    return (_heuristic(conn, path), "")


def format_money(value: Any, currency: str | None = None) -> str:
    """Render a monetary value for the UI. USD: '$247.83'. Non-USD:
    '1,240.00 EUR'. Invalid input returns the raw repr."""
    if value is None or value == "":
        return ""
    try:
        d = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return str(value)
    ccy = (currency or "USD").upper()
    sign = "-" if d < 0 else ""
    abs_d = abs(d)
    formatted = f"{abs_d:,.2f}"  # e.g. "1,234.56"
    if ccy == "USD":
        return f"{sign}${formatted}"
    return f"{sign}{formatted} {ccy}"
