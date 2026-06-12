# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Shared helper for appending `custom "…"` directives to Connector-owned
files.

Steps 1-6 of the reconstruct roadmap all emit custom directives; this
module gives them a common writer with snapshot / bean-check / rollback
so each subsystem's code stays focused on its own semantics.

Value encoding: Beancount's metadata type system maps to these Python
types via the ``MetaValue`` alias below. Booleans become the bare
tokens ``TRUE`` / ``FALSE`` (not quoted strings — a common foot-gun);
strings are double-quoted with backslash escape; decimals/amounts are
emitted as bare numbers with an optional currency suffix; dates use
ISO-8601 bare; accounts are emitted as bare account paths (no quotes).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Union

from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    ensure_include_in_main,
    run_bean_check_vs_baseline,
)

log = logging.getLogger(__name__)


# Values encodable in a directive argument list or metadata value.
# The tuple `(Decimal, str)` means "this decimal should be rendered
# with a currency suffix" — e.g. an amount field in a budget directive.
MetaValue = Union[
    str,
    bool,
    int,
    float,
    Decimal,
    date,
    datetime,
    "Account",
    "Amount",
    None,
]


@dataclass(frozen=True)
class Account:
    """Wrap a colon-separated path so the renderer emits it bare
    (no quotes). Beancount parses bare accounts as an Account value,
    not a string."""

    path: str

    def __str__(self) -> str:
        return self.path


@dataclass(frozen=True)
class Amount:
    """A decimal paired with a currency. Renders as ``12.34 USD``."""

    value: Decimal
    currency: str = "USD"

    def __str__(self) -> str:
        return f"{Decimal(self.value):.2f} {self.currency}"


def _render_arg(value: MetaValue) -> str:
    """Render a value for use in the argument list of a custom
    directive header (`custom "type" <arg1> <arg2> ...`)."""
    if value is None:
        return '""'
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, Account):
        return value.path
    if isinstance(value, Amount):
        return str(value)
    if isinstance(value, Decimal):
        return f"{value}"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return f'"{value.isoformat()}"'
    if isinstance(value, date):
        return value.isoformat()
    # Fall through to quoted string.
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def _render_meta_value(value: MetaValue) -> str:
    """Same rules as argument rendering, except strings and
    datetimes are always quoted (even if the caller passed a raw
    timestamp) so the resulting line is unambiguous to Beancount's
    parser."""
    if value is None:
        return '""'
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, Account):
        return value.path
    if isinstance(value, Amount):
        return str(value)
    if isinstance(value, Decimal):
        return f"{value}"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return f'"{value.isoformat()}"'
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def render_directive(
    *,
    directive_date: date,
    directive_type: str,
    args: Iterable[MetaValue] = (),
    meta: dict[str, MetaValue] | None = None,
) -> str:
    """Pure text renderer. No I/O. Used for testing without needing
    a ledger on disk."""
    arg_parts = [_render_arg(a) for a in args]
    header = f"\n{directive_date.isoformat()} custom \"{directive_type}\""
    if arg_parts:
        header += " " + " ".join(arg_parts)
    lines = [header]
    if meta:
        for key, value in meta.items():
            # Key validation: lamella-* (or a few known-safe unprefixed
            # keys we already emit). Enforce the namespace rule at
            # write time.
            if not key.startswith("lamella-"):
                raise ValueError(
                    f"metadata key {key!r} must be lamella-* prefixed "
                    f"(directive {directive_type!r})"
                )
            lines.append(f"  {key}: {_render_meta_value(value)}")
    return "\n".join(lines) + "\n"


def ensure_file_with_header(path: Path, header: str) -> None:
    """Create the file with the given header if missing. Header is
    typically a ``;;``-comment identifying the file as Connector-owned."""
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header, encoding="utf-8")


def append_custom_directive(
    *,
    target: Path,
    main_bean: Path,
    header: str,
    directive_date: date,
    directive_type: str,
    args: Iterable[MetaValue] = (),
    meta: dict[str, MetaValue] | None = None,
    run_check: bool = True,
) -> str:
    """Append a single ``custom "…"`` directive to ``target``, with
    the usual Connector-owned-file discipline: ensure the file exists
    with its header, ensure ``main.bean`` includes it, snapshot both
    files, append, then bean-check (tolerant of pre-existing errors).
    On any new ledger error, restore both files. Returns the rendered
    block (useful for logging + tests).

    ``args`` is the directive's positional arguments; ``meta`` is the
    indented metadata continuation. Both go through the same value
    renderer, so ``Amount(Decimal("14.99"), "USD")`` and
    ``Account("Expenses:Personal:Food")`` produce bare Beancount
    values, while plain strings get quoted.

    All metadata keys must be prefixed ``lamella-*``. Write-time guard so
    the namespace rule can't rot.
    """
    if not main_bean.exists():
        raise FileNotFoundError(f"main.bean not found at {main_bean}")

    backup_main = main_bean.read_bytes()
    target_existed = target.exists()
    backup_target = target.read_bytes() if target_existed else None

    baseline = ""
    if run_check:
        _, baseline = capture_bean_check(main_bean)

    ensure_file_with_header(target, header)
    ensure_include_in_main(main_bean, target)

    block = render_directive(
        directive_date=directive_date,
        directive_type=directive_type,
        args=args,
        meta=meta,
    )
    with target.open("a", encoding="utf-8") as fh:
        fh.write(block)

    if run_check:
        try:
            run_bean_check_vs_baseline(main_bean, baseline)
        except BeanCheckError:
            main_bean.write_bytes(backup_main)
            if backup_target is None:
                target.unlink(missing_ok=True)
            else:
                target.write_bytes(backup_target)
            raise

    return block


def read_custom_directives(
    entries: Iterable[Any], directive_type: str
) -> list[Any]:
    """Filter loaded Beancount entries down to ``Custom`` entries of
    the given type. Returned in load order (deterministic per parse)."""
    from beancount.core.data import Custom

    out: list[Any] = []
    for entry in entries:
        if isinstance(entry, Custom) and entry.type == directive_type:
            out.append(entry)
    return out


def custom_arg(entry: Any, index: int) -> Any:
    """Extract the ``index``-th positional argument value from a
    ``Custom`` entry. Returns ``None`` when the arg is absent. Handles
    both the ``(value, dtype)`` tuple form and the plain-value form
    depending on beancount version."""
    try:
        values = entry.values
    except AttributeError:
        return None
    if index >= len(values):
        return None
    item = values[index]
    if hasattr(item, "value"):
        return item.value
    return item


def custom_meta(entry: Any, key: str, default: Any = None) -> Any:
    """Look up a metadata key on a Custom entry, returning ``default``
    if absent. Beancount stores metadata as a plain dict on each entry;
    we go through getattr to stay tolerant of library changes."""
    meta = getattr(entry, "meta", None) or {}
    if key in meta:
        return meta[key]
    return default
