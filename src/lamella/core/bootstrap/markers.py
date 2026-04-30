# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Removal marker writer and parser for Connector-owned ledger comments.

Per ``docs/specs/LEDGER_LAYOUT.md`` §7.4, every tool that removes a
directive from a ledger file by commenting it out uses
``format_removal_marker`` here. Every tool that reads markers back
(the editor's "un-comment this removal" action, forensic tools)
uses ``parse_removal_marker``. No other module hand-assembles or
regex-parses the marker text.

Format::

    ; [lamella-removed YYYY-MM-DD reason=<reason> tool=<tool>]
    ; <original line verbatim>

Reason and tool tokens must match ``[a-z][a-z0-9-]*`` to keep
markers greppable without regex quoting.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date as _date

__all__ = ["MarkerInfo", "format_removal_marker", "parse_removal_marker"]

_TOKEN_RE = re.compile(r"^[a-z][a-z0-9-]*$")

_MARKER_LINE_RE = re.compile(
    r"^;\s*\[lamella-removed\s+"
    r"(?P<date>\d{4}-\d{2}-\d{2})\s+"
    r"reason=(?P<reason>[a-z][a-z0-9-]*)\s+"
    r"tool=(?P<tool>[a-z][a-z0-9-]*)\]"
)


@dataclass(frozen=True)
class MarkerInfo:
    """One parsed removal marker block."""

    date: _date
    reason: str
    tool: str
    original_line: str


def _validate_token(value: str, label: str) -> None:
    if not _TOKEN_RE.match(value):
        raise ValueError(
            f"{label} must match [a-z][a-z0-9-]*, got {value!r}"
        )


def format_removal_marker(
    original_line: str,
    *,
    reason: str,
    tool: str,
    on: _date | None = None,
) -> str:
    """Return the two-line comment block for a removed directive.

    The block is two `;`-prefixed lines, each ending in a newline.
    Callers append this to a ledger file at the position the
    original directive used to occupy.

    ``on`` defaults to today's date. Supplying it is required for
    deterministic tests and fixture generation.
    """
    _validate_token(reason, "reason")
    _validate_token(tool, "tool")
    when = on or _date.today()
    data_line = original_line.rstrip("\n")
    return (
        f"; [lamella-removed {when.isoformat()} reason={reason} tool={tool}]\n"
        f"; {data_line}\n"
    )


def parse_removal_marker(text: str) -> list[MarkerInfo]:
    """Scan ``text`` for removal markers and return one info per match.

    A marker is two adjacent lines: a marker header matching the
    regex, immediately followed by a ``; ``-prefixed line holding
    the verbatim original content. Markers whose header is at EOF,
    or whose next line is not a ``; `` comment, are treated as
    malformed and silently skipped (forensic-friendly: a partial
    marker from a mid-edit shouldn't throw).
    """
    results: list[MarkerInfo] = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        header = _MARKER_LINE_RE.match(lines[i])
        if not header or i + 1 >= len(lines):
            i += 1
            continue
        next_line = lines[i + 1]
        if not next_line.startswith("; "):
            i += 1
            continue
        results.append(
            MarkerInfo(
                date=_date.fromisoformat(header["date"]),
                reason=header["reason"],
                tool=header["tool"],
                original_line=next_line[2:],
            )
        )
        i += 2
    return results
