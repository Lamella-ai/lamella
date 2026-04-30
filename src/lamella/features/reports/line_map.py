# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class LineMapEntry:
    line: int | str
    description: str
    account_patterns: tuple[str, ...]
    _compiled: tuple[re.Pattern[str], ...] = field(default=(), repr=False, compare=False)

    def matches(self, account: str) -> bool:
        for rx in self._compiled:
            if rx.match(account):
                return True
        return False


@dataclass(frozen=True)
class LineMap:
    entries: tuple[LineMapEntry, ...]

    def classify(self, account: str) -> LineMapEntry | None:
        for entry in self.entries:
            if entry.matches(account):
                return entry
        return None


def _compile(patterns: list[str]) -> tuple[re.Pattern[str], ...]:
    return tuple(re.compile(p) for p in patterns)


def load_line_map(path: Path) -> LineMap:
    if not path.exists():
        raise FileNotFoundError(f"line map not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or []
    if not isinstance(raw, list):
        raise ValueError(f"{path}: expected a top-level list of lines")
    entries: list[LineMapEntry] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"{path}: entry {idx} is not a mapping")
        line = item.get("line")
        description = item.get("description") or ""
        patterns = item.get("account_patterns") or []
        if line is None:
            raise ValueError(f"{path}: entry {idx} missing `line`")
        if not isinstance(patterns, list):
            raise ValueError(f"{path}: entry {idx} `account_patterns` must be a list")
        entries.append(
            LineMapEntry(
                line=line,
                description=description,
                account_patterns=tuple(patterns),
                _compiled=_compile([str(p) for p in patterns]),
            )
        )
    return LineMap(entries=tuple(entries))
