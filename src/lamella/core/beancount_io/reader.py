# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Iterator

from beancount import loader
from beancount.core.data import Transaction

from lamella.utils._legacy_meta import normalize_entries

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadedLedger:
    entries: list
    errors: list
    options: dict
    mtime_signature: tuple[tuple[str, float], ...]

    def transactions(self) -> Iterator[Transaction]:
        for entry in self.entries:
            if isinstance(entry, Transaction):
                yield entry


class LedgerReader:
    """Cache Beancount parse output keyed by the mtime signature of main.bean
    and every file it includes. Re-loads only when something changed on disk.
    """

    def __init__(self, main_bean: Path):
        self.main_bean = main_bean
        self._lock = Lock()
        self._cached: LoadedLedger | None = None

    def _current_signature(self, filenames: list[str] | None = None) -> tuple[tuple[str, float], ...]:
        files: list[str]
        if filenames:
            files = sorted(filenames)
        else:
            files = [str(self.main_bean)]
        sig: list[tuple[str, float]] = []
        for name in files:
            try:
                sig.append((name, Path(name).stat().st_mtime))
            except FileNotFoundError:
                sig.append((name, 0.0))
        return tuple(sig)

    def load(self, *, force: bool = False) -> LoadedLedger:
        with self._lock:
            if not force and self._cached is not None:
                if self._current_signature([n for n, _ in self._cached.mtime_signature]) \
                        == self._cached.mtime_signature:
                    return self._cached
            if not self.main_bean.exists():
                empty = LoadedLedger(entries=[], errors=[], options={}, mtime_signature=())
                self._cached = empty
                return empty
            entries, errors, options = loader.load_file(str(self.main_bean))
            # Rewrite legacy bcg-* metadata, tags, and Custom directive
            # types to the lamella-* prefix in memory so every downstream
            # consumer can stay single-namespace. Cheap walk; runs only
            # when load_file actually fired (mtime changed).
            entries = normalize_entries(entries)
            included = options.get("include", []) or []
            files = [str(self.main_bean), *[str(p) for p in included]]
            sig = self._current_signature(files)
            self._cached = LoadedLedger(
                entries=entries, errors=errors, options=options, mtime_signature=sig
            )
            real_errors = _filter_informational(errors)
            if real_errors:
                log.warning(
                    "Beancount returned %d errors when loading %s", len(real_errors), self.main_bean
                )
                for msg in real_errors[:5]:
                    log.warning("  %s", msg[:300])
            return self._cached

    def iter_txns(self) -> Iterator[Transaction]:
        return self.load().transactions()

    def invalidate(self) -> None:
        with self._lock:
            self._cached = None


def _filter_informational(errors: list) -> list[str]:
    # Mirrors bootstrap.detection._fatal_error_messages — the
    # auto_accounts plugin emits "Auto-inserted Open directives for N
    # accounts" as an error with a <auto_insert_open> pseudo-source,
    # which is informational, not a parse failure.
    out: list[str] = []
    for e in errors:
        msg = getattr(e, "message", str(e))
        if "Auto-inserted" in msg:
            continue
        source = getattr(e, "source", None)
        filename = source.get("filename", "") if isinstance(source, dict) else ""
        if isinstance(filename, str) and filename.startswith("<"):
            continue
        out.append(msg)
    return out
