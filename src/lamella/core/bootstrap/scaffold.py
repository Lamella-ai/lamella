# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Scaffold a fresh canonical ledger at a given directory path.

Implements the "Start fresh" flow described in
``docs/specs/LEDGER_LAYOUT.md`` §8.3. Creates the twelve canonical
supporting files, then ``main.bean``, stamps the schema version,
optionally runs bean-check, and rolls back on any failure.
Refuses if any canonical file already exists at the target path
(the user should use the Import flow instead).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date as _date
from pathlib import Path
from typing import Callable

from .templates import (
    CANONICAL_FILES,
    render_connector_header,
    render_main_bean,
    render_user_header,
)

__all__ = [
    "ScaffoldError",
    "ScaffoldResult",
    "scaffold_fresh",
]

_LOG = logging.getLogger(__name__)

BeanCheck = Callable[[Path], list[str]]


class ScaffoldError(Exception):
    """The scaffolder refused or failed. The message is user-safe."""


@dataclass(frozen=True)
class ScaffoldResult:
    """Return value of a successful scaffold."""

    ledger_dir: Path
    created: tuple[Path, ...]


def _render_supporting_file(name: str, owner: str, *, on: _date | None) -> str:
    if owner == "user":
        return render_user_header(name, on=on)
    if owner == "lamella":
        return render_connector_header(name, on=on)
    raise ValueError(f"unknown owner {owner!r} for file {name}")


def scaffold_fresh(
    ledger_dir: Path,
    *,
    on: _date | None = None,
    bean_check: BeanCheck | None = None,
) -> ScaffoldResult:
    """Create a fresh canonical ledger inside ``ledger_dir``.

    ``ledger_dir`` must already exist (we don't create the root
    directory ourselves — mount responsibility). The scaffolder
    creates 13 files inside it: main.bean plus the twelve files
    enumerated in ``templates.CANONICAL_FILES``.

    Refuses if any target file already exists. The user's path
    forward in that case is the Import flow.

    If ``bean_check`` is supplied, it is invoked with the path to
    the new ``main.bean`` after all files are written. It must
    return a list of error strings; a non-empty list aborts and
    rolls back. Unit tests can omit it; production callers should
    pass a bean-check runner that enforces the invariant from
    CLAUDE.md that ledgers pass bean-check after every write.
    """
    if not ledger_dir.is_dir():
        raise ScaffoldError(
            f"ledger directory does not exist: {ledger_dir}"
        )

    target_names: list[str] = ["main.bean"] + [f.name for f in CANONICAL_FILES]
    existing = sorted(n for n in target_names if (ledger_dir / n).exists())
    if existing:
        raise ScaffoldError(
            "refused to scaffold: one or more canonical files already "
            f"exist at {ledger_dir}: {', '.join(existing)}. "
            "Use the Import flow instead."
        )

    created: list[Path] = []
    try:
        # Supporting files first so `include` targets exist on disk
        # before main.bean references them.
        for cfile in CANONICAL_FILES:
            path = ledger_dir / cfile.name
            path.write_text(
                _render_supporting_file(cfile.name, cfile.owner, on=on),
                encoding="utf-8",
            )
            created.append(path)

        main_path = ledger_dir / "main.bean"
        main_path.write_text(render_main_bean(on=on), encoding="utf-8")
        created.append(main_path)

        if bean_check is not None:
            errors = bean_check(main_path)
            if errors:
                detail = "; ".join(errors[:3])
                if len(errors) > 3:
                    detail += f" ... ({len(errors) - 3} more)"
                raise ScaffoldError(
                    "bean-check reported errors on a fresh scaffold "
                    f"(scaffolder template bug): {detail}"
                )
    except Exception:
        for path in created:
            try:
                path.unlink()
            except OSError:
                pass
        raise

    _LOG.info(
        "scaffolded fresh ledger at %s (%d files)",
        ledger_dir,
        len(created),
    )
    return ScaffoldResult(ledger_dir=ledger_dir, created=tuple(created))
