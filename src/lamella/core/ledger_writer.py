# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Generic ledger-writer helpers — bean-check + connector_links.bean.

Extracted from the historic ``lamella.features.receipts.linker`` module in Phase 8b
subgroup 2h. The exports here are not receipts-specific; they are
cross-cutting infrastructure consumed by ~75 import sites across loans,
registry, settings, rules, mileage, vehicles, properties, routes, and
the receipts feature itself.

The receipts-specific :class:`DocumentLinker` remains in
``lamella.features.receipts.linker`` (will move to ``features/receipts/`` in 5e).
"""

from __future__ import annotations

import logging
import re
import subprocess
from pathlib import Path
from typing import Iterable

from lamella.core.fs import validate_safe_path

log = logging.getLogger(__name__)


CONNECTOR_LINKS_HEADER = "; Managed by Lamella. Do not hand-edit.\n"


class WriteError(RuntimeError):
    pass


class BeanCheckError(RuntimeError):
    pass


def _include_line_for(path: Path) -> str:
    return f'include "{path.name}"\n'


def ensure_include_in_main(
    main_bean: Path,
    connector_links: Path,
    *,
    allowed_roots: Iterable[Path] | None = None,
) -> bool:
    """Append an include directive for `connector_links` to `main_bean` if it's
    not already present. Returns True if the file was modified.

    ``allowed_roots`` is forwarded to :func:`lamella.core.fs.validate_safe_path`
    before the write. When omitted the parent directory of ``main_bean`` is used
    as the sole allowed root, which is correct for every caller that derives
    ``main_bean`` from ``settings.ledger_dir``.
    """
    if not main_bean.exists():
        raise WriteError(f"main.bean not found at {main_bean}")
    existing = main_bean.read_text(encoding="utf-8")
    needle = f'include "{connector_links.name}"'
    if needle in existing:
        return False
    roots = list(allowed_roots) if allowed_roots is not None else [main_bean.parent]
    safe_main = validate_safe_path(main_bean, allowed_roots=roots)
    suffix = "" if existing.endswith("\n") else "\n"
    addition = f'{suffix}\n; Added by Lamella\n{needle}\n'
    safe_main.write_text(existing + addition, encoding="utf-8")
    return True


def ensure_connector_links_exists(
    connector_links: Path,
    *,
    allowed_roots: Iterable[Path] | None = None,
) -> None:
    """Create ``connector_links`` with the managed-file header if absent.

    ``allowed_roots`` is forwarded to :func:`lamella.core.fs.validate_safe_path`
    before the write. When omitted the parent directory of ``connector_links``
    is used as the sole allowed root, which is correct for every caller that
    derives the path from ``settings.ledger_dir``.
    """
    if connector_links.exists():
        return
    roots = list(allowed_roots) if allowed_roots is not None else [connector_links.parent]
    safe_cl = validate_safe_path(connector_links, allowed_roots=roots)
    safe_cl.parent.mkdir(parents=True, exist_ok=True)
    safe_cl.write_text(CONNECTOR_LINKS_HEADER, encoding="utf-8")


def run_bean_check(main_bean: Path) -> None:
    """Strict bean-check. Raises on any non-zero exit. Used when we have
    no baseline to compare against (rare — prefer the tolerant variants)."""
    try:
        result = subprocess.run(
            ["bean-check", str(main_bean)],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except FileNotFoundError:
        log.warning("bean-check not on PATH; skipping validation")
        return
    except subprocess.TimeoutExpired as exc:
        raise BeanCheckError("bean-check timed out") from exc
    if result.returncode != 0:
        raise BeanCheckError(result.stderr or result.stdout or "bean-check failed")


def capture_bean_check(main_bean: Path) -> tuple[int, str]:
    """Run bean-check and return (return_code, combined_output). Never
    raises. Used as a baseline snapshot."""
    try:
        result = subprocess.run(
            ["bean-check", str(main_bean)],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except FileNotFoundError:
        log.warning("bean-check not on PATH; skipping validation")
        return 0, ""
    except subprocess.TimeoutExpired:
        return 1, "bean-check timed out"
    return result.returncode, (result.stderr or "") + (result.stdout or "")


_AUTO_OPEN_COUNT_RE = re.compile(
    r"Auto-inserted Open directives for \d+ accounts?:"
)


def _normalize_error_line(s: str) -> str:
    # Strip volatile counts out of plugin chatter so baseline comparison
    # stays stable when the count ticks up by one (e.g., our override
    # introduces one new auto-inserted account).
    return _AUTO_OPEN_COUNT_RE.sub(
        "Auto-inserted Open directives for N accounts:", s
    )


def _error_lines(output: str) -> set[str]:
    """Parse bean-check output into a set of distinct error lines.
    Each error is roughly a line starting with `<…>:N:` or a filename
    prefix. Blank-line and plugin-info lines get normalized so that
    pre-existing plugin chatter doesn't count as "new error"."""
    lines: set[str] = set()
    for raw in (output or "").splitlines():
        stripped = raw.strip()
        if not stripped:
            continue
        # Drop purely indented continuation lines (tracebacks, lists).
        if raw.startswith("  ") or raw.startswith("\t") or raw.startswith("- "):
            continue
        # Python logging chatter emitted to stderr by beancount's loader
        # (e.g. picklecache invalidation warnings) isn't a bean-check
        # error; it only appears on some writes (once the .picklecache
        # file exists), which makes the baseline comparison report it
        # as a "new error" on every write. Skip those.
        if stripped.startswith(("WARNING:", "INFO:", "DEBUG:")):
            continue
        lines.add(_normalize_error_line(stripped))
    return lines


def run_bean_check_vs_baseline(
    main_bean: Path, baseline_output: str
) -> None:
    """Run bean-check now and raise ONLY if new error lines appeared
    relative to `baseline_output`. Pre-existing errors (the user's
    ledger already had them before we touched anything) are tolerated.
    """
    rc, current_output = capture_bean_check(main_bean)
    if rc == 0:
        return
    base_lines = _error_lines(baseline_output)
    curr_lines = _error_lines(current_output)
    new_errors = curr_lines - base_lines
    if not new_errors:
        # Same errors as before our write — no regression.
        log.info("bean-check non-zero but no new errors (pre-existing: %d)", len(base_lines))
        return
    raise BeanCheckError("\n".join(sorted(new_errors)))
