# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Snapshot + write + parse-check + rollback envelope.

Originally lived in :mod:`lamella.web.routes.setup` as a
private helper for recovery handlers (fix-duplicate-closes,
fix-orphan-overrides, etc.). Phase 4.1's vehicle rename writes
across three connector-owned files in one logical operation, and
both routes benefit from the same all-or-nothing snapshot model —
so the helper graduates to a shared util.

Why the parse-check uses :func:`beancount.loader.load_file` plus a
message-only set-diff rather than
:func:`lamella.features.receipts.linker.run_bean_check_vs_baseline`:
the latter keys each error on ``"<file>:<line>: <msg>"``. A line-
deleting edit shifts line numbers for every remaining error
downstream, so any identity-preserving dedupe/prune reads as "new
errors introduced" and fires a false rollback. ``load_file`` plus
a message-only diff asks "does the ledger still parse with the
same (or a subset of) fatal errors?" — which is the right question
when the goal is *make broken less broken, or roll back*.

On any new fatal error or write-level exception, every entry in
``files_to_snapshot`` is restored byte-for-byte (or unlinked if it
didn't exist pre-write) and a :class:`BeanCheckError` is raised so
the caller can redirect with a useful error code.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable


def recovery_write_envelope(
    *,
    main_bean: Path,
    files_to_snapshot: list[Path],
    write_fn: Callable[[], None],
) -> None:
    """Run ``write_fn`` under a snapshot+parse-check+rollback envelope.

    See module docstring for the rationale. Public name for shared
    use; ``routes.setup._recovery_write_envelope`` is a thin alias
    for backward-compat with existing call sites.
    """
    from beancount import loader
    from lamella.core.bootstrap.detection import _fatal_error_messages
    from lamella.core.ledger_writer import BeanCheckError

    snaps: list[tuple[Path, bytes | None]] = [
        (p, p.read_bytes() if p.exists() else None)
        for p in files_to_snapshot
    ]

    try:
        _entries, pre_errors, _opts = loader.load_file(str(main_bean))
    except Exception:  # noqa: BLE001
        pre_errors = []
    pre_fatal = set(_fatal_error_messages(pre_errors))

    def _restore() -> None:
        for path, pre_bytes in snaps:
            if pre_bytes is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(pre_bytes)

    try:
        write_fn()
    except Exception:
        _restore()
        raise

    try:
        _entries, post_errors, _opts = loader.load_file(str(main_bean))
    except Exception as exc:  # noqa: BLE001
        _restore()
        raise BeanCheckError(
            f"post-write load_file raised: {type(exc).__name__}: {exc}"
        ) from exc
    post_fatal = set(_fatal_error_messages(post_errors))
    new_fatal = post_fatal - pre_fatal
    if new_fatal:
        _restore()
        raise BeanCheckError("; ".join(sorted(new_fatal))[:300])
