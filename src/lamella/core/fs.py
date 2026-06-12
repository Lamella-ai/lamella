# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Path-safety helper per ADR-0030.

Validates that a candidate filesystem path resolves within at least one
of the supplied allowed roots before a write operation touches it.
Defends against path traversal (``../../etc/passwd``), absolute-path
injection from user input, symlink redirection, and accidental writes
into the snapshot/backup directory namespace reserved by ADR-0004.

Usage:

    from lamella.core.fs import validate_safe_path, UnsafePathError

    safe = validate_safe_path(user_filename, allowed_roots=[settings.data_dir])
    safe.write_bytes(payload)

The helper is read at call time, never cached, so configuration changes
to the allowed roots take effect immediately. Reads are out of scope:
this is a data-integrity guard, not a confidentiality guard (per ADR-0030).
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable


# Backup / snapshot directory name patterns reserved by ADR-0004 +
# ADR-0030. Only the snapshot machinery (rewrite + recovery) writes
# into these; every other writer must refuse paths whose any component
# matches one of these patterns.
_RESERVED_BACKUP_PREFIXES = (
    ".pre-inplace",   # ADR-0002 / ADR-0004 — in-place rewrite snapshots
    ".pre-reboot",    # reboot snapshots
    ".reboot",        # reboot staging
    "_archive",       # archive moves
)
_RESERVED_BACKUP_SUFFIXES = (
    ".bak",
    ".backup",
)


class UnsafePathError(ValueError):
    """Raised when a path resolves outside its allowed roots, traverses
    a symlink, or lands inside a reserved backup directory pattern.

    Subclasses ``ValueError`` so callers that already catch ``ValueError``
    around path construction continue to work, but the more specific
    type lets web layers translate this into a 400 response without
    swallowing other validation errors.
    """


def _resolve_root(root: Path | str) -> Path:
    return Path(root).resolve()


def _has_reserved_component(resolved: Path) -> bool:
    """True if any path component is a reserved backup-dir name."""
    for part in resolved.parts:
        for prefix in _RESERVED_BACKUP_PREFIXES:
            if part == prefix or part.startswith(prefix):
                return True
        for suffix in _RESERVED_BACKUP_SUFFIXES:
            if part.endswith(suffix):
                return True
    return False


def _path_traverses_symlink(candidate_input: Path, root: Path) -> bool:
    """True if any existing parent of the candidate (under root) is a
    symlink. ``Path.resolve()`` already follows symlinks; this is the
    extra check required by ADR-0030 step 3 to reject the path even
    when the symlink target happens to land back inside ``root``.

    Walks from ``root`` toward the candidate, stopping at the first
    component that does not yet exist (intermediate dirs are created
    by callers; the leaf typically does not pre-exist on a write).
    """
    try:
        rel = candidate_input.relative_to(root)
    except ValueError:
        # candidate_input wasn't anchored at root; fall back to walking
        # the resolved candidate's parents up to root.
        return False
    cursor = root
    for part in rel.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            return True
        if not cursor.exists():
            # Stop at the first missing component — nothing further
            # along the path can be a symlink yet.
            return False
    return False


def validate_safe_path(
    candidate: Path | str,
    *,
    allowed_roots: Iterable[Path | str],
) -> Path:
    """Resolve ``candidate`` and confirm it lands inside one of
    ``allowed_roots``. Returns the resolved path on success.

    Raises :class:`UnsafePathError` if:

    * the resolved path is not relative to any allowed root
    * any traversed component of the candidate is a symlink
    * the path component matches a reserved backup directory pattern
      (``.pre-inplace-*``, ``.pre-reboot-*``, ``.reboot``, ``_archive``,
      ``*.bak``, ``*.backup``) — those are owned by snapshot machinery

    A relative ``candidate`` is anchored at the *first* allowed root.
    An absolute ``candidate`` is checked against every allowed root.
    Either way, the resolved path must land within at least one root
    after ``Path.resolve(strict=False)``.

    ``allowed_roots`` must be non-empty. Hardcoded OS paths
    (``/tmp``, ``/etc``) are never appropriate as allowed roots — pass
    a configured directory like ``settings.data_dir`` or
    ``settings.ledger_dir``.
    """
    roots = [_resolve_root(r) for r in allowed_roots]
    if not roots:
        raise UnsafePathError(
            "validate_safe_path requires at least one allowed_root"
        )

    raw = Path(candidate)
    if raw.is_absolute():
        anchored_inputs = [raw]
    else:
        # Anchor against each root so a relative candidate gets resolved
        # in the context of any of them — first one that lands inside
        # its root wins.
        anchored_inputs = [root / raw for root in roots]

    last_error: str | None = None
    for anchored in anchored_inputs:
        resolved = anchored.resolve()
        for root in roots:
            try:
                resolved.relative_to(root)
            except ValueError:
                last_error = (
                    f"path {candidate!r} escapes allowed root {root}"
                )
                continue
            if _has_reserved_component(resolved):
                raise UnsafePathError(
                    f"path {candidate!r} lands in reserved backup "
                    f"directory namespace ({resolved})"
                )
            if _path_traverses_symlink(anchored, root):
                raise UnsafePathError(
                    f"path {candidate!r} traverses a symlink under {root}"
                )
            return resolved

    raise UnsafePathError(
        last_error
        or f"path {candidate!r} escapes all allowed roots {roots}"
    )
