# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Migration interface — base class for recovery-shell migrations.

Phase 5.2 of SETUP_IMPLEMENTATION.md. Each subclass wraps one
existing transform module (or one SQLite SQL file). The recovery
surface dispatches to a concrete Migration via
:data:`MIGRATION_REGISTRY` based on the Finding emitted by
``detect_schema_drift``.

Design notes
------------

**Migrations don't own the snapshot envelope.** ``apply()`` is plain
Python that does the work; the heal action wraps the call inside
``with_bean_snapshot`` so the ordering (snapshot → SQLite tx →
apply → bean-check → commit/rollback) is owned by the recovery
layer. This keeps Migration subclasses small and testable in
isolation — a unit test can call ``apply()`` against a temp ledger
without standing up the snapshot infrastructure.

**Migration is an instance, not a class.** The registry stores
one concrete instance per axis. ``find_for_finding(finding)``
dispatches by axis (the SQLite axis catches up to head regardless
of how far behind; the ledger axis maps from/to versions to a
specific migration). That keeps the registry small for the
common case and lets future axes plug in without churn.

**Failure semantics — Shape 1 (sequential atomic units).** Per the
locked Phase 5 spec: each Migration is its own atomic unit. The
heal action commits each Migration before starting the next.
Mid-batch failure leaves prior migrations applied and never starts
later ones. The user sees "migration A ✓ applied, migration B ✗
failed: <reason>, migrations C+ not attempted" and clicks Resume
after fixing whatever broke B.
"""
from __future__ import annotations

import abc
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lamella.core.config import Settings


__all__ = [
    "DryRunKind",
    "DryRunResult",
    "Migration",
    "register_migration",
]


# --- registry decorator (gap §11.5) -----------------------------------------
#
# The recovery shell historically wired Migration subclasses into a
# hand-edited dict in ``migrations/__init__.py``. A new subclass added
# without that wiring step was silently dead: the file imported, the
# class compiled, but ``find_for_finding`` would never return it. The
# ``@register_migration`` decorator + auto-discovery scan in
# ``migrations/__init__.py`` removes the manual step — every Migration
# subclass declares its registration adjacent to the class body, and
# the package import eagerly walks every module to populate the
# axis-keyed registries.
#
# The decorator stores its arguments on the class itself
# (``__lamella_migration_keys__``) rather than mutating a global. The
# auto-discovery scanner reads the attribute when sweeping subclasses,
# which keeps the decorator a pure marker — calling it has no side
# effects on import order, no implicit registry singletons, no
# circular-import hazard between the registry and the subclasses it
# would otherwise hold references to.
#
# Keys shape:
#
# - ``axis="sqlite"``: keys is empty / None. There's exactly one
#   sqlite-axis migration; the registry stores it under a sentinel.
# - ``axis="ledger"``: keys is a tuple of ``(from_version, to_version)``
#   tuples. Each key resolves the same singleton instance (the
#   detector renders ``from_version`` as either ``"0"`` or ``"none"``;
#   we register under both shapes so dispatch works regardless of
#   detector branch).

# Sentinel for sqlite-axis registration (no per-version keys).
_SQLITE_REGISTRATION_SENTINEL = object()


def register_migration(
    *,
    keys: tuple[tuple[str, int], ...] | None = None,
):
    """Mark a :class:`Migration` subclass for auto-discovery.

    Usage::

        @register_migration()                                # sqlite axis
        class CatchUpSqliteMigrations(Migration):
            AXIS = "sqlite"
            ...

        @register_migration(keys=(("none", 1), ("0", 1)))    # ledger axis
        class MigrateLedgerV0ToV1(Migration):
            AXIS = "ledger"
            ...

    The decorator is a pure marker — it stamps
    ``__lamella_migration_keys__`` on the class and returns the class
    unchanged. The auto-discovery scanner in
    ``migrations/__init__.py`` reads the attribute when sweeping
    every Migration subclass loaded under
    ``lamella.features.recovery.migrations``.

    For ``axis="sqlite"`` migrations, leave ``keys`` as the default
    ``None`` — the registry has exactly one sqlite singleton and
    dispatches every (axis="sqlite", *) finding to it. Passing keys
    on a sqlite-axis migration raises a clearer error from the
    discovery scanner than letting the bad registration sit unused.
    """
    if keys is None:
        marker = _SQLITE_REGISTRATION_SENTINEL
    else:
        # Defensive normalization: accept lists too, normalize to a
        # tuple of tuples so the discovery scanner doesn't have to
        # re-coerce.
        marker = tuple(
            (str(k[0]), int(k[1])) for k in keys
        )

    def _decorate(cls):
        cls.__lamella_migration_keys__ = marker
        return cls

    return _decorate


# Allowed dry-run kinds. Each maps to a different UI rendering:
#
# - ``rename``     — pure rename (column / file). Diff renders inline.
# - ``additive``   — schema additions (new column, new table, new
#                    directive type). UI renders "this adds N items;
#                    existing data is not modified."
# - ``recompute``  — recomputes derived state (e.g. balance recompute,
#                    posting rewrite). Honest preview is expensive
#                    (would need a scratch copy); UI renders
#                    "this migration cannot be previewed cleanly;
#                    confirm to apply."
# - ``unsupported``— migration declared no dry-run. UI surfaces a
#                    confirm step before apply (per the locked spec
#                    "Confirmation step beats lying about previewability").
DryRunKind = str  # one of: 'rename' | 'additive' | 'recompute' | 'unsupported'

_VALID_KINDS = ("rename", "additive", "recompute", "unsupported")


@dataclass(frozen=True)
class DryRunResult:
    """What ``Migration.dry_run()`` returns. Frozen so the route
    layer can stash it across the GET (preview) → POST (apply) round
    trip without worrying about mutation.

    The ``counts`` field is optional — only ``recompute`` migrations
    populate it. The UI uses it to render "this would write N
    classifications, M budgets, K rules" without listing each row.

    The ``detail`` field carries markdown if the migration wants to
    spell out specifics (e.g. "the following four columns will be
    added to ``loans``: …"). Renderers may strip markdown if
    rendering plain text.
    """

    kind: DryRunKind
    summary: str
    """One-line headline. Always present."""

    detail: str | None = None
    """Markdown body, optional."""

    counts: dict[str, int] = field(default_factory=dict)
    """Per-row-type counts. Populated by recompute migrations."""

    def __post_init__(self) -> None:
        if self.kind not in _VALID_KINDS:
            raise ValueError(
                f"DryRunResult.kind must be one of {_VALID_KINDS}, "
                f"got {self.kind!r}"
            )


class MigrationError(Exception):
    """Raised by Migration.apply() to signal a domain-specific failure
    that the heal action should surface verbatim to the user. Generic
    Python exceptions get classified by ``failure_message_for``;
    subclassing this lets a Migration short-circuit that classification
    when it knows the exact user-facing message it wants."""


class Migration(abc.ABC):
    """One unit of schema migration the recovery shell can run.

    Subclasses set the four class variables, then implement
    :meth:`dry_run` and :meth:`apply`. Most don't need to override
    :meth:`failure_message_for` — the default classifier handles
    bean-check / OSError / SQLite-constraint exceptions adequately.
    """

    # --- subclass declares ------------------------------------------------

    AXIS: str
    """``'sqlite'`` or ``'ledger'`` — must match the Finding axis the
    detector emits. The lookup function in ``__init__`` dispatches
    on this."""

    SUPPORTS_DRY_RUN: bool = True
    """If False, the heal-action surface forces a confirm step before
    apply (per the locked spec — better than rendering a fake preview)."""

    # --- subclass implements ----------------------------------------------

    @abc.abstractmethod
    def declared_paths(self, settings: Settings) -> tuple[Path, ...]:
        """Files this migration writes to. The heal-action snapshot
        envelope only protects files in this set — writing outside it
        is undefined behavior. SQLite-only migrations return an empty
        tuple. Ledger-touching migrations enumerate every connector-
        owned ``.bean`` file they may modify."""

    @abc.abstractmethod
    def dry_run(
        self, conn: sqlite3.Connection, settings: Settings,
    ) -> DryRunResult:
        """Compute what :meth:`apply` would do without making the
        change. Pure — no writes, no DB mutations.

        For ``SUPPORTS_DRY_RUN = False`` migrations, return a
        ``DryRunResult(kind='unsupported', summary=…)``. Even
        unsupported migrations call this so the route can render a
        consistent confirm screen — the difference is the kind."""

    @abc.abstractmethod
    def apply(
        self, conn: sqlite3.Connection, settings: Settings,
    ) -> None:
        """Execute the migration. The caller (heal-action) wraps this
        in:

            1. SQLite ``BEGIN`` (if AXIS == 'sqlite').
            2. ``with_bean_snapshot(self.declared_paths(...))``
               (if AXIS == 'ledger' or declared_paths is non-empty).
            3. This call.
            4. Bean-check (if any ledger paths declared).
            5. SQLite ``COMMIT`` on success, ``ROLLBACK`` on any
               exception.

        On exception, raise — the heal action's envelope rolls back.
        Don't catch and swallow; don't log-and-continue. ``MigrationError``
        is the dialect-specific exception class for "I want this exact
        message surfaced to the user"; raise it when the Python
        exception classifier wouldn't produce a clear message."""

    # --- failure message classification (override optional) ---------------

    def failure_message_for(self, exc: BaseException) -> str:
        """Translate an exception raised by :meth:`apply` into a
        user-facing message. Default implementation handles the four
        failure modes the Phase 5 spec calls out:

        - ``BeanSnapshotCheckError`` — bean-check failed post-write.
          Message uses the truncated error list from the exception
          (already capped at 200 chars by the BeanSnapshot class).
        - ``sqlite3.IntegrityError`` — constraint violation. Message
          surfaces the constraint name when available.
        - ``OSError`` (PermissionError, disk full, etc.) — surface
          the filename + os-specific reason.
        - ``MigrationError`` — surface the message verbatim (the
          subclass already chose user-facing wording).
        - Anything else — surface the exception type name + the first
          line of str(exc), with a "report a bug" hint. This is the
          path that fires when a code bug surfaces; we want enough to
          file an issue without leaking a stack trace.

        Subclasses override only when migration-specific context
        sharpens the message — most don't need to."""
        # Lazy import to avoid a top-level cycle (snapshot.py imports
        # nothing from this module, but base.py importing snapshot
        # creates one at recovery-package load time).
        from lamella.features.recovery.snapshot import BeanSnapshotCheckError

        if isinstance(exc, MigrationError):
            return str(exc)
        if isinstance(exc, BeanSnapshotCheckError):
            joined = "; ".join(exc.errors[:3])
            if len(exc.errors) > 3:
                joined += f" (+{len(exc.errors) - 3} more)"
            return f"bean-check failed after migration: {joined}"
        if isinstance(exc, sqlite3.IntegrityError):
            return f"SQLite constraint violation: {exc}"
        if isinstance(exc, sqlite3.OperationalError):
            return f"SQLite operational error: {exc}"
        if isinstance(exc, PermissionError):
            return f"permission denied: {exc.filename or exc}"
        if isinstance(exc, OSError):
            # Disk-full, no-space, broken-pipe, etc.
            return f"file write failed: {exc}"
        # Truncate generic messages so a rogue exception with a 50KB
        # str() doesn't blow out the response.
        first_line = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
        truncated = first_line[:200]
        return (
            f"unexpected error ({type(exc).__name__}): {truncated}"
        )

    # --- introspection helpers --------------------------------------------

    def __repr__(self) -> str:
        return f"<Migration {self.AXIS} {type(self).__name__}>"
