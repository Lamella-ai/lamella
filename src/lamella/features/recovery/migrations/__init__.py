# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Migration registry — maps schema_drift Findings to Migration instances.

Phase 5.3 of SETUP_IMPLEMENTATION.md. The recovery surface dispatches
heals via :func:`find_for_finding`: detector emits a Finding with
``axis``, ``from_version``, ``to_version`` in its proposed_fix; the
heal action looks up the matching Migration and runs ``apply()``
inside the snapshot envelope.

Per the locked Phase 5 spec, dispatch is by axis:

- ``sqlite`` axis: returns the single :class:`CatchUpSqliteMigrations`
  instance regardless of from/to. ``db.migrate`` figures out what to
  apply by querying ``schema_migrations`` directly.
- ``ledger`` axis: returns the Migration registered for the exact
  ``(from_version, to_version)`` tuple. Phase 5 ships v0 → v1; future
  versions register additional tuples here.

Registration model (gap §11.5)
------------------------------

Migration subclasses self-register via the ``@register_migration``
decorator from ``base.py``. The decorator stamps a class attribute
that this module's :func:`_discover_migrations` scanner reads when
sweeping every module under ``lamella.features.recovery.migrations``
on package import. Adding a new Migration subclass is now a
two-step change (write the class, decorate it); the registry-wiring
step that previously had to be edited by hand here is gone.

The discovery scan is eager and runs once at module import. Two
behavioral guarantees vs. the prior hand-wired registry:

1. **Same set of migrations registered.** Each existing decorator
   call corresponds 1:1 to an entry in the prior hand-wired
   ``_LEDGER_REGISTRY`` dict. The discovery walk produces the same
   axis-keyed lookups; ``find_for_finding`` and
   :func:`all_migrations` behave identically.

2. **Singleton sharing across aliases.** A Migration registered with
   multiple keys (e.g. ``MigrateLedgerV0ToV1`` under both
   ``("none", 1)`` and ``("0", 1)``) shares one instance across
   every alias — same as the prior layout — so an audit pass that
   sanity-checks declared paths sees one instance per concrete
   subclass rather than N alias copies.
"""
from __future__ import annotations

import importlib
import inspect
import pkgutil

from lamella.features.recovery.migrations.base import (
    DryRunResult,
    Migration,
    MigrationError,
    _SQLITE_REGISTRATION_SENTINEL,
    register_migration,
)
from lamella.features.recovery.models import Finding


__all__ = [
    "DryRunResult",
    "Migration",
    "MigrationError",
    "register_migration",
    "find_for_finding",
    "all_migrations",
]


# Module-level registries populated by :func:`_discover_migrations`
# at import time. The shapes match what the prior hand-wired layout
# exposed:
#
# - ``_SQLITE_MIGRATION``: the single sqlite-axis Migration instance,
#   or ``None`` if no subclass is registered (programmer error
#   surfaced by ``find_for_finding`` returning None for sqlite axis).
# - ``_LEDGER_REGISTRY``: ``{(from_version_str, to_version_int): Migration}``.
_SQLITE_MIGRATION: Migration | None = None
_LEDGER_REGISTRY: dict[tuple[str, int], Migration] = {}


def _discover_migrations() -> None:
    """Walk every module under
    ``lamella.features.recovery.migrations`` and rebuild the
    axis-keyed registries from each subclass's
    ``@register_migration`` marker.

    Iteration order is the package's directory order (sorted by
    pkgutil), which matches the prior hand-wired registry's effective
    order (the import block at the top of the old ``__init__`` listed
    sqlite then ledger; pkgutil sorts alphabetically, putting
    ``catch_up_sqlite`` before ``migrate_ledger_v0_to_v1``).

    Idempotent: a re-run after a new module is added picks the new
    subclass up. Tests use this to assert auto-discovery behavior
    end-to-end without mocking pkgutil.
    """
    global _SQLITE_MIGRATION, _LEDGER_REGISTRY

    sqlite_instance: Migration | None = None
    ledger: dict[tuple[str, int], Migration] = {}

    # Cache of subclass → singleton instance so multiple keys for the
    # same subclass share one instance (preserves the alias-sharing
    # behavior the prior hand-wired layout had for v0→v1).
    instances: dict[type[Migration], Migration] = {}

    package_name = __name__  # 'lamella.features.recovery.migrations'
    package = importlib.import_module(package_name)
    for module_info in pkgutil.iter_modules(package.__path__):
        if module_info.name == "base":
            # Skip the base module — it defines the abstract base
            # class itself plus the decorator. No concrete migrations
            # live there.
            continue
        if module_info.ispkg:
            # The recovery shell doesn't currently nest sub-packages
            # under migrations/, but skip them defensively if a future
            # change does — discovery would need a recursive walk to
            # support that.
            continue
        full_name = f"{package_name}.{module_info.name}"
        module = importlib.import_module(full_name)
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is Migration:
                continue
            if not issubclass(obj, Migration):
                continue
            # Only register classes defined in this module — re-imports
            # of the same Migration through transitive imports
            # shouldn't trigger a duplicate.
            if obj.__module__ != full_name:
                continue
            marker = getattr(obj, "__lamella_migration_keys__", None)
            if marker is None:
                # Subclass exists but isn't decorated. The Phase 5
                # contract says every Migration must be discoverable;
                # an undecorated subclass is silently unreachable
                # otherwise — exactly the bug gap §11.5 fixes. Surface
                # it loudly so the developer knows to add the
                # decorator before shipping.
                raise RuntimeError(
                    f"Migration subclass {obj.__module__}.{obj.__qualname__} "
                    "is missing the @register_migration decorator — "
                    "every concrete Migration must be decorated so the "
                    "auto-discovery scan can register it. Add "
                    "@register_migration() (sqlite axis) or "
                    "@register_migration(keys=(...)) (ledger axis) "
                    "above the class body."
                )

            instance = instances.get(obj)
            if instance is None:
                instance = obj()
                instances[obj] = instance

            if marker is _SQLITE_REGISTRATION_SENTINEL:
                if obj.AXIS != "sqlite":
                    raise RuntimeError(
                        f"Migration {obj.__qualname__} is decorated as "
                        "sqlite-axis (no keys) but declares "
                        f"AXIS={obj.AXIS!r}. Pass keys=… to "
                        "@register_migration for ledger-axis migrations."
                    )
                if (
                    sqlite_instance is not None
                    and sqlite_instance.__class__ is not obj
                ):
                    raise RuntimeError(
                        "Multiple sqlite-axis migrations registered: "
                        f"{sqlite_instance.__class__.__qualname__} and "
                        f"{obj.__qualname__}. The recovery shell expects "
                        "exactly one sqlite singleton."
                    )
                sqlite_instance = instance
            else:
                if obj.AXIS != "ledger":
                    raise RuntimeError(
                        f"Migration {obj.__qualname__} is decorated with "
                        "keys=… (ledger-axis) but declares "
                        f"AXIS={obj.AXIS!r}. Drop the keys arg for "
                        "sqlite-axis migrations."
                    )
                for key in marker:
                    existing = ledger.get(key)
                    if existing is not None and existing is not instance:
                        raise RuntimeError(
                            f"Ledger-axis registration conflict at key "
                            f"{key!r}: both "
                            f"{existing.__class__.__qualname__} and "
                            f"{obj.__qualname__} claim it."
                        )
                    ledger[key] = instance

    _SQLITE_MIGRATION = sqlite_instance
    _LEDGER_REGISTRY = ledger


# Eager discovery at package import. This runs exactly once — the
# Python import system caches the module post-load, so the scan
# doesn't re-fire on subsequent ``import lamella.features.recovery.migrations``.
_discover_migrations()


def find_for_finding(finding: Finding) -> Migration | None:
    """Look up the Migration that heals ``finding``.

    Returns ``None`` when no migration is registered — the route layer
    surfaces that as "this drift category has no auto-heal yet, ask
    a maintainer". Phase 5 ships only the (sqlite, *) catch-up and
    the (ledger, none|0, 1) backfill; later versions extend.
    """
    if finding.category != "schema_drift":
        return None
    fix = finding.proposed_fix_dict
    axis = fix.get("axis")
    if axis == "sqlite":
        return _SQLITE_MIGRATION
    if axis == "ledger":
        from_v = str(fix.get("from_version"))
        to_v = fix.get("to_version")
        if not isinstance(to_v, int):
            return None
        return _LEDGER_REGISTRY.get((from_v, to_v))
    return None


def all_migrations() -> tuple[Migration, ...]:
    """Every distinct Migration instance, de-duplicated. The ledger
    registry stores aliases (``"none"`` and ``"0"`` both point at
    the same instance); the audit pass that sanity-checks
    non-overlapping declared paths shouldn't see the duplicates."""
    seen: set[int] = set()
    out: list[Migration] = []
    candidates: list[Migration] = []
    if _SQLITE_MIGRATION is not None:
        candidates.append(_SQLITE_MIGRATION)
    candidates.extend(_LEDGER_REGISTRY.values())
    for m in candidates:
        if id(m) in seen:
            continue
        seen.add(id(m))
        out.append(m)
    return tuple(out)
