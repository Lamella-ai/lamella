# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Deprecation shim for legacy environment variable names.

Pydantic Settings on this project has no ``env_prefix`` configured, so a
field ``data_dir`` is read from env var ``DATA_DIR`` (case-insensitive),
``ledger_dir`` from ``LEDGER_DIR``, etc. The user-facing convention,
however, is to prefix every operator-set variable with ``LAMELLA_`` —
that's what the README, FEATURES.md, and the deployment templates
all document. This shim bridges the two: it copies the documented
``LAMELLA_*`` env vars into the bare-name slot pydantic actually
reads, and also accepts a small set of pre-rebrand legacy names.

Mapping table (new pydantic source name ← list of accepted legacy /
documented aliases). The first entry of each list is the documented,
non-deprecated name; the rest fire ``DeprecationWarning`` once when
set:

- ``DATA_DIR``        ← ``LAMELLA_DATA_DIR`` (current),
                        ``CONNECTOR_DATA_DIR`` (pre-rebrand),
                        ``LAMELLA_CONNECTOR_DATA_DIR`` (audit-claimed
                        doubled-prefix name; never actually read by
                        pydantic but supported here for operators
                        who set it after seeing the audit memo).
- ``MIGRATIONS_DIR``  ← ``LAMELLA_MIGRATIONS_DIR`` (current),
                        ``CONNECTOR_MIGRATIONS_DIR`` (pre-rebrand).
- ``CONFIG_DIR``      ← ``LAMELLA_CONFIG_DIR`` (current),
                        ``CONNECTOR_CONFIG_DIR`` (pre-rebrand).
- ``SKIP_DISCOVERY_GUARD`` ← ``LAMELLA_SKIP_DISCOVERY_GUARD`` (current),
                             ``BCG_SKIP_DISCOVERY_GUARD`` (pre-rebrand).

For one release, every name in the list works. The first entry is
the documented form going forward. Subsequent entries log a
one-shot ``DeprecationWarning`` so existing deployments keep
running but the operator sees the rename in their logs.

Strategy:

- ``apply_env_aliases()`` runs at process startup. For each new name
  whose canonical (first) alias is set, the value is copied into
  the new-name slot. For each legacy (non-first) alias that is
  set, the value is copied into the new-name slot AND fires a
  one-shot DeprecationWarning. If multiple aliases are set with
  conflicting values, the canonical alias wins; conflicts among
  legacy aliases are resolved by the order listed below.

- ``read_env(canonical_alias)`` is for direct reads (``db.py``,
  ``registry/discovery.py``). Pass the documented ``LAMELLA_*``
  name; it falls back through the legacy aliases with a warning.

Drop this module (and its callers) once the legacy names are out
of every documented .env / docker-compose.yml / deployment template.
"""
from __future__ import annotations

import os
import warnings

# new pydantic-source name → ordered list of accepted aliases.
# Element 0 is the documented (non-deprecated) form.
LEGACY_ENV: dict[str, list[str]] = {
    "DATA_DIR": [
        "LAMELLA_DATA_DIR",
        "CONNECTOR_DATA_DIR",
        "LAMELLA_CONNECTOR_DATA_DIR",
    ],
    "MIGRATIONS_DIR": [
        "LAMELLA_MIGRATIONS_DIR",
        "CONNECTOR_MIGRATIONS_DIR",
    ],
    "CONFIG_DIR": [
        "LAMELLA_CONFIG_DIR",
        "CONNECTOR_CONFIG_DIR",
    ],
    "SKIP_DISCOVERY_GUARD": [
        "LAMELLA_SKIP_DISCOVERY_GUARD",
        "BCG_SKIP_DISCOVERY_GUARD",
    ],
}

_warned: set[str] = set()
_aliases_applied: bool = False


def _warn_legacy_once(legacy: str, canonical: str) -> None:
    if legacy in _warned:
        return
    _warned.add(legacy)
    warnings.warn(
        f"Environment variable {legacy!r} is deprecated; "
        f"set {canonical!r} instead. Both work for now; the legacy "
        f"name will be removed in a future release.",
        DeprecationWarning,
        stacklevel=2,
    )


def _resolve_alias_value(aliases: list[str]) -> tuple[str | None, str | None]:
    """Walk ``aliases`` in priority order (canonical first, then legacy
    names in declaration order). Return ``(value, source_alias)`` for the
    first one set, or ``(None, None)`` if none are set.
    """
    for alias in aliases:
        if alias in os.environ:
            return os.environ[alias], alias
    return None, None


def apply_env_aliases() -> None:
    """Copy values between alias env-var names so the pydantic-source
    name (the dict key) sees whichever form the operator actually set.
    Idempotent — safe to call more than once.

    Resolution rules per new-name group:
    - If the pydantic-source name is already set in the environment,
      no copy is performed (operator clearly knows what they're doing).
    - Otherwise, walk the alias list in declaration order. The first
      alias that is set wins; its value is copied into the source
      name. If the winning alias is the canonical (first) form, no
      warning fires. If it's a legacy alias, a one-shot
      DeprecationWarning fires pointing at the canonical form.
    - If neither the source name nor any alias is set, the env stays
      empty and the pydantic field falls back to its declared default.
    """
    global _aliases_applied
    for source_name, aliases in LEGACY_ENV.items():
        if source_name in os.environ:
            # Operator set the bare pydantic-source name directly;
            # don't second-guess.
            continue
        canonical = aliases[0]
        value, source_alias = _resolve_alias_value(aliases)
        if value is None:
            continue
        os.environ[source_name] = value
        if source_alias != canonical:
            _warn_legacy_once(source_alias, canonical)
    _aliases_applied = True


def read_env(canonical_alias: str, default: str | None = None) -> str | None:
    """Look up ``canonical_alias`` (the documented user-facing name),
    falling back through any pre-rebrand legacy aliases mapped to the
    same pydantic-source name.

    If only a legacy alias is set, log a one-shot DeprecationWarning
    and return its value. If multiple aliases are set, the canonical
    form wins silently.
    """
    if canonical_alias in os.environ:
        return os.environ[canonical_alias]
    # Find the new-name group whose canonical (aliases[0]) matches.
    for source_name, aliases in LEGACY_ENV.items():
        if not aliases or aliases[0] != canonical_alias:
            continue
        # Walk legacy aliases (skip the canonical we already missed).
        for legacy in aliases[1:]:
            if legacy in os.environ:
                _warn_legacy_once(legacy, canonical_alias)
                return os.environ[legacy]
        # Last resort: maybe the pydantic-source name was set directly.
        if source_name in os.environ:
            return os.environ[source_name]
        break
    return default
