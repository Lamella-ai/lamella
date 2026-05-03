# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Detect the state of a ledger on app boot.

Implements ``docs/specs/LEDGER_LAYOUT.md`` §8.1 (first-run detection) and
§8.2 (structural-emptiness definition). The result is consumed by
the startup path: if setup is required, the FastAPI app serves
``/setup`` instead of the dashboard until the user completes
scaffold or import.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from beancount import loader
from beancount.core import data as bdata

__all__ = [
    "LATEST_LEDGER_VERSION",
    "LedgerState",
    "DetectionResult",
    "detect_ledger_state",
]

_LOG = logging.getLogger(__name__)

#: The current canonical layout version. Bumped when LEDGER_LAYOUT.md
#: receives a breaking change (per §12.3). Also ties to the software's
#: semver major version.
#:
#: v1 → v2 (2026): bcg-* metadata prefix retired on disk. v1 ledgers carry
#: ``bcg-*`` keys (handled at load time by ``_legacy_meta``); v2 ledgers
#: are clean on disk. The ``MigrateLedgerV1ToV2`` step rewrites every
#: ``.bean`` file under the ledger root and bumps the stamp.
#:
#: v2 → v3 (2026): every Transaction must carry ``lamella-txn-id`` at the
#: txn-meta level. Pre-v3 ledgers hold a mix of stamped (post-Phase-7
#: writes) and unstamped (legacy / hand-edited) entries; the resolver
#: at /txn/{token} accepted either UUIDv7 or the legacy hex content
#: hash to compensate. v3 retires the hex form by guaranteeing every
#: entry has a UUID — ``MigrateLedgerV2ToV3`` walks every ``.bean``
#: file via ``normalize_txn_identity.run`` and mints lineage on disk
#: for entries that lack one.
#:
#: v3 → v4 (2026, ADR-0061): the receipt-* directive vocabulary
#: generalizes to document-*. The v4 reader still accepts every
#: legacy ``receipt-link``, ``receipt-dismissed``, etc. directive; the
#: v4 writer only emits ``document-*``. ``MigrateLedgerV3ToV4`` is a
#: stamp-only bump — existing receipt-* directives stay until the
#: surrounding ``.bean`` file is touched for any other reason, at
#: which point the writer naturally produces ``document-*``. Bumping
#: the stamp prevents accidental downgrade to v3 software, which
#: would silently ignore document-* directives it doesn't recognise.
LATEST_LEDGER_VERSION = 4


class LedgerState(str, Enum):
    MISSING = "missing"
    UNPARSEABLE = "unparseable"
    STRUCTURALLY_EMPTY = "structurally_empty"
    NEEDS_VERSION_STAMP = "needs_version_stamp"
    NEEDS_MIGRATION = "needs_migration"
    READY = "ready"


@dataclass(frozen=True)
class DetectionResult:
    state: LedgerState
    main_bean_path: Path
    parse_errors: tuple[str, ...] = ()
    ledger_version: int | None = None
    content_entry_count: int = 0

    @property
    def needs_setup(self) -> bool:
        """True if the app should redirect to ``/setup``.

        Includes NEEDS_VERSION_STAMP — without the stamp, reconstruct
        has no guarantee that directives were written by a compatible
        app version, so we block the dashboard and prompt the user to
        confirm + stamp. The stamp isn't tax-relevant; it's the
        schema-version marker that lets future migrations run safely.
        """
        return self.state in {
            LedgerState.MISSING,
            LedgerState.UNPARSEABLE,
            LedgerState.STRUCTURALLY_EMPTY,
            LedgerState.NEEDS_VERSION_STAMP,
        }

    @property
    def can_serve_dashboard(self) -> bool:
        """True if the app has enough ledger to boot its normal UI."""
        return self.state in {
            LedgerState.READY,
            LedgerState.NEEDS_VERSION_STAMP,
            LedgerState.NEEDS_MIGRATION,
        }


def detect_ledger_state(main_bean_path: Path) -> DetectionResult:
    """Classify the ledger at ``main_bean_path`` into one ``LedgerState``.

    Classification rules, in order:

    1. File doesn't exist → ``MISSING``.
    2. ``beancount.loader.load_file`` raises or returns fatal errors
       → ``UNPARSEABLE``.
    3. Parses and carries a ``lamella-ledger-version`` stamp:
       - version == ``LATEST_LEDGER_VERSION`` → ``READY``.
       - version < ``LATEST_LEDGER_VERSION`` → ``NEEDS_MIGRATION``.
       (Content or no content — the stamp is the signal the user
       has been through the setup wizard or an Import run. A
       freshly-scaffolded ledger lands here with zero transactions,
       and that's fine.)
    4. Parses, no version stamp, has ``Transaction`` / ``Balance`` /
       ``Pad`` content → ``NEEDS_VERSION_STAMP`` (gentle path per
       LEDGER_LAYOUT.md §6.4 — stamp on next write).
    5. Parses, no version stamp, no content → ``STRUCTURALLY_EMPTY``
       (per §8.2 — a ledger that's never been initialized).

    Production callers typically only branch on ``needs_setup`` vs.
    ``can_serve_dashboard``; the specific state is useful for
    logging and for ``/setup``'s explanation text.
    """
    if not main_bean_path.is_file():
        return DetectionResult(
            state=LedgerState.MISSING, main_bean_path=main_bean_path
        )

    try:
        entries, errors, _options = loader.load_file(str(main_bean_path))
        # Normalize legacy bcg-* metadata/types to lamella-* so the
        # version-stamp check below sees the new prefix even on old
        # ledgers that haven't been rewritten yet.
        from lamella.utils._legacy_meta import normalize_entries
        entries = normalize_entries(entries)
    except Exception as exc:  # beancount's loader rarely raises, but be safe
        _LOG.warning("failed to load ledger at %s: %s", main_bean_path, exc)
        return DetectionResult(
            state=LedgerState.UNPARSEABLE,
            main_bean_path=main_bean_path,
            parse_errors=(str(exc),),
        )

    fatal = _fatal_error_messages(errors)
    if fatal:
        return DetectionResult(
            state=LedgerState.UNPARSEABLE,
            main_bean_path=main_bean_path,
            parse_errors=tuple(fatal),
        )

    content_count = sum(
        1
        for e in entries
        if isinstance(e, (bdata.Transaction, bdata.Balance, bdata.Pad))
    )
    version = _extract_ledger_version(entries)

    # Version stamp is the "past setup" signal. Check it before the
    # content check so a just-scaffolded empty ledger is READY, not
    # STRUCTURALLY_EMPTY.
    if version is not None:
        if version < LATEST_LEDGER_VERSION:
            return DetectionResult(
                state=LedgerState.NEEDS_MIGRATION,
                main_bean_path=main_bean_path,
                content_entry_count=content_count,
                ledger_version=version,
            )
        return DetectionResult(
            state=LedgerState.READY,
            main_bean_path=main_bean_path,
            content_entry_count=content_count,
            ledger_version=version,
        )

    # No version stamp — pre-spec ledger or uninitialized.
    if content_count > 0:
        return DetectionResult(
            state=LedgerState.NEEDS_VERSION_STAMP,
            main_bean_path=main_bean_path,
            content_entry_count=content_count,
            ledger_version=None,
        )
    return DetectionResult(
        state=LedgerState.STRUCTURALLY_EMPTY,
        main_bean_path=main_bean_path,
        content_entry_count=0,
    )


def _fatal_error_messages(errors: list) -> list[str]:
    """Return the real (non-informational) error messages.

    Beancount's errors list folds together parse failures, balance
    assertion failures, and informational messages like
    ``<auto_insert_open>: Auto-inserted Open directives for N
    accounts``. We drop informational ones — they aren't reasons
    to route the user through setup.
    """
    out: list[str] = []
    for e in errors:
        msg = getattr(e, "message", str(e))
        if "Auto-inserted" in msg:
            continue
        source = getattr(e, "source", None)
        source_filename = ""
        if isinstance(source, dict):
            source_filename = source.get("filename", "") or ""
        if isinstance(source_filename, str) and source_filename.startswith("<"):
            # Pseudo-source like <auto_insert_open>, not a real error.
            continue
        out.append(msg)
    return out


def _extract_ledger_version(entries: list) -> int | None:
    """Return the integer ``lamella-ledger-version``, or ``None`` if absent.

    Also accepts the legacy ``bcg-ledger-version`` directive type so a v1
    ledger that hasn't been rewritten on-disk yet detects as v1 (and
    routes to the v1→v2 migration) without depending on the
    ``_legacy_meta`` at-load shim. Detection must remain correct even if
    the shim is removed in a later version.
    """
    for e in entries:
        if not isinstance(e, bdata.Custom):
            continue
        if e.type not in ("lamella-ledger-version", "bcg-ledger-version"):
            continue
        if not e.values:
            continue
        raw = e.values[0]
        # Beancount versions differ on whether custom values are wrapped.
        if hasattr(raw, "value"):
            raw = raw.value
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    return None
