# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Heal actions for legacy-path findings: close + move-and-close.

Phase 3 of /setup/recovery (SETUP_IMPLEMENTATION.md). Each action
takes one Finding plus the env it needs and returns a HealResult.
Per-finding atomicity — every action runs inside its own
``with_bean_snapshot`` envelope. Bulk apply is Phase 6's job.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

from beancount.core.data import Open, Transaction

from lamella.features.recovery.models import (
    Finding,
    HealResult,
)
from lamella.features.recovery.snapshot import (
    BeanSnapshotCheckError,
    with_bean_snapshot,
)

_LOG = logging.getLogger(__name__)


class HealRefused(Exception):
    """Action refused before any write — preconditions failed,
    canonical destination didn't pass guards, etc. Caller turns
    this into a user-visible 4xx with the message."""


def heal_legacy_path(
    finding: Finding,
    *,
    conn: sqlite3.Connection,
    settings: Any,
    reader: Any,
    bean_check: Any | None = None,
    bulk_context: Any | None = None,
) -> HealResult:
    """Dispatch a legacy-path finding to its specific action based
    on ``finding.proposed_fix['action']``. Phase 3 supports
    ``close`` and ``move``.

    Atomicity (Phase 6.1.3.5):

    - ``bulk_context=None`` (default, single-finding heal): the inner
      action wraps its writes in its own ``with_bean_snapshot``
      envelope so a per-finding failure restores the declared file
      set byte-identically.
    - ``bulk_context`` provided (orchestrator participation): the
      orchestrator owns the outer envelope. The inner action skips
      its own snapshot wrap and writes directly; on failure (raise
      or HealRefused), the orchestrator's outer envelope restores
      every file it snapshotted at group start. This is what gives
      Groups 2+3 per-group atomicity ("any finding fails → whole
      group rolls back") instead of the per-finding best-effort
      semantics Phase 6.1.3 shipped.
    """
    if finding.category != "legacy_path":
        raise HealRefused(
            f"heal_legacy_path: not a legacy_path finding "
            f"(got category={finding.category!r})"
        )
    fix = finding.proposed_fix_dict
    action = fix.get("action")
    if action == "close":
        return _heal_close(
            finding, conn=conn, settings=settings, reader=reader,
            bean_check=bean_check, bulk_context=bulk_context,
        )
    if action == "move":
        canonical = fix.get("canonical")
        if not canonical:
            raise HealRefused(
                "move action requires 'canonical' in proposed_fix"
            )
        return _heal_move_and_close(
            finding, canonical=canonical,
            conn=conn, settings=settings, reader=reader,
            bean_check=bean_check, bulk_context=bulk_context,
        )
    raise HealRefused(f"unknown legacy_path action: {action!r}")


# --- close ----------------------------------------------------------------


def _heal_close(
    finding: Finding,
    *,
    conn: sqlite3.Connection,
    settings: Any,
    reader: Any,
    bean_check: Any | None = None,
    bulk_context: Any | None = None,
) -> HealResult:
    """Write a Close directive for the legacy account into
    ``connector_accounts.bean``. Refuses if the account currently
    has any postings — closing it then would orphan the postings."""
    legacy = finding.target
    entries = list(reader.load().entries)

    posting_count = sum(
        1
        for e in entries
        if isinstance(e, Transaction)
        for p in e.postings
        if getattr(p, "account", None) == legacy
    )
    if posting_count > 0:
        raise HealRefused(
            f"refusing to close {legacy}: account has "
            f"{posting_count} posting(s). Use 'Move and close' "
            "instead, or re-classify the postings first."
        )

    open_directive = next(
        (e for e in entries if isinstance(e, Open) and e.account == legacy),
        None,
    )
    if open_directive is None:
        # Account isn't actually opened — nothing to close. The
        # detector should have skipped it but be defensive.
        return HealResult(
            success=True,
            message=f"{legacy} is not open in the ledger; nothing to close.",
            files_touched=(),
            finding_id=finding.id,
        )

    connector_accounts = Path(settings.connector_accounts_path)
    main_bean = Path(settings.ledger_main)
    declared = [connector_accounts, main_bean]

    from lamella.core.registry.accounts_writer import AccountsWriter
    writer = AccountsWriter(
        main_bean=main_bean,
        connector_accounts=connector_accounts,
    )

    if bulk_context is not None:
        # Outer-envelope mode — orchestrator owns the snapshot. We
        # skip our own with_bean_snapshot wrap and write directly;
        # on raise/HealRefused, the orchestrator's outer envelope
        # rolls back every file it snapshotted at group start. We
        # still register paths so the orchestrator's audit captures
        # what touched what.
        bulk_context.add_paths(tuple(declared))
        writer.write_close(legacy, closed_on=date.today())
        reader.invalidate()
        return HealResult(
            success=True,
            message=f"Closed {legacy}.",
            files_touched=(connector_accounts,),
            finding_id=finding.id,
        )

    try:
        with with_bean_snapshot(
            declared,
            bean_check=bean_check,
            bean_check_path=main_bean,
        ) as snap:
            # write_close already runs its own bean-check + restore.
            # Wrapping in our envelope is belt-and-suspenders for the
            # cases where the writer's check passes but the optional
            # outer bean_check finds something else.
            writer.write_close(legacy, closed_on=date.today())
            snap.add_touched(connector_accounts)
    except BeanSnapshotCheckError as exc:
        return HealResult(
            success=False,
            message=f"bean-check rejected the close: {exc}",
            files_touched=(),
            finding_id=finding.id,
        )

    reader.invalidate()
    return HealResult(
        success=True,
        message=f"Closed {legacy}.",
        files_touched=(connector_accounts,),
        finding_id=finding.id,
    )


# --- move-and-close --------------------------------------------------------


def _heal_move_and_close(
    finding: Finding,
    *,
    canonical: str,
    conn: sqlite3.Connection,
    settings: Any,
    reader: Any,
    bean_check: Any | None = None,
    bulk_context: Any | None = None,
) -> HealResult:
    """Rewrite every posting referencing the legacy account to point
    at the canonical destination, scaffold the canonical Open
    directive if needed, then close the legacy account.

    Order matters:
      1. Identify every (file, lineno) pair where the legacy
         account is posted.
      2. Identify the earliest reference date (so a backdated Open
         for the canonical satisfies bean-check).
      3. Open the canonical account if it isn't already opened —
         dated <= earliest reference.
      4. Rewrite every posting line legacy → canonical.
      5. Close the legacy account.

    Each step bean-checks against a baseline so a partial failure
    rolls back. The outer snapshot envelope adds atomicity across
    steps in case one of the inner writers leaves partial state on
    a non-bean-check failure (OS-level write error, etc.).
    """
    legacy = finding.target
    main_bean = Path(settings.ledger_main)
    connector_accounts = Path(settings.connector_accounts_path)
    ledger_dir = main_bean.parent

    entries = list(reader.load().entries)

    # 1. Posting locations.
    sites: list[tuple[Path, int, Any]] = []  # (source_file, txn_lineno, posting)
    earliest_ref: date | None = None
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        for p in e.postings:
            if getattr(p, "account", None) != legacy:
                continue
            meta = e.meta or {}
            filename = meta.get("filename")
            lineno = meta.get("lineno")
            if not filename or not lineno:
                raise HealRefused(
                    f"posting referencing {legacy} on {e.date} has no "
                    "source location metadata; can't rewrite safely."
                )
            sites.append((Path(filename), int(lineno), p))
            if earliest_ref is None or e.date < earliest_ref:
                earliest_ref = e.date

    # 2. Open paths set (used for guard re-check + scaffold decision).
    opened: set[str] = {e.account for e in entries if isinstance(e, Open)}

    # Re-validate the canonical destination against the same guards
    # the detector used. If the world changed between detector run
    # and heal click, refuse.
    from lamella.features.recovery.findings.legacy_paths import (
        _passes_destination_guards,
    )
    if not _passes_destination_guards(canonical, opened):
        raise HealRefused(
            f"canonical destination {canonical!r} no longer passes "
            "the move-target guards (parent path not opened or not "
            "part of an existing branch). Open the parent path "
            "first, or pick a different canonical."
        )

    # 3. Decide the Open date for the canonical (if it's not already opened).
    needs_open = canonical not in opened
    open_date: date | None = None
    if needs_open:
        # Backdate to <= earliest posting reference, falling back to
        # 1900-01-01 when there are no postings.
        open_date = earliest_ref or date(1900, 1, 1)

    # 4. Run inside the snapshot envelope. We declare every file we
    #    might touch up front: connector_accounts.bean (Open + Close)
    #    + every source file that holds a posting we'll rewrite.
    declared: list[Path] = [connector_accounts, main_bean]
    for src, _, _ in sites:
        if src not in declared:
            declared.append(src)

    rewrites_done = 0
    files_touched: list[Path] = []

    from lamella.core.registry.accounts_writer import AccountsWriter
    from lamella.core.rewrite.txn_inplace import (
        InPlaceRewriteError,
        rewrite_fixme_to_account,
    )
    writer = AccountsWriter(
        main_bean=main_bean,
        connector_accounts=connector_accounts,
    )

    def _do_writes(snap=None):
        nonlocal rewrites_done
        # 4a. Open canonical if needed.
        if needs_open:
            writer.write_opens(
                [canonical],
                opened_on=open_date,
                comment=f"auto-scaffolded for legacy-path heal of {legacy}",
                existing_paths=opened,
            )
            if snap is not None:
                snap.add_touched(connector_accounts)

        # 4b. Rewrite every posting.
        for src, lineno, posting in sites:
            amount = posting.units.number if posting.units else None
            try:
                rewrite_fixme_to_account(
                    source_file=src,
                    line_number=lineno,
                    old_account=legacy,
                    new_account=canonical,
                    expected_amount=amount,
                    ledger_dir=ledger_dir,
                    main_bean=main_bean,
                    run_check=True,
                )
            except InPlaceRewriteError as exc:
                raise HealRefused(
                    f"posting rewrite failed at {src}:{lineno}: {exc}"
                ) from exc
            rewrites_done += 1
            if src not in files_touched:
                files_touched.append(src)
            if snap is not None:
                snap.add_touched(src)

        # 4c. Close the legacy account.
        writer.write_close(legacy, closed_on=date.today())
        if snap is not None:
            snap.add_touched(connector_accounts)
        if connector_accounts not in files_touched:
            files_touched.append(connector_accounts)

    if bulk_context is not None:
        # Outer-envelope mode — see heal_legacy_path docstring. The
        # orchestrator's snapshot covers all declared paths; we just
        # write through and let raises propagate for it to catch.
        bulk_context.add_paths(tuple(declared))
        _do_writes(snap=None)
        reader.invalidate()
        msg_parts = [f"Moved {rewrites_done} posting(s) to {canonical}"]
        if needs_open:
            msg_parts.append(f"opened {canonical}")
        msg_parts.append(f"closed {legacy}")
        return HealResult(
            success=True,
            message="; ".join(msg_parts) + ".",
            files_touched=tuple(files_touched),
            finding_id=finding.id,
        )

    try:
        with with_bean_snapshot(
            declared,
            bean_check=bean_check,
            bean_check_path=main_bean,
        ) as snap:
            _do_writes(snap=snap)
    except BeanSnapshotCheckError as exc:
        return HealResult(
            success=False,
            message=f"bean-check rejected the move-and-close: {exc}",
            files_touched=(),
            finding_id=finding.id,
        )
    except HealRefused:
        raise

    reader.invalidate()
    msg_parts = [f"Moved {rewrites_done} posting(s) to {canonical}"]
    if needs_open:
        msg_parts.append(f"opened {canonical}")
    msg_parts.append(f"closed {legacy}")
    return HealResult(
        success=True,
        message="; ".join(msg_parts) + ".",
        files_touched=tuple(files_touched),
        finding_id=finding.id,
    )
