# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Account-open guard helpers — shared between manual classify
(``staging_review``) and the SimpleFIN ingest rule auto-apply path.

Centralizes three checks that have to run before any code writes a
posting against a target account:

1. Is the account opened in the ledger?
2. If yes, is its Open dated on or before the txn date?
3. If no Open exists at all, can we safely auto-scaffold one?

Each helper returns ``None`` on success (account is safely targetable
on the given date — possibly after auto-healing) or a short
human-readable error string explaining why we declined.

Auto-heal behavior:

* ``check_account_open_on`` — if the obstructing Open lives in
  ``connector_accounts.bean`` (a file we own), rewrites its date in
  place to cover the txn date.
* ``ensure_target_account_open`` — if the account isn't opened but
  extends a legitimate existing branch (parent path is itself open
  or is a prefix of an open path), auto-scaffolds an Open in
  ``connector_accounts.bean`` dated on or before the txn date.

The original site of these helpers was ``lamella.web.routes.staging_review``;
they were lifted here so the SimpleFIN ingest's rule auto-apply path
can run the same guard before writing rule-classified rows. Keeping
them out of ``routes/`` avoids a non-route module having to import
from ``routes/``.
"""
from __future__ import annotations

import logging
from datetime import date as _date
from pathlib import Path as _Path

from beancount.core import account as account_lib
from beancount.core.data import Close, Open

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings

log = logging.getLogger(__name__)


def ensure_target_account_open(
    reader: LedgerReader,
    settings: Settings,
    account: str,
    txn_date,
) -> str | None:
    """If `account` extends a legitimate existing branch but isn't yet
    opened, auto-scaffold it in ``connector_accounts.bean`` dated on or
    before ``txn_date``. Returns ``None`` on success (account is open
    after this call) or an error string explaining why we declined.

    Rule: auto-scaffold only when (a) the typed name is syntactically
    valid Beancount, (b) it has at least 3 segments
    (``Root:Entity:Leaf``), and (c) its parent path is either itself
    opened OR is a prefix of some opened account path. This blocks
    drive-by creation of brand-new top-level entities or
    typo'd parallel branches (``Expenses:AcmeCoLLC2:...``) while
    permitting deepening of an established hierarchy.
    """
    if not account_lib.is_valid(account):
        return (
            f"target account {account!r} is not a valid Beancount "
            "account name. Account paths must start with one of "
            "Assets/Liabilities/Equity/Income/Expenses and each "
            "segment must begin with a capital letter or digit."
        )

    try:
        entries = reader.load().entries
    except Exception:  # noqa: BLE001
        return None  # Don't block on a transient reader error.

    opened_paths: set[str] = set()
    for e in entries:
        if isinstance(e, Open):
            opened_paths.add(e.account)

    if account in opened_paths:
        return None  # Already opened — caller's date check handles the rest.

    parts = account.split(":")
    not_opened_msg = (
        f"target account {account!r} is not opened in the ledger. "
        "Add an `open` directive (e.g. via /settings/accounts) "
        "before posting to it."
    )
    if len(parts) < 3:
        return not_opened_msg

    parent = ":".join(parts[:-1])
    parent_prefix = parent + ":"
    parent_legitimate = parent in opened_paths or any(
        p.startswith(parent_prefix) for p in opened_paths
    )
    # Relaxation: even when the immediate parent isn't a known prefix,
    # permit auto-scaffolding when the path's ENTITY (segment 1) is
    # already attested somewhere in the ledger. Catches the user's
    # "I'm typing a brand-new sub-branch under my real business" case
    # — e.g. Expenses:Acme:COGS:Materials when the entity has
    # other Expenses:Acme:* accounts but no COGS yet. Without
    # this, every new category under an existing entity required a
    # /settings/accounts trip first, which is exactly the friction
    # the auto-scaffold path is supposed to remove.
    entity_attested = False
    if len(parts) >= 2:
        entity = parts[1]
        # The entity is "attested" when SOMETHING under any of the
        # five top-level roots already exists for it. We don't constrain
        # to the same root the new path uses — adding the first Income
        # account for an entity that already has Expenses entries is
        # legitimate.
        for root in ("Assets", "Liabilities", "Equity", "Income", "Expenses"):
            entity_prefix = f"{root}:{entity}:"
            if any(p.startswith(entity_prefix) for p in opened_paths):
                entity_attested = True
                break
    if not parent_legitimate and not entity_attested:
        return (
            f"target account {account!r} is not opened in the ledger "
            f"and its parent {parent!r} is not part of any existing "
            "account branch. Add an `open` directive via "
            "/settings/accounts first."
        )

    from lamella.core.registry.accounts_writer import AccountsWriter
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        writer.write_opens(
            [account],
            opened_on=txn_date,
            comment=f"auto-scaffolded by classify (extends {parent})",
            existing_paths=opened_paths,
        )
    except Exception as exc:  # noqa: BLE001
        return (
            f"failed to auto-scaffold target account {account!r}: {exc}"
        )
    reader.invalidate()
    return None


def check_account_open_on(
    reader: LedgerReader,
    account: str,
    target_date,
    settings: Settings | None = None,
) -> str | None:
    """Return None if `account` is open on `target_date`, else a
    short human-readable explanation suitable for an HTTP 400
    detail or a log line. Callers run this BEFORE writing so a
    target whose open date is after the txn date gets a clean
    error instead of a bean-check rollback.

    Auto-backdate: if the obstructing Open directive lives in
    ``connector_accounts.bean`` (a file we own), rewrite its date in
    place to cover ``target_date`` instead of refusing. ``settings``
    must be passed for this auto-heal to fire — without it we fall
    back to the explanatory error so the caller still gets a clean
    failure.
    """
    try:
        entries = reader.load().entries
    except Exception:  # noqa: BLE001
        return None  # Don't block on a transient reader error.
    open_date = None
    open_entry = None
    close_date = None
    for e in entries:
        if isinstance(e, Open) and e.account == account:
            if open_date is None or e.date < open_date:
                open_date = e.date
                open_entry = e
        elif isinstance(e, Close) and e.account == account:
            if close_date is None or e.date > close_date:
                close_date = e.date
    if open_date is None:
        return (
            f"target account {account!r} is not opened in the ledger. "
            "Add an `open` directive (e.g. via /settings/accounts) "
            "before posting to it."
        )
    if open_date > target_date:
        if settings is not None and open_entry is not None:
            healed = _try_backdate_open(
                settings=settings, reader=reader, account=account,
                open_entry=open_entry, target_date=target_date,
            )
            if healed:
                return None
        return (
            f"target account {account!r} is opened on {open_date} but "
            f"this transaction is dated {target_date}. Backdate the "
            f"open directive (preferably in connector_accounts.bean) "
            f"or pick a different target account that was already "
            f"open on the txn date."
        )
    if close_date is not None and close_date < target_date:
        return (
            f"target account {account!r} was closed on {close_date}. "
            f"Reopen it or pick a different target account."
        )
    return None


def _try_backdate_open(
    *,
    settings: Settings,
    reader: LedgerReader,
    account: str,
    open_entry,
    target_date,
) -> bool:
    """Backdate an Open directive in connector_accounts.bean so it
    covers `target_date`. Returns True on success.

    Refuses (returns False) when the Open lives in a file we don't
    own — in that case the caller surfaces the original error so
    the user can edit their own ledger file by hand.

    The new date is ``min(target_date, settings.account_default_open_date)``
    so a single auto-heal also covers any older transactions the user
    classifies later.
    """
    from lamella.core.ledger_writer import BeanCheckError
    from lamella.core.registry.accounts_writer import AccountsWriter
    try:
        owning_file = (open_entry.meta or {}).get("filename")
    except Exception:  # noqa: BLE001
        owning_file = None
    if not owning_file:
        return False
    try:
        if _Path(owning_file).resolve() != _Path(
            settings.connector_accounts_path
        ).resolve():
            return False
    except Exception:  # noqa: BLE001
        return False
    try:
        default_iso = (settings.account_default_open_date or "").strip()
        default_date = _date.fromisoformat(default_iso)
    except (TypeError, ValueError):
        default_date = _date(1900, 1, 1)
    new_date = min(target_date, default_date)
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        ok = writer.rewrite_open_date(account, new_date)
    except BeanCheckError as exc:
        log.warning(
            "auto-backdate Open for %s -> %s failed bean-check: %s",
            account, new_date, exc,
        )
        return False
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "auto-backdate Open for %s -> %s failed: %s",
            account, new_date, exc,
        )
        return False
    if ok:
        reader.invalidate()
        log.info(
            "auto-backdated Open for %s in connector_accounts.bean "
            "to %s (was after txn date %s)",
            account, new_date, target_date,
        )
    return ok
