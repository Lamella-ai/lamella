# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Shared predicates for the /setup surface.

Phase 1.3 (see ``FEATURE_SETUP_IMPLEMENTATION.md``) consolidates the
filter logic that used to be inlined in every handler in
``routes/setup.py`` and every ``_check_*`` in
``bootstrap/setup_progress.py``. The motivation is the §7 #4 + #5
incident class: a display counter and an action gate each computed
"how many unmigrated postings does this account have?" with a
slightly different filter, so the UI said "0 postings, safe to close"
and the close handler said "98 postings, refusing". Drift is
unavoidable when the filter lives in five places; impossible when it
lives in one.

The predicates are:

- :func:`is_override_txn` — tag test for our own ``#lamella-override``
  writes.
- :func:`already_migrated_hashes` — txn_hashes referenced by any
  existing override block's ``lamella-override-of`` meta. These originals
  have already been migrated and must not be double-counted or re-
  migrated.
- :func:`open_paths` — accounts currently open in the ledger
  (``Open`` minus matching ``Close``).
- :func:`count_unmigrated_postings` — postings on a single account
  that aren't overrides and whose original hasn't been migrated. The
  filter the post-commit-4c12404 sites use.
- :func:`unmigrated_postings_by_account` — batched version.
- :func:`iter_unmigrated_txns_on` — yield (txn, hash) for each
  unmigrated transaction carrying a posting on the target account.
  Shared by the vehicle-migrate + entity-migrate affected-list
  builders.
- :func:`is_vehicle_orphan` — the name-based regex classifier used
  by ``/setup/vehicles`` to surface non-canonical vehicle paths.

None of these helpers write to the ledger. They're pure queries
against a list of beancount entries.
"""
from __future__ import annotations

import re
from typing import Iterable, Iterator

from beancount.core.data import Close, Open, Transaction

from lamella.core.beancount_io.txn_hash import txn_hash as _txn_hash


# --- Tag + meta helpers ----------------------------------------------------


def is_override_txn(entry) -> bool:
    """True if ``entry`` is one of our own ``#lamella-override`` writes.

    Used to skip override transactions when counting "real" postings on
    an account — otherwise a fully-migrated orphan's originals + the
    matching overrides produce double-counting.
    """
    return "lamella-override" in (getattr(entry, "tags", None) or set())


def already_migrated_hashes(entries: Iterable) -> set[str]:
    """Set of original-txn hashes referenced by any override block's
    ``lamella-override-of`` meta.

    An original posting whose hash is in this set has been migrated to
    a different account via an override and should not be counted as a
    live posting on the source account.
    """
    out: set[str] = set()
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        if not is_override_txn(e):
            continue
        of_hash = (e.meta or {}).get("lamella-override-of")
        if of_hash:
            out.add(str(of_hash).strip('"'))
    return out


# --- Open/Close set diff ---------------------------------------------------


def open_paths(entries: Iterable) -> set[str]:
    """Accounts currently open in the ledger: Opens minus explicit
    Closes.

    Note that the scaffolder writers in ``registry/*_companion.py``
    intentionally use a broader reading (any entry with a ``.account``
    attribute), so a Close'd path is treated as "existing" and the
    scaffold click is a no-op. Reopening a closed account is a
    deliberate action that belongs in a different flow.
    """
    opens = {e.account for e in entries if isinstance(e, Open)}
    closes = {e.account for e in entries if isinstance(e, Close)}
    return opens - closes


# --- Posting counts --------------------------------------------------------


def count_unmigrated_postings(
    entries: Iterable,
    account_path: str,
    *,
    already_migrated: set[str] | None = None,
) -> int:
    """Count real (non-override, not-already-migrated) postings on
    ``account_path``.

    Pass a pre-computed ``already_migrated`` set when calling this in
    a loop over multiple paths — otherwise each call rebuilds the same
    set from the same entries.
    """
    if already_migrated is None:
        already_migrated = already_migrated_hashes(entries)
    count = 0
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        if is_override_txn(e):
            continue
        if _txn_hash(e) in already_migrated:
            continue
        for p in e.postings or ():
            if p.account == account_path:
                count += 1
                break
    return count


def unmigrated_postings_by_account(
    entries: Iterable,
    paths: Iterable[str],
    *,
    already_migrated: set[str] | None = None,
) -> dict[str, int]:
    """Batched version of :func:`count_unmigrated_postings` scoped to a
    set of paths. Returns a dict ``{path: count}`` with zero entries
    omitted."""
    target = set(paths)
    if already_migrated is None:
        already_migrated = already_migrated_hashes(entries)
    counts: dict[str, int] = {}
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        if is_override_txn(e):
            continue
        if _txn_hash(e) in already_migrated:
            continue
        for p in e.postings or ():
            if p.account in target:
                counts[p.account] = counts.get(p.account, 0) + 1
    return counts


def iter_unmigrated_txns_on(
    entries: Iterable,
    account_path: str,
    *,
    already_migrated: set[str] | None = None,
) -> Iterator[tuple]:
    """Yield ``(txn, hash)`` for every transaction carrying a posting
    on ``account_path`` that (a) isn't an override itself and (b)
    hasn't been migrated via an existing override. Used by the vehicle-
    migrate and entity-migrate "affected list" builders."""
    if already_migrated is None:
        already_migrated = already_migrated_hashes(entries)
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        if is_override_txn(e):
            continue
        h = _txn_hash(e)
        if h in already_migrated:
            continue
        for p in e.postings or ():
            if p.account == account_path:
                yield e, h
                break


# --- Vehicle orphan detection ----------------------------------------------


CANONICAL_VEHICLE_EXPENSE_RE = re.compile(
    r"^Expenses:[A-Za-z0-9]+:Vehicle:[A-Za-z0-9_]+:[A-Za-z0-9_]+$"
)
CANONICAL_VEHICLE_ASSET_RE = re.compile(
    r"^Assets:[A-Za-z0-9]+:Vehicle:[A-Za-z0-9_]+$"
)
# Vehicle-signal keywords that the orphan classifier looks for. Kept
# narrow on purpose: ``Auto``, ``Fuel``, ``Gas``, ``Gasoline`` are
# **deliberately excluded** because they are legitimate Schedule C
# line 9 / Schedule F line 17 / Schedule A Transportation chart-
# category names. Including them caused the bulk close-unused-orphans
# handler to wrongly close 10 entity-level chart accounts on a
# populated production ledger (Expenses:<Entity>:Auto for six Schedule
# C entities, Expenses:DeltaFarmRanch:Fuel for a Schedule F farm,
# Expenses:Personal:Transportation:Fuel for Schedule A). The keywords
# below are unambiguous vehicle signals: the literal ``Vehicle`` /
# ``Vehicles`` segment, US-market manufacturer names, specific model
# names the user has had in their fleet, and unique vehicle words
# (``Caravan``, ``Trailer``).
VEHICLE_KEYWORDS_RE = re.compile(
    r"(Vehicles?|Toyota|Honda|Ford|Dodge|Chevy|Chevrolet|"
    r"Tesla|Nissan|Hyundai|Subaru|Sequoia|Camry|Corolla|Civic|"
    r"Promaster|Caravan|Trailer)",
    re.IGNORECASE,
)


def is_vehicle_orphan(path: str) -> bool:
    """True if ``path`` looks vehicle-related but doesn't match the
    canonical ``Expenses:<Entity>:Vehicle:<Slug>:<Cat>`` or
    ``Assets:<Entity>:Vehicle:<Slug>`` shape.

    Used by ``/setup/vehicles`` to flag non-canonical paths for
    migration and by the bulk close-unused-orphans handler. **Must
    be conservative** — a false-positive here ends up as a Close
    directive on a legitimate chart account (see the keyword regex
    docstring for the production incident this guards against)."""
    if not path.startswith(("Assets:", "Expenses:")):
        return False
    if not VEHICLE_KEYWORDS_RE.search(path):
        return False
    if (
        CANONICAL_VEHICLE_EXPENSE_RE.match(path)
        or CANONICAL_VEHICLE_ASSET_RE.match(path)
    ):
        return False
    return True


# --- Delete-refusal predicates ---------------------------------------------
#
# Per the user's directive: delete is only legitimate for empty scaffolding.
# Any record carrying user-typed information OR with transactions on owned
# accounts must refuse delete. The path forward for "I want this gone" with
# non-empty content is deactivate / close, not delete.
#
# Auto-populated fields (NOT user information):
#   - entities: ``slug`` only (seed_entities only INSERTs the slug column)
#   - accounts_meta: ``account_path``, ``display_name`` (heuristic),
#     ``entity_slug`` (from path segment 1), ``kind`` (path-keyword inference,
#     can be NULL when ambiguous), ``seeded_from_ledger=1``
#
# User-typed fields (block delete when set):
#   - entities: ``display_name``, ``entity_type``, ``tax_schedule``
#   - accounts_meta: ``kind`` (when set — heuristic falls back to NULL),
#     ``institution``, ``last_four``, ``simplefin_account_id``, ``notes``,
#     ``icon``


class DeleteRefusal(Exception):
    """Raised when a delete-handler gate refuses the operation. Carries
    a human-readable, actionable message — caller surfaces it via a
    redirect ``?error=`` parameter so the manage-page template renders
    it verbatim."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


def assert_safe_to_delete_entity(
    conn,
    entries,
    slug: str,
    *,
    accounts_referencing_slug,
) -> None:
    """Refuse the delete if the entity carries user-typed information
    OR if any account it owns has live transactions. Raises
    :class:`DeleteRefusal` with an actionable message.

    ``accounts_referencing_slug`` is a callable (typically
    ``routes.setup._accounts_referencing_slug``) that returns rows for
    every accounts_meta row whose path uses the slug or whose
    ``entity_slug`` column matches. Passed in to avoid an import
    cycle with the route module."""
    row = conn.execute(
        "SELECT slug, display_name, entity_type, tax_schedule "
        "FROM entities WHERE slug = ?",
        (slug,),
    ).fetchone()
    if row is None:
        return  # nothing to delete; let the SQL DELETE be a no-op

    info_fields: list[str] = []
    if (row["display_name"] or "").strip():
        info_fields.append(f"display_name={row['display_name']!r}")
    if (row["entity_type"] or "").strip():
        info_fields.append(f"entity_type={row['entity_type']!r}")
    if (row["tax_schedule"] or "").strip():
        info_fields.append(f"tax_schedule={row['tax_schedule']!r}")

    rows = list(accounts_referencing_slug(conn, slug, only_open=False))
    paths = {r["account_path"] for r in rows}
    txn_counts = unmigrated_postings_by_account(entries, paths) if paths else {}
    blocking_txn_count = sum(txn_counts.values())
    blocking_account_count = sum(1 for n in txn_counts.values() if n > 0)

    if not info_fields and blocking_txn_count == 0:
        return  # empty scaffolding — safe to delete

    parts: list[str] = []
    if blocking_txn_count > 0:
        parts.append(
            f"{blocking_txn_count} transaction"
            + ("" if blocking_txn_count == 1 else "s")
            + f" across {blocking_account_count} account"
            + ("" if blocking_account_count == 1 else "s")
        )
    if info_fields:
        parts.append("user-set fields (" + ", ".join(info_fields[:3]) + ")")
    blocking = " and ".join(parts)
    next_steps: list[str] = []
    if blocking_txn_count > 0:
        next_steps.append("migrate or remove the postings")
        next_steps.append("close every account via 'Close unused opens'")
    if info_fields:
        next_steps.append("clear the user-set fields via the edit form")
    next_steps.append("then try delete again")
    next_steps.append("or use Deactivate to keep the row but hide it")
    raise DeleteRefusal(
        f"Cannot delete '{slug}': {blocking}. To delete: "
        + "; ".join(next_steps)
        + "."
    )


def assert_safe_to_delete_account_meta(
    conn,
    entries,
    account_path: str,
) -> None:
    """Refuse the accounts_meta delete if the row carries user-typed
    fields OR if the path has live transactions. Raises
    :class:`DeleteRefusal` with an actionable message."""
    row = conn.execute(
        "SELECT account_path, kind, institution, last_four, "
        "       simplefin_account_id, notes, icon "
        "FROM accounts_meta WHERE account_path = ?",
        (account_path,),
    ).fetchone()
    if row is None:
        return

    info_fields: list[str] = []
    for field in (
        "kind", "institution", "last_four", "simplefin_account_id",
        "notes", "icon",
    ):
        value = row[field]
        if value is not None and str(value).strip():
            info_fields.append(f"{field}={value!r}")

    posting_count = count_unmigrated_postings(entries, account_path)

    if not info_fields and posting_count == 0:
        return

    parts: list[str] = []
    if posting_count > 0:
        parts.append(
            f"{posting_count} posting"
            + ("" if posting_count == 1 else "s")
        )
    if info_fields:
        parts.append("user-set fields (" + ", ".join(info_fields[:3]) + ")")
    blocking = " and ".join(parts)
    next_steps: list[str] = []
    if posting_count > 0:
        next_steps.append("migrate the postings or close the account first")
    if info_fields:
        next_steps.append("clear the user-set fields via the row's edit form")
    next_steps.append("then try delete again")
    raise DeleteRefusal(
        f"Cannot delete '{account_path}': {blocking}. To delete: "
        + "; ".join(next_steps)
        + "."
    )
