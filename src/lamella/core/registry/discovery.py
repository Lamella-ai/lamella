# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Discover entities / vehicles / accounts from the Beancount ledger.

We never hardcode a list of businesses or vehicles. Instead, on every
boot (and on demand from the admin pages) we walk the ledger's Open
directives and detect which slugs are already in use, then `INSERT OR
IGNORE` them into the registry tables.

Convention:
  - Second path segment under Assets/Liabilities/Expenses/Income/Equity
    is the entity slug — except "Vehicles" which is a separate
    dimension and "FIXME" which is a placeholder.
  - Under `Expenses:Vehicles:*` the third segment is a vehicle slug.
  - Every Open directive becomes a candidate row in accounts_meta.

Discovery is additive: a slug appearing in the ledger but not in the
registry gets a row with NULL display_name (for entities / vehicles) or
a heuristic display_name (for accounts). Removing a slug from the
ledger does NOT delete the registry row — historical context matters.

**accounts_meta.kind_source provenance** (writers in this module):

This module is the source for the non-NULL ``kind_source`` values
in ``accounts_meta``. Two writers, two values:

  - ``seed_accounts_from_ledger`` writes ``'keyword'`` whenever
    :func:`_infer_account_kind` matches a path token (Phase 2
    keyword inference — see migration 053).
  - :func:`infer_kinds_by_sibling` writes ``'sibling'`` when peer
    accounts under the same ``Liabilities:{Entity}:{Institution}:*``
    branch share one kind unanimously.

The save path in ``registry/service.py`` is the only writer that
sets ``kind_source = NULL`` (terminal user-confirmed state).

Downstream consumers MUST treat ``'keyword'`` and ``'sibling'``
as overwritable signals (the sibling pass deliberately overwrites
``'keyword'``-stamped rows; the next pass after that doesn't
touch sibling-stamped rows because they already have stronger
provenance). NULL-source non-NULL-kind rows are user-confirmed
and never auto-rewritten.

Full state-space + UI hint behavior documented in
``migrations/053_accounts_meta_kind_source.sql``.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from collections import defaultdict
from typing import Iterable

from beancount.core.data import Open

from lamella.core.registry.alias import _heuristic

log = logging.getLogger(__name__)


class LedgerStructureMismatch(RuntimeError):
    """Raised when the ledger's account structure doesn't match the
    entity-first convention this tool expects. Caller may surface the
    message to the user and offer the bypass env var."""


# Guard parameters. Tuned to let fresh installs through while catching
# structural mismatch in populated ledgers.
_GUARD_MIN_ACCOUNTS = 10        # below this we're not confident enough to complain
_GUARD_MIN_PARSEABLE_RATIO = 0.20  # 20% of accounts must have a parseable entity segment
_GUARD_BYPASS_ENV_NEW = "LAMELLA_SKIP_DISCOVERY_GUARD"
_GUARD_BYPASS_ENV_LEGACY = "BCG_SKIP_DISCOVERY_GUARD"


_ENTITY_ROOTS = ("Assets", "Liabilities", "Expenses", "Income", "Equity")
_VEHICLE_ROOT_SEGMENT = "Vehicles"
# Roots that legitimize a second-segment slug as an entity. Equity-only
# second segments (OpeningBalances, Retained, etc.) are system accounts,
# not entities.
_ENTITY_CONFIRMING_ROOTS = ("Assets", "Liabilities")
# Public — also imported by ``beancount_io.balances`` and dashboard
# rendering so the same exclusion list keeps system slugs out of every
# entity-faced surface (entity registry, dashboard balance cards,
# entity dropdowns, …).
EXCLUDED_ENTITY_SEGMENTS = frozenset({
    "FIXME",
    "Vehicles",
    "Clearing",
    "OpeningBalances",
    "Opening-Balances",
    "Retained",
    "Unattributed",
    "Uncategorized",
    "PayPal",
    "Venmo",
    "Zelle",
    "RegularTransacionForSummariesFrom",
    "RegularTransacionForSummariesTo",
    "RegularTransactionForSummariesFrom",
    "RegularTransactionForSummariesTo",
})
_EXCLUDED_ENTITY_SEGMENTS = EXCLUDED_ENTITY_SEGMENTS


def discover_entity_slugs(entries: Iterable) -> set[str]:
    """Return the set of entity slugs appearing as the second segment
    of any Open directive under Assets / Liabilities roots.

    We require Assets or Liabilities presence because slugs that only
    show up under Equity (OpeningBalances, Retained, etc.) are system
    accounts, not entities. Expenses-only or Income-only slugs are also
    suspect but allowed if they also show up in Assets/Liabilities
    somewhere else.

    Phase 1.4: filters out slugs that carry a ``custom "entity-deleted"``
    tombstone in the ledger. Without the filter, deleting an entity
    via the UI was undone on the next boot — ``seed_entities`` would
    re-INSERT the row from the still-present ``Open Expenses:<slug>:*``
    directives. The tombstone is the §7 #7 fix's missing piece.
    """
    from lamella.core.registry.entity_writer import read_deleted_entity_slugs
    # Materialize once: read_deleted_entity_slugs walks Custom entries,
    # discover walks Opens. Two passes over a generator would yield empty
    # second time. Caller (sync_from_ledger) already lists() the entries.
    if not isinstance(entries, list):
        entries = list(entries)
    deleted = read_deleted_entity_slugs(entries)
    confirmed: set[str] = set()
    elsewhere: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Open):
            continue
        parts = entry.account.split(":")
        if len(parts) < 2:
            continue
        if parts[0] not in _ENTITY_ROOTS:
            continue
        slug = parts[1]
        if not slug or slug in _EXCLUDED_ENTITY_SEGMENTS:
            continue
        if slug in deleted:
            continue
        if parts[0] in _ENTITY_CONFIRMING_ROOTS:
            confirmed.add(slug)
        else:
            elsewhere.add(slug)
    # Only report slugs that appear in Assets or Liabilities (real money).
    return confirmed


def discover_vehicle_slugs(entries: Iterable) -> dict[str, str | None]:
    """Return {vehicle_slug: owning_entity_slug_or_None}.

    Supports both path conventions:
      - Personal:  Expenses:Vehicles:{slug}:*  or  Assets:Vehicles:{slug}
      - Business:  Expenses:{Entity}:Vehicles:{slug}:*
                   or Assets:{Entity}:Vehicles:{slug}

    Returns a dict because we need to know which entity (if any) owns
    each vehicle for proper scaffolding / display.

    Phase 1.4: filters out slugs carrying a ``custom "vehicle-deleted"``
    tombstone — same shape as the entity-deleted filter.
    """
    from lamella.features.vehicles.reader import read_deleted_vehicle_slugs
    if not isinstance(entries, list):
        entries = list(entries)
    deleted = read_deleted_vehicle_slugs(entries)
    out: dict[str, str | None] = {}
    for entry in entries:
        if not isinstance(entry, Open):
            continue
        parts = entry.account.split(":")
        if len(parts) < 3:
            continue
        # Personal: Expenses/Assets : Vehicles : <slug> : ...
        if parts[1] == _VEHICLE_ROOT_SEGMENT and parts[0] in ("Expenses", "Assets"):
            slug = parts[2]
            if slug and slug not in out and slug not in deleted:
                out[slug] = None
            continue
        # Business: Expenses/Assets : <Entity> : Vehicles : <slug> : ...
        if (
            len(parts) >= 4
            and parts[0] in ("Expenses", "Assets")
            and parts[2] == _VEHICLE_ROOT_SEGMENT
            and parts[1] not in _EXCLUDED_ENTITY_SEGMENTS
        ):
            slug = parts[3]
            entity = parts[1]
            if slug and slug not in deleted:
                # Business-owned wins if we've seen the slug both ways
                # (unlikely but possible).
                out[slug] = entity
    return out


def _read_deleted_property_slugs(entries: Iterable) -> set[str]:
    """Return slugs whose latest directive is a
    ``custom "property-deleted"`` tombstone. Mirrors the reader pattern
    in vehicles/reader.read_deleted_vehicle_slugs and
    entity_writer.read_deleted_entity_slugs."""
    from beancount.core.data import Custom
    from lamella.core.transform.custom_directive import custom_arg
    deleted: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "property-deleted":
            continue
        slug = custom_arg(entry, 0)
        if isinstance(slug, str) and slug.strip():
            deleted.add(slug.strip())
    return deleted


def discover_property_slugs(entries: Iterable) -> dict[str, str | None]:
    """Find real-property slugs in Assets:*:Property:* or Assets:Property:*.

    Phase 1.4: filters out slugs carrying a ``custom "property-deleted"``
    tombstone (the reader at properties/reader.py already honors the
    tombstone for reconstruct; this adds the filter to the boot-time
    discovery side so a deleted property can't resurrect from
    still-present Open directives).
    """
    if not isinstance(entries, list):
        entries = list(entries)
    deleted = _read_deleted_property_slugs(entries)
    out: dict[str, str | None] = {}
    for entry in entries:
        if not isinstance(entry, Open):
            continue
        parts = entry.account.split(":")
        if len(parts) < 3 or parts[0] != "Assets":
            continue
        if parts[1] == "Property" or parts[1] == "Properties":
            if (
                len(parts) >= 3 and parts[2]
                and parts[2] not in out
                and parts[2] not in deleted
            ):
                out[parts[2]] = None
            continue
        if (
            len(parts) >= 4
            and parts[1] not in _EXCLUDED_ENTITY_SEGMENTS
            and parts[2] in ("Property", "Properties")
        ):
            slug = parts[3]
            if slug and slug not in deleted:
                out[slug] = parts[1]
    return out


_LOAN_KEYWORDS = (
    "Mortgage", "Loan", "Heloc", "HELOC", "EIDL", "PPP",
    "StudentLoan", "StudLoan", "AutoLoan",
)


def discover_loan_candidates(entries: Iterable) -> list[dict]:
    """Return a list of plausible loan accounts from the ledger.

    A Liabilities account whose path contains a loan keyword (Mortgage,
    Loan, HELOC, EIDL, …) is surfaced as a candidate. Returned as
    [{account_path, entity, institution, suggested_slug, loan_type}].
    Used by /settings/loans to offer "link to existing account".
    """
    out: list[dict] = []
    seen_paths: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Open):
            continue
        path = entry.account
        if not path.startswith("Liabilities:"):
            continue
        if path in seen_paths:
            continue
        matched_keyword = None
        for kw in _LOAN_KEYWORDS:
            if kw.lower() in path.lower():
                matched_keyword = kw
                break
        if not matched_keyword:
            continue
        seen_paths.add(path)
        parts = path.split(":")
        entity = parts[1] if len(parts) >= 2 else None
        # Institution is typically the 3rd segment: Liabilities:Entity:Institution:Product
        institution = parts[2] if len(parts) >= 4 else None
        slug_seed = parts[-1]
        if slug_seed == matched_keyword and institution:
            # Path like Liabilities:Personal:BankTwo:Mortgage → slug "BankTwoMortgage"
            slug_seed = institution + matched_keyword
        kw_lower = matched_keyword.lower()
        loan_type = "mortgage" if "mortg" in kw_lower else (
            "heloc" if "heloc" == kw_lower else "other"
        )
        # HELOC / line-of-credit discoveries default to is_revolving=True;
        # the user can clear it before saving if it's actually a fixed-term
        # second-lien. Anything else defaults to False — amortizing.
        is_revolving = loan_type == "heloc" or "loc" in kw_lower
        out.append({
            "account_path": path,
            "entity": entity,
            "institution": institution,
            "suggested_slug": slug_seed,
            "loan_type": loan_type,
            "is_revolving": is_revolving,
        })
    return out


def discover_account_paths(entries: Iterable) -> list[str]:
    """Every Open directive's raw account path, in the order Beancount
    returns them (which is deterministic per parse)."""
    out: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Open):
            continue
        p = entry.account
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def _infer_account_kind(path: str) -> str | None:
    # Best-effort map from an account path to an ACCOUNT_KINDS value,
    # used to seed accounts_meta.kind on discovery so liquid-cash /
    # AI context / admin-listing queries aren't silently empty for
    # brand-new deploys. Only fires on unambiguous path keywords;
    # anything unclear stays NULL and the user labels in /settings/accounts.
    if not path:
        return None
    parts = path.split(":")
    root = parts[0]
    leaf = parts[-1]
    lower = path.lower()
    leaf_l = leaf.lower()

    if root == "Assets":
        # Virtual / clearing accounts: leaf is Transfers or Clearing,
        # or path contains a Transfers/Clearing segment (catches the
        # canonical Assets:{Entity}:Transfers:InFlight shape).
        if (
            "clearing" in lower
            or ":transfers:" in lower
            or leaf_l in ("transfers", "clearing", "inflight")
        ):
            return "virtual"
        if leaf_l == "cash":
            return "cash"
        if "checking" in leaf_l or leaf_l == "checkwriting":
            return "checking"
        if "savings" in leaf_l:
            return "savings"
        if any(k in lower for k in (
            ":brokerage", ":rothira", ":roth_ira", ":401k", ":403b",
            ":traditionalira", ":sepira", ":ira", ":investments",
        )):
            return "brokerage"
        # Payout-source accounts — marketplaces and payment processors
        # that hold funds on the user's behalf and emit periodic
        # disbursements to checking. Detected by leaf name (the user's
        # convention is `Assets:{Entity}:eBay`, `…:PayPal`, `…:Stripe`,
        # …, with sub-levels like `…:Amazon:Seller` for collisions).
        # Match anywhere in the path — including `:amazon:seller` and
        # `:amazon:Seller` shapes — so the inference catches both flat
        # and nested layouts. Order matters: this runs after virtual/
        # cash/checking/savings/brokerage so a user's literal `Cash`
        # leaf doesn't get reclassified by a coincidence.
        PAYOUT_KEYWORDS = (
            "ebay", "paypal", "stripe", "shopify", "square",
            "etsy", "venmo", "cashapp", "cash_app",
            ":amazon:", ":amazonseller", "amazonseller",
        )
        if any(k in lower for k in PAYOUT_KEYWORDS):
            return "payout"
        return "asset"

    if root == "Liabilities":
        # Tax-payables come first — "salestaxpayable" contains "payable"
        # and we want the tax classification to win over the
        # intercompany-payable bail-out below.
        TAX_KEYWORDS = (
            "salestax", "taxespayable", "taxpayable",
            "usetax", "vatpayable", "withholding",
        )
        if any(k in lower for k in TAX_KEYWORDS):
            return "tax_liability"
        # Intercompany payables (Liabilities:Entity:Payable:ToOther) are
        # neither a card nor a loan — leave them unclassified so the
        # user labels them by hand.
        if ":payable:" in lower or lower.endswith(":payable"):
            return None
        # Line-of-credit / HELOC. Substring match (not anchored to ':')
        # so "BankOneLineOfCredit" leaves classify, not just
        # ":LineOfCredit".
        if any(k in lower for k in ("lineofcredit", "heloc")):
            return "line_of_credit"
        # Loan-shaped accounts. Long keywords use plain substring match
        # so "BankTwoMortgage" or "BankOneAutoLoan" classify
        # without a leading ':'. Short / ambiguous keywords (sba, ppp)
        # require a colon boundary because "USBank" contains "sba",
        # "Apple" contains "ppl", etc.
        LOAN_KEYWORDS_SUBSTR = (
            "mortgage", "eidl", "autoloan", "studentloan", "studloan",
        )
        LOAN_KEYWORDS_BOUNDED = ("sba", "ppp")
        if (
            any(k in lower for k in LOAN_KEYWORDS_SUBSTR)
            or any(f":{k}" in lower for k in LOAN_KEYWORDS_BOUNDED)
            or leaf_l in (
                "mortgage", "loan", "eidl", "sba", "ppp", "autoloan",
            )
        ):
            return "loan"
        # Known credit-card issuers and card-shaped leaf names.
        # Bank One small-business cards (AffiliateD, BusinessElite),
        # Chase World Elite / Propel Amex, U.S. Bank Cash+ etc. are
        # card brands that carry no "credit"/"visa" token but should
        # still classify as credit_card.
        CC_KEYWORDS = (
            "visa", "mastercard", "amex", "americanexpress", "deltacard",
            "discover", "capitalone", "chasefreedom", "primevisa",
            "amazonprime", "amazonorders", "costcociti", "platinum",
            "creditcard", ":credit",
            "signify", "businesselite", "worldelite", "propelamex",
            "cashwise", "cashplus",
        )
        if any(k in lower for k in CC_KEYWORDS):
            return "credit_card"
        return None

    return None


def _short_name_for_path(path: str) -> str:
    """Heuristic-only short display name (no registry lookups).

    Used during the migration seed before any entity rows exist. We
    strip the root, drop a trailing institution segment if it matches
    a known bank prefix earlier in the path, and camel-split the
    remainder.
    """
    # Reuse the same logic as alias._heuristic but with an empty
    # connection — entities table is empty during the seed phase.
    class _NullConn:
        def execute(self, *args, **kwargs):
            return _NullCursor()

    class _NullCursor:
        def fetchone(self):
            return None

    return _heuristic(_NullConn(), path)  # type: ignore[arg-type]


def seed_entities(conn: sqlite3.Connection, entries: Iterable) -> int:
    """Insert discovered entity slugs. Returns count of new rows."""
    slugs = discover_entity_slugs(entries)
    added = 0
    for slug in slugs:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO entities (slug) VALUES (?)",
            (slug,),
        )
        if cursor.rowcount:
            added += 1
    return added


def _purge_system_slug_entities(conn: sqlite3.Connection) -> int:
    """Remove entity rows whose slug is a known system segment
    (``OpeningBalances`` / ``Clearing`` / ``Retained`` / …).

    Pre-blocklist boots could insert these as if they were entities.
    The dashboard renders one balance card per entity, so a stale row
    surfaces as a phantom "OpeningBalances" entity with all-zero
    balances. Discovery already excludes these segments, but rows
    inserted before the exclusion list tightened persist until
    purged.

    Idempotent — safe to call on every boot. No tombstone needed
    because discovery itself never re-creates these slugs (they're
    in the exclusion list).
    """
    rows = conn.execute(
        "SELECT slug FROM entities"
    ).fetchall()
    purged = 0
    for r in rows:
        slug = r["slug"]
        if not slug or slug not in EXCLUDED_ENTITY_SEGMENTS:
            continue
        conn.execute("DELETE FROM entities WHERE slug = ?", (slug,))
        purged += 1
    if purged:
        conn.commit()
        log.info(
            "registry: purged %d system-slug entity row(s) "
            "(OpeningBalances/Clearing/etc.)",
            purged,
        )
    return purged


def seed_vehicles(conn: sqlite3.Connection, entries: Iterable) -> int:
    vmap = discover_vehicle_slugs(entries)
    added = 0
    for slug, entity in vmap.items():
        # Does the entity_slug column exist? (post-migration-011)
        try:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO vehicles (slug, entity_slug) VALUES (?, ?)",
                (slug, entity),
            )
        except sqlite3.OperationalError:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO vehicles (slug) VALUES (?)",
                (slug,),
            )
        if cursor.rowcount:
            added += 1
    return added


def seed_properties(conn: sqlite3.Connection, entries: Iterable) -> int:
    pmap = discover_property_slugs(entries)
    added = 0
    for slug, entity in pmap.items():
        try:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO properties (slug, property_type, entity_slug) "
                "VALUES (?, 'other', ?)",
                (slug, entity),
            )
        except sqlite3.OperationalError:
            # properties table not present (pre-migration-011) — skip.
            return 0
        if cursor.rowcount:
            added += 1
    return added


def seed_accounts_meta(conn: sqlite3.Connection, entries: Iterable) -> int:
    """Insert discovered account paths with heuristic short display names.

    Collision handling: if two paths would produce the same short name,
    disambiguate by appending ' ({institution})' to both. The institution
    is the third path segment when it matches a known bank prefix.

    Phase 1.4: filters out paths carrying an ``account-meta-deleted``
    tombstone so a hard-deleted accounts_meta row doesn't resurrect on
    the next boot from still-present Open directives. Mirrors the
    entity / vehicle / property tombstone shape.
    """
    from lamella.core.registry.account_meta_writer import (
        read_deleted_account_paths,
    )
    if not isinstance(entries, list):
        entries = list(entries)
    deleted_paths = read_deleted_account_paths(entries)
    paths = [p for p in discover_account_paths(entries) if p not in deleted_paths]

    # First pass: compute heuristic name per path.
    short_by_path: dict[str, str] = {}
    for path in paths:
        short_by_path[path] = _short_name_for_path(path)

    # Detect collisions and disambiguate.
    buckets: dict[str, list[str]] = defaultdict(list)
    for path, short in short_by_path.items():
        buckets[short].append(path)

    def _institution(path: str) -> str | None:
        parts = path.split(":")
        # Assets:Entity:Institution:Account → institution is parts[2]
        if len(parts) >= 3:
            return parts[2]
        return None

    display_by_path: dict[str, str] = {}
    for short, group in buckets.items():
        if len(group) == 1:
            display_by_path[group[0]] = short
            continue
        for path in group:
            inst = _institution(path)
            if inst:
                display_by_path[path] = f"{short} ({inst})"
            else:
                display_by_path[path] = short

    # Second pass: determine entity_slug from the second segment; insert
    # each path with the resolved display name.
    added = 0
    for path in paths:
        parts = path.split(":")
        entity_slug: str | None = None
        if len(parts) >= 2 and parts[1] not in _EXCLUDED_ENTITY_SEGMENTS:
            entity_slug = parts[1]

        # Skip Expenses:* and Income:* from accounts_meta — they're not
        # "accounts" in the bank-account sense. We seed them only for
        # Assets/Liabilities/Equity roots where the user will want a
        # display name in the UI.
        if parts and parts[0] not in ("Assets", "Liabilities", "Equity"):
            continue

        inferred_kind = _infer_account_kind(path)
        # kind_source = 'keyword' iff the keyword inference produced a
        # value. Phase 2: lets the recovery editor distinguish "AI
        # guessed" from "user typed" so we can render a confirmation
        # hint and so the sibling pass doesn't transitively re-infer
        # off its own output.
        kind_source = "keyword" if inferred_kind is not None else None
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO accounts_meta
                (account_path, display_name, entity_slug, kind, kind_source,
                 seeded_from_ledger, created_at)
            VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
            """,
            (
                path, display_by_path.get(path, path), entity_slug,
                inferred_kind, kind_source,
            ),
        )
        if cursor.rowcount:
            added += 1

    # Backfill kind for previously-seeded rows that predate this inference.
    # Only touches rows where the user hasn't set anything yet. Stamps
    # kind_source='keyword' alongside so later passes can tell this apart
    # from a user-set kind.
    for path in paths:
        inferred = _infer_account_kind(path)
        if inferred is None:
            continue
        conn.execute(
            "UPDATE accounts_meta "
            "   SET kind = ?, kind_source = 'keyword' "
            " WHERE account_path = ? AND (kind IS NULL OR kind = '')",
            (inferred, path),
        )
    return added


def _sibling_prefix(path: str) -> str | None:
    """Return the ``Root:Entity:Institution:`` prefix peers must share,
    or ``None`` if ``path`` is too shallow / wrong-rooted to participate
    in sibling inference.

    Phase 2 contract:
      - Liabilities:{Entity}:{Institution}:{Leaf...} → 4+ segments OK
      - Assets:{Entity}:{Institution}:{Leaf...}      → 4+ segments OK
      - Anything shallower (Liabilities:Personal:EIDL,
        Assets:BetaCorp:Cash) is too broad — neighboring accounts at
        Liabilities:Personal:* mix loans, cards, and intercompany
        payables. Skip.
      - Excluded entity segments (Vehicles, Clearing, OpeningBalances,
        …) don't define a real entity, so the prefix isn't meaningful
        for inference. Skip.
    """
    parts = path.split(":")
    if len(parts) < 4:
        return None
    if parts[0] not in ("Liabilities", "Assets"):
        return None
    if parts[1] in _EXCLUDED_ENTITY_SEGMENTS:
        return None
    return ":".join(parts[:3]) + ":"


def infer_kinds_by_sibling(conn: sqlite3.Connection) -> int:
    """Fill NULL-kind rows whose siblings under the same
    ``Root:Entity:Institution:`` branch are all classified the same way.

    Phase 2 of /setup/recovery (see SETUP_IMPLEMENTATION.md). The
    keyword pass at :func:`_infer_account_kind` is per-account in
    isolation; brand-named accounts like ``BankOne:AffiliateD`` or
    ``BankOne:BusinessElite`` carry no card keyword, so the user
    had to pick the kind manually for every one.

    Decision rules (locked, see SETUP_IMPLEMENTATION.md Phase 2 spec):

    - **Eligible peers** are accounts under the same prefix
      (computed by :func:`_sibling_prefix`), with a non-NULL ``kind``,
      whose ``kind_source`` is NULL or ``'keyword'``. We don't
      transitively re-infer off our own output.
    - **Strict zero-conflicts**: if every eligible peer shares one
      kind (≥1 peer required), apply that kind to the NULL row and
      stamp ``kind_source='sibling'``. Any conflict (e.g., one
      credit_card + one loan) and we leave the row NULL — the user
      labels by hand.

    Returns the count of rows updated.
    """
    rows = conn.execute(
        "SELECT account_path FROM accounts_meta WHERE kind IS NULL OR kind = ''"
    ).fetchall()
    if not rows:
        return 0

    updated = 0
    for r in rows:
        path = r["account_path"]
        prefix = _sibling_prefix(path)
        if prefix is None:
            continue
        peers = conn.execute(
            """
            SELECT DISTINCT kind
              FROM accounts_meta
             WHERE account_path LIKE ? || '%'
               AND account_path != ?
               AND kind IS NOT NULL
               AND kind != ''
               AND (kind_source IS NULL OR kind_source = 'keyword')
            """,
            (prefix, path),
        ).fetchall()
        peer_kinds = {p["kind"] for p in peers}
        if len(peer_kinds) != 1:
            # Zero peers OR conflicting peers → no signal we trust.
            continue
        consensus = next(iter(peer_kinds))
        cur = conn.execute(
            """
            UPDATE accounts_meta
               SET kind = ?, kind_source = 'sibling'
             WHERE account_path = ?
               AND (kind IS NULL OR kind = '')
            """,
            (consensus, path),
        )
        updated += cur.rowcount
    return updated


def sibling_hint_for(
    conn: sqlite3.Connection, path: str
) -> str | None:
    """Build the user-facing rationale string for a sibling-derived
    kind. Returns text like "based on the other Bank One accounts
    under BetaCorp" or ``None`` if no hint can be constructed.

    Used by the recovery editor template to render the confirmation
    cue without storing the rationale in the DB. Cheap re-derivation
    is fine — it only fires for rows the user is currently labeling.
    """
    prefix = _sibling_prefix(path)
    if prefix is None:
        return None
    parts = path.split(":")
    if len(parts) < 4:
        return None
    entity = parts[1]
    institution = parts[2]
    peer_count = conn.execute(
        """
        SELECT COUNT(*) AS n
          FROM accounts_meta
         WHERE account_path LIKE ? || '%'
           AND account_path != ?
           AND kind IS NOT NULL
           AND kind != ''
           AND (kind_source IS NULL OR kind_source = 'keyword')
        """,
        (prefix, path),
    ).fetchone()
    n = peer_count["n"] if peer_count else 0
    if n <= 0:
        return None
    other = "other account" if n == 1 else f"other {n} accounts"
    return f"based on the {other} under {entity}:{institution}"


def sync_simplefin_account_map(conn: sqlite3.Connection, map_path) -> int:
    """Read a simplefin_account_map YAML file ({simplefin_id: account_path})
    and copy each mapping onto the matching accounts_meta row. Returns
    the count of rows updated. Idempotent; leaves unmatched accounts
    alone."""
    try:
        from lamella.features.bank_sync.ingest import load_account_map
    except Exception:
        return 0
    try:
        mapping = load_account_map(map_path)
    except Exception as exc:  # noqa: BLE001
        log.warning("simplefin map load failed: %s", exc)
        return 0
    if not mapping:
        return 0
    updated = 0
    for simplefin_id, account_path in mapping.items():
        if not simplefin_id or not account_path:
            continue
        cursor = conn.execute(
            "UPDATE accounts_meta SET simplefin_account_id = ? "
            "WHERE account_path = ? AND (simplefin_account_id IS NULL OR simplefin_account_id = '')",
            (str(simplefin_id), account_path),
        )
        if cursor.rowcount:
            updated += int(cursor.rowcount)
    return updated


def assert_entity_first_structure(entries: Iterable) -> None:
    """Refuse to run discovery when the ledger clearly isn't entity-first.

    This tool derives entity slugs from the second segment of each Open
    directive (``Expenses:<Entity>:<Category>``). If pointed at a ledger
    organized differently (``Expenses:<Category>:<Entity>`` or non-English
    roots or flat structure), discovery would silently produce empty
    registries and every UI page that depends on entity grouping would
    show nothing — looking broken rather than mis-configured.

    We check the **ratio** of accounts whose second segment parses as an
    entity, not the absolute count — so fresh installs with 3 accounts
    aren't blocked. Bypass with ``LAMELLA_SKIP_DISCOVERY_GUARD=1``
    (legacy ``BCG_SKIP_DISCOVERY_GUARD`` also accepted; intended for
    setup / debugging; not for production).
    """
    from lamella.utils._legacy_env import read_env
    if read_env(_GUARD_BYPASS_ENV_NEW):
        return
    total = 0
    parseable = 0
    for entry in entries:
        if not isinstance(entry, Open):
            continue
        parts = entry.account.split(":")
        if len(parts) < 2 or parts[0] not in _ENTITY_ROOTS:
            continue
        total += 1
        if parts[1] and parts[1] not in _EXCLUDED_ENTITY_SEGMENTS:
            parseable += 1
    if total < _GUARD_MIN_ACCOUNTS:
        return  # too small to judge
    ratio = parseable / total if total else 0.0
    if ratio >= _GUARD_MIN_PARSEABLE_RATIO:
        return
    raise LedgerStructureMismatch(
        "This tool expects accounts in Expenses:<Entity>:<Category> form "
        f"(entity-first). Found {total} accounts under Assets/Liabilities/"
        f"Expenses/Income/Equity, only {parseable} with a parseable entity "
        f"segment ({ratio:.0%}). If your ledger uses a different "
        f"convention, this tool won't work as-is. If you're a new user "
        f"setting up and have few accounts, set {_GUARD_BYPASS_ENV_NEW}=1 to "
        f"bypass (legacy {_GUARD_BYPASS_ENV_LEGACY}=1 also accepted)."
    )


def sync_from_ledger(
    conn: sqlite3.Connection,
    entries: Iterable,
    simplefin_map_path=None,
) -> dict[str, int]:
    """Run all discovery passes. Idempotent — safe to call on every boot.
    Returns counts of newly inserted rows per table.

    Raises ``LedgerStructureMismatch`` when the ledger isn't entity-first
    and the bypass env var isn't set. Caller decides whether to turn
    that into an HTTP 500, a startup abort, or a user-visible warning.
    """
    # Entries is typically a generator; materialize once so we can scan
    # twice (guard + seeders) without re-parsing.
    entries_list = list(entries)
    assert_entity_first_structure(entries_list)
    stats = {
        "entities_purged": _purge_system_slug_entities(conn),
        "entities": seed_entities(conn, entries_list),
        "vehicles": seed_vehicles(conn, entries_list),
        "properties": seed_properties(conn, entries_list),
        "accounts_meta": seed_accounts_meta(conn, entries_list),
    }
    # Phase 2: after the keyword pass has seeded what it can, fill in
    # NULL-kind rows from sibling consensus. Bank One AffiliateD under
    # Liabilities:BetaCorp:BankOne:* infers credit_card from the rest
    # of the branch even though the leaf token has no card keyword.
    stats["accounts_kind_sibling"] = infer_kinds_by_sibling(conn)
    if simplefin_map_path is not None:
        stats["simplefin_mappings"] = sync_simplefin_account_map(conn, simplefin_map_path)
    if any(stats.values()):
        log.info("registry discovery inserted rows: %s", stats)
    return stats
