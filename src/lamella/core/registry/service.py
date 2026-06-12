# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Registry service — CRUD + ledger-aware helpers for entities,
accounts, vehicles, loans, and merchant memory.

Kept separate from the routes so the card UX, the admin pages, and
the AI prompt builder can all call the same functions.
"""
from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

from lamella.core.registry.alias import alias_for


# ---------------------------------------------------------------------------
# Slug validation + normalization
# ---------------------------------------------------------------------------

_SLUG_RE = re.compile(r"^[A-Z][A-Za-z0-9-]*$")
_SLUG_NORM_STRIP = re.compile(r"[^A-Za-z0-9]+")

# Beancount's five canonical roots — first segment of every
# account path. Anything else is rejected by the Beancount parser.
_BEANCOUNT_ROOTS = frozenset({
    "Assets", "Liabilities", "Equity", "Income", "Expenses",
})


class InvalidAccountSegmentError(ValueError):
    """Raised when a Beancount account path has a segment that
    violates the [A-Z][A-Za-z0-9-]* rule (per ADR-0045) or whose
    first segment isn't one of the five canonical roots.

    Carries `path` and `bad_segments` so callers can surface a
    precise error to the user without re-parsing the path."""

    def __init__(self, path: str, bad_segments: list[str], reason: str):
        self.path = path
        self.bad_segments = bad_segments
        self.reason = reason
        super().__init__(
            f"invalid account path {path!r}: {reason} "
            f"(offending segments: {bad_segments!r})"
        )


def validate_beancount_account(path: str) -> str:
    """Validate a full Beancount account path per ADR-0045.

    Splits on ``:`` and asserts every segment matches the slug
    regex (`[A-Z][A-Za-z0-9-]*`). The first segment must be one
    of the five canonical Beancount roots. Returns the path
    unchanged on success; raises :class:`InvalidAccountSegmentError`
    on failure.

    Catches the bug class where a code path constructed an
    account string from a hand-coded constant ("eBay", "X", "1st")
    and tried to write it to a `.bean` file, only for bean-check
    to reject it after the write — leaving the user with a
    silent failure or a half-written ledger.

    Wire this into every account-write boundary (writers,
    overrides, scaffolders) so the failure is local to the call
    site, not surfaced as a parser error from a downstream tool.
    """
    if not path or not isinstance(path, str):
        raise InvalidAccountSegmentError(
            str(path), [], "path must be a non-empty string",
        )
    segments = path.split(":")
    if segments[0] not in _BEANCOUNT_ROOTS:
        raise InvalidAccountSegmentError(
            path, [segments[0]],
            f"first segment must be one of {sorted(_BEANCOUNT_ROOTS)}",
        )
    bad = [s for s in segments if not _SLUG_RE.match(s)]
    if bad:
        raise InvalidAccountSegmentError(
            path, bad,
            "every segment must start with an uppercase letter and "
            "contain only letters, digits, or hyphens "
            "(Beancount grammar; ADR-0045)",
        )
    return path


# Roots for which the entity-first rule applies.  Equity:OpeningBalances
# and Equity:Retained are system paths where the segment after the root
# is *not* an entity slug (e.g. ``Equity:OpeningBalances:Personal:...``
# has "OpeningBalances" as a system label, not an entity slug, followed
# by the entity slug at position 2).  We deliberately exclude Equity
# from the entity-first check so those system paths are not rejected.
_ENTITY_FIRST_ROOTS = frozenset({"Assets", "Liabilities", "Income", "Expenses"})


def validate_entity_first_path(
    path: str,
    known_entity_slugs: frozenset[str] | set[str] | None = None,
) -> str:
    """Enforce the ADR-0042 entity-first rule on a Beancount account path.

    After the canonical-root check enforced by
    :func:`validate_beancount_account`, this function asserts that the
    *second* segment of every ``Assets:``, ``Liabilities:``, ``Income:``,
    and ``Expenses:`` path is a registered entity slug — not a category
    label like ``Vehicles``, ``Property``, or ``Custom``.

    The rule in full:
      - ``Assets:<Entity>:<Institution>:<Name>``
      - ``Liabilities:<Entity>:<Institution>:<Name>``
      - ``Expenses:<Entity>:<Category>[:…]``
      - ``Income:<Entity>:<Category>[:…]``

    Violations (canonical-shape examples):
      - ``Assets:Vehicles:VAcmeVan2``   — "Vehicles" is not an entity
      - ``Assets:Personal:Property:TestProperty1`` — "Property" is in
        position 3 (correct); this path IS valid once "Personal" is
        confirmed as an entity
      - ``Assets:Personal:Vehicle:VAcmeVan3`` — same: valid when
        "Personal" is a known entity
      - ``Expenses:Vehicles:VAcmeVan2:Fuel`` — "Vehicles" is not an entity

    ``Equity`` paths are exempt because system paths like
    ``Equity:OpeningBalances:…`` and ``Equity:Retained:…`` have a
    system label in position 1.

    Parameters
    ----------
    path:
        A Beancount account path that has already passed
        :func:`validate_beancount_account` (i.e. structurally valid segments).
    known_entity_slugs:
        If provided, the second segment must be a member of this set.
        When ``None``, only structural requirements are checked (segment
        count ≥ 2 for applicable roots); the caller is responsible for
        verifying registry membership separately.

    Returns ``path`` unchanged on success.  Raises
    :class:`InvalidAccountSegmentError` on violation.
    """
    if not path:
        raise InvalidAccountSegmentError(
            str(path), [], "path must be a non-empty string",
        )
    segments = path.split(":")
    root = segments[0]
    if root not in _ENTITY_FIRST_ROOTS:
        # Equity and any unexpected roots are not subject to this rule.
        return path
    if len(segments) < 2:
        raise InvalidAccountSegmentError(
            path, [root],
            f"ADR-0042 requires at least two segments for {root}: paths; "
            "found only the root",
        )
    entity_seg = segments[1]
    if known_entity_slugs is not None and entity_seg not in known_entity_slugs:
        raise InvalidAccountSegmentError(
            path, [entity_seg],
            f"ADR-0042: second segment must be a registered entity slug; "
            f"{entity_seg!r} is not in the known entity registry. "
            f"Use Assets:<Entity>:… / Expenses:<Entity>:… not "
            f"Assets:{entity_seg}:… where '{entity_seg}' is a category label",
        )
    return path


def is_valid_slug(slug: str) -> bool:
    return bool(slug) and bool(_SLUG_RE.match(slug))


def normalize_slug(raw: str, fallback_display_name: str | None = None) -> str | None:
    """Produce a Beancount-legal slug from user input.

    Order of attempts:
      1. If `raw` already validates, return it.
      2. Run `raw` through `suggest_slug` (strips punctuation, PascalCases,
         prefixes "X" if it would start with a digit).
      3. Fall back to `suggest_slug(fallback_display_name)`.
      4. Return None if nothing produced a valid slug.
    """
    if raw and is_valid_slug(raw):
        return raw
    if raw:
        candidate = suggest_slug(raw)
        if candidate and is_valid_slug(candidate):
            return candidate
    if fallback_display_name:
        candidate = suggest_slug(fallback_display_name)
        if candidate and is_valid_slug(candidate):
            return candidate
    return None


def disambiguate_slug(
    conn: sqlite3.Connection, base: str, table: str,
) -> str:
    """Append a numeric suffix to ``base`` until the result doesn't
    already exist in ``table.slug``. Used so POST /vehicles (and
    peers) can gracefully handle 5 identical-year/make/model vehicles
    without 500-ing on the UNIQUE constraint.

    Convention: ``V2019RamVanOne`` taken → return
    ``V2019RamVanOne2``, then ``…3``, … . Skips suffix ``1`` so
    the first duplicate gets an obvious ``2``.

    ``table`` is injected raw — callers pass a known constant like
    'vehicles' or 'properties'. Refuses unrecognized table names so
    this can't become a SQL-injection foothold.
    """
    if table not in {"vehicles", "properties", "loans"}:
        raise ValueError(f"disambiguate_slug: unknown table {table!r}")
    if not base:
        return base
    # Fast path: base itself available.
    row = conn.execute(
        f"SELECT 1 FROM {table} WHERE slug = ? LIMIT 1", (base,),
    ).fetchone()
    if row is None:
        return base
    # Scan suffixes 2..999 for the first free.
    for n in range(2, 1000):
        candidate = f"{base}{n}"
        row = conn.execute(
            f"SELECT 1 FROM {table} WHERE slug = ? LIMIT 1", (candidate,),
        ).fetchone()
        if row is None:
            return candidate
    # Fallback — no caller should ever hit this with 1000 fleet vehicles.
    return base


def suggest_slug(display_name: str) -> str:
    """Suggest a Beancount-legal slug from a human display name.

    Strategy:
      - Strip punctuation and whitespace.
      - Title-case word boundaries to produce PascalCase.
      - If the result starts with a digit, prefix with "X".
    """
    if not display_name:
        return ""
    cleaned = _SLUG_NORM_STRIP.sub(" ", display_name).strip()
    if not cleaned:
        return ""
    words = [w for w in cleaned.split() if w]
    pascal = "".join(w[0].upper() + w[1:] for w in words)
    if not pascal:
        return ""
    if not pascal[0].isalpha():
        pascal = "X" + pascal
    if not pascal[0].isupper():
        pascal = pascal[0].upper() + pascal[1:]
    return pascal


def fuzzy_match_slug(
    conn: sqlite3.Connection, display_name: str, known_slugs: list[str] | None = None,
) -> str | None:
    """Given a human-typed name, return a known slug that likely matches.

    Used by admin forms to suggest "did you mean WidgetCo?" when the user
    types "WIDGET CO LLC." Returns None if no good match.
    """
    if not display_name:
        return None
    if known_slugs is None:
        known_slugs = [
            r["slug"]
            for r in conn.execute("SELECT slug FROM entities").fetchall()
        ]
    norm_input = _SLUG_NORM_STRIP.sub("", display_name).lower()
    if not norm_input:
        return None
    for slug in known_slugs:
        if _SLUG_NORM_STRIP.sub("", slug).lower() == norm_input:
            return slug
    # Prefix match as a softer fallback.
    for slug in known_slugs:
        norm_slug = _SLUG_NORM_STRIP.sub("", slug).lower()
        if norm_input.startswith(norm_slug) or norm_slug.startswith(norm_input):
            return slug
    return None


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------


@dataclass
class Entity:
    slug: str
    display_name: str | None
    entity_type: str | None
    tax_schedule: str | None
    start_date: str | None
    ceased_date: str | None
    is_active: int
    sort_order: int
    notes: str | None


def list_entities(
    conn: sqlite3.Connection, *, include_inactive: bool = False
) -> list[Entity]:
    sql = "SELECT * FROM entities"
    if not include_inactive:
        sql += " WHERE is_active = 1"
    sql += " ORDER BY sort_order, COALESCE(display_name, slug)"
    rows = conn.execute(sql).fetchall()
    return [Entity(**{k: r[k] for k in r.keys() if k in Entity.__dataclass_fields__}) for r in rows]


def upsert_entity(
    conn: sqlite3.Connection,
    *,
    slug: str,
    display_name: str | None = None,
    entity_type: str | None = None,
    tax_schedule: str | None = None,
    start_date: str | None = None,
    ceased_date: str | None = None,
    is_active: int = 1,
    sort_order: int | None = None,
    notes: str | None = None,
    classify_context: str | None = None,
) -> None:
    if not is_valid_slug(slug):
        raise ValueError(f"invalid entity slug {slug!r}")
    # Build columns dynamically so unspecified values preserve existing data.
    fields = {
        "display_name": display_name,
        "entity_type": entity_type,
        "tax_schedule": tax_schedule,
        "start_date": start_date,
        "ceased_date": ceased_date,
        "is_active": is_active,
        "notes": notes,
        "classify_context": classify_context,
    }
    if sort_order is not None:
        fields["sort_order"] = sort_order
    existing = conn.execute("SELECT slug FROM entities WHERE slug = ?", (slug,)).fetchone()
    if existing is None:
        cols = ["slug"] + list(fields.keys())
        placeholders = ", ".join("?" for _ in cols)
        conn.execute(
            f"INSERT INTO entities ({', '.join(cols)}) VALUES ({placeholders})",
            [slug, *fields.values()],
        )
    else:
        # Only update fields that were explicitly passed (not None, except
        # for nullable dates / notes where None means "clear").
        # For simplicity, update all provided fields — admins can clear
        # by submitting blanks.
        assignments = ", ".join(f"{k} = ?" for k in fields.keys())
        conn.execute(
            f"UPDATE entities SET {assignments} WHERE slug = ?",
            [*fields.values(), slug],
        )


# ---------------------------------------------------------------------------
# Accounts
# ---------------------------------------------------------------------------


@dataclass
class AccountMeta:
    account_path: str
    display_name: str
    kind: str | None
    institution: str | None
    last_four: str | None
    entity_slug: str | None
    simplefin_account_id: str | None
    is_active: int
    seeded_from_ledger: int
    opened_on: str | None
    closed_on: str | None
    notes: str | None
    created_at: str | None = None
    kind_source: str | None = None


ACCOUNT_KINDS = (
    "checking", "savings", "credit_card", "line_of_credit",
    "loan", "tax_liability", "brokerage", "cash", "asset", "virtual",
    # `payout`: marketplace / payment-processor account that holds funds
    # on the user's behalf between sales and disbursement (eBay, PayPal,
    # Stripe, Shopify, Square, Etsy, Amazon Seller, Venmo, Cash App,
    # etc.). Payouts to checking are structurally a transfer FROM this
    # account, not income — sales / fees / refunds go directly into
    # the payout account from a separate import path. Detected
    # automatically by `lamella.features.bank_sync.payout_sources`.
    "payout",
)


def list_accounts(
    conn: sqlite3.Connection, *, include_closed: bool = False
) -> list[AccountMeta]:
    sql = "SELECT * FROM accounts_meta"
    if not include_closed:
        sql += " WHERE closed_on IS NULL"
    sql += " ORDER BY COALESCE(entity_slug, ''), display_name"
    rows = conn.execute(sql).fetchall()
    out: list[AccountMeta] = []
    for r in rows:
        kwargs = {k: r[k] for k in r.keys() if k in AccountMeta.__dataclass_fields__}
        out.append(AccountMeta(**kwargs))
    return out


def update_account(
    conn: sqlite3.Connection,
    account_path: str,
    *,
    display_name: str | None = None,
    kind: str | None = None,
    institution: str | None = None,
    last_four: str | None = None,
    entity_slug: str | None = None,
    simplefin_account_id: str | None = None,
    notes: str | None = None,
    closed_on: str | None = None,
) -> None:
    fields: dict[str, Any] = {}
    if display_name is not None:
        fields["display_name"] = display_name
    if kind is not None:
        fields["kind"] = kind or None
        # Phase 2: any user-driven update of `kind` is a confirmation —
        # clear the kind_source provenance marker so the sibling-hint
        # UI doesn't keep nagging on a row the user has already
        # signed off on.
        fields["kind_source"] = None
    if institution is not None:
        fields["institution"] = institution or None
    if last_four is not None:
        fields["last_four"] = (last_four or None)
    if entity_slug is not None:
        fields["entity_slug"] = entity_slug or None
    if simplefin_account_id is not None:
        fields["simplefin_account_id"] = simplefin_account_id or None
    if notes is not None:
        fields["notes"] = notes or None
    if closed_on is not None:
        fields["closed_on"] = closed_on or None
    if not fields:
        return
    fields["updated_at"] = "CURRENT_TIMESTAMP"
    # Build dynamic SQL but use a literal for updated_at.
    cols = [k for k in fields if k != "updated_at"]
    sets = ", ".join(f"{k} = ?" for k in cols) + ", updated_at = CURRENT_TIMESTAMP"
    values = [fields[k] for k in cols] + [account_path]
    conn.execute(
        f"UPDATE accounts_meta SET {sets} WHERE account_path = ?",
        values,
    )


# ---------------------------------------------------------------------------
# Schedule C / F scaffold generator
# ---------------------------------------------------------------------------

_STRIP_SUFFIX_RE = re.compile(r"\(\$\|:\)$")


def _canonical_category_from_pattern(pattern: str, entity_slug: str) -> str | None:
    """Turn a schedule_c/f_lines.yml account_pattern into a concrete
    Open-able path for a given entity.

    Pattern example: ``Expenses:[^:]+:ContractLabor($|:)``
    Concrete path:  ``Expenses:{entity_slug}:ContractLabor``
    """
    # Trim the ($|:) suffix.
    p = _STRIP_SUFFIX_RE.sub("", pattern).rstrip("$")
    # Replace [^:]+ with the entity slug.
    p = p.replace("[^:]+", entity_slug, 1)
    # Reject anything still containing regex metacharacters.
    if any(ch in p for ch in "*+?()[]|\\"):
        return None
    # Must start with "Expenses:" and have at least two more segments
    # (entity + category).
    if not p.startswith("Expenses:"):
        return None
    parts = p.split(":")
    if len(parts) < 3:
        return None
    return p


_OTHER_PREFIX_RE = re.compile(r"^\s*other\b", re.IGNORECASE)


def load_categories_yaml_for_entity(settings, entity) -> list[dict]:
    """Pick the right YAML for an entity and return the parsed list.

    Routing:
      tax_schedule == "C"              -> schedule_c_lines.yml
      tax_schedule == "F"              -> schedule_f_lines.yml
      tax_schedule in ("A", "Personal") -> personal_categories.yml
      slug == "Personal" (no schedule) -> personal_categories.yml
      entity_type == "personal"        -> personal_categories.yml
      else                             -> [] (caller falls back to ledger scan)

    Keeps the "which file" decision in one place so card.py, the
    classify whitelist builder, the Personal-scaffold action, and the
    reports page all agree.

    Accepts either an object with attributes (dataclass Entity) OR a
    dict-like row (sqlite3.Row). `getattr` silently returns None for
    every field on a Row, which silently broke the Personal path —
    so we read dict-style first and fall back to attribute access.
    """
    import yaml as _yaml

    def _read(key: str) -> str:
        if entity is None:
            return ""
        # dict-like (sqlite3.Row supports [...] but not .get nor getattr)
        try:
            val = entity[key]
        except (KeyError, IndexError, TypeError):
            val = getattr(entity, key, None)
        return (val or "").strip() if isinstance(val, str) else (str(val or "").strip())

    sched = _read("tax_schedule").upper()
    slug = _read("slug")
    et = _read("entity_type").lower()
    if sched == "C":
        p = settings.schedule_c_lines_path
    elif sched == "F":
        p = settings.schedule_f_lines_path
    elif sched in ("A", "PERSONAL") or slug == "Personal" or et == "personal":
        p = settings.personal_categories_path
    else:
        return []
    if not p.exists():
        return []
    try:
        return _yaml.safe_load(p.read_text(encoding="utf-8")) or []
    except Exception:  # noqa: BLE001
        return []


def scaffold_paths_for_entity(schedule_yaml: list[dict], entity_slug: str) -> list[dict]:
    """Given a parsed schedule_c/f_lines.yml list and an entity slug,
    return [{path, description, line}] — one canonical Open-able path
    per Schedule line.

    Rules:
      * "Other expenses" (line 27) becomes `Expenses:{slug}:Other`
        regardless of patterns. Sub-accounts (Shipping, Software, …) are
        the user's choice to add manually under Other: later.
      * **Car and truck (line 9) is never scaffolded as a bucket.** The
        canonical shape is per-vehicle (`Expenses:<Entity>:Vehicle:
        <Slug>:<Category>`) via ensure_vehicle_chart. Scaffolding
        `Expenses:<Entity>:Auto` here would just create a redundant
        account that's either unused (Auto bucket vs per-vehicle
        detail) or splits postings between two equivalent
        Schedule-C-line-9 buckets. The schedule_c_lines regex
        patterns still map any historical Auto/CarAndTruck/Mileage
        accounts for report aggregation.
      * Every other line uses the first account_pattern that produces
        a clean concrete path.
    """
    seen: set[str] = set()
    out: list[dict] = []
    for entry in schedule_yaml or []:
        line = entry.get("line")
        desc = entry.get("description", "") or ""
        patterns = entry.get("account_patterns") or []

        # Line 9 (Car and truck) → intentionally skipped. See docstring.
        if line == 9:
            continue

        # "Other" lines get a canonical Other bucket, not one of the
        # sub-category patterns.
        if _OTHER_PREFIX_RE.match(desc):
            path = f"Expenses:{entity_slug}:Other"
            if path not in seen:
                seen.add(path)
                out.append({"path": path, "description": desc, "line": line})
            continue

        if not patterns:
            continue
        for pat in patterns:
            path = _canonical_category_from_pattern(pat, entity_slug)
            if not path or path in seen:
                continue
            seen.add(path)
            out.append({"path": path, "description": desc, "line": line})
            break  # one path per line is enough
    return out


# ---------------------------------------------------------------------------
# Merchant memory
# ---------------------------------------------------------------------------


def merchant_key_for(narration: str | None, payee: str | None) -> str | None:
    """Derive a stable key we can use to remember past categorizations.

    Prefers payee (usually clean). Falls back to the first 4 words of
    narration lowercased, which is stable across most bank feeds.
    """
    if payee:
        return payee.strip().lower()
    if not narration:
        return None
    tokens = re.findall(r"[A-Za-z0-9]+", narration.lower())
    if not tokens:
        return None
    return " ".join(tokens[:4])


def bump_merchant_memory(
    conn: sqlite3.Connection,
    *,
    merchant_key: str,
    target_account: str,
    entity_slug: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO merchant_memory
            (merchant_key, target_account, entity_slug, last_used_at, use_count)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, 1)
        ON CONFLICT (merchant_key, target_account) DO UPDATE SET
            last_used_at = CURRENT_TIMESTAMP,
            use_count    = use_count + 1,
            entity_slug  = COALESCE(excluded.entity_slug, merchant_memory.entity_slug)
        """,
        (merchant_key, target_account, entity_slug),
    )


def decrement_merchant_memory(
    conn: sqlite3.Connection,
    *,
    merchant_key: str,
    target_account: str,
) -> None:
    # Scope by the full PK (merchant_key, target_account). Without
    # merchant_key the decrement would stomp every row that points at
    # this account — across every other merchant and entity.
    conn.execute(
        """
        UPDATE merchant_memory
           SET use_count = use_count - 1
         WHERE merchant_key = ?
           AND target_account = ?
           AND use_count > 0
        """,
        (merchant_key, target_account),
    )


def recent_for_merchant(
    conn: sqlite3.Connection, merchant_key: str, limit: int = 3
) -> list[dict]:
    rows = conn.execute(
        """
        SELECT target_account, entity_slug, use_count, last_used_at
        FROM merchant_memory
        WHERE merchant_key = ?
        ORDER BY last_used_at DESC
        LIMIT ?
        """,
        (merchant_key, limit),
    ).fetchall()
    return [
        {
            "target_account": r["target_account"],
            "entity_slug": r["entity_slug"],
            "use_count": r["use_count"],
            "last_used_at": r["last_used_at"],
            "display_name": alias_for(conn, r["target_account"]),
        }
        for r in rows
    ]
