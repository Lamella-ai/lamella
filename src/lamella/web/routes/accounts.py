# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Open-account listing for UI autocompletes.

Returns every Open directive from the ledger grouped by root type so the
review form can populate a <datalist> for the target-account input. The
user types "exp gas" and the browser shows matching accounts —
no more guessing at the exact string `Expenses:Vehicle:Fuel`.

Cached per-request via LedgerReader's internal cache; refreshes when the
ledger mtime changes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from beancount.core.data import Open

from lamella.core.beancount_io import LedgerReader
from lamella.web.deps import get_ledger_reader

router = APIRouter()


_ROOTS = ("Expenses", "Assets", "Liabilities", "Income", "Equity")


# ────────────────────────────────────────────────────────────────────
# /api/accounts/suggest — ranking helpers (B6 Step 0)
#
# Per decisions-pending.md §1.3, ranking is:
#   1. type-match (root == kind), enforced by the WHERE clause
#   2. entity-match (current entity's accounts before others)
#   3. card-binding match (frequent targets for this card first)
#   4. text-match proximity (best fuzzy against the typed input)
#
# Recency is intentionally OUT. No accounts_meta.last_picked_at column;
# no migration. See decisions-pending §1.3.
# ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _AccountCandidate:
    path: str
    display_name: str
    entity: str | None
    kind: str | None  # the Beancount root: Expenses, Assets, etc.


def _candidate_accounts(
    conn,
    reader: LedgerReader,
    *,
    kind: str | None,
) -> list[_AccountCandidate]:
    """Build the candidate list. Sources, in order:
      - every Open directive in the ledger
      - every account path referenced by accounts_meta / loans /
        vehicles (so newly-configured-but-not-yet-opened accounts
        show up)
    Filtered to `kind` when set.

    The result is deduped by path and labeled via registry.alias.
    """
    from lamella.core.registry.alias import alias_for

    seen: dict[str, _AccountCandidate] = {}

    def _add(path: str | None, *, entity_hint: str | None = None) -> None:
        if not path:
            return
        if path in seen:
            return
        root = path.split(":", 1)[0] if ":" in path else path
        if kind and root != kind:
            return
        try:
            display = alias_for(conn, path)
        except Exception:  # noqa: BLE001
            display = path
        # Resolve the entity from accounts_meta when present; fall
        # back to the entity_hint (typically the second path
        # segment) so cross-entity ranking still works for accounts
        # that haven't been registered.
        entity = entity_hint
        try:
            row = conn.execute(
                "SELECT entity_slug FROM accounts_meta WHERE account_path = ?",
                (path,),
            ).fetchone()
            if row and row["entity_slug"]:
                entity = row["entity_slug"]
        except Exception:  # noqa: BLE001
            pass
        if entity is None and ":" in path:
            parts = path.split(":")
            if len(parts) >= 2:
                entity = parts[1]
        seen[path] = _AccountCandidate(
            path=path,
            display_name=display or path,
            entity=entity,
            kind=root,
        )

    # 1. Open directives.
    for entry in reader.load().entries:
        if isinstance(entry, Open):
            _add(entry.account)

    # 2. Registry-only paths (loans, accounts_meta).
    try:
        for row in conn.execute(
            "SELECT account_path, entity_slug FROM accounts_meta"
        ).fetchall():
            _add(row["account_path"], entity_hint=row["entity_slug"])
    except Exception:  # noqa: BLE001
        pass
    try:
        for row in conn.execute(
            "SELECT liability_account_path, interest_account_path, "
            "escrow_account_path FROM loans"
        ).fetchall():
            for col in ("liability_account_path", "interest_account_path", "escrow_account_path"):
                try:
                    _add(row[col])
                except (KeyError, IndexError):
                    continue
    except Exception:  # noqa: BLE001
        pass

    return list(seen.values())


def _card_target_freq(conn, card_account: str | None) -> dict[str, int]:
    """How often each target account has been the categorize-target
    for charges on this card. Sourced from merchant_memory when the
    table records per-card scope; otherwise empty.

    Returns a {target_account: count} dict. Empty when the data
    isn't available — ranking still works, just without the
    card-binding tier.
    """
    if not card_account:
        return {}
    out: dict[str, int] = {}
    # merchant_memory has columns (merchant, target_account, hits, ...)
    # without a card-scope column today. Fall back to a coarse signal:
    # accounts_meta.entity_slug for the card vs. account paths whose
    # second segment matches that entity.
    try:
        row = conn.execute(
            "SELECT entity_slug FROM accounts_meta WHERE account_path = ?",
            (card_account,),
        ).fetchone()
    except Exception:  # noqa: BLE001
        row = None
    card_entity = (row and row["entity_slug"]) or None
    if not card_entity and card_account.count(":") >= 1:
        parts = card_account.split(":")
        if len(parts) >= 2:
            card_entity = parts[1]
    if not card_entity:
        return {}
    # Pull merchant_memory rows whose entity matches the card's
    # entity. Best-effort; the column set varies by migration era,
    # so wrap in try/except.
    try:
        for r in conn.execute(
            "SELECT target_account, hits FROM merchant_memory "
            "WHERE entity_slug = ?",
            (card_entity,),
        ).fetchall():
            tgt = r["target_account"]
            if not tgt:
                continue
            out[tgt] = out.get(tgt, 0) + int(r["hits"] or 0)
    except Exception:  # noqa: BLE001
        return {}
    return out


def _score(
    cand: _AccountCandidate,
    *,
    q_lower: str,
    entity_hint: str | None,
    boost: bool,
    card_freq: dict[str, int],
) -> Optional[float]:
    """Lower is better. None = rejected (no text match when q given)."""
    display_lower = (cand.display_name or "").lower()
    path_lower = cand.path.lower()

    if q_lower:
        if display_lower.startswith(q_lower):
            text_tier = 0.0
        elif q_lower in display_lower:
            text_tier = 1.0
        elif q_lower in path_lower:
            text_tier = 2.0
        else:
            return None
    else:
        text_tier = 1.5  # neutral when ranking on focus

    # Tier-2 ranking: entity match (decisions-pending §1.3 step 2).
    entity_match = bool(boost and entity_hint and cand.entity == entity_hint)
    entity_shift = -0.3 if entity_match else 0.0

    # Tier-3 ranking: card-binding match (decisions-pending §1.3
    # step 3). Card-frequent targets shift within text tier.
    card_count = card_freq.get(cand.path, 0)
    card_shift = -0.2 if card_count > 0 else 0.0
    # Larger card counts add a small additional pull but never
    # cross a text-similarity tier.
    if card_count > 1:
        card_shift -= min(0.05 * card_count, 0.15)

    return text_tier + entity_shift + card_shift


def rank_accounts(
    *,
    q: str,
    kind: str | None,
    entity: str | None,
    boost: bool,
    limit: int,
    card_account: str | None,
    conn,
    reader: LedgerReader,
) -> tuple[list[_AccountCandidate], int]:
    """Top-N candidates and the count beyond the limit. See
    decisions-pending §1.3 for the ranking contract."""
    q_lower = (q or "").lower().strip()
    candidates = _candidate_accounts(conn, reader, kind=kind)
    card_freq = _card_target_freq(conn, card_account)

    scored: list[tuple[float, _AccountCandidate]] = []
    for cand in candidates:
        score = _score(
            cand,
            q_lower=q_lower,
            entity_hint=entity,
            boost=boost,
            card_freq=card_freq,
        )
        if score is None:
            continue
        scored.append((score, cand))

    # Stable secondary sort by path so identical scores produce
    # deterministic ordering across requests.
    scored.sort(key=lambda s: (s[0], s[1].path))
    top = [cand for _, cand in scored[:limit]]
    remaining = max(0, len(scored) - limit)
    return top, remaining


@router.get("/api/accounts")
def list_accounts(
    request: Request,
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """All open accounts plus friendly display labels, so datalists can
    render human-readable names alongside the raw Beancount path.

    Also surfaces account paths that a loan / vehicle / property row
    REFERENCES but that may not yet have an `open` directive in the
    ledger — so the user's newly-created mortgage liability still
    appears in autocompletes even before the first payment is recorded.
    """
    from lamella.core.registry.alias import alias_for

    conn = request.app.state.db
    by_root: dict[str, list[str]] = {r: [] for r in _ROOTS}
    all_accounts: list[str] = []
    labels: dict[str, str] = {}
    seen: set[str] = set()

    def _add(name: str | None) -> None:
        if not name or name in seen:
            return
        seen.add(name)
        all_accounts.append(name)
        try:
            labels[name] = alias_for(conn, name)
        except Exception:  # noqa: BLE001
            labels[name] = name
        root = name.split(":", 1)[0]
        if root in by_root:
            by_root[root].append(name)

    for entry in reader.load().entries:
        if isinstance(entry, Open):
            _add(entry.account)

    # Pull additional account paths from registry tables so the
    # autocomplete shows what the user configured even if the Open
    # directive wasn't written (or was written to a different file
    # the ledger doesn't include).
    for table, columns in (
        ("loans", ("liability_account_path", "interest_account_path", "escrow_account_path")),
        ("accounts_meta", ("account_path",)),
    ):
        try:
            col_sql = ", ".join(columns)
            rows = conn.execute(f"SELECT {col_sql} FROM {table}").fetchall()
        except Exception:  # noqa: BLE001
            continue
        for row in rows:
            for col in columns:
                try:
                    _add(row[col])
                except (KeyError, IndexError):
                    continue

    for root in by_root:
        by_root[root].sort()
    all_accounts.sort()
    return JSONResponse({"all": all_accounts, "labels": labels, "by_root": by_root})


@router.get("/api/accounts-meta-suggestions")
def accounts_meta_suggestions(
    request: Request,
):
    """Distinct institution / kind / last_four values already in
    accounts_meta — for populating datalists in admin forms."""
    from sqlite3 import OperationalError

    conn = request.app.state.db
    out = {"institutions": [], "kinds": [], "last_fours": []}
    try:
        out["institutions"] = [
            r["institution"] for r in conn.execute(
                "SELECT DISTINCT institution FROM accounts_meta "
                "WHERE institution IS NOT NULL AND institution <> '' "
                "ORDER BY institution"
            ).fetchall()
        ]
        out["kinds"] = [
            r["kind"] for r in conn.execute(
                "SELECT DISTINCT kind FROM accounts_meta "
                "WHERE kind IS NOT NULL AND kind <> '' "
                "ORDER BY kind"
            ).fetchall()
        ]
        out["last_fours"] = [
            r["last_four"] for r in conn.execute(
                "SELECT DISTINCT last_four FROM accounts_meta "
                "WHERE last_four IS NOT NULL AND last_four <> '' "
                "ORDER BY last_four"
            ).fetchall()
        ]
    except OperationalError:
        pass
    return JSONResponse(out)


@router.get("/api/accounts/suggest")
def suggest_accounts(
    request: Request,
    q: str = "",
    kind: str = "",
    entity: str = "",
    boost: int = 1,
    limit: int = 5,
    mode: str = "picker",
    card_account: str = "",
    allow_create: int = 0,
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """B6 Step 0 — server-rendered popup partial for the new
    account-picker component.

    Returns the rendered fragment from
    `partials/_account_picker_popup.html` so HTMX can swap it
    directly into the picker's popup container. Ranking follows
    decisions-pending.md §1.3 (no recency).

    Per decisions-pending §1.2, every kind-filtered call surfaces
    the "remove filter" escape-hatch in the popup partial — the
    template renders an X-clear-filter affordance whenever
    `kind` is set.
    """
    conn = request.app.state.db
    templates = request.app.state.templates

    # Defensive bounds — the brief allows [1, 50].
    try:
        limit_n = max(1, min(50, int(limit)))
    except (TypeError, ValueError):
        limit_n = 5

    kind_norm = kind.strip() if kind else ""
    if kind_norm and kind_norm not in _ROOTS:
        # Unknown kind — fall back to no filter rather than 400ing,
        # because the picker macro emits whatever the caller passed
        # and a stale filter shouldn't break the popup.
        kind_norm = ""
    kind_filter_active = bool(kind_norm)

    entity_norm = entity.strip() if entity else ""
    boost_active = bool(int(boost)) if str(boost).strip() else True

    items, remaining = rank_accounts(
        q=q or "",
        kind=kind_norm or None,
        entity=entity_norm or None,
        boost=boost_active,
        limit=limit_n,
        card_account=card_account.strip() or None,
        conn=conn,
        reader=reader,
    )

    item_dicts = [
        {
            "path": c.path,
            "display_name": c.display_name,
            "entity": c.entity,
            "kind": c.kind,
        }
        for c in items
    ]

    return templates.TemplateResponse(
        request,
        "partials/_account_picker_popup.html",
        {
            "items": item_dicts,
            "has_more": remaining > 0,
            "remaining": remaining,
            "q": q or "",
            "kind": kind_norm,
            "entity": entity_norm,
            "boost_active": boost_active,
            "allow_create": bool(int(allow_create)) if str(allow_create).strip() else False,
            "kind_filter_active": kind_filter_active,
        },
    )
