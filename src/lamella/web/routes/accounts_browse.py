# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Browse-mode accounts UX — the replacement for the monolithic
/settings/accounts editor.

Routes:
  GET  /accounts
      Grouped-by-entity index. Each row shows current ledger balance,
      kind, institution, last-four. Click → /accounts/{path}.

  GET  /accounts/{account_path:path}
      Detail: friendly header, current balance, 12-month balance-over-
      time chart, recent transactions, balance-anchor section.

  GET  /accounts/{account_path:path}/edit
  POST /accounts/{account_path:path}/edit
      Dedicated single-account edit form with every field exposed.

  GET  /businesses/{slug}/accounts/edit
  POST /businesses/{slug}/accounts/edit
      Compact bulk editor: display_name + kind for one entity's rows.

Keep /settings/accounts working as-is — the new UX ships alongside
so users can switch over before the old page retires.
"""
from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date as date_t, timedelta
from decimal import Decimal
from typing import Any, Iterable

from beancount.core.data import Transaction
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.identity import get_txn_id
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.core.registry.service import (
    ACCOUNT_KINDS,
    list_entities,
    update_account,
)

log = logging.getLogger(__name__)

router = APIRouter()


# --- helpers ----------------------------------------------------------


def _entity_from_path(account_path: str) -> str:
    """Derive an entity slug from a Beancount account path. Works for
    ``Assets:Acme:Checking`` → ``Acme``, ``Expenses:Personal:Groceries``
    → ``Personal``. Returns empty string for system roots like
    ``Equity:Opening-Balances`` whose second segment isn't an entity.
    """
    if not account_path:
        return ""
    parts = account_path.split(":")
    if len(parts) < 2:
        return ""
    if parts[0] not in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
        return ""
    return parts[1]


def _friendly_label(row: dict) -> str:
    """Build a human-readable label from an accounts_meta row, avoiding
    entity_slug duplication when it's already in display_name."""
    parts: list[str] = []
    disp = row.get("display_name") if (
        row.get("display_name") and row["display_name"] != row["account_path"]
    ) else None
    has_entity_in_disp = bool(
        disp and row.get("entity_slug") and row["entity_slug"] in disp
    )
    if row.get("entity_slug") and not has_entity_in_disp:
        parts.append(row["entity_slug"])
    if row.get("institution"):
        parts.append(row["institution"])
    if disp:
        parts.append(disp)
    if row.get("last_four"):
        parts.append(f"…{row['last_four']}")
    return " · ".join(parts) if parts else row["account_path"]


def _entity_label(conn: sqlite3.Connection, slug: str | None) -> str:
    if not slug:
        return "Unassigned"
    row = conn.execute(
        "SELECT display_name FROM entities WHERE slug = ?", (slug,)
    ).fetchone()
    if row and row["display_name"]:
        return row["display_name"]
    return slug


def _postings_for_account(entries: Iterable[Any], account_path: str):
    """Yield (date, Decimal amount, payee, narration, txn_hash_str,
    lamella_txn_id) for every posting to account_path. Caller sorts /
    buckets. ``lamella_txn_id`` is the immutable UUIDv7 lineage id —
    UI link-builders must use it for /txn/{id} links."""
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        for p in e.postings:
            if p.account != account_path:
                continue
            if p.units is None or p.units.number is None:
                continue
            yield (
                e.date,
                Decimal(p.units.number),
                getattr(e, "payee", None),
                e.narration or "",
                txn_hash(e),
                get_txn_id(e),
            )


def _current_balance(entries: list, account_path: str) -> Decimal:
    total = Decimal("0")
    for _d, amt, _p, _n, _h, _lid in _postings_for_account(entries, account_path):
        total += amt
    return total


def _monthly_balance_series(
    entries: list, account_path: str, *, months: int = 12,
) -> dict:
    """Return {labels:[…], balances:[…]} with the END-OF-MONTH running
    balance for the last N months. The oldest bucket carries the
    cumulative balance through its end (so chart starts from a real
    cumulative number)."""
    today = date_t.today()
    # First day of the (months-1) months ago is the left edge.
    anchor = today.replace(day=1)
    bucket_ends: list[date_t] = []
    for i in range(months - 1, -1, -1):
        # End of each month
        first = _month_offset(anchor, -i)
        nxt = _month_offset(first, 1)
        bucket_ends.append(nxt - timedelta(days=1))

    posts = sorted(
        _postings_for_account(entries, account_path), key=lambda t: t[0],
    )
    running = Decimal("0")
    pi = 0
    balances: list[str] = []
    labels: list[str] = []
    for end in bucket_ends:
        while pi < len(posts) and posts[pi][0] <= end:
            running += posts[pi][1]
            pi += 1
        balances.append(str(running))
        labels.append(end.strftime("%b %y"))
    return {"labels": labels, "balances": balances}


def _month_offset(d: date_t, months: int) -> date_t:
    m_index = (d.year * 12 + d.month - 1) + months
    year = m_index // 12
    month = m_index % 12 + 1
    return date_t(year, month, 1)


# --- routes -----------------------------------------------------------


@router.get("/accounts/new-modal", response_class=HTMLResponse)
def account_new_modal(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """HTMX fragment — "+ Add account" modal. Slim form: full
    Beancount path, owning entity, kind, institution, last-four.
    Detailed fields (SimpleFIN account id, opened-on date, companion
    bundle, etc.) are edited on /accounts/{path}/edit."""
    entities = [
        dict(r) for r in conn.execute(
            "SELECT slug, display_name FROM entities "
            "WHERE is_active = 1 ORDER BY display_name, slug"
        ).fetchall()
    ]
    return request.app.state.templates.TemplateResponse(
        request, "partials/_account_modal_new.html",
        {"entities": entities},
    )


_SHOW_FILTERS: dict[str, tuple[str, ...]] = {
    "bank": ("Assets:", "Liabilities:"),
    "expense": ("Expenses:",),
    "income": ("Income:",),
    "equity": ("Equity:",),
    "all": (),
}

# True bank-shaped account kinds. Asset+Liability tree filtering pulls
# in vehicles, properties, transfer-in-flight virtual accounts, etc.
# that the user does NOT consider "bank accounts" — drop them by kind
# when show=bank. Unregistered accounts (kind unset) under Assets /
# Liabilities still get included so the user can register them.
_BANK_KINDS: frozenset[str] = frozenset({
    "checking", "savings", "credit_card", "line_of_credit",
    "brokerage", "cash", "money_market", "hsa",
    "loan", "tax_liability",
})
# Kinds that LIVE under Assets / Liabilities but aren't really bank
# accounts in the user-facing sense.
_NON_BANK_KINDS: frozenset[str] = frozenset({
    "asset",   # vehicles, properties — physical assets, not money
    "virtual", # internal transfer / clearing accounts
    "payout",  # marketplace processor balances (eBay, Stripe, PayPal)
})


@router.get("/accounts", response_class=HTMLResponse)
def accounts_index(
    request: Request,
    show: str = "bank",
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Grouped-by-entity account list with current balances.

    Source-of-truth is the ledger itself: every account opened via an
    ``Open`` directive shows up here. Accounts that ALSO have a row in
    ``accounts_meta`` get their friendly label / institution /
    last-four / kind / SimpleFIN link merged in. Accounts opened in
    the ledger but never registered are still listed and badged
    "Unregistered" so the user can click through to /settings/accounts
    and fill in metadata.

    ``show`` filters the visible roots (Assets+Liabilities by default
    — that's the "my bank accounts" surface). Set to ``expense`` /
    ``income`` / ``equity`` / ``all`` to widen.
    """
    from beancount.core.data import Open

    show = (show or "bank").strip().lower()
    if show not in _SHOW_FILTERS:
        show = "bank"
    show_prefixes = _SHOW_FILTERS[show]

    meta_by_path = {
        r["account_path"]: dict(r) for r in conn.execute(
            "SELECT account_path, display_name, kind, entity_slug, "
            "       institution, last_four, closed_on "
            "FROM accounts_meta"
        ).fetchall()
    }

    entries = reader.load().entries

    # Cache-friendly: walk the ledger once for both Open-directive
    # discovery + balance accumulation.
    open_paths: set[str] = set()
    closed_paths: set[str] = set()
    totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    currencies: dict[str, str] = {}
    for e in entries:
        if isinstance(e, Open):
            open_paths.add(e.account)
            continue
        if not isinstance(e, Transaction):
            continue
        for p in e.postings:
            if p.units is None or p.units.number is None:
                continue
            totals[p.account] += Decimal(p.units.number)
            currencies.setdefault(p.account, p.units.currency or "USD")

    # Union: every Open path + every path that has activity (defensive
    # — Beancount allows a posting against an account without a prior
    # Open in some plugins).
    every_path = open_paths | set(totals.keys())

    # Pre-compute the per-root counts BEFORE filtering so the segmented
    # toggle in the template can show "(N)" badges next to each option
    # — useful so the user knows whether expanding to "all" is going to
    # surface 12 expense accounts or 12,000.
    counts_by_root: dict[str, int] = defaultdict(int)
    for path in every_path:
        root = path.split(":", 1)[0]
        counts_by_root[root] += 1
    show_counts = {
        "bank": counts_by_root.get("Assets", 0) + counts_by_root.get("Liabilities", 0),
        "expense": counts_by_root.get("Expenses", 0),
        "income": counts_by_root.get("Income", 0),
        "equity": counts_by_root.get("Equity", 0),
        "all": len(every_path),
    }

    if show_prefixes:
        every_path = {
            p for p in every_path
            if any(p.startswith(prefix) for prefix in show_prefixes)
        }
    # When the user is on the "Bank accounts" view, drop the
    # non-bank-shaped Asset/Liability rows (vehicles, properties,
    # virtual transfer-in-flight, marketplace payouts). Accounts
    # without a registered kind are kept so the user can register
    # them — they're more likely to be bank accounts than not.
    if show == "bank":
        kept: set[str] = set()
        for p in every_path:
            kind = (meta_by_path.get(p) or {}).get("kind") or ""
            if not kind:
                kept.add(p)  # unregistered — show so the user can label
            elif kind in _NON_BANK_KINDS:
                continue
            elif kind in _BANK_KINDS:
                kept.add(p)
            else:
                # Unknown kind value — be conservative and show, since
                # the user can re-classify or the kind may be a recent
                # addition not yet in _BANK_KINDS / _NON_BANK_KINDS.
                kept.add(p)
        every_path = kept

    rows: list[dict] = []
    for path in sorted(every_path):
        meta = meta_by_path.get(path, {})
        rows.append({
            "account_path": path,
            "display_name": meta.get("display_name") or "",
            "kind": meta.get("kind") or "",
            "entity_slug": meta.get("entity_slug") or _entity_from_path(path),
            "institution": meta.get("institution") or "",
            "last_four": meta.get("last_four") or "",
            "closed_on": meta.get("closed_on"),
            "balance": totals.get(path, Decimal("0")),
            "currency": currencies.get(path, "USD"),
            "registered": path in meta_by_path,
        })

    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        r["label"] = _friendly_label(r)
        groups[r["entity_slug"] or ""].append(r)

    # Order: labeled entities alphabetically by display name, "Unassigned" last.
    ordered: list[dict] = []
    for slug in sorted(groups, key=lambda s: (s == "", _entity_label(conn, s).lower())):
        accounts = sorted(groups[slug], key=lambda r: r["account_path"])
        # Entity-level rollup totals by account root so the group header
        # is useful at a glance.
        rollup = defaultdict(lambda: Decimal("0"))
        for a in accounts:
            root = a["account_path"].split(":")[0]
            rollup[root] += a["balance"]
        ordered.append({
            "slug": slug or None,
            "entity_label": _entity_label(conn, slug or None),
            "accounts": accounts,
            "count": len(accounts),
            "rollup": sorted(rollup.items()),
        })

    return request.app.state.templates.TemplateResponse(
        request, "accounts_index.html",
        {
            "groups": ordered,
            "show": show,
            "show_counts": show_counts,
        },
    )


@router.get("/accounts/{account_path:path}/edit", response_class=HTMLResponse)
def account_edit_page(
    account_path: str,
    request: Request,
    saved: str | None = None,
    conn: sqlite3.Connection = Depends(get_db),
):
    row = conn.execute(
        "SELECT * FROM accounts_meta WHERE account_path = ?",
        (account_path,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="account not found")
    entities = list_entities(conn, include_inactive=True)
    a = dict(row)
    return request.app.state.templates.TemplateResponse(
        request, "account_edit.html",
        {
            "a": a,
            "label": _friendly_label(a),
            "entities": entities,
            "account_kinds": ACCOUNT_KINDS,
            "saved": saved,
        },
    )


@router.get("/accounts/{account_path:path}/edit-modal", response_class=HTMLResponse)
def account_edit_modal(
    account_path: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """HTMX fragment — quick-edit modal for one account. Mirrors
    /entities, /vehicles, /properties patterns. Save POST returns
    HX-Refresh so the /accounts dashboard reloads with the updated
    row visible. Companions scaffold + SimpleFIN link stay on the
    focused detail page at /accounts/{path}/edit."""
    row = conn.execute(
        "SELECT * FROM accounts_meta WHERE account_path = ?",
        (account_path,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="account not found")
    entities = list_entities(conn, include_inactive=True)
    return request.app.state.templates.TemplateResponse(
        request, "partials/_account_modal_edit.html",
        {
            "account": dict(row),
            "entities": entities,
            "account_kinds": ACCOUNT_KINDS,
        },
    )


@router.post("/accounts/{account_path:path}/edit")
def account_edit_save(
    account_path: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    display_name: str = Form(""),
    kind: str = Form(""),
    institution: str = Form(""),
    last_four: str = Form(""),
    entity_slug: str = Form(""),
    simplefin_account_id: str = Form(""),
    notes: str = Form(""),
    closed_on: str = Form(""),
    ensure_companions_flag: str = Form("1", alias="ensure_companions"),
):
    row = conn.execute(
        "SELECT account_path, kind FROM accounts_meta WHERE account_path = ?",
        (account_path,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="account not found")
    prior_kind = row["kind"]
    new_kind = kind.strip() or None
    update_account(
        conn, account_path,
        display_name=display_name.strip() or None,
        kind=new_kind,
        institution=institution.strip() or None,
        last_four=last_four.strip() or None,
        entity_slug=entity_slug.strip() or None,
        simplefin_account_id=simplefin_account_id.strip() or None,
        notes=notes.strip() or None,
        closed_on=closed_on.strip() or None,
    )
    if new_kind != prior_kind:
        try:
            from lamella.core.registry.kind_writer import (
                append_account_kind,
                append_account_kind_cleared,
            )
            if new_kind:
                append_account_kind(
                    connector_config=settings.connector_config_path,
                    main_bean=settings.ledger_main,
                    account_path=account_path, kind=new_kind,
                )
            else:
                append_account_kind_cleared(
                    connector_config=settings.connector_config_path,
                    main_bean=settings.ledger_main,
                    account_path=account_path,
                )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "account-kind directive write failed for %s: %s",
                account_path, exc,
            )
    # After any meta change, scaffold companion accounts (Interest,
    # Bank:Fees, Bank:Cashback, OpeningBalances) so new kinds / new
    # entity assignments surface the right Schedule-C-compatible
    # expense + income + equity accounts without the user having to
    # hand-type them. Idempotent — already-opened companions are
    # skipped by AccountsWriter.
    opened_count = 0
    if ensure_companions_flag and ensure_companions_flag not in ("0", "false", "no"):
        try:
            from lamella.core.registry.companion_accounts import ensure_companions
            opened = ensure_companions(
                conn=conn, settings=settings, reader=reader,
                account_path=account_path,
            )
            opened_count = len(opened)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "companion scaffolding failed for %s: %s",
                account_path, exc,
            )
    # HTMX-aware: callers from the /accounts dashboard quick-edit modal
    # get HX-Refresh so the table reloads with the updated row visible.
    # Non-HTMX callers (focused /accounts/{path}/edit page) get the
    # legacy 303 redirect to the per-account detail page.
    headers = {k.lower(): v for k, v in request.headers.items()}
    if "hx-request" in headers:
        return HTMLResponse(
            "", status_code=200, headers={"HX-Refresh": "true"},
        )
    qs = "saved=1" + (f"&opened={opened_count}" if opened_count else "")
    return RedirectResponse(
        f"/accounts/{account_path}?{qs}", status_code=303,
    )


@router.get("/accounts/{account_path:path}/balance-chart.json")
def account_balance_chart_json(
    account_path: str,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    months: int = 12,
):
    """Monthly balance series for the chart. Separate route so the
    detail page can render instantly and the chart fetches async."""
    exists = conn.execute(
        "SELECT 1 FROM accounts_meta WHERE account_path = ?",
        (account_path,),
    ).fetchone()
    if exists is None:
        raise HTTPException(status_code=404, detail="account not found")
    months = max(3, min(int(months), 60))
    entries = reader.load().entries
    return JSONResponse(
        _monthly_balance_series(entries, account_path, months=months),
    )


@router.get("/accounts/{account_path:path}", response_class=HTMLResponse)
def account_detail_page(
    account_path: str,
    request: Request,
    saved: str | None = None,
    opened: int | None = None,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    row = conn.execute(
        "SELECT * FROM accounts_meta WHERE account_path = ?",
        (account_path,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="account not found")
    a = dict(row)
    from lamella.core.registry.alias import entity_label
    a["entity_display_name"] = entity_label(conn, a.get("entity_slug"))
    entries = reader.load().entries
    posts = sorted(
        _postings_for_account(entries, account_path),
        key=lambda t: t[0], reverse=True,
    )
    balance = sum((p[1] for p in posts), Decimal("0"))
    recent = [
        {
            "date": p[0], "amount": p[1],
            "payee": p[2], "narration": p[3], "hash": p[4],
            "lamella_txn_id": p[5],
        }
        for p in posts[:50]
    ]
    anchor_rows = [
        dict(r) for r in conn.execute(
            "SELECT id, as_of_date, balance, currency, source, notes "
            "FROM account_balance_anchors WHERE account_path = ? "
            "ORDER BY as_of_date",
            (account_path,),
        ).fetchall()
    ]
    # What *should* exist for this kind of account — rendered in the
    # "Companion accounts" section so the user sees what the system
    # thinks is scaffolded. Missing paths link to an ensure action.
    from lamella.core.registry.companion_accounts import companion_paths_for
    existing_paths = {
        getattr(e, "account", "") for e in entries
        if getattr(e, "account", None)
    }
    companions = []
    missing_companions = 0
    for cp in companion_paths_for(
        account_path=a["account_path"], kind=a["kind"],
        entity_slug=a["entity_slug"], institution=a["institution"],
    ):
        exists = cp.path in existing_paths
        companions.append({
            "path": cp.path,
            "purpose": cp.purpose,
            "exists": exists,
        })
        if not exists:
            missing_companions += 1

    # Collect anomalies to show on the banner. Detectors live here
    # inline rather than in a separate module since they use a lot
    # of local context (balance, anchors, companion completeness).
    from lamella.features.loans.anomalies_pkg import (
        AnomalyAction, AnomalyFinding, collect as _collect_anomalies,
    )
    anomalies = []
    # Balance-anchor drift detector.
    if anchor_rows:
        latest_anchor = anchor_rows[-1]
        try:
            anchor_balance = Decimal(str(latest_anchor["balance"]))
            drift = balance - anchor_balance
            # Account for postings made AFTER the anchor date — the
            # anchor is "true at that date," so drift = current
            # balance - anchor balance minus any postings beyond the
            # anchor date's range. Simpler here: just flag significant
            # mismatch and let the user confirm.
            threshold = Decimal("1.00")
            if abs(drift) > threshold:
                anomalies.append(AnomalyFinding(
                    code="balance_drift",
                    severity="warn",
                    title=(
                        f"Balance drift vs anchor {latest_anchor['as_of_date']}: "
                        f"{drift:+,.2f} {latest_anchor.get('currency') or 'USD'}"
                    ),
                    description=(
                        f"Most recent anchor says this account was "
                        f"{anchor_balance:,.2f} on {latest_anchor['as_of_date']}. "
                        f"Ledger currently shows {balance:,.2f}. The difference "
                        f"may be legitimate (posting activity after the anchor "
                        f"date) or a real discrepancy — record another anchor "
                        f"to confirm the current balance."
                    ),
                    actions=(
                        AnomalyAction(
                            label="Record a current-balance anchor",
                            href=f"/settings/accounts/{account_path}/balances",
                        ),
                    ),
                ))
        except Exception:  # noqa: BLE001
            pass
    # Missing-companion-accounts detector.
    if missing_companions:
        anomalies.append(AnomalyFinding(
            code="missing_companions",
            severity="info",
            title=(
                f"{missing_companions} companion account"
                f"{'s' if missing_companions != 1 else ''} not yet opened"
            ),
            description=(
                f"This {a['kind'] or 'account'} usually needs {missing_companions} "
                f"additional Schedule-C-compatible account"
                f"{'s' if missing_companions != 1 else ''} "
                f"(Interest / Bank:Fees / Bank:Cashback / OpeningBalances / "
                f"Transfers:InFlight). The AI classifier can't propose targets "
                f"that aren't open; scaffolding the missing ones fixes that."
            ),
            actions=(
                AnomalyAction(
                    label=f"Create the {missing_companions} missing",
                    href=f"/accounts/{account_path}/ensure-companions",
                    method="post",
                    primary=True,
                ),
            ),
        ))
    # Missing-entity-type detector.
    if a["entity_slug"]:
        et_row = conn.execute(
            "SELECT entity_type FROM entities WHERE slug = ?",
            (a["entity_slug"],),
        ).fetchone()
        if et_row is None or not (et_row["entity_type"] or "").strip():
            anomalies.append(AnomalyFinding(
                code="entity_type_unset",
                severity="info",
                title=(
                    f"Entity '{a['entity_slug']}' has no entity_type set"
                ),
                description=(
                    "Without entity_type, the commingle-vs-intercompany "
                    "resolver can't decide whether Personal↔this-entity "
                    "flows should be 2-leg or 4-leg — it defaults to "
                    "4-leg for safety. Set the entity's structure "
                    "(personal, sole_proprietorship, llc, etc.) to get "
                    "the correct override shape."
                ),
                actions=(
                    AnomalyAction(
                        label="Set entity type",
                        href="/settings/entities",
                    ),
                ),
            ))

    return request.app.state.templates.TemplateResponse(
        request, "account_detail.html",
        {
            "a": a,
            "label": _friendly_label(a),
            "balance": balance,
            "posting_count": len(posts),
            "recent": recent,
            "anchors": anchor_rows,
            "saved": saved,
            "opened": opened or 0,
            "companions": companions,
            "anomalies": _collect_anomalies(anomalies),
        },
    )


@router.post("/accounts/{account_path:path}/opening-balance")
def account_record_opening_balance(
    account_path: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    as_of_date: str = Form(...),
    amount: str = Form(...),
    currency: str = Form("USD"),
    source_note: str = Form(""),
):
    """Record a known balance for this account on a specific date.

    Writes a ``custom "balance-anchor"`` directive — a pure
    reference point. NOT a Beancount ``pad`` (which would
    synthesize a "Padding inserted for Balance of …"
    transaction); we deliberately don't materialize fake postings
    just to make the numbers add up. The audit report computes
    drift between consecutive anchors and the postings between
    them, so the user sees gaps as gaps instead of having them
    silently filled.

    Mental model: same as a loan with a known balance on a date.
    You record what you know; later transactions move forward
    from there.
    """
    from fastapi.responses import RedirectResponse
    from datetime import date as _date_t
    from lamella.features.dashboard.balances.writer import append_balance_anchor
    from lamella.core.ledger_writer import BeanCheckError

    row = conn.execute(
        "SELECT account_path, kind, entity_slug, institution FROM accounts_meta "
        "WHERE account_path = ?",
        (account_path,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="account not found")

    # Parse inputs
    try:
        parsed_date = _date_t.fromisoformat(as_of_date.strip())
    except Exception:
        return RedirectResponse(
            f"/accounts/{account_path}?error=bad-date",
            status_code=303,
        )
    amt_clean = amount.strip().replace(",", "").replace("$", "")
    try:
        from decimal import Decimal
        parsed_amount = Decimal(amt_clean)
    except Exception:
        return RedirectResponse(
            f"/accounts/{account_path}?error=bad-amount",
            status_code=303,
        )

    notes = source_note.strip() or None
    try:
        append_balance_anchor(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            account_path=account_path,
            as_of_date=parsed_date,
            balance=f"{parsed_amount:.2f}",
            currency=currency.strip() or "USD",
            source="manual",
            notes=notes,
        )
    except BeanCheckError as exc:
        log.error("opening-balance bean-check failed: %s", exc)
        return RedirectResponse(
            f"/accounts/{account_path}?error=bean-check&detail={exc}",
            status_code=303,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("opening-balance write failed")
        return RedirectResponse(
            f"/accounts/{account_path}?error={type(exc).__name__}&detail={exc}",
            status_code=303,
        )
    reader.invalidate()
    return RedirectResponse(
        f"/accounts/{account_path}?saved=1&opening_balance=1",
        status_code=303,
    )


@router.post("/accounts/{account_path:path}/ensure-companions")
def account_ensure_companions(
    account_path: str,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """One-shot: scaffold every missing companion account for this
    row (based on its kind). Idempotent — existing opens are skipped.
    Returns the user to the detail page with ?opened=N in the URL so
    the banner can confirm what was done.
    """
    from lamella.core.registry.companion_accounts import ensure_companions
    try:
        opened = ensure_companions(
            conn=conn, settings=settings, reader=reader,
            account_path=account_path,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("ensure-companions failed for %s: %s", account_path, exc)
        return RedirectResponse(
            f"/accounts/{account_path}?saved=0", status_code=303,
        )
    return RedirectResponse(
        f"/accounts/{account_path}?saved=1&opened={len(opened)}",
        status_code=303,
    )


# --- bulk business-level edit ----------------------------------------


@router.get(
    "/businesses/{slug}/accounts/edit", response_class=HTMLResponse,
)
@router.get(
    "/personal/{slug}/accounts/edit", response_class=HTMLResponse,
)
def business_accounts_edit_page(
    slug: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    ent = conn.execute(
        "SELECT slug, display_name, entity_type FROM entities WHERE slug = ?",
        (slug,),
    ).fetchone()
    if ent is None:
        raise HTTPException(status_code=404, detail="entity not found")
    # Honor the canonical /personal vs /businesses split — same redirect
    # the businesses_router uses on the dashboard so a deep-link to
    # /businesses/<personal-slug>/accounts/edit forwards to /personal.
    from lamella.web.routes.businesses import _redirect_if_wrong_prefix
    redirect = _redirect_if_wrong_prefix(request, slug, ent["entity_type"])
    if redirect is not None:
        return redirect
    rows = [
        dict(r) for r in conn.execute(
            "SELECT account_path, display_name, kind "
            "FROM accounts_meta WHERE entity_slug = ? "
            "ORDER BY account_path",
            (slug,),
        ).fetchall()
    ]
    for r in rows:
        r["label"] = _friendly_label(r)
    return request.app.state.templates.TemplateResponse(
        request, "business_accounts_edit.html",
        {
            "entity": dict(ent),
            "accounts": rows,
            "account_kinds": ACCOUNT_KINDS,
        },
    )


@router.post("/businesses/{slug}/accounts/edit")
@router.post("/personal/{slug}/accounts/edit")
async def business_accounts_edit_save(
    slug: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    ent = conn.execute(
        "SELECT slug, entity_type FROM entities WHERE slug = ?", (slug,),
    ).fetchone()
    if ent is None:
        raise HTTPException(status_code=404, detail="entity not found")
    from lamella.web.routes.businesses import (
        _canonical_prefix, _redirect_if_wrong_prefix,
    )
    redirect = _redirect_if_wrong_prefix(request, slug, ent["entity_type"])
    if redirect is not None:
        return redirect
    form = await request.form()

    # Form field convention: display_name[<path>], kind[<path>]. We
    # iterate accounts_meta for this entity and update only the rows
    # the user actually changed.
    existing_rows = conn.execute(
        "SELECT account_path, display_name, kind FROM accounts_meta "
        "WHERE entity_slug = ?", (slug,),
    ).fetchall()
    changed = 0
    for r in existing_rows:
        path = r["account_path"]
        new_disp_key = f"display_name[{path}]"
        new_kind_key = f"kind[{path}]"
        new_disp = (form.get(new_disp_key) or "").strip() or None
        new_kind = (form.get(new_kind_key) or "").strip() or None
        prior_kind = r["kind"]
        if new_disp == r["display_name"] and new_kind == prior_kind:
            continue
        update_account(
            conn, path,
            display_name=new_disp, kind=new_kind,
        )
        changed += 1
        if new_kind != prior_kind:
            try:
                from lamella.core.registry.kind_writer import (
                    append_account_kind, append_account_kind_cleared,
                )
                if new_kind:
                    append_account_kind(
                        connector_config=settings.connector_config_path,
                        main_bean=settings.ledger_main,
                        account_path=path, kind=new_kind,
                    )
                else:
                    append_account_kind_cleared(
                        connector_config=settings.connector_config_path,
                        main_bean=settings.ledger_main,
                        account_path=path,
                    )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "account-kind directive write failed for %s: %s",
                    path, exc,
                )
    prefix = _canonical_prefix(ent["entity_type"])
    return RedirectResponse(
        f"{prefix}/{slug}/accounts/edit?saved={changed}",
        status_code=303,
    )
