# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Admin page for accounts (bank, credit, loan, brokerage, cash, etc.).

Unified table merging:
  - Every Open directive discovered in the ledger.
  - Every row in accounts_meta.

The user labels each row (kind, last-four, institution, entity,
SimpleFIN link) and saves. Unlabeled rows get an "Unlabeled" badge at
the top of the list for quick bulk editing. Adding an account that
doesn't exist in the ledger yet writes an Open directive to
connector_accounts.bean.
"""
from __future__ import annotations

import logging
from datetime import date

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from beancount.core.data import Open

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.registry.accounts_writer import AccountsWriter
from lamella.core.registry.service import (
    ACCOUNT_KINDS,
    list_accounts,
    list_entities,
    update_account,
)

log = logging.getLogger(__name__)

router = APIRouter()


def _is_system_account(path: str) -> bool:
    """System accounts the user rarely cares to label: opening balances,
    retained earnings, payment-processor clearing buckets, summary
    synthetic accounts. Hidden from the default view."""
    if not path:
        return False
    if path.startswith("Equity:OpeningBalances:") or path == "Equity:OpeningBalances":
        return True
    if path.startswith("Equity:Retained:") or path == "Equity:Retained":
        return True
    if path.startswith("Equity:RegularTransa") or path.startswith("Equity:Unattributed"):
        return True
    if path.startswith("Assets:Clearing:") or path == "Assets:Clearing":
        return True
    if path.startswith("Assets:PayPal:Clearing"):
        return True
    return False


@router.get("/settings/accounts", response_class=HTMLResponse)
def accounts_admin_legacy_redirect(request: Request):
    """``/settings/accounts`` was the original bulk-form admin page.
    The browse view at ``/accounts`` (accounts_browse.py) now carries
    the same edit surface in a per-row modal plus balances and
    per-entity bulk edit, so the legacy admin is folded into the
    canonical browse URL. Old bookmarks and internal templates that
    still link to ``/settings/accounts`` keep working via this 303
    redirect; preserves ``?entity=<slug>`` and ``?show=...`` query
    params so callers that filtered the legacy page still land on
    the matching browse view.
    """
    qs = str(request.url.query) if request.url.query else ""
    target = "/accounts"
    if qs:
        target = f"{target}?{qs}"
    return RedirectResponse(url=target, status_code=303)


@router.get("/settings/accounts/legacy", response_class=HTMLResponse)
def accounts_page(
    request: Request,
    saved: str | None = None,
    show_system: bool = False,
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    # Rows in accounts_meta; merge with any Open directives not yet in accounts_meta.
    all_meta = list_accounts(conn, include_closed=True)
    meta_rows = all_meta if show_system else [
        m for m in all_meta if not _is_system_account(m.account_path)
    ]
    meta_paths_all = {m.account_path for m in all_meta}

    ledger_unlabeled: list[str] = []
    for entry in reader.load().entries:
        if isinstance(entry, Open):
            root = entry.account.split(":", 1)[0]
            # All five canonical roots show up — Expenses + Income are
            # included so the user can bulk-edit display_name + entity
            # on category accounts. Bank-only fields (kind / institution
            # / last_four / SimpleFIN) hide automatically for those
            # roots via the row template's data-account-root toggle.
            if root not in ("Assets", "Liabilities", "Equity", "Expenses", "Income"):
                continue
            if entry.account in meta_paths_all:
                continue
            if not show_system and _is_system_account(entry.account):
                continue
            ledger_unlabeled.append(entry.account)

    entities = list_entities(conn, include_inactive=False)
    entity_display: dict[str, str] = {e.slug: (e.display_name or e.slug) for e in entities}

    # Group accounts by entity_slug, then by kind within each entity.
    # Accounts without an entity go under "Unassigned."
    groups: dict[str, dict[str, list]] = {}
    for m in meta_rows:
        key = m.entity_slug or ""
        by_kind = groups.setdefault(key, {})
        k = m.kind or "(unlabeled)"
        by_kind.setdefault(k, []).append(m)
    # Add discovered-but-unlabeled ledger accounts too — group by the
    # second path segment (likely the entity slug).
    for path in ledger_unlabeled:
        parts = path.split(":")
        key = parts[1] if len(parts) >= 2 and parts[1] not in ("Clearing", "OpeningBalances", "Retained", "Vehicles", "Property", "Properties") else ""
        by_kind = groups.setdefault(key, {})
        by_kind.setdefault("(discovered)", []).append({
            "account_path": path, "display_name": "", "kind": None,
            "institution": None, "last_four": None, "entity_slug": key or None,
            "simplefin_account_id": None, "closed_on": None,
        })

    # Order: entities with display names first, alphabetical; then
    # unassigned; then "Unassigned" last.
    ordered_groups: list[dict] = []
    labeled_keys = sorted(
        (k for k in groups.keys() if k and k in entity_display),
        key=lambda k: entity_display[k].lower(),
    )
    other_keys = sorted(k for k in groups.keys() if k and k not in entity_display)
    unassigned = [k for k in groups.keys() if not k]
    for k in labeled_keys + other_keys + unassigned:
        kinds = groups[k]
        kind_order = sorted(kinds.keys(), key=lambda s: (
            0 if s in ACCOUNT_KINDS else (1 if s == "(discovered)" else 2), s
        ))
        display = entity_display.get(k, k or "Unassigned")
        ordered_groups.append({
            "slug": k or "unassigned",
            "display": display,
            "kinds": [(kind, kinds[kind]) for kind in kind_order],
            "count": sum(len(v) for v in kinds.values()),
        })

    hidden_count = 0
    if not show_system:
        hidden_count = sum(1 for m in all_meta if _is_system_account(m.account_path))

    ctx = {
        "groups": ordered_groups,
        "total_accounts": sum(g["count"] for g in ordered_groups),
        "ledger_unlabeled_count": len(ledger_unlabeled),
        "entities": entities,
        "account_kinds": ACCOUNT_KINDS,
        "saved": saved,
        "show_system": show_system,
        "hidden_count": hidden_count,
    }
    return request.app.state.templates.TemplateResponse(
        request, "settings_accounts.html", ctx
    )


@router.post("/settings/accounts/add-subcategory")
async def add_subcategory(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Quick 'add a deeper category under an existing one'.

    Takes a parent path (e.g. Expenses:Acme:Rent) and a leaf name
    (e.g. StorageUnitKnox) and writes `Expenses:Acme:Rent:StorageUnitKnox`
    to connector_accounts.bean. Lets the user keep Schedule C/F at the
    top level but drill down for specific vendors underneath.
    """
    from lamella.core.registry.service import suggest_slug

    form = await request.form()
    parent = (form.get("parent") or "").strip()
    leaf = (form.get("leaf") or "").strip()
    if not parent or not leaf:
        raise HTTPException(status_code=400, detail="parent and leaf are required")
    # Slugify the leaf so it's a valid Beancount segment.
    leaf_slug = leaf if leaf[:1].isupper() and all(c.isalnum() or c == '-' for c in leaf) else suggest_slug(leaf)
    if not leaf_slug:
        raise HTTPException(status_code=400, detail=f"cannot derive a valid segment from {leaf!r}")
    path = f"{parent}:{leaf_slug}"

    # Collect existing-ledger paths so writer skips duplicates.
    existing_paths: set[str] = set()
    for entry in reader.load().entries:
        acct = getattr(entry, "account", None)
        if isinstance(acct, str):
            existing_paths.add(acct)
    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        writer.write_opens(
            [path],
            comment=f"Sub-category added under {parent}",
            existing_paths=existing_paths,
        )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    reader.invalidate()

    # If we know an entity_slug for the parent, record an accounts_meta row.
    parts = parent.split(":")
    entity_slug = parts[1] if len(parts) >= 2 else None
    conn.execute(
        """
        INSERT OR IGNORE INTO accounts_meta
            (account_path, display_name, entity_slug,
             seeded_from_ledger, created_at)
        VALUES (?, ?, ?, 0, CURRENT_TIMESTAMP)
        """,
        (path, f"{parent.split(':')[-1]} · {leaf_slug}", entity_slug),
    )

    redirect_to = form.get("redirect_to") or f"/settings/accounts?saved=added-{path}"
    return RedirectResponse(redirect_to, status_code=303)


@router.post("/settings/accounts-bulk-save")
async def bulk_save_accounts(
    request: Request,
    conn = Depends(get_db),
):
    """Save many rows at once. Form fields are keyed by account path:
    display_name[<path>]=..., kind[<path>]=..., etc. Empty fields are
    treated as "no change" — only non-empty values overwrite."""
    form = await request.form()
    # Collect per-path updates.
    updates: dict[str, dict[str, str]] = {}
    for key, value in form.multi_items():
        if "[" not in key or not key.endswith("]"):
            continue
        field, path = key.split("[", 1)
        path = path[:-1]
        if not path:
            continue
        updates.setdefault(path, {})[field] = str(value)
    saved = 0
    for path, fields in updates.items():
        if not fields:
            continue
        existing = conn.execute(
            "SELECT account_path FROM accounts_meta WHERE account_path = ?",
            (path,),
        ).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO accounts_meta "
                "(account_path, display_name, seeded_from_ledger, created_at) "
                "VALUES (?, ?, 0, CURRENT_TIMESTAMP)",
                (path, fields.get("display_name", path).strip() or path),
            )
        update_account(
            conn,
            path,
            display_name=fields.get("display_name", "").strip() or None,
            kind=fields.get("kind", "").strip() or None,
            institution=fields.get("institution", "").strip() or None,
            last_four=fields.get("last_four", "").strip() or None,
            entity_slug=fields.get("entity_slug", "").strip() or None,
            simplefin_account_id=fields.get("simplefin_account_id", "").strip() or None,
        )
        saved += 1
    return RedirectResponse(
        f"/settings/accounts?saved=bulk-{saved}", status_code=303
    )


@router.post("/settings/accounts-cleanup-system")
def cleanup_system_accounts(
    conn = Depends(get_db),
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Remove unlabeled system-account rows from accounts_meta. Only
    deletes rows that are (a) unlabeled (``kind IS NULL``), (b) match
    a known system-account path pattern, (c) carry no user-typed
    information beyond auto-seeded fields, and (d) have zero
    transactions on the path.

    Each removal writes a ``custom "account-meta-deleted"`` tombstone
    before the SQL DELETE so ``seed_accounts_meta`` doesn't re-INSERT
    the row from the still-present Open directive on the next boot.
    Skips silently on delete-refusal — bulk cleanup shouldn't fail
    on a single blocking row.
    """
    from lamella.core.registry.account_meta_writer import (
        append_account_meta_deleted,
    )
    from lamella.core.ledger_writer import BeanCheckError
    from lamella.features.setup.posting_counts import (
        DeleteRefusal, assert_safe_to_delete_account_meta,
    )
    entries = list(reader.load().entries)
    rows = conn.execute(
        "SELECT account_path FROM accounts_meta "
        "WHERE kind IS NULL"
    ).fetchall()
    removed = 0
    skipped = 0
    for r in rows:
        path = r["account_path"]
        if not _is_system_account(path):
            continue
        try:
            assert_safe_to_delete_account_meta(conn, entries, path)
        except DeleteRefusal as exc:
            log.info("cleanup-system skip %s: %s", path, exc.message)
            skipped += 1
            continue
        try:
            append_account_meta_deleted(
                connector_config=settings.connector_config_path,
                main_bean=settings.ledger_main,
                account_path=path,
            )
        except BeanCheckError as exc:
            log.warning(
                "account-meta-deleted tombstone for %s skipped: %s — "
                "leaving DB row in place to avoid silent ledger/DB drift",
                path, exc,
            )
            continue
        conn.execute("DELETE FROM accounts_meta WHERE account_path = ?", (path,))
        removed += 1
    return RedirectResponse(
        f"/settings/accounts?saved=cleanup-removed-{removed}", status_code=303
    )


@router.post("/settings/accounts/{account_path:path}")
def save_account(
    account_path: str,
    request: Request,
    conn = Depends(get_db),
    settings: Settings = Depends(get_settings),
    display_name: str = Form(""),
    kind: str = Form(""),
    institution: str = Form(""),
    last_four: str = Form(""),
    entity_slug: str = Form(""),
    simplefin_account_id: str = Form(""),
    notes: str = Form(""),
):
    path = account_path.lstrip("/")
    prior_kind_row = conn.execute(
        "SELECT kind FROM accounts_meta WHERE account_path = ?", (path,),
    ).fetchone()
    prior_kind = (prior_kind_row["kind"] if prior_kind_row else None)
    existing = conn.execute(
        "SELECT account_path FROM accounts_meta WHERE account_path = ?",
        (path,),
    ).fetchone()
    if existing is None:
        conn.execute(
            "INSERT INTO accounts_meta "
            "(account_path, display_name, seeded_from_ledger, created_at) "
            "VALUES (?, ?, 0, CURRENT_TIMESTAMP)",
            (path, display_name.strip() or path),
        )
    new_kind = kind.strip() or None
    update_account(
        conn,
        path,
        display_name=display_name.strip() or None,
        kind=new_kind,
        institution=institution.strip() or None,
        last_four=last_four.strip() or None,
        entity_slug=entity_slug.strip() or None,
        simplefin_account_id=simplefin_account_id.strip() or None,
        notes=notes.strip() or None,
    )
    # Persist the kind choice to the ledger so a DB wipe rebuilds it.
    # Only emit when it actually changed — avoids churn on every save
    # of unrelated fields.
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
                    account_path=path, kind=new_kind,
                )
            else:
                append_account_kind_cleared(
                    connector_config=settings.connector_config_path,
                    main_bean=settings.ledger_main,
                    account_path=path,
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("account-kind directive write failed for %s: %s", path, exc)
    # HTMX: return the updated row so the UI swaps in place.
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        row = conn.execute(
            "SELECT * FROM accounts_meta WHERE account_path = ?", (path,)
        ).fetchone()
        entities = list_entities(conn, include_inactive=False)
        ctx = {
            "a": dict(row),
            "entities": entities,
            "account_kinds": ACCOUNT_KINDS,
            "saved_marker": True,
        }
        return request.app.state.templates.TemplateResponse(
            request, "partials/account_row.html", ctx
        )
    return RedirectResponse(f"/settings/accounts?saved={path}", status_code=303)


@router.post("/settings/accounts-new")
async def add_account(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Create a brand-new account. Writes the Open directive to
    connector_accounts.bean and the meta row to accounts_meta."""
    form = await request.form()
    account_path = (form.get("account_path") or "").strip()
    if not account_path:
        raise HTTPException(status_code=400, detail="account_path required")
    if ":" not in account_path:
        raise HTTPException(status_code=400, detail="account_path must be a colon-separated Beancount path")
    # ADR-0007 — Top:Entity:Leaf shape. Reject typos like "Asserts:..." or
    # "Asseets:..." here so the user gets a clear error instead of bean-check
    # failing later with an opaque message. The five canonical roots are
    # the only legal Beancount top segments; the second segment must match
    # the entity_slug when one was provided in the form.
    _VALID_ROOTS = {"Assets", "Liabilities", "Income", "Expenses", "Equity"}
    _path_parts = account_path.split(":")
    if _path_parts[0] not in _VALID_ROOTS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"top segment '{_path_parts[0]}' is not a valid Beancount root. "
                f"Must be one of: {', '.join(sorted(_VALID_ROOTS))}."
            ),
        )
    if len(_path_parts) < 3 or any(not p for p in _path_parts):
        raise HTTPException(
            status_code=400,
            detail="account_path must have at least Top:Entity:Leaf segments",
        )
    _entity_form = (form.get("entity_slug") or "").strip()
    if _entity_form and _path_parts[1] != _entity_form:
        raise HTTPException(
            status_code=400,
            detail=(
                f"entity segment '{_path_parts[1]}' doesn't match selected "
                f"entity '{_entity_form}'. Fix one or the other."
            ),
        )

    display_name = (form.get("display_name") or "").strip() or account_path
    kind = (form.get("kind") or "").strip() or None
    institution = (form.get("institution") or "").strip() or None
    last_four = (form.get("last_four") or "").strip() or None
    entity_slug = (form.get("entity_slug") or "").strip() or None
    simplefin_account_id = (form.get("simplefin_account_id") or "").strip() or None
    opened_on_str = (form.get("opened_on") or "").strip()
    opened_on = date.fromisoformat(opened_on_str) if opened_on_str else date.today()

    # Refuse to write if the account already exists in the ledger.
    for entry in reader.load().entries:
        if isinstance(entry, Open) and entry.account == account_path:
            raise HTTPException(
                status_code=409,
                detail=f"account already exists in ledger: {account_path}",
            )

    # Companion bundle: open the appropriate Interest / Bank:Fees /
    # Bank:Cashback / Equity:OpeningBalances accounts for this kind,
    # under Schedule-C-compatible paths. Toggled by the same checkbox
    # as before but now driven by companion_paths_for so the set is
    # consistent across create + edit + reconstruct.
    from lamella.core.registry.companion_accounts import companion_paths_for
    bundle = (form.get("create_bundle") or "").strip() == "1"
    bundle_paths: list[str] = [account_path]
    if bundle and entity_slug:
        companions = companion_paths_for(
            account_path=account_path,
            kind=kind,
            entity_slug=entity_slug,
            institution=institution,
        )
        bundle_paths.extend(cp.path for cp in companions)

    # Collect existing-ledger paths so writer skips duplicates.
    existing_paths: set[str] = set()
    for entry in reader.load().entries:
        acct = getattr(entry, "account", None)
        if isinstance(acct, str):
            existing_paths.add(acct)

    writer = AccountsWriter(
        main_bean=settings.ledger_main,
        connector_accounts=settings.connector_accounts_path,
    )
    try:
        writer.write_opens(
            bundle_paths, opened_on=opened_on, existing_paths=existing_paths,
        )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")

    # Record in accounts_meta (only the primary account — siblings stay
    # ledger-only with no registry metadata by default).
    conn.execute(
        """
        INSERT OR REPLACE INTO accounts_meta
            (account_path, display_name, kind, institution, last_four,
             entity_slug, simplefin_account_id, opened_on,
             seeded_from_ledger, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0,
                COALESCE(
                    (SELECT created_at FROM accounts_meta WHERE account_path = ?),
                    CURRENT_TIMESTAMP
                ))
        """,
        (
            account_path, display_name, kind, institution, last_four,
            entity_slug, simplefin_account_id, opened_on.isoformat(),
            account_path,
        ),
    )
    reader.invalidate()
    extras = len(bundle_paths) - 1
    # Modal-add path: HTMX caller gets HX-Refresh so /accounts re-
    # renders with the new account in place; legacy form POST gets
    # the 303 back to /settings/accounts.
    headers = {k.lower(): v for k, v in request.headers.items()}
    if "hx-request" in headers:
        return HTMLResponse(
            "", status_code=200, headers={"HX-Refresh": "true"},
        )
    return RedirectResponse(
        f"/settings/accounts?saved={account_path}"
        + (f"-plus-{extras}-bundled" if extras else ""),
        status_code=303,
    )
