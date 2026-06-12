# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import (
    get_ai_service,
    get_app_settings_store,
    get_db,
    get_ledger_reader,
    get_review_service,
    get_rule_service,
    get_settings,
)
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.service import RuleService
from lamella.core.settings.store import AppSettingsStore
from lamella.adapters.simplefin.client import (
    SimpleFINAuthError,
    SimpleFINClient,
    SimpleFINError,
    _looks_like_access_url,
    claim_setup_token,
)
from lamella.features.bank_sync.ingest import SimpleFINIngest, load_account_map
from lamella.web.temporal import render_local_ts

log = logging.getLogger(__name__)

router = APIRouter()

MODE_VALUES = {"disabled", "shadow", "active"}


@dataclass(frozen=True)
class DiscoveredAccount:
    account_id: str
    name: str
    org_name: str | None
    currency: str
    balance: str | None
    mapped_to: str | None           # canonical account_path (or None)
    mapped_to_label: str | None     # fancy label for autocomplete prefill


@dataclass(frozen=True)
class MappableAccount:
    """An Asset/Liability account the user can map a SimpleFIN account to."""
    account_path: str
    display_name: str
    institution: str | None
    last_four: str | None
    entity_slug: str | None
    kind: str | None


def _upsert_discovered_accounts(
    conn: sqlite3.Connection, response: Any
) -> int:
    """Persist accounts returned from the bridge so the /simplefin page
    can render mapping dropdowns after a page reload without re-hitting
    the network."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    count = 0
    for account in getattr(response, "accounts", []) or []:
        org = account.org or {} if getattr(account, "org", None) else {}
        org_name = org.get("name") if isinstance(org, dict) else None
        org_domain = org.get("domain") if isinstance(org, dict) else None
        conn.execute(
            """
            INSERT INTO simplefin_discovered_accounts
              (account_id, name, org_name, org_domain, currency, balance, discovered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
              name = excluded.name,
              org_name = excluded.org_name,
              org_domain = excluded.org_domain,
              currency = excluded.currency,
              balance = excluded.balance,
              discovered_at = excluded.discovered_at
            """,
            (
                str(account.id),
                account.name or "",
                org_name,
                org_domain,
                (account.currency or "USD").upper(),
                str(account.balance) if account.balance is not None else None,
                now,
            ),
        )
        count += 1
    return count


def _discovered_accounts(
    conn: sqlite3.Connection,
    *,
    path_to_label: dict[str, str] | None = None,
) -> list[DiscoveredAccount]:
    rows = conn.execute(
        """
        SELECT d.account_id, d.name, d.org_name, d.currency, d.balance,
               m.account_path AS mapped_to
          FROM simplefin_discovered_accounts d
          LEFT JOIN accounts_meta m
                 ON m.simplefin_account_id = d.account_id
         ORDER BY COALESCE(d.org_name, ''), d.name, d.account_id
        """
    ).fetchall()
    labels = path_to_label or {}
    out: list[DiscoveredAccount] = []
    for r in rows:
        mapped = r["mapped_to"]
        out.append(
            DiscoveredAccount(
                account_id=r["account_id"],
                name=r["name"] or r["account_id"],
                org_name=r["org_name"],
                currency=r["currency"] or "USD",
                balance=r["balance"],
                mapped_to=mapped,
                mapped_to_label=labels.get(mapped) if mapped else None,
            )
        )
    return out


_KIND_LABEL = {
    "checking": "Checking",
    "savings": "Savings",
    "credit_card": "Credit Card",
    "line_of_credit": "Line of Credit",
    "loan": "Loan",
    "tax_liability": "Tax Payable",
    "brokerage": "Brokerage",
    "cash": "Cash",
    "asset": "Asset",
    "virtual": "Virtual",
}


def _fancy_label(m: "MappableAccount") -> str:
    """Build a human-friendly label for an account picker. The label is the
    user-visible value in the autocomplete; the server resolves it back to
    ``account_path`` via :func:`_resolve_label_to_path`. Must be unique
    per account_path — we append the path in brackets if the constructed
    label isn't obviously distinctive."""
    parts: list[str] = []
    if m.institution:
        parts.append(m.institution)
    kind = _KIND_LABEL.get((m.kind or "").lower()) if m.kind else None
    if kind:
        parts.append(kind)
    elif m.display_name and m.display_name != m.account_path:
        parts.append(m.display_name)
    if m.last_four:
        parts.append(f"…{m.last_four}")
    entity_bits: list[str] = []
    if m.entity_slug:
        entity_bits.append(m.entity_slug)
    core = " ".join(parts).strip() or m.account_path
    label = core
    if entity_bits:
        label = f"{label} ({', '.join(entity_bits)})"
    # Every label ends with the canonical path in brackets so the server
    # can reverse it unambiguously even if two accounts share a fancy name.
    return f"{label}  [{m.account_path}]"


def _resolve_label_to_path(
    value: str, paths: set[str], label_to_path: dict[str, str]
) -> str | None:
    """Accept either a fancy label from the datalist or a raw account path.
    Returns the canonical ``account_path`` or None if the value doesn't
    resolve to a known account."""
    v = (value or "").strip()
    if not v:
        return None
    # Preferred: extract trailing [Account:Path] suffix.
    if v.endswith("]") and "[" in v:
        start = v.rfind("[")
        candidate = v[start + 1 : -1].strip()
        if candidate in paths:
            return candidate
    if v in paths:
        return v
    if v in label_to_path:
        return label_to_path[v]
    return None


def _mappable_accounts(conn: sqlite3.Connection) -> list[MappableAccount]:
    """Asset and Liability accounts the user could plausibly map to a
    SimpleFIN account. Pulled from accounts_meta, which is itself seeded
    from Open directives on boot — so every real ledger account shows up."""
    rows = conn.execute(
        """
        SELECT account_path, display_name, institution, last_four,
               entity_slug, kind
          FROM accounts_meta
         WHERE is_active = 1
           AND (account_path LIKE 'Assets:%' OR account_path LIKE 'Liabilities:%')
         ORDER BY account_path
        """
    ).fetchall()
    return [
        MappableAccount(
            account_path=r["account_path"],
            display_name=r["display_name"] or r["account_path"],
            institution=r["institution"],
            last_four=r["last_four"],
            entity_slug=r["entity_slug"],
            kind=r["kind"],
        )
        for r in rows
    ]


@dataclass(frozen=True)
class IngestRow:
    id: int
    started_at: datetime | None
    started_label: str
    finished_at: datetime | None
    trigger: str
    new_txns: int
    duplicate_txns: int
    classified_by_rule: int
    classified_by_ai: int
    fixme_txns: int
    bean_check_ok: bool
    error: str | None
    mode: str


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _render_started_label(value: Any, *, tz_name: str) -> str:
    rendered = render_local_ts(value, tz_name=tz_name, with_seconds=False)
    return rendered or "—"


def _recent_ingests(
    conn: sqlite3.Connection, *, tz_name: str, limit: int = 25
) -> list[IngestRow]:
    rows = conn.execute(
        """
        SELECT * FROM simplefin_ingests
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    out: list[IngestRow] = []
    for r in rows:
        mode = ""
        raw_summary = r["result_summary"] if "result_summary" in r.keys() else None
        if raw_summary:
            try:
                mode = (json.loads(raw_summary) or {}).get("mode") or ""
            except ValueError:
                mode = ""
        out.append(
            IngestRow(
                id=int(r["id"]),
                started_at=_parse_ts(r["started_at"]),
                started_label=_render_started_label(r["started_at"], tz_name=tz_name),
                finished_at=_parse_ts(r["finished_at"]),
                trigger=r["trigger"],
                new_txns=int(r["new_txns"] or 0),
                duplicate_txns=int(r["duplicate_txns"] or 0),
                classified_by_rule=int(r["classified_by_rule"] or 0),
                classified_by_ai=int(r["classified_by_ai"] or 0),
                fixme_txns=int(r["fixme_txns"] or 0),
                bean_check_ok=bool(r["bean_check_ok"]),
                error=r["error"],
                mode=mode,
            )
        )
    return out


def _seven_day_counters(conn: sqlite3.Connection) -> dict[str, int]:
    since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(timespec="seconds")
    row = conn.execute(
        """
        SELECT
          COALESCE(SUM(new_txns), 0)       AS new_txns,
          COALESCE(SUM(duplicate_txns), 0) AS duplicate_txns,
          COALESCE(SUM(fixme_txns), 0)     AS fixme_txns,
          COALESCE(SUM(classified_by_rule), 0) AS by_rule,
          COALESCE(SUM(classified_by_ai), 0)   AS by_ai
        FROM simplefin_ingests
        WHERE started_at >= ?
        """,
        (since,),
    ).fetchone()
    return {
        "new_txns": int(row["new_txns"] or 0),
        "duplicate_txns": int(row["duplicate_txns"] or 0),
        "fixme_txns": int(row["fixme_txns"] or 0),
        "by_rule": int(row["by_rule"] or 0),
        "by_ai": int(row["by_ai"] or 0),
    }


def _render(
    request: Request,
    *,
    settings: Settings,
    conn: sqlite3.Connection,
    saved: bool = False,
    message: str | None = None,
    error: str | None = None,
) -> HTMLResponse:
    account_map = load_account_map(settings.simplefin_account_map_resolved)
    map_path = settings.simplefin_account_map_resolved
    map_exists = map_path.exists()
    map_raw = ""
    if map_exists:
        try:
            map_raw = map_path.read_text(encoding="utf-8")
        except OSError:
            map_raw = ""
    mappable = _mappable_accounts(conn)
    # Pre-compute fancy labels once so the template and the discovered
    # rows agree on what "mapped to Assets:X" looks like.
    path_to_label = {m.account_path: _fancy_label(m) for m in mappable}
    discovered = _discovered_accounts(conn, path_to_label=path_to_label)
    ctx = {
        "mode": settings.simplefin_mode,
        "interval_hours": settings.simplefin_fetch_interval_hours,
        "lookback_days": settings.simplefin_lookback_days,
        "ingests": _recent_ingests(conn, tz_name=settings.app_tz or "UTC"),
        "counters": _seven_day_counters(conn),
        "account_map": sorted(account_map.items()),
        "account_map_path": str(map_path),
        "account_map_raw": map_raw,
        "discovered": discovered,
        "mappable": mappable,
        "fancy_labels": [path_to_label[m.account_path] for m in mappable],
        "has_access_url": bool(
            settings.simplefin_access_url
            and settings.simplefin_access_url.get_secret_value()
        ),
        "saved": saved,
        "message": message,
        "error": error,
    }
    return request.app.state.templates.TemplateResponse(request, "simplefin.html", ctx)


@router.get("/simplefin", response_class=HTMLResponse)
def simplefin_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    return _render(request, settings=settings, conn=conn)


@router.post("/simplefin/mode", response_class=HTMLResponse)
def set_mode(
    request: Request,
    mode: str = Form(...),
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
    conn: sqlite3.Connection = Depends(get_db),
):
    mode = (mode or "").strip().lower()
    if mode not in MODE_VALUES:
        raise HTTPException(status_code=400, detail="invalid mode")

    store.set("simplefin_mode", mode)
    settings.apply_kv_overrides({"simplefin_mode": mode})

    _reregister_scheduler(request, settings)
    return _render(
        request,
        settings=settings,
        conn=conn,
        saved=True,
        message=f"Mode set to {mode}.",
    )


@router.post("/simplefin/settings", response_class=HTMLResponse)
def update_simplefin_settings(
    request: Request,
    simplefin_access_url: str | None = Form(default=None),
    simplefin_fetch_interval_hours: str | None = Form(default=None),
    simplefin_lookback_days: str | None = Form(default=None),
    settings: Settings = Depends(get_settings),
    store: AppSettingsStore = Depends(get_app_settings_store),
    conn: sqlite3.Connection = Depends(get_db),
):
    if simplefin_access_url:
        raw = simplefin_access_url.strip()
        if _looks_like_access_url(raw):
            store.set("simplefin_access_url", raw)
        else:
            try:
                resolved = claim_setup_token(raw)
            except (SimpleFINAuthError, SimpleFINError) as exc:
                settings.apply_kv_overrides(store.all())
                return _render(
                    request, settings=settings, conn=conn,
                    error=f"SimpleFIN setup token claim failed: {exc}",
                )
            store.set("simplefin_access_url", resolved)
    for key, raw in (
        ("simplefin_fetch_interval_hours", simplefin_fetch_interval_hours),
        ("simplefin_lookback_days", simplefin_lookback_days),
    ):
        if raw is None:
            continue
        v = raw.strip()
        if v:
            try:
                int(v)
            except ValueError:
                continue
            store.set(key, v)

    settings.apply_kv_overrides(store.all())
    _reregister_scheduler(request, settings)
    return _render(request, settings=settings, conn=conn, saved=True)


@router.post("/simplefin/account-map", response_class=HTMLResponse)
def save_account_map(
    request: Request,
    contents: str = Form(...),
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Bulk YAML import for SimpleFIN → Beancount mappings.

    Accepts either a flat ``{simplefin_id: account_path}`` mapping or one
    wrapped under an ``accounts:`` key. Writes the file (still used as the
    first-boot fallback) AND upserts each pair into
    ``accounts_meta.simplefin_account_id`` — which is the runtime source
    of truth. Pasting YAML here should always land on the page
    immediately; prior behavior wrote the file but never touched the DB,
    which looked like a silent no-op."""
    import yaml

    try:
        data = yaml.safe_load(contents) or {}
    except yaml.YAMLError as exc:
        return _render(
            request, settings=settings, conn=conn,
            error=f"invalid YAML: {exc}",
        )
    if not isinstance(data, dict):
        return _render(
            request, settings=settings, conn=conn,
            error="YAML must be a mapping of SimpleFIN account id → Beancount account",
        )
    pairs = data.get("accounts") if "accounts" in data and isinstance(data.get("accounts"), dict) else data
    if not isinstance(pairs, dict):
        return _render(
            request, settings=settings, conn=conn,
            error="YAML must be a mapping of SimpleFIN account id → Beancount account",
        )

    path = settings.simplefin_account_map_resolved
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")

    # Known paths in accounts_meta — YAML entries pointing at unknown
    # accounts are reported as skipped instead of silently dropped.
    known_paths = {
        str(r["account_path"])
        for r in conn.execute("SELECT account_path FROM accounts_meta").fetchall()
    }
    applied = 0
    skipped: list[str] = []
    try:
        for sf_id, acct_path in pairs.items():
            sid = str(sf_id or "").strip()
            ap = str(acct_path or "").strip()
            if not sid or not ap:
                continue
            if ap not in known_paths:
                skipped.append(f"{sid} → {ap}")
                continue
            # Any row currently claiming this simplefin_id gets released,
            # then the chosen path claims it. Re-mapping is allowed.
            conn.execute(
                "UPDATE accounts_meta SET simplefin_account_id = NULL "
                "WHERE simplefin_account_id = ?",
                (sid,),
            )
            cursor = conn.execute(
                "UPDATE accounts_meta SET simplefin_account_id = ? "
                "WHERE account_path = ?",
                (sid, ap),
            )
            if cursor.rowcount:
                applied += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    bits = [f"file saved to {path.name}", f"{applied} mapping(s) applied to DB"]
    if skipped:
        bits.append(
            f"{len(skipped)} skipped (account not opened in ledger): "
            + ", ".join(skipped[:3])
            + ("…" if len(skipped) > 3 else "")
        )
    return _render(request, settings=settings, conn=conn, saved=True,
                   message="; ".join(bits))


@router.post("/simplefin/fetch", response_class=HTMLResponse)
def fetch_now(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    rules: RuleService = Depends(get_rule_service),
    reviews: ReviewService = Depends(get_review_service),
    ai: AIService = Depends(get_ai_service),
):
    """Manual SimpleFIN fetch — runs as a background job so the user
    sees progress in the modal instead of "refresh in a few seconds".

    Fetches all mapped accounts from the SimpleFIN bridge, classifies
    new transactions, writes the classified entries to the ledger,
    then runs recurring + budget post-ingest hooks.
    """
    if settings.simplefin_mode == "disabled":
        raise HTTPException(status_code=400, detail="SimpleFIN is disabled")
    if not (settings.simplefin_access_url and settings.simplefin_access_url.get_secret_value()):
        raise HTTPException(status_code=400, detail="SimpleFIN access URL is not configured")
    # Setup-completeness gate: refuse to fetch when entities /
    # accounts / charts aren't locked down. The post-ingest hooks
    # (AI classify, receipt auto-match, transfer-pair detection)
    # would otherwise spend tokens classifying new transactions
    # into a chart that isn't ready. Visit /setup/recovery.
    if not getattr(request.app.state, "setup_required_complete", False):
        raise HTTPException(
            status_code=409,
            detail=(
                "SimpleFIN fetch is blocked until required setup is "
                "complete. Open /setup/recovery and finish the entity / "
                "account / chart steps first — those are what the AI "
                "classifier needs in place."
            ),
        )

    def _work(ctx):
        import asyncio as _asyncio
        ctx.emit("Starting SimpleFIN fetch", outcome="info")
        loop = _asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_run_fetch(ctx))
        finally:
            loop.close()

    async def _run_fetch(ctx):
        from lamella.features.bank_sync.writer import SimpleFINWriter

        client = SimpleFINClient(
            access_url=settings.simplefin_access_url.get_secret_value()  # type: ignore[union-attr]
        )
        try:
            writer = SimpleFINWriter(
                main_bean=settings.ledger_main,
                simplefin_path=settings.simplefin_transactions_path,
            )
            ingest = SimpleFINIngest(
                conn=conn,
                settings=settings,
                reader=reader,
                rules=rules,
                reviews=reviews,
                writer=writer,
                ai=ai,
            )
            ctx.emit("Contacting SimpleFIN bridge …", outcome="info")
            try:
                ingest_result = await ingest.run(client=client, trigger="manual")
            except SimpleFINError as exc:
                log.warning("manual simplefin fetch failed: %s", exc)
                ctx.emit(f"SimpleFIN fetch failed: {exc}", outcome="error")
                return {"error": str(exc)}
            classified_total = (
                ingest_result.classified_by_rule
                + ingest_result.classified_by_ai
            )
            ctx.emit(
                f"Ingest complete — accounts={len(ingest_result.per_account)}, "
                f"new_txns={ingest_result.new_txns}, "
                f"classified={classified_total}",
                outcome="success",
            )
            dispatcher = getattr(request.app.state, "dispatcher", None)
            if dispatcher is not None and getattr(ingest_result, "large_fixmes", None):
                from lamella.features.bank_sync.notify_hook import (
                    dispatch_large_fixmes,
                )
                await dispatch_large_fixmes(
                    dispatcher=dispatcher, result=ingest_result,
                )
                ctx.emit("Dispatched large-uncategorized-txn notifications", outcome="info")
            try:
                from beancount.core.data import Transaction
                from lamella.features.budgets.alerts import (
                    _channels_from_setting,
                    evaluate_and_alert,
                )
                from lamella.features.recurring.confirmations import (
                    monitor_after_ingest,
                )

                txns = [
                    e for e in reader.load(force=True).entries
                    if isinstance(e, Transaction)
                ]
                await monitor_after_ingest(
                    conn=conn, new_transactions=txns, dispatcher=dispatcher,
                )
                ctx.emit("Checked for recurring-subscription confirmations", outcome="info")
                await evaluate_and_alert(
                    conn=conn,
                    dispatcher=dispatcher,
                    entries=reader.load().entries,
                    channels=_channels_from_setting(settings.budget_alert_channels),
                )
                ctx.emit("Evaluated budget alerts", outcome="info")

                # Receipt auto-match: walk every unlinked txn in the
                # last 60 days and auto-link high-confidence Paperless
                # candidates. Runs unattended after every fetch so the
                # user no longer has to manually click "Hunt receipts"
                # for every ingest — the matcher that existed for
                # months but only ran from the UI finally runs on its
                # own.
                try:
                    from lamella.features.receipts.auto_match import sweep_recent
                    sweep = sweep_recent(
                        conn=conn, reader=reader, settings=settings,
                        emit=lambda msg, outcome="info": ctx.emit(msg, outcome=outcome),
                    )
                    if sweep.matched:
                        ctx.emit(
                            f"Auto-matched {sweep.matched} receipt(s) "
                            f"(scanned {sweep.scanned}, already-linked "
                            f"{sweep.already_linked}, no-candidate "
                            f"{sweep.no_candidate})",
                            outcome="success",
                        )
                    else:
                        ctx.emit(
                            f"Receipt auto-match: 0 matches over "
                            f"{sweep.scanned} txn(s) "
                            f"(already-linked {sweep.already_linked}, "
                            f"no-candidate {sweep.no_candidate}, "
                            f"low-confidence {sweep.low_confidence})",
                            outcome="info",
                        )
                    for err in sweep.errors[:5]:
                        ctx.emit(f"Auto-match: {err}", outcome="error")
                except Exception as exc:  # noqa: BLE001
                    log.warning("receipt auto-match sweep failed: %s", exc)
                    ctx.emit(f"Auto-match sweep failed: {exc}", outcome="error")
            except Exception as exc:  # noqa: BLE001
                log.warning("post-ingest hooks failed: %s", exc)
                ctx.emit(f"Post-ingest hooks failed: {exc}", outcome="error")
            return {
                "accounts": len(ingest_result.per_account),
                "new_txns": ingest_result.new_txns,
                "classified": classified_total,
                "fixme_txns": ingest_result.fixme_txns,
            }
        finally:
            await client.aclose()

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="simplefin-fetch",
        title="SimpleFIN fetch",
        fn=_work,
        return_url="/simplefin",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/simplefin"},
    )


@router.post("/simplefin/discover", response_class=HTMLResponse)
async def discover_accounts(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Contact the bridge, enumerate accounts, and cache them so the page
    can render a mapping dropdown per account. Does not write to the
    ledger — safe to run regardless of mode, as long as we have an
    access URL."""
    if not (settings.simplefin_access_url and settings.simplefin_access_url.get_secret_value()):
        return _render(
            request, settings=settings, conn=conn,
            error="Set a SimpleFIN access URL or setup token first.",
        )

    client = SimpleFINClient(
        access_url=settings.simplefin_access_url.get_secret_value()  # type: ignore[union-attr]
    )
    try:
        try:
            response = await client.fetch_accounts(lookback_days=1)
        except (SimpleFINAuthError, SimpleFINError) as exc:
            return _render(
                request, settings=settings, conn=conn,
                error=f"SimpleFIN discovery failed: {exc}",
            )
    finally:
        await client.aclose()

    count = _upsert_discovered_accounts(conn, response)
    return _render(
        request, settings=settings, conn=conn, saved=True,
        message=f"Discovered {count} account(s) from the bridge.",
    )


@router.post("/simplefin/map", response_class=HTMLResponse)
async def save_account_mapping(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Save the user's chosen SimpleFIN → Beancount mappings. Form fields
    arrive as ``map[<simplefin_id>] = <account_path>``. An empty value
    clears the mapping for that SimpleFIN id.

    Writes to ``accounts_meta.simplefin_account_id``, which the ingest
    reads at run time (see ``SimpleFINIngest._load_account_map``). The
    YAML map stays available as a fallback but is no longer the primary
    edit surface."""
    form = await request.form()
    mappable = _mappable_accounts(conn)
    known_paths = {m.account_path for m in mappable}
    label_to_path = {_fancy_label(m): m.account_path for m in mappable}

    updated = 0
    cleared = 0
    unresolved: list[str] = []
    for key, value in form.multi_items():
        if not key.startswith("map[") or not key.endswith("]"):
            continue
        simplefin_id = key[4:-1]
        chosen_raw = (value or "").strip()
        # Always release any existing claim on this SimpleFIN id first so
        # only one account_path ever owns it.
        conn.execute(
            "UPDATE accounts_meta SET simplefin_account_id = NULL "
            "WHERE simplefin_account_id = ?",
            (simplefin_id,),
        )
        if not chosen_raw:
            cleared += 1
            continue
        resolved = _resolve_label_to_path(chosen_raw, known_paths, label_to_path)
        if resolved is None:
            unresolved.append(f"{simplefin_id}: {chosen_raw}")
            continue
        cursor = conn.execute(
            "UPDATE accounts_meta SET simplefin_account_id = ? "
            "WHERE account_path = ?",
            (simplefin_id, resolved),
        )
        if cursor.rowcount:
            updated += 1
    conn.commit()

    bits: list[str] = []
    if updated:
        bits.append(f"{updated} mapped")
    if cleared:
        bits.append(f"{cleared} cleared")
    if unresolved:
        bits.append(
            f"{len(unresolved)} not recognized (pick from the list): "
            + ", ".join(unresolved[:2])
            + ("…" if len(unresolved) > 2 else "")
        )
    message = ", ".join(bits) if bits else "no changes"
    return _render(
        request, settings=settings, conn=conn, saved=True,
        message=f"Account mapping saved ({message}).",
    )


def _reregister_scheduler(request: Request, settings: Settings) -> None:
    """If a scheduler is attached to app state, refresh the simplefin job
    so an interval/mode change takes effect without a restart."""
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler is None:
        return
    register_fn = getattr(request.app.state, "_simplefin_register", None)
    if register_fn is None:
        return
    try:
        register_fn()
    except Exception as exc:  # noqa: BLE001
        log.warning("simplefin scheduler re-register failed: %s", exc)
