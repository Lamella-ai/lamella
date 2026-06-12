# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Plain-language balances + activity summary for the dashboard.

Reads accounts_meta + ledger entries to produce
[(entity_display, [(account_display, last_four, balance, kind)])]
grouped structures with no raw path strings. Also counts daily activity
and surfaces "next up" + "needs attention" hints.

Per-entity dashboard widgets — KPI tiles, rolling P&L, expense
composition + 12-month trend, top payees, inventory discovery — also
live here. Each ``compute_*`` helper is read-through cached against
``business_cache``: a payload stamped with the ledger mtime is reused
as long as the mtime hasn't moved since it was written.
"""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Callable, Iterable, Iterator

from beancount.core.data import Open, Transaction

from lamella.core.beancount_io.reader import LoadedLedger
from lamella.core.registry.alias import alias_for


@dataclass(frozen=True)
class AccountLine:
    display_name: str
    last_four: str | None
    kind: str | None
    balance: Decimal
    available: Decimal | None
    account_path: str


@dataclass(frozen=True)
class EntityGroup:
    slug: str
    display_name: str
    lines: list[AccountLine]


def _balances_by_path(entries: Iterable) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for entry in entries:
        if isinstance(entry, Transaction):
            for p in entry.postings:
                if p.units and p.units.number is not None and (p.units.currency in (None, "USD")):
                    totals[p.account] += Decimal(p.units.number)
    return dict(totals)


def money_groups(conn: sqlite3.Connection, entries: Iterable) -> list[EntityGroup]:
    """Return per-entity groups of account balances, restricted to
    Assets and Liabilities (not Expenses/Income/Equity — those aren't
    "money" in the dashboard sense)."""
    balances = _balances_by_path(entries)

    # Pull all accounts_meta rows that are still active.
    rows = conn.execute(
        """
        SELECT account_path, display_name, kind, institution, last_four,
               entity_slug
        FROM accounts_meta
        WHERE closed_on IS NULL AND is_active = 1
        ORDER BY entity_slug, display_name
        """
    ).fetchall()
    entities_meta = {
        r["slug"]: (r["display_name"] or r["slug"])
        for r in conn.execute("SELECT slug, display_name FROM entities WHERE is_active = 1").fetchall()
    }

    by_entity: dict[str, list[AccountLine]] = defaultdict(list)
    for r in rows:
        path = r["account_path"]
        if not path:
            continue
        root = path.split(":", 1)[0]
        if root not in ("Assets", "Liabilities"):
            continue
        bal = balances.get(path, Decimal("0"))
        display = r["display_name"] or alias_for(conn, path)
        line = AccountLine(
            display_name=display,
            last_four=r["last_four"],
            kind=r["kind"],
            balance=bal,
            available=None,
            account_path=path,
        )
        by_entity[r["entity_slug"] or ""].append(line)

    groups: list[EntityGroup] = []
    for slug, lines in by_entity.items():
        groups.append(EntityGroup(
            slug=slug or "unlabeled",
            display_name=entities_meta.get(slug, slug or "Unlabeled"),
            lines=lines,
        ))
    # Sort: Personal-like first, then alphabetical.
    groups.sort(key=lambda g: (0 if g.slug.lower() == "personal" else 1, g.display_name.lower()))
    return groups


def activity_summary(conn: sqlite3.Connection) -> dict[str, int]:
    """Counts for 'Today' dashboard row.

    Semantics:
      - ``new_txns_today``: review_queue rows whose ``created_at`` is
        today. Proxy for "transactions added to the ledger today".
      - ``receipts_linked_today``: document_links rows whose
        ``linked_at`` (the moment a Paperless doc got bound to a txn)
        is today. NOT the txn date — a receipt linked today against a
        purchase from last month still counts.
    """
    today_iso = date.today().isoformat()
    # New FIXME txns created today (via simplefin_ingests perhaps, or
    # just review_queue rows created today).
    new_txns = conn.execute(
        "SELECT COUNT(*) AS n FROM review_queue WHERE DATE(created_at) = ?",
        (today_iso,),
    ).fetchone()["n"]
    needs_cat_rq = conn.execute(
        "SELECT COUNT(*) AS n FROM review_queue WHERE resolved_at IS NULL"
    ).fetchone()["n"]
    # Linked TODAY = link row's linked_at is today. The corresponding
    # transaction can be from any date.
    receipts_today = conn.execute(
        "SELECT COUNT(*) AS n FROM document_links WHERE DATE(linked_at) = ?",
        (today_iso,),
    ).fetchone()["n"]
    # Deferred >= 3: "needs attention".
    needs_attention = conn.execute(
        "SELECT COUNT(*) AS n FROM review_queue "
        "WHERE resolved_at IS NULL AND deferred_count >= 3"
    ).fetchone()["n"]
    # NEXTGEN Phase B2 full swing: SimpleFIN now defers un-classified
    # rows to staging instead of emitting FIXMEs to the bean file, so
    # the authoritative "needs categorizing" count lives in
    # staged_transactions + staged_decisions. Sum both surfaces for a
    # single total that reflects reality until the legacy review_queue
    # path is retired.
    try:
        staged_pending = conn.execute(
            """
            SELECT COUNT(*) AS n
              FROM staged_transactions t
              LEFT JOIN staged_decisions d ON d.staged_id = t.id
             WHERE t.status IN ('new', 'classified', 'matched')
               AND (d.needs_review = 1 OR d.staged_id IS NULL)
            """
        ).fetchone()["n"]
    except sqlite3.OperationalError:
        staged_pending = 0
    return {
        "new_txns_today": int(new_txns or 0),
        "needs_categorizing": int(needs_cat_rq or 0) + int(staged_pending or 0),
        "needs_categorizing_staged": int(staged_pending or 0),
        "needs_categorizing_legacy_fixme": int(needs_cat_rq or 0),
        "receipts_linked_today": int(receipts_today or 0),
        "needs_attention": int(needs_attention or 0),
    }


# ---------------------------------------------------------------------------
# Per-entity dashboard widgets (Phase 1 of BUSINESS_SECTION_IMPROVEMENTS.md).
# ---------------------------------------------------------------------------


PERIOD_LABELS: tuple[str, ...] = ("30d", "mtd", "1mo", "ytd", "1yr", "all")
DEFAULT_PERIOD = "1mo"


def _is_fixme_account(acct: str | None) -> bool:
    return bool(acct) and acct.split(":")[-1].upper() == "FIXME"


def _account_belongs_to(acct: str, slug: str) -> bool:
    """An account 'belongs to' an entity if its slug appears as any
    segment past the root (entity-first convention OR sub-segment)."""
    if not acct or not slug:
        return False
    parts = acct.split(":")
    return slug in parts[1:]


def _txn_has_fixme(entry: Transaction) -> bool:
    return any(_is_fixme_account(p.account) for p in entry.postings)


def ledger_mtime_int(loaded: LoadedLedger) -> int:
    """Single integer signature for cache invalidation: max mtime across
    all files Beancount loaded. Any write to any included file moves it."""
    if not loaded.mtime_signature:
        return 0
    return int(max(mt for _, mt in loaded.mtime_signature))


@dataclass(frozen=True)
class PeriodWindow:
    label: str
    current_start: date
    current_end: date            # inclusive
    prior_start: date
    prior_end: date              # inclusive

    @property
    def cache_key(self) -> str:
        if self.label in PERIOD_LABELS:
            return self.label
        return f"custom:{self.current_start.isoformat()}:{self.current_end.isoformat()}"

    @property
    def is_all_time(self) -> bool:
        return self.label == "all"


def resolve_period(
    label: str,
    *,
    today: date | None = None,
    custom_start: date | None = None,
    custom_end: date | None = None,
) -> PeriodWindow:
    """Map a period label to a (current, prior) date window pair.

    Prior windows match the current window's length so deltas compare
    apples to apples — never a partial current period against a complete
    prior one."""
    today = today or date.today()
    label = (label or DEFAULT_PERIOD).strip().lower()

    if label == "30d" or label == "1mo":
        cur_end = today
        cur_start = today - timedelta(days=29)         # 30-day inclusive window
        prior_end = cur_start - timedelta(days=1)
        prior_start = prior_end - timedelta(days=29)
        return PeriodWindow(label, cur_start, cur_end, prior_start, prior_end)

    if label == "mtd":
        cur_start = today.replace(day=1)
        cur_end = today
        elapsed = (cur_end - cur_start).days
        # Same number of elapsed days a month ago.
        prev_month_anchor = cur_start - timedelta(days=1)
        prior_start = prev_month_anchor.replace(day=1)
        prior_end = prior_start + timedelta(days=elapsed)
        return PeriodWindow(label, cur_start, cur_end, prior_start, prior_end)

    if label == "ytd":
        cur_start = today.replace(month=1, day=1)
        cur_end = today
        prior_start = cur_start.replace(year=cur_start.year - 1)
        try:
            prior_end = cur_end.replace(year=cur_end.year - 1)
        except ValueError:                              # Feb 29 on a non-leap prior year
            prior_end = cur_end.replace(year=cur_end.year - 1, day=28)
        return PeriodWindow(label, cur_start, cur_end, prior_start, prior_end)

    if label == "1yr":
        cur_end = today
        cur_start = today - timedelta(days=364)
        prior_end = cur_start - timedelta(days=1)
        prior_start = prior_end - timedelta(days=364)
        return PeriodWindow(label, cur_start, cur_end, prior_start, prior_end)

    if label == "all":
        cur_end = today
        cur_start = date(1900, 1, 1)
        return PeriodWindow(label, cur_start, cur_end, cur_start, cur_start)

    if label == "custom" and custom_start and custom_end:
        cur_start, cur_end = custom_start, custom_end
        span = (cur_end - cur_start).days
        prior_end = cur_start - timedelta(days=1)
        prior_start = prior_end - timedelta(days=span)
        return PeriodWindow("custom", cur_start, cur_end, prior_start, prior_end)

    # Unknown label → fall back to default.
    return resolve_period(DEFAULT_PERIOD, today=today)


# --- cache layer -----------------------------------------------------------


def _cache_get(
    conn: sqlite3.Connection, slug: str, widget: str, period_key: str, mtime: int,
) -> dict | None:
    row = conn.execute(
        """
        SELECT payload_json FROM business_cache
        WHERE entity_slug = ? AND widget_key = ? AND period_key = ?
              AND ledger_mtime = ?
        """,
        (slug, widget, period_key, mtime),
    ).fetchone()
    if row is None:
        return None
    try:
        return json.loads(row["payload_json"])
    except (TypeError, ValueError):
        return None


def _cache_put(
    conn: sqlite3.Connection, slug: str, widget: str, period_key: str,
    payload: dict, mtime: int,
) -> None:
    conn.execute(
        """
        INSERT INTO business_cache
            (entity_slug, widget_key, period_key, payload_json, computed_at, ledger_mtime)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(entity_slug, widget_key, period_key) DO UPDATE SET
            payload_json = excluded.payload_json,
            computed_at = CURRENT_TIMESTAMP,
            ledger_mtime = excluded.ledger_mtime
        """,
        (slug, widget, period_key, json.dumps(payload, default=str), mtime),
    )
    conn.commit()


def _cached(
    conn: sqlite3.Connection, slug: str, widget: str, period_key: str,
    mtime: int, builder: Callable[[], dict],
) -> dict:
    hit = _cache_get(conn, slug, widget, period_key, mtime)
    if hit is not None:
        return hit
    payload = builder()
    _cache_put(conn, slug, widget, period_key, payload, mtime)
    return payload


# --- ledger walks ----------------------------------------------------------


def _entity_txns(entries: Iterable, slug: str) -> Iterator[Transaction]:
    """Yield every Transaction with at least one posting that belongs to
    ``slug``. Caller still needs to inspect individual postings."""
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if any(_account_belongs_to(p.account or "", slug) for p in entry.postings):
            yield entry


def _sum_postings(
    entries: Iterable, slug: str, *, root: str, start: date, end: date,
    exclude_fixme_txns: bool = False,
) -> Decimal:
    """Sum signed posting amounts for accounts under ``Root:slug:*`` in
    [start, end]. ``root`` is 'Income' or 'Expenses'. When
    ``exclude_fixme_txns`` is set, any transaction with a FIXME leg is
    skipped entirely (not just the FIXME leg)."""
    prefix = f"{root}:{slug}:"
    total = Decimal("0")
    for entry in _entity_txns(entries, slug):
        if entry.date < start or entry.date > end:
            continue
        if exclude_fixme_txns and _txn_has_fixme(entry):
            continue
        for p in entry.postings:
            acct = p.account or ""
            if not acct.startswith(prefix):
                continue
            if p.units is None or p.units.number is None:
                continue
            total += Decimal(p.units.number)
    return total


# --- public compute_* helpers ---------------------------------------------


def _expense_sign_by_account(
    entries: Iterable, slug: str, start: date, end: date, *, exclude_fixme_txns: bool = False,
) -> dict[str, Decimal]:
    """Infer each expense account's sign convention in the window.

    Returns a per-account multiplier (+1 or -1). If an account's net raw
    postings are negative, we treat it as an inverted-sign feed for this
    window and flip amounts to expense-positive when aggregating.
    """
    prefix = f"Expenses:{slug}:"
    totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for entry in _entity_txns(entries, slug):
        if entry.date < start or entry.date > end:
            continue
        if exclude_fixme_txns and _txn_has_fixme(entry):
            continue
        for p in entry.postings:
            acct = p.account or ""
            if not acct.startswith(prefix):
                continue
            if p.units is None or p.units.number is None:
                continue
            totals[acct] += Decimal(p.units.number)
    out: dict[str, Decimal] = {}
    for acct, total in totals.items():
        out[acct] = Decimal("-1") if total < 0 else Decimal("1")
    return out

def compute_revenue(
    entries: Iterable, slug: str, start: date, end: date,
) -> Decimal:
    """Sum of Income:<slug>:* postings, negated. Beancount stores income
    as credits (negative numbers); negating gives a positive headline
    revenue figure."""
    raw = _sum_postings(entries, slug, root="Income", start=start, end=end)
    return -raw


def compute_expenses(
    entries: Iterable, slug: str, start: date, end: date,
    *, exclude_fixme: bool = True,
) -> Decimal:
    """Sum of Expenses:<slug>:* postings. Skips entire transactions that
    carry a FIXME leg by default — matches the spec: 'skips any posting
    on a transaction that has a FIXME leg.'"""
    sign_by_acct = _expense_sign_by_account(
        entries, slug, start, end, exclude_fixme_txns=exclude_fixme,
    )
    prefix = f"Expenses:{slug}:"
    total = Decimal("0")
    for entry in _entity_txns(entries, slug):
        if entry.date < start or entry.date > end:
            continue
        if exclude_fixme and _txn_has_fixme(entry):
            continue
        for p in entry.postings:
            acct = p.account or ""
            if not acct.startswith(prefix):
                continue
            if p.units is None or p.units.number is None:
                continue
            mult = sign_by_acct.get(acct, Decimal("1"))
            total += Decimal(p.units.number) * mult
    return -total


def compute_liquid_cash(conn: sqlite3.Connection, entries: Iterable, slug: str) -> Decimal:
    """All-time balance across the entity's checking/savings/cash accounts.

    accounts_meta is the source of truth for which paths count as
    'liquid' — kind IN ('checking','savings','cash') AND active. The
    ledger is then walked once per qualifying path (or rather, all
    qualifying paths in one pass) for the running balance."""
    rows = conn.execute(
        """
        SELECT account_path FROM accounts_meta
        WHERE entity_slug = ? AND is_active = 1 AND closed_on IS NULL
              AND kind IN ('checking', 'savings', 'cash')
        """,
        (slug,),
    ).fetchall()
    paths = {r["account_path"] for r in rows if r["account_path"]}
    if not paths:
        return Decimal("0")
    total = Decimal("0")
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        for p in entry.postings:
            if p.account in paths and p.units and p.units.number is not None:
                total += Decimal(p.units.number)
    return total


def compute_period_kpis(
    conn: sqlite3.Connection, loaded: LoadedLedger, slug: str, period: PeriodWindow,
) -> dict:
    """Header-strip values for the four KPI tiles. Cached per
    (slug, 'kpis', period.cache_key)."""
    mtime = ledger_mtime_int(loaded)
    entries = loaded.entries

    def _build() -> dict:
        rev_cur = compute_revenue(entries, slug, period.current_start, period.current_end)
        exp_cur = compute_expenses(entries, slug, period.current_start, period.current_end)
        net_cur = rev_cur + exp_cur

        if period.is_all_time:
            rev_prior = exp_prior = net_prior = Decimal("0")
        else:
            rev_prior = compute_revenue(entries, slug, period.prior_start, period.prior_end)
            exp_prior = compute_expenses(entries, slug, period.prior_start, period.prior_end)
            net_prior = rev_prior + exp_prior

        cash = compute_liquid_cash(conn, entries, slug)
        return {
            "revenue_current": str(rev_cur),
            "revenue_prior": str(rev_prior),
            "expenses_current": str(exp_cur),
            "expenses_prior": str(exp_prior),
            "net_current": str(net_cur),
            "net_prior": str(net_prior),
            "cash": str(cash),
            "current_start": period.current_start.isoformat(),
            "current_end": period.current_end.isoformat(),
            "prior_start": period.prior_start.isoformat(),
            "prior_end": period.prior_end.isoformat(),
        }

    return _cached(conn, slug, "kpis", period.cache_key, mtime, _build)


def compute_monthly_pnl(
    conn: sqlite3.Connection, loaded: LoadedLedger, slug: str,
) -> dict:
    """Trailing 12 months: revenue, expenses (FIXME-clean), net per month.
    Period-selector independent. Cached as widget='pnl_monthly', period='12m'."""
    mtime = ledger_mtime_int(loaded)
    entries = loaded.entries

    def _build() -> dict:
        today = date.today()
        # 12 month buckets ending in the current month.
        buckets: list[tuple[date, date, str]] = []
        anchor = today.replace(day=1)
        for i in range(11, -1, -1):
            month_start = _month_offset(anchor, -i)
            month_end = _month_end(month_start)
            label = month_start.strftime("%b %y")
            buckets.append((month_start, month_end, label))

        revenue: list[str] = []
        expenses: list[str] = []
        net: list[str] = []
        labels: list[str] = []
        for start, end, label in buckets:
            r = compute_revenue(entries, slug, start, end)
            e = compute_expenses(entries, slug, start, end)
            revenue.append(str(r))
            expenses.append(str(e))
            net.append(str(r + e))
            labels.append(label)
        return {"months": labels, "revenue": revenue, "expenses": expenses, "net": net}

    return _cached(conn, slug, "pnl_monthly", "12m", mtime, _build)


def _month_offset(d: date, months: int) -> date:
    """Add ``months`` (can be negative) to a first-of-month date."""
    m_index = (d.year * 12 + d.month - 1) + months
    year = m_index // 12
    month = m_index % 12 + 1
    return date(year, month, 1)


def _month_end(first: date) -> date:
    nxt = _month_offset(first, 1)
    return nxt - timedelta(days=1)


def compute_expense_composition(
    conn: sqlite3.Connection, loaded: LoadedLedger, slug: str, period: PeriodWindow,
    *, top_n: int = 10,
) -> dict:
    """Top-N expense accounts in the selected period, with deltas vs. the
    equivalent prior window. Cached per (slug, 'expense_composition', period)."""
    mtime = ledger_mtime_int(loaded)
    entries = loaded.entries

    def _build() -> dict:
        cur = _expense_rollup_by_account(
            entries, slug, period.current_start, period.current_end,
        )
        if period.is_all_time:
            prior: dict[str, Decimal] = {}
        else:
            prior = _expense_rollup_by_account(
                entries, slug, period.prior_start, period.prior_end,
            )
        # Top-N by current spend (skip empty buckets).
        top = sorted(cur.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
        rows = []
        for acct, amt in top:
            prev = prior.get(acct, Decimal("0"))
            delta_pct: float | None
            if prev > 0:
                delta_pct = float((amt - prev) / prev * 100)
            elif amt > 0:
                delta_pct = None     # "new" — render as a dash, not "+inf%"
            else:
                delta_pct = 0.0
            rows.append({
                "account": acct,
                "amount": str(amt),
                "prior": str(prev),
                "delta_pct": delta_pct,
            })
        return {"rows": rows}

    return _cached(conn, slug, "expense_composition", period.cache_key, mtime, _build)


def _expense_rollup_by_account(
    entries: Iterable, slug: str, start: date, end: date,
) -> dict[str, Decimal]:
    """Per-account expense totals in [start, end], excluding any
    transaction that carries a FIXME leg. Returns positive numbers."""
    prefix = f"Expenses:{slug}:"
    out: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    sign_by_acct = _expense_sign_by_account(entries, slug, start, end, exclude_fixme_txns=True)
    for entry in _entity_txns(entries, slug):
        if entry.date < start or entry.date > end:
            continue
        if _txn_has_fixme(entry):
            continue
        for p in entry.postings:
            acct = p.account or ""
            if not acct.startswith(prefix):
                continue
            if p.units is None or p.units.number is None:
                continue
            mult = sign_by_acct.get(acct, Decimal("1"))
            out[acct] -= Decimal(p.units.number) * mult
    return dict(out)


def compute_expense_trend(
    conn: sqlite3.Connection, loaded: LoadedLedger, slug: str, *, top_n: int = 6,
) -> dict:
    """Stacked-area data: top-N expense categories over the trailing 12
    months. Independent of period selector. Cached as
    (slug, 'expense_trend', '12m')."""
    mtime = ledger_mtime_int(loaded)
    entries = loaded.entries

    def _build() -> dict:
        today = date.today()
        anchor = today.replace(day=1)
        months: list[tuple[date, date, str]] = []
        for i in range(11, -1, -1):
            ms = _month_offset(anchor, -i)
            months.append((ms, _month_end(ms), ms.strftime("%b %y")))

        # Pick categories using the full 12-month window so the trend
        # surfaces structural top spenders, not a recent blip.
        full_start, full_end = months[0][0], months[-1][1]
        full_rollup = _expense_rollup_by_account(entries, slug, full_start, full_end)
        top_accounts = [a for a, _ in sorted(full_rollup.items(), key=lambda kv: kv[1], reverse=True)[:top_n]]

        series: dict[str, list[str]] = {a: [] for a in top_accounts}
        other: list[str] = []
        for ms, me, _label in months:
            month_rollup = _expense_rollup_by_account(entries, slug, ms, me)
            other_total = Decimal("0")
            for acct, amt in month_rollup.items():
                if acct in series:
                    pass
                else:
                    other_total += amt
            for acct in top_accounts:
                series[acct].append(str(month_rollup.get(acct, Decimal("0"))))
            other.append(str(other_total))
        return {
            "months": [m[2] for m in months],
            "categories": top_accounts,
            "series": series,
            "other": other,
        }

    return _cached(conn, slug, "expense_trend", "12m", mtime, _build)


def compute_top_payees(
    conn: sqlite3.Connection, loaded: LoadedLedger, slug: str, period: PeriodWindow,
    *, limit: int = 20,
) -> dict:
    """Top payees by absolute expense spend in the selected period.
    Groups by Transaction.payee — there is no payee registry today, so
    no 1099 flag yet (spec defers that until a payee table exists)."""
    mtime = ledger_mtime_int(loaded)
    entries = loaded.entries
    prefix = f"Expenses:{slug}:"

    def _build() -> dict:
        totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        counts: dict[str, int] = defaultdict(int)
        sign_by_acct = _expense_sign_by_account(
            entries, slug, period.current_start, period.current_end, exclude_fixme_txns=True,
        )
        for entry in _entity_txns(entries, slug):
            if entry.date < period.current_start or entry.date > period.current_end:
                continue
            if _txn_has_fixme(entry):
                continue
            payee = (entry.payee or "").strip()
            if not payee:
                continue
            for p in entry.postings:
                acct = p.account or ""
                if not acct.startswith(prefix):
                    continue
                if p.units is None or p.units.number is None:
                    continue
                mult = sign_by_acct.get(acct, Decimal("1"))
                totals[payee] -= Decimal(p.units.number) * mult
            counts[payee] += 1
        # Drop payees with zero/negative net (refunds, etc).
        ranked = sorted(
            ((p, t, counts[p]) for p, t in totals.items() if t < 0),
            key=lambda row: row[1],
        )[:limit]
        return {
            "rows": [
                {"payee": p, "amount": str(t), "txn_count": c}
                for p, t, c in ranked
            ],
        }

    return _cached(conn, slug, "top_payees", period.cache_key, mtime, _build)


def discover_inventory_entities(entries: Iterable) -> set[str]:
    """Return slugs that have any ``Assets:<slug>:Inventory:*`` Open
    directive. Drives the conditional rendering of the inventory card."""
    out: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Open):
            continue
        path = entry.account or ""
        parts = path.split(":")
        if len(parts) >= 4 and parts[0] == "Assets" and parts[2] == "Inventory":
            out.add(parts[1])
    return out


def compute_inventory_summary(
    conn: sqlite3.Connection, loaded: LoadedLedger, slug: str,
) -> dict:
    """Inventory card payload — current balance, YTD change, YTD COGS.
    Independent of the period selector (always YTD for the deltas).
    Cached as (slug, 'inventory', 'ytd')."""
    mtime = ledger_mtime_int(loaded)
    entries = loaded.entries
    inv_prefix = f"Assets:{slug}:Inventory:"
    cogs_prefix = f"Expenses:{slug}:COGS:"

    def _build() -> dict:
        today = date.today()
        ytd_start = today.replace(month=1, day=1)
        balance = Decimal("0")
        ytd_delta = Decimal("0")
        ytd_cogs = Decimal("0")
        for entry in entries:
            if not isinstance(entry, Transaction):
                continue
            for p in entry.postings:
                acct = p.account or ""
                if p.units is None or p.units.number is None:
                    continue
                amt = Decimal(p.units.number)
                if acct.startswith(inv_prefix):
                    balance += amt
                    if entry.date >= ytd_start:
                        ytd_delta += amt
                if acct.startswith(cogs_prefix) and entry.date >= ytd_start:
                    if not _txn_has_fixme(entry):
                        ytd_cogs += amt
        return {
            "balance": str(balance),
            "ytd_delta": str(ytd_delta),
            "ytd_cogs": str(ytd_cogs),
        }

    return _cached(conn, slug, "inventory", "ytd", mtime, _build)


def compute_vehicle_summary(
    conn: sqlite3.Connection, loaded: LoadedLedger, slug: str, mileage_rate: float,
) -> list[dict]:
    """One row per active vehicle linked to this entity: YTD business
    miles, standard-mileage deduction at the configured rate, YTD actual
    expenses booked under Expenses:<slug>:Vehicles:<vehicle>:*. Not
    cached — rows are derived from SQLite + a small ledger slice and the
    payload is small."""
    today = date.today()
    year = today.year
    rows = conn.execute(
        """
        SELECT v.slug, v.display_name,
               m.business_miles
        FROM vehicles v
        LEFT JOIN vehicle_yearly_mileage m
               ON m.vehicle_slug = v.slug AND m.year = ?
        WHERE COALESCE(v.is_active, 1) = 1 AND v.entity_slug = ?
        ORDER BY v.slug
        """,
        (year, slug),
    ).fetchall()
    if not rows:
        return []

    ytd_start = today.replace(month=1, day=1)
    actual: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for entry in loaded.entries:
        if not isinstance(entry, Transaction) or entry.date < ytd_start:
            continue
        if _txn_has_fixme(entry):
            continue
        for p in entry.postings:
            acct = p.account or ""
            if not acct.startswith(f"Expenses:{slug}:Vehicles:"):
                continue
            parts = acct.split(":")
            if len(parts) < 4:
                continue
            vehicle_seg = parts[3]
            if p.units is None or p.units.number is None:
                continue
            actual[vehicle_seg] += Decimal(p.units.number)

    out: list[dict] = []
    for r in rows:
        miles = int(r["business_miles"] or 0)
        deduction = Decimal(str(mileage_rate)) * Decimal(miles)
        out.append({
            "slug": r["slug"],
            "display_name": r["display_name"] or r["slug"],
            "business_miles": miles,
            "standard_deduction": str(deduction),
            "ytd_actual_expenses": str(actual.get(r["slug"], Decimal("0"))),
            "mileage_rate": mileage_rate,
        })
    return out


def compute_property_summary(
    conn: sqlite3.Connection, loaded: LoadedLedger, slug: str,
) -> list[dict]:
    """One row per active property linked to this entity: YTD rental
    income, YTD operating expenses, NOI. Income paths are matched
    flexibly to handle both ``Income:<slug>:Property:<prop>:Rent`` and
    plain ``Income:<slug>:Property:<prop>``."""
    rows = conn.execute(
        """
        SELECT slug, display_name, address
        FROM properties
        WHERE is_active = 1 AND entity_slug = ?
        ORDER BY slug
        """,
        (slug,),
    ).fetchall()
    if not rows:
        return []

    today = date.today()
    ytd_start = today.replace(month=1, day=1)
    rents: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    opex: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))

    income_root = f"Income:{slug}:Property:"
    expense_root = f"Expenses:{slug}:Property:"
    for entry in loaded.entries:
        if not isinstance(entry, Transaction) or entry.date < ytd_start:
            continue
        if _txn_has_fixme(entry):
            continue
        for p in entry.postings:
            acct = p.account or ""
            if p.units is None or p.units.number is None:
                continue
            amt = Decimal(p.units.number)
            if acct.startswith(income_root):
                parts = acct.split(":")
                if len(parts) >= 4:
                    rents[parts[3]] += amt
            elif acct.startswith(expense_root):
                parts = acct.split(":")
                if len(parts) >= 4:
                    opex[parts[3]] += amt

    out: list[dict] = []
    for r in rows:
        prop_slug = r["slug"]
        income = -rents.get(prop_slug, Decimal("0"))    # credits are negative
        expenses = opex.get(prop_slug, Decimal("0"))
        out.append({
            "slug": prop_slug,
            "display_name": r["display_name"] or prop_slug,
            "address": r["address"],
            "ytd_income": str(income),
            "ytd_expenses": str(expenses),
            "ytd_noi": str(income - expenses),
        })
    return out


def expense_account_label(conn: sqlite3.Connection, account_path: str, slug: str) -> str:
    """Human-readable label for an expense account on this dashboard.
    Strips the ``Expenses:<slug>:`` prefix if present so the bar chart
    doesn't repeat the entity name on every row."""
    label = alias_for(conn, account_path)
    prefix = f"Expenses · {slug}"
    if label.lower().startswith(prefix.lower() + " · "):
        return label[len(prefix) + 3:]
    # Fall back to the trailing path segment if alias_for returned the raw path.
    if label == account_path and account_path.startswith(f"Expenses:{slug}:"):
        return account_path[len(f"Expenses:{slug}:"):].replace(":", " · ")
    return label
