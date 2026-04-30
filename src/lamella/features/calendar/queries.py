# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Calendar queries over ledger + SQLite sources.

Two public entry points:

* ``activity_in_range(conn, entries, start, end, settings)`` — per-day
  aggregate for the month grid. One pass over ``entries`` (ledger txns
  are only in-memory) plus a handful of aggregate SQL queries. Single
  request should load < 1s for a dense month.
* ``day_activity(conn, entries, day, settings)`` — itemized per-source
  lists for the day view. Not aggregated; the UI groups + renders.

Dirty detection lives here. A day is ``dirty`` when it has a
``last_reviewed_at`` AND at least one activity timestamp exceeds it.
The activity timestamps are:

  * ``mileage_entries.created_at`` — new or back-imported mileage.
  * ``notes.captured_at`` — new day notes or range notes.
  * ``paperless_doc_index.modified_at`` — new OR re-OCR'd OR edited docs.
  * ``txn_classification_modified.modified_at`` — AI-apply or manual
    override write.
  * ``review_queue.created_at`` — a FIXME landed (SimpleFIN ingest).

The ledger is the source of truth for txns but txns themselves don't
carry a write-time. New-txn activity is picked up via the FIXME
review_queue row SimpleFIN creates per txn.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Iterable

log = logging.getLogger(__name__)


# --------------------------------------------------------------- types

@dataclass
class DayAggregate:
    review_date: date
    net_signed: Decimal = Decimal("0")
    txn_count: int = 0
    note_count: int = 0
    mileage_count: int = 0
    paperless_count: int = 0
    last_reviewed_at: datetime | None = None
    max_activity_at: datetime | None = None

    @property
    def has_activity(self) -> bool:
        return (
            self.txn_count
            or self.note_count
            or self.mileage_count
            or self.paperless_count
        ) > 0

    @property
    def status(self) -> str:
        if not self.has_activity:
            return "empty"
        if self.last_reviewed_at is None:
            return "unreviewed"
        if self.max_activity_at and self.max_activity_at > self.last_reviewed_at:
            return "dirty"
        return "reviewed"


@dataclass
class DayTxn:
    txn_hash: str
    date: date
    narration: str
    payee: str | None = None
    # Raw account paths so the template can run them through |alias
    # and |account_label for human-readable display. `account_summary`
    # stays for callers (AI audit, summarize_day prompt) that need
    # the concatenated form.
    from_account: str = ""
    to_account: str = ""
    account_summary: str = ""  # "from → to" — kept for back-compat
    amount: Decimal = Decimal("0")
    currency: str = "USD"
    is_fixme: bool = False
    # Every non-FIXME posting leg in this txn with its (account,
    # signed_amount). Used by the daily deltas card.
    postings: list[tuple[str, Decimal]] = field(default_factory=list)
    # Classification context — surfaced on the day view so the user
    # sees entity + category + status at a glance instead of having
    # to open every txn.
    entity: str | None = None          # second path segment of the non-FIXME side
    category_leaf: str | None = None   # last segment of the categorized account
    card_account: str | None = None    # raw path of the Assets/Liabilities leg
    expense_account: str | None = None # raw path of the Expenses/Income leg
    kind: str = "other"                # "expense" | "income" | "transfer" | "other"
    has_receipt: bool = False          # any receipt_links row for this hash
    is_rule_matched: bool = False      # meta carries lamella-rule-id
    is_ai_classified: bool = False     # meta carries lamella-ai-classified
    is_override: bool = False          # meta carries lamella-override-of
    receipt_required: bool = False     # txn is an expense over the threshold
    # Immutable UUIDv7 lineage id — used for /txn/{id} link-building.
    # Post-v3 every Transaction has one (the migration mints lineage on
    # disk for every entry that lacks it). Templates emit links via
    # this; ``txn_hash`` is retained for content-hash joins (receipts,
    # rule-matches, etc.). Optional default so test fixtures that
    # build minimal DayTxn instances by-position keep working.
    lamella_txn_id: str | None = None


@dataclass
class DayNote:
    id: int
    body: str
    captured_at: datetime | None
    is_day_note: bool  # active_from == active_to == day, unscoped


@dataclass
class DayMileage:
    id: int
    entry_date: date
    entry_time: str | None
    vehicle: str | None
    miles: float
    entity: str | None
    purpose: str | None
    created_at: datetime | None


@dataclass
class DayPaperless:
    paperless_id: int
    title: str | None
    created_date: date | None
    modified_at: datetime | None
    correspondent: str | None


@dataclass
class AccountDelta:
    """One account's net change on the day. Sorted by magnitude so
    the UI can show 'biggest moves first'."""

    account: str
    delta: Decimal
    currency: str = "USD"


@dataclass
class DayView:
    day: date
    transactions: list[DayTxn] = field(default_factory=list)
    notes: list[DayNote] = field(default_factory=list)
    mileage: list[DayMileage] = field(default_factory=list)
    paperless: list[DayPaperless] = field(default_factory=list)
    last_reviewed_at: datetime | None = None
    day_review_row: dict[str, Any] | None = None
    flags: list[dict[str, Any]] = field(default_factory=list)
    net_signed: Decimal = Decimal("0")
    # At-a-glance totals / deltas computed in day_activity.
    total_expenses: Decimal = Decimal("0")
    total_income: Decimal = Decimal("0")
    # Every non-FIXME account that moved today, with the sum of its
    # signed postings. Sort ascending by magnitude before render.
    account_deltas: list[AccountDelta] = field(default_factory=list)

    @property
    def status(self) -> str:
        has_activity = bool(
            self.transactions or self.notes or self.mileage or self.paperless
        )
        if not has_activity:
            return "empty"
        if self.last_reviewed_at is None:
            return "unreviewed"
        max_activity = _max_activity_for_day(
            self.transactions, self.notes, self.mileage, self.paperless
        )
        if max_activity and max_activity > self.last_reviewed_at:
            return "dirty"
        return "reviewed"


# --------------------------------------------------------------- parse helpers

def _parse_ts(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    s = str(raw).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        # Some stored timestamps use "YYYY-MM-DD HH:MM:SS" (space sep).
        try:
            return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


def _parse_date(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    s = str(raw).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _max(a: datetime | None, b: datetime | None) -> datetime | None:
    if a is None:
        return b
    if b is None:
        return a
    # Compare naive vs aware safely by coercing both to naive UTC-equivalent.
    an = a.replace(tzinfo=None) if a.tzinfo else a
    bn = b.replace(tzinfo=None) if b.tzinfo else b
    return a if an >= bn else b


def _max_activity_for_day(
    txns: list[DayTxn],
    notes: list[DayNote],
    mileage: list[DayMileage],
    paperless: list[DayPaperless],
) -> datetime | None:
    best: datetime | None = None
    for n in notes:
        best = _max(best, n.captured_at)
    for m in mileage:
        best = _max(best, m.created_at)
    for p in paperless:
        best = _max(best, p.modified_at)
    return best


# --------------------------------------------------------------- month-grid

def activity_in_range(
    conn: sqlite3.Connection,
    entries: Iterable[Any],
    start: date,
    end: date,
    *,
    settings=None,  # currently unused; reserved for future TZ-aware derivations
) -> dict[date, DayAggregate]:
    """Aggregate per-day activity for the month grid. ``end`` is inclusive.

    Ledger txns come from a single in-memory pass over ``entries``
    (typical month walks a few hundred entries at most). SQL queries
    use indexed range scans on entry_date / created_date / captured_at.
    """
    out: dict[date, DayAggregate] = {}

    def ensure(d: date) -> DayAggregate:
        agg = out.get(d)
        if agg is None:
            agg = DayAggregate(review_date=d)
            out[d] = agg
        return agg

    from beancount.core.data import Transaction
    from lamella.core.beancount_io import txn_hash as _txn_hash

    # Txn-hash → date map for all txns in the range. Used below to
    # attribute pinned memo activity back to the txn's date (not the
    # memo's captured_at date — a memo written today about a txn
    # from last Monday should make Monday's cell dirty, not today's).
    txn_date_by_hash: dict[str, date] = {}

    # --- ledger transactions (and signed net) ---------------------------
    # Signed net = sum of positive Income/Expense postings − negative ones.
    # We use the Asset/Liability side as "signed from the user's POV" —
    # negative goes out (expense), positive comes in (income).
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        txn_date = entry.date
        if not isinstance(txn_date, date) or txn_date < start or txn_date > end:
            continue
        agg = ensure(txn_date)
        agg.txn_count += 1
        try:
            txn_date_by_hash[_txn_hash(entry)] = txn_date
        except Exception:  # noqa: BLE001
            pass
        for posting in entry.postings or ():
            units = posting.units
            if units is None or units.number is None:
                continue
            acct = posting.account or ""
            # Treat Assets: + net as money in, Liabilities: + as money in
            # (credit card charge), so a cleaner "net money movement" is
            # the Expenses + Income sum. Sum Income as negative (reduces
            # outflow); sum Expenses as positive (outflow).
            if acct.startswith("Expenses:"):
                agg.net_signed += Decimal(units.number)
            elif acct.startswith("Income:"):
                agg.net_signed += Decimal(units.number)

    # --- notes ---------------------------------------------------------
    # Three kinds of notes to aggregate:
    #   (a) active-window notes (active_from..active_to overlaps range)
    #   (b) proximity notes (captured_at falls in range, no window)
    #   (c) txn-pinned memos (txn_hash matches a txn in the range) —
    #       attributed to the TXN'S DATE, not captured_at. A memo
    #       written today about a txn from last week must flip that
    #       past day to dirty, not today's cell.
    cols = [r[1] for r in conn.execute("PRAGMA table_info(notes)")]
    has_txn_hash = "txn_hash" in cols
    if has_txn_hash:
        rows = conn.execute(
            """
            SELECT captured_at, active_from, active_to, txn_hash
              FROM notes
             WHERE (active_from IS NOT NULL AND active_to IS NOT NULL
                    AND active_from <= ? AND active_to >= ?)
                OR (active_from IS NULL AND
                    substr(captured_at, 1, 10) BETWEEN ? AND ?)
                OR txn_hash IS NOT NULL
            """,
            (end.isoformat(), start.isoformat(),
             start.isoformat(), end.isoformat()),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT captured_at, active_from, active_to
              FROM notes
             WHERE (active_from IS NOT NULL AND active_to IS NOT NULL
                    AND active_from <= ? AND active_to >= ?)
                OR (active_from IS NULL AND
                    substr(captured_at, 1, 10) BETWEEN ? AND ?)
            """,
            (end.isoformat(), start.isoformat(),
             start.isoformat(), end.isoformat()),
        ).fetchall()
    for r in rows:
        af = _parse_date(r["active_from"])
        at = _parse_date(r["active_to"])
        cap = _parse_ts(r["captured_at"])
        pinned_hash = r["txn_hash"] if has_txn_hash else None
        if pinned_hash:
            # (c) Pinned memo. Attribute ONLY to the txn's date —
            # never to captured_at. If the referenced txn isn't in
            # the query range, the memo doesn't belong in this
            # grid window at all. Silently drop rather than fall
            # through to the capture-day branch (which would flag
            # the wrong day as dirty).
            if pinned_hash in txn_date_by_hash:
                d = txn_date_by_hash[pinned_hash]
                agg = ensure(d)
                agg.note_count += 1
                agg.max_activity_at = _max(agg.max_activity_at, cap)
            continue
        if af and at:
            cur = max(af, start)
            last = min(at, end)
            while cur <= last:
                agg = ensure(cur)
                agg.note_count += 1
                agg.max_activity_at = _max(agg.max_activity_at, cap)
                cur += timedelta(days=1)
        elif cap is not None:
            d = cap.date()
            if start <= d <= end:
                agg = ensure(d)
                agg.note_count += 1
                agg.max_activity_at = _max(agg.max_activity_at, cap)

    # --- mileage -------------------------------------------------------
    rows = conn.execute(
        "SELECT entry_date, created_at FROM mileage_entries "
        "WHERE entry_date BETWEEN ? AND ?",
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    for r in rows:
        d = _parse_date(r["entry_date"])
        if d is None:
            continue
        agg = ensure(d)
        agg.mileage_count += 1
        agg.max_activity_at = _max(agg.max_activity_at, _parse_ts(r["created_at"]))

    # --- paperless -----------------------------------------------------
    rows = conn.execute(
        "SELECT created_date, modified_at FROM paperless_doc_index "
        "WHERE created_date BETWEEN ? AND ?",
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    for r in rows:
        d = _parse_date(r["created_date"])
        if d is None:
            continue
        agg = ensure(d)
        agg.paperless_count += 1
        agg.max_activity_at = _max(agg.max_activity_at, _parse_ts(r["modified_at"]))

    # --- txn_classification_modified (dirty-only signal) --------------
    rows = conn.execute(
        "SELECT txn_date, modified_at FROM txn_classification_modified "
        "WHERE txn_date BETWEEN ? AND ?",
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    for r in rows:
        d = _parse_date(r["txn_date"])
        if d is None:
            continue
        agg = out.get(d)
        if agg is None:
            # The override's txn date lies outside ledger range — skip.
            continue
        agg.max_activity_at = _max(agg.max_activity_at, _parse_ts(r["modified_at"]))

    # --- review_queue.created_at (new FIXME dirty signal) -------------
    # SimpleFIN's ingest path opens a review_queue row per new txn.
    try:
        rows = conn.execute(
            "SELECT source_ref, created_at FROM review_queue "
            "WHERE substr(created_at, 1, 10) BETWEEN ? AND ?",
            (start.isoformat(), end.isoformat()),
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    # created_at gives us *when* the queue row landed; we don't yet have
    # the txn_date without parsing source_ref. Fall back to created_at
    # date — new-FIXME activity is roughly same-day as the txn date for
    # SimpleFIN ingest.
    for r in rows:
        ts = _parse_ts(r["created_at"])
        if ts is None:
            continue
        d = ts.date()
        if start <= d <= end:
            agg = ensure(d)
            agg.max_activity_at = _max(agg.max_activity_at, ts)

    # --- day_reviews ---------------------------------------------------
    rows = conn.execute(
        "SELECT review_date, last_reviewed_at FROM day_reviews "
        "WHERE review_date BETWEEN ? AND ?",
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    for r in rows:
        d = _parse_date(r["review_date"])
        if d is None:
            continue
        agg = ensure(d)
        agg.last_reviewed_at = _parse_ts(r["last_reviewed_at"])

    return out


# --------------------------------------------------------------- day view

def day_activity(
    conn: sqlite3.Connection,
    entries: Iterable[Any],
    day: date,
    *,
    settings=None,
) -> DayView:
    view = DayView(day=day)

    from beancount.core.data import Transaction

    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date != day:
            continue
        from_acct = "?"
        to_acct = "?"
        amount = Decimal("0")
        currency = "USD"
        is_fixme = False
        postings_detail: list[tuple[str, Decimal]] = []
        card_account: str | None = None
        expense_account: str | None = None
        for posting in entry.postings or ():
            units = posting.units
            acct = posting.account or ""
            if "FIXME" in acct.upper():
                is_fixme = True
            if acct.startswith(("Assets:", "Liabilities:")) and card_account is None:
                card_account = acct
            if acct.startswith(("Expenses:", "Income:")) and expense_account is None:
                if "FIXME" not in acct.upper():
                    expense_account = acct
            if units is None or units.number is None:
                continue
            signed = Decimal(units.number)
            postings_detail.append((acct, signed))
            if signed < 0:
                from_acct = acct
            else:
                to_acct = acct
                amount = signed
                currency = units.currency or currency
        meta = getattr(entry, "meta", None) or {}
        # Prefer a stable hash — recompute lazily via beancount_io.
        from lamella.core.beancount_io import txn_hash
        from lamella.core.identity import get_txn_id
        h = txn_hash(entry)
        lid = get_txn_id(entry)

        # Kind classification: what shape is this transaction?
        root_set = {
            (p.account or "").split(":", 1)[0] for p in entry.postings or ()
        }
        if "Income" in root_set:
            kind = "income"
        elif "Expenses" in root_set:
            kind = "expense"
        elif root_set.issubset({"Assets", "Liabilities", "Equity"}):
            kind = "transfer"
        else:
            kind = "other"

        # Entity: second path segment of the categorized leg. Fall
        # back to the card side if the expense side is FIXME.
        entity: str | None = None
        for candidate in (expense_account, to_acct, from_acct, card_account):
            if not candidate or "FIXME" in candidate.upper():
                continue
            parts = candidate.split(":")
            if len(parts) >= 2 and parts[1] not in ("FIXME",):
                entity = parts[1]
                break

        category_leaf: str | None = None
        if expense_account:
            category_leaf = expense_account.split(":")[-1]
        elif not is_fixme and to_acct:
            category_leaf = to_acct.split(":")[-1]

        view.transactions.append(
            DayTxn(
                txn_hash=h,
                date=entry.date,
                narration=entry.narration or "",
                payee=getattr(entry, "payee", None),
                from_account=from_acct,
                to_account=to_acct,
                account_summary=f"{from_acct} → {to_acct}",
                amount=amount,
                currency=currency,
                is_fixme=is_fixme,
                postings=postings_detail,
                entity=entity,
                category_leaf=category_leaf,
                card_account=card_account,
                expense_account=expense_account,
                kind=kind,
                is_rule_matched=bool(meta.get("lamella-rule-id")),
                is_ai_classified=bool(meta.get("lamella-ai-classified")),
                is_override="lamella-override-of" in (meta or {}),
                lamella_txn_id=lid,
            )
        )
        for posting in entry.postings or ():
            units = posting.units
            acct = posting.account or ""
            if units is None or units.number is None:
                continue
            signed = Decimal(units.number)
            if acct.startswith("Expenses:"):
                view.net_signed += signed
                # Positive expense postings = money going OUT.
                if signed > 0 and "FIXME" not in acct.upper():
                    view.total_expenses += signed
            elif acct.startswith("Income:"):
                view.net_signed += signed
                # Income postings are negative by convention (credit).
                if signed < 0:
                    view.total_income += -signed

    # Per-account deltas — sum the signed postings (excluding FIXME
    # legs so they don't show up with ambiguous direction). Sort by
    # magnitude so the biggest moves appear first.
    delta_map: dict[tuple[str, str], Decimal] = {}
    for t in view.transactions:
        for acct, signed in t.postings:
            if "FIXME" in acct.upper():
                continue
            key = (acct, t.currency)
            delta_map[key] = delta_map.get(key, Decimal("0")) + signed
    deltas = [
        AccountDelta(account=acct, delta=d, currency=ccy)
        for (acct, ccy), d in delta_map.items()
        if d != 0
    ]
    deltas.sort(key=lambda x: abs(x.delta), reverse=True)
    view.account_deltas = deltas

    # Batch receipt-link lookup for every txn on the day — one
    # indexed query, then mark each DayTxn.has_receipt.
    if view.transactions:
        hashes = [t.txn_hash for t in view.transactions]
        placeholders = ",".join("?" * len(hashes))
        try:
            rows = conn.execute(
                f"SELECT txn_hash FROM receipt_links WHERE txn_hash IN ({placeholders})",
                tuple(hashes),
            ).fetchall()
            linked = {r["txn_hash"] for r in rows}
        except sqlite3.OperationalError:
            linked = set()
        for t in view.transactions:
            if t.txn_hash in linked:
                t.has_receipt = True

    # Notes: any note active on `day` — includes day notes (single-day,
    # unscoped) as well as range notes covering `day`. Also include
    # memos pinned to any txn on this day so the user sees them here
    # regardless of when the memo was written.
    day_txn_hashes = [t.txn_hash for t in view.transactions]
    cols = [r[1] for r in conn.execute("PRAGMA table_info(notes)")]
    has_txn_hash = "txn_hash" in cols
    if has_txn_hash and day_txn_hashes:
        placeholders = ",".join("?" * len(day_txn_hashes))
        rows = conn.execute(
            f"""
            SELECT id, body, captured_at, active_from, active_to,
                   entity_scope, card_scope, txn_hash
              FROM notes
             WHERE (active_from IS NOT NULL AND active_to IS NOT NULL
                    AND ? BETWEEN active_from AND active_to)
                OR (active_from IS NULL AND
                    substr(captured_at, 1, 10) = ?)
                OR txn_hash IN ({placeholders})
             ORDER BY captured_at
            """,
            (day.isoformat(), day.isoformat(), *day_txn_hashes),
        ).fetchall()
    elif has_txn_hash:
        rows = conn.execute(
            """
            SELECT id, body, captured_at, active_from, active_to,
                   entity_scope, card_scope, txn_hash
              FROM notes
             WHERE (active_from IS NOT NULL AND active_to IS NOT NULL
                    AND ? BETWEEN active_from AND active_to)
                OR (active_from IS NULL AND
                    substr(captured_at, 1, 10) = ?)
             ORDER BY captured_at
            """,
            (day.isoformat(), day.isoformat()),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, body, captured_at, active_from, active_to,
                   entity_scope, card_scope
              FROM notes
             WHERE (active_from IS NOT NULL AND active_to IS NOT NULL
                    AND ? BETWEEN active_from AND active_to)
                OR (active_from IS NULL AND
                    substr(captured_at, 1, 10) = ?)
             ORDER BY captured_at
            """,
            (day.isoformat(), day.isoformat()),
        ).fetchall()
    for r in rows:
        af = _parse_date(r["active_from"])
        at = _parse_date(r["active_to"])
        is_day = (
            af is not None
            and at is not None
            and af == at == day
            and not r["entity_scope"]
            and not r["card_scope"]
        )
        view.notes.append(
            DayNote(
                id=int(r["id"]),
                body=r["body"],
                captured_at=_parse_ts(r["captured_at"]),
                is_day_note=is_day,
            )
        )

    # Mileage.
    rows = conn.execute(
        "SELECT id, entry_date, entry_time, vehicle, miles, entity, purpose, created_at "
        "FROM mileage_entries WHERE entry_date = ? ORDER BY COALESCE(entry_time, '00:00')",
        (day.isoformat(),),
    ).fetchall()
    for r in rows:
        view.mileage.append(
            DayMileage(
                id=int(r["id"]),
                entry_date=_parse_date(r["entry_date"]) or day,
                entry_time=r["entry_time"],
                vehicle=r["vehicle"],
                miles=float(r["miles"] or 0),
                entity=r["entity"],
                purpose=r["purpose"],
                created_at=_parse_ts(r["created_at"]),
            )
        )

    # Paperless.
    rows = conn.execute(
        "SELECT paperless_id, title, created_date, modified_at, correspondent_name "
        "FROM paperless_doc_index WHERE created_date = ? "
        "ORDER BY COALESCE(modified_at, '')",
        (day.isoformat(),),
    ).fetchall()
    for r in rows:
        view.paperless.append(
            DayPaperless(
                paperless_id=int(r["paperless_id"]),
                title=r["title"],
                created_date=_parse_date(r["created_date"]),
                modified_at=_parse_ts(r["modified_at"]),
                correspondent=r["correspondent_name"],
            )
        )

    # Day-review row.
    row = conn.execute(
        "SELECT review_date, last_reviewed_at, ai_summary, ai_summary_at, "
        "       ai_audit_result, ai_audit_result_at "
        "FROM day_reviews WHERE review_date = ?",
        (day.isoformat(),),
    ).fetchone()
    if row is not None:
        view.last_reviewed_at = _parse_ts(row["last_reviewed_at"])
        view.day_review_row = dict(row)

    return view
