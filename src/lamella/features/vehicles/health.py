# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Vehicle data-health registry.

Each check is a small function registered via `@register_check(...)`
with its schema requirements (`requires_columns`, `requires_tables`).
`compute_health(...)` iterates the registry and silently skips any
check whose requirements aren't yet satisfied — so Phase 4/5/6 can
register additional checks without forcing dead code into Phase 2.

Checks return zero or more `HealthIssue` records. The detail-page
card renders one clickable row per issue, where `fix_url` takes the
user straight to the remediation flow (filtered mileage list, edit
form, etc.). `severity` is 'info' / 'warn' / 'error' — purely for
the renderer to pick a style; nothing gates on it.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date as date_t
from typing import Callable

log = logging.getLogger(__name__)

__all__ = [
    "HealthIssue",
    "HealthCheckContext",
    "compute_health",
    "register_check",
]


@dataclass(frozen=True)
class HealthIssue:
    kind: str                       # stable identifier (e.g. "missing_splits")
    severity: str                   # 'info' | 'warn' | 'error'
    title: str                      # one-line user-facing label
    detail: str | None = None       # longer explanation, optional
    fix_url: str | None = None      # click target
    count: int = 1                  # how many rows contributed
    meta: dict = field(default_factory=dict)  # machine-readable extras


@dataclass
class HealthCheckContext:
    """Narrow bundle passed to every check function. Keeps callers
    from poking at raw sqlite when a helper exists."""
    conn: sqlite3.Connection
    vehicle: dict
    year: int
    # Precomputed, shared across checks to avoid N queries:
    filter_values: list[str]        # vehicle_slug + display_name fallback


_CheckFn = Callable[[HealthCheckContext], list[HealthIssue]]


@dataclass(frozen=True)
class _RegisteredCheck:
    name: str
    fn: _CheckFn
    requires_columns: tuple[str, ...]   # 'table.column' entries
    requires_tables: tuple[str, ...]


_REGISTRY: list[_RegisteredCheck] = []


def register_check(
    name: str,
    *,
    requires_columns: tuple[str, ...] = (),
    requires_tables: tuple[str, ...] = (),
) -> Callable[[_CheckFn], _CheckFn]:
    """Decorator. A check is skipped silently if any of its required
    columns / tables don't exist in the current schema — so later
    phases can add columns and register checks against them without
    forcing back-fill work in the earlier phase."""
    def decorator(fn: _CheckFn) -> _CheckFn:
        _REGISTRY.append(
            _RegisteredCheck(
                name=name,
                fn=fn,
                requires_columns=tuple(requires_columns),
                requires_tables=tuple(requires_tables),
            )
        )
        return fn
    return decorator


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, spec: str) -> bool:
    try:
        table, col = spec.split(".", 1)
    except ValueError:
        return False
    if not _table_exists(conn, table):
        return False
    for row in conn.execute(f"PRAGMA table_info({table})"):
        if row[1] == col:
            return True
    return False


def _check_requirements_met(
    conn: sqlite3.Connection, check: _RegisteredCheck,
) -> bool:
    for t in check.requires_tables:
        if not _table_exists(conn, t):
            return False
    for c in check.requires_columns:
        if not _column_exists(conn, c):
            return False
    return True


def compute_health(
    conn: sqlite3.Connection, *, vehicle: dict, year: int,
) -> list[HealthIssue]:
    """Run every registered check whose schema requirements are
    satisfied. Returns the combined issue list in stable order
    (registration order)."""
    filter_values = _filter_values(vehicle)
    ctx = HealthCheckContext(
        conn=conn, vehicle=vehicle, year=year, filter_values=filter_values,
    )
    issues: list[HealthIssue] = []
    for check in _REGISTRY:
        if not _check_requirements_met(conn, check):
            continue
        try:
            found = check.fn(ctx) or []
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "health check %s failed for %s: %s",
                check.name, vehicle.get("slug"), exc,
            )
            continue
        issues.extend(found)
    return issues


def _filter_values(vehicle: dict) -> list[str]:
    """Tolerant mileage-match values — matches the pattern in
    routes/vehicles.py::_mileage_filter_values so the health panel
    sees the same trips the detail page shows."""
    values = {vehicle.get("slug")}
    if vehicle.get("display_name"):
        values.add(vehicle["display_name"])
    return [v for v in values if v]


def _year_bounds(year: int) -> tuple[str, str]:
    return f"{year:04d}-01-01", f"{year + 1:04d}-01-01"


# -------------- Phase 2 checks --------------------------------------


@register_check(
    "missing_splits",
    requires_columns=(
        "mileage_trip_meta.commuting_miles",
        "mileage_entries.miles",
    ),
)
def _check_missing_splits(ctx: HealthCheckContext) -> list[HealthIssue]:
    """Trips in the selected year whose sidecar has no split recorded
    in any bucket (business / commuting / personal all NULL or
    missing). Business-use % depends on these."""
    start, end = _year_bounds(ctx.year)
    names = ctx.filter_values
    if not names:
        return []
    placeholders = ",".join(["?"] * len(names))
    row = ctx.conn.execute(
        f"""
        SELECT COUNT(*) AS n
          FROM mileage_entries e
          LEFT JOIN mileage_trip_meta m
                 ON m.entry_date = e.entry_date
                AND m.vehicle = e.vehicle
                AND m.miles = e.miles
         WHERE e.entry_date >= ? AND e.entry_date < ?
           AND (e.vehicle_slug = ? OR e.vehicle IN ({placeholders}))
           AND (
               m.business_miles  IS NULL
               AND m.commuting_miles IS NULL
               AND m.personal_miles  IS NULL
           )
        """,
        (start, end, ctx.vehicle.get("slug"), *names),
    ).fetchone()
    n = int(row["n"] or 0) if row else 0
    if n <= 0:
        return []
    display = ctx.vehicle.get("display_name") or ctx.vehicle.get("slug") or ""
    return [
        HealthIssue(
            kind="missing_splits",
            severity="warn",
            title=f"{n} trip{'s' if n != 1 else ''} without a business/commuting/personal split",
            detail=(
                "Schedule C Part IV needs per-trip category. Without a "
                "split, the actual-expense method can't compute a "
                "business-use percentage."
            ),
            fix_url=f"/mileage/all?vehicle={display}&fix=splits",
            count=n,
        ),
    ]


@register_check(
    "ambiguous_business_use",
    requires_tables=("mileage_entries", "mileage_trip_meta"),
)
def _check_ambiguous_business_use(ctx: HealthCheckContext) -> list[HealthIssue]:
    """Fires when the year has trips but ZERO recorded splits of any
    kind. This is the exact condition that triggers the Phase 2
    breaking-change banner, surfaced at the detail-page level too."""
    start, end = _year_bounds(ctx.year)
    names = ctx.filter_values
    if not names:
        return []
    placeholders = ",".join(["?"] * len(names))
    trips = ctx.conn.execute(
        f"""
        SELECT COUNT(*) AS n
          FROM mileage_entries
         WHERE entry_date >= ? AND entry_date < ?
           AND (vehicle_slug = ? OR vehicle IN ({placeholders}))
        """,
        (start, end, ctx.vehicle.get("slug"), *names),
    ).fetchone()
    if not trips or int(trips["n"] or 0) <= 0:
        return []
    splits = ctx.conn.execute(
        f"""
        SELECT COUNT(*) AS n
          FROM mileage_trip_meta
         WHERE entry_date >= ? AND entry_date < ?
           AND vehicle IN ({placeholders})
           AND (
               business_miles  IS NOT NULL
               OR commuting_miles IS NOT NULL
               OR personal_miles  IS NOT NULL
           )
        """,
        (start, end, *names),
    ).fetchone()
    if splits and int(splits["n"] or 0) > 0:
        return []
    display = ctx.vehicle.get("display_name") or ctx.vehicle.get("slug") or ""
    return [
        HealthIssue(
            kind="ambiguous_business_use",
            severity="warn",
            title=f"{ctx.year} business-use percentage is unknown",
            detail=(
                "No trip in this year has a business/commuting/personal "
                "split recorded. The actual-expense deduction shows as "
                "unknown until a split is logged — even one representative "
                "trip is enough to establish the ratio."
            ),
            fix_url=f"/mileage/all?vehicle={display}&fix=splits",
        ),
    ]


@register_check(
    "orphaned_trip",
    requires_tables=("mileage_entries", "vehicles"),
)
def _check_orphaned_trip(ctx: HealthCheckContext) -> list[HealthIssue]:
    """Trips referencing a vehicle whose slug / display_name no longer
    matches any row in `vehicles`. Fires across all years for this
    vehicle's _known_ identifiers — i.e. if the user renamed the
    vehicle, older rows stored under the previous display_name become
    orphans relative to the new name.

    We detect this by looking at trips under any of this vehicle's
    filter values whose vehicle_slug is NULL and whose `vehicle`
    string isn't the current display_name or slug.
    """
    slug = ctx.vehicle.get("slug")
    display = ctx.vehicle.get("display_name")
    if not slug:
        return []
    row = ctx.conn.execute(
        """
        SELECT COUNT(*) AS n
          FROM mileage_entries
         WHERE vehicle_slug IS NULL
           AND vehicle IS NOT NULL
           AND vehicle NOT IN (
               SELECT slug FROM vehicles
                UNION
               SELECT display_name FROM vehicles
                WHERE display_name IS NOT NULL
           )
           AND vehicle = ?
        """,
        (display or slug,),
    ).fetchone()
    n = int(row["n"] or 0) if row else 0
    if n <= 0:
        return []
    return [
        HealthIssue(
            kind="orphaned_trip",
            severity="warn",
            title=f"{n} orphaned trip{'s' if n != 1 else ''}",
            detail=(
                "These trips reference a vehicle name that no longer "
                "matches any row in the vehicle registry. Usually caused "
                "by a rename — stamp them with the current slug to fix."
            ),
            fix_url=f"/mileage/all?vehicle={display or slug}&fix=orphan",
            count=n,
        ),
    ]


@register_check(
    "odometer_non_monotonic",
    requires_tables=("mileage_entries",),
)
def _check_odometer_non_monotonic(ctx: HealthCheckContext) -> list[HealthIssue]:
    """Flag trips whose `odometer_end` is strictly less than the
    maximum `odometer_end` of any strictly-earlier-dated trip for
    the same vehicle. Deliberately uses max-of-earlier rather than
    previous-row comparison so a backdated import doesn't light up
    the panel — the check only fires when a later-dated trip actually
    goes backwards against what was observed before it.
    """
    names = ctx.filter_values
    if not names:
        return []
    placeholders = ",".join(["?"] * len(names))
    rows = ctx.conn.execute(
        f"""
        SELECT e.id, e.entry_date, e.odometer_end
          FROM mileage_entries e
         WHERE (e.vehicle_slug = ? OR e.vehicle IN ({placeholders}))
           AND e.odometer_end IS NOT NULL
        """,
        (ctx.vehicle.get("slug"), *names),
    ).fetchall()
    if not rows:
        return []
    # Sort by date ascending — any row whose odometer_end is less
    # than the running max-of-strictly-earlier is a violation.
    dated: list[tuple[date_t, int, int]] = []
    for r in rows:
        try:
            d = date_t.fromisoformat(str(r["entry_date"])[:10])
        except ValueError:
            continue
        dated.append((d, int(r["id"]), int(r["odometer_end"])))
    dated.sort(key=lambda t: (t[0], t[1]))
    max_earlier: int | None = None
    violations: list[int] = []
    for i, (d, rid, odo) in enumerate(dated):
        # "Strictly earlier" — build max only from rows with date < d.
        if i > 0:
            earlier = [row_odo for row_d, _, row_odo in dated[:i] if row_d < d]
            max_earlier = max(earlier) if earlier else None
        if max_earlier is not None and odo < max_earlier:
            violations.append(rid)
    if not violations:
        return []
    n = len(violations)
    display = ctx.vehicle.get("display_name") or ctx.vehicle.get("slug") or ""
    return [
        HealthIssue(
            kind="odometer_non_monotonic",
            severity="warn",
            title=f"{n} trip{'s' if n != 1 else ''} with a decreasing odometer",
            detail=(
                "A later-dated trip records an odometer reading below "
                "the highest reading on an earlier date. Usually a typo "
                "or a swapped start/end pair."
            ),
            fix_url=f"/mileage/all?vehicle={display}",
            count=n,
            meta={"entry_ids": violations},
        ),
    ]


@register_check(
    "missing_purpose",
    requires_tables=("mileage_entries",),
)
def _check_missing_purpose(ctx: HealthCheckContext) -> list[HealthIssue]:
    """Trips in the selected year missing all four substantiation
    fields (purpose, from_loc, to_loc, notes). The IRS mileage log
    export (Phase 5) won't be able to render these rows with a
    defensible business purpose.

    Zero-mile rows are excluded — those are "no trips today" markers
    (or maintenance days whose notes substantiate the entry on their
    own). A 0-mile row contributes 0 to the deduction, so IRS
    substantiation rules don't apply; flagging them is noise."""
    start, end = _year_bounds(ctx.year)
    names = ctx.filter_values
    if not names:
        return []
    placeholders = ",".join(["?"] * len(names))
    row = ctx.conn.execute(
        f"""
        SELECT COUNT(*) AS n
          FROM mileage_entries
         WHERE entry_date >= ? AND entry_date < ?
           AND (vehicle_slug = ? OR vehicle IN ({placeholders}))
           AND COALESCE(miles, 0) > 0
           AND COALESCE(purpose,  '') = ''
           AND COALESCE(from_loc, '') = ''
           AND COALESCE(to_loc,   '') = ''
           AND COALESCE(notes,    '') = ''
        """,
        (start, end, ctx.vehicle.get("slug"), *names),
    ).fetchone()
    n = int(row["n"] or 0) if row else 0
    if n <= 0:
        return []
    display = ctx.vehicle.get("display_name") or ctx.vehicle.get("slug") or ""
    return [
        HealthIssue(
            kind="missing_purpose",
            severity="info",
            title=f"{n} trip{'s' if n != 1 else ''} without a purpose or destination",
            detail=(
                "IRS substantiation wants a business purpose and "
                "destination per trip. The Phase 5 mileage-log PDF will "
                "show these rows with blank columns — fine for now, "
                "worth filling in before filing."
            ),
            fix_url=f"/mileage/all?vehicle={display}&fix=purpose",
            count=n,
        ),
    ]


@register_check(
    "renewal_past_due",
    requires_tables=("vehicle_renewals",),
)
def _check_renewal_past_due(ctx: HealthCheckContext) -> list[HealthIssue]:
    """Active vehicle_renewals rows with due_date < today. Fires
    regardless of year filter — past-due is past-due."""
    slug = ctx.vehicle.get("slug")
    if not slug:
        return []
    today_iso = date_t.today().isoformat()
    rows = ctx.conn.execute(
        "SELECT id, renewal_kind, due_date FROM vehicle_renewals "
        "WHERE vehicle_slug = ? AND is_active = 1 AND due_date < ? "
        "ORDER BY due_date ASC",
        (slug, today_iso),
    ).fetchall()
    if not rows:
        return []
    return [
        HealthIssue(
            kind="renewal_past_due",
            severity="warn",
            title=(
                f"{r['renewal_kind']} was due {r['due_date']} "
                f"({(date_t.fromisoformat(str(r['due_date'])[:10]) - date_t.today()).days} days ago)"
            ),
            detail=(
                "Renewals past their due date surface here until you "
                "mark them complete (or deactivate the row on the "
                "detail page)."
            ),
            fix_url=f"/vehicles/{slug}#renewals",
            count=1,
            meta={"renewal_id": int(r["id"])},
        )
        for r in rows
    ]


@register_check(
    "stale_valuation",
    requires_tables=("vehicle_valuations",),
)
def _check_stale_valuation(ctx: HealthCheckContext) -> list[HealthIssue]:
    """No vehicle_valuations row within the last 12 months. Only
    informational — a vehicle held for long periods without appraisals
    is perfectly normal; the health card surfaces it so the user can
    decide."""
    slug = ctx.vehicle.get("slug")
    if not slug:
        return []
    cutoff_iso = (
        date_t.today().replace(year=date_t.today().year - 1)
    ).isoformat() if date_t.today().month != 2 or date_t.today().day != 29 else (
        date_t.today().replace(year=date_t.today().year - 1, day=28)
    ).isoformat()
    row = ctx.conn.execute(
        "SELECT COUNT(*) AS n FROM vehicle_valuations "
        "WHERE vehicle_slug = ? AND as_of_date >= ?",
        (slug, cutoff_iso),
    ).fetchone()
    if row and int(row["n"] or 0) > 0:
        return []
    # Only fire if at least one valuation exists overall — brand-new
    # vehicles without any valuation shouldn't spam the panel.
    total = ctx.conn.execute(
        "SELECT COUNT(*) AS n FROM vehicle_valuations WHERE vehicle_slug = ?",
        (slug,),
    ).fetchone()
    if not total or int(total["n"] or 0) == 0:
        return []
    return [
        HealthIssue(
            kind="stale_valuation",
            severity="info",
            title="No valuation recorded in the last 12 months",
            detail=(
                "Periodic valuations keep the cost-basis panel accurate "
                "and simplify the disposal flow when you eventually sell."
            ),
            fix_url=f"/vehicles/{slug}#add-valuation",
        ),
    ]


@register_check(
    "business_use_swing",
    requires_columns=("mileage_trip_meta.business_miles",),
)
def _check_business_use_swing(ctx: HealthCheckContext) -> list[HealthIssue]:
    """Year-over-year business-use% that crossed the 50% listed-
    property threshold. Purely informational — §280F recapture
    considerations may apply; the user's CPA decides."""
    slug = ctx.vehicle.get("slug")
    names = ctx.filter_values
    if not slug or not names:
        return []
    placeholders = ",".join(["?"] * len(names))

    def _pct(year: int) -> float | None:
        row = ctx.conn.execute(
            f"""
            SELECT COALESCE(SUM(e.miles), 0) AS total,
                   COALESCE(SUM(m.business_miles), 0) AS biz
              FROM mileage_entries e
              LEFT JOIN mileage_trip_meta m
                     ON m.entry_date = e.entry_date
                    AND m.vehicle = e.vehicle
                    AND m.miles = e.miles
             WHERE e.entry_date >= ? AND e.entry_date < ?
               AND (e.vehicle_slug = ? OR e.vehicle IN ({placeholders}))
            """,
            (
                f"{year:04d}-01-01", f"{year + 1:04d}-01-01",
                slug, *names,
            ),
        ).fetchone()
        total = float(row["total"] or 0) if row else 0
        biz = float(row["biz"] or 0) if row else 0
        if total <= 0:
            return None
        return biz / total

    cur = _pct(ctx.year)
    prior = _pct(ctx.year - 1)
    if cur is None or prior is None:
        return []
    # Trigger when they straddle the 0.5 threshold in either direction.
    if (cur >= 0.5) == (prior >= 0.5):
        return []
    direction = "dropped below" if cur < 0.5 else "crossed above"
    return [
        HealthIssue(
            kind="business_use_swing",
            severity="info",
            title=(
                f"Business-use {direction} 50% "
                f"({ctx.year - 1}: {prior * 100:.0f}% → {ctx.year}: {cur * 100:.0f}%)"
            ),
            detail=(
                "Listed-property §280F rules often hinge on the 50% "
                "threshold. A swing either direction can trigger "
                "depreciation recapture or change what's deductible — "
                "confirm with your tax professional."
            ),
            fix_url=f"/vehicles/{slug}?year={ctx.year}",
        ),
    ]


@register_check(
    "yearly_row_drift",
    requires_columns=("vehicle_yearly_mileage.business_miles",),
)
def _check_yearly_row_drift(ctx: HealthCheckContext) -> list[HealthIssue]:
    """Fires when the user-entered vehicle_yearly_mileage.business_miles
    differs from the trip rollup by more than 5%. Indicates the yearly
    row is stale vs. the trip log (common after fresh trip imports)."""
    slug = ctx.vehicle.get("slug")
    if not slug:
        return []
    yrow = ctx.conn.execute(
        "SELECT business_miles FROM vehicle_yearly_mileage "
        "WHERE vehicle_slug = ? AND year = ?",
        (slug, ctx.year),
    ).fetchone()
    if not yrow or yrow["business_miles"] is None:
        return []
    yearly_biz = float(yrow["business_miles"])

    names = ctx.filter_values
    if not names:
        return []
    placeholders = ",".join(["?"] * len(names))
    start, end = _year_bounds(ctx.year)
    trip_row = ctx.conn.execute(
        f"""
        SELECT COALESCE(SUM(m.business_miles), 0) AS biz
          FROM mileage_entries e
          LEFT JOIN mileage_trip_meta m
                 ON m.entry_date = e.entry_date
                AND m.vehicle = e.vehicle
                AND m.miles = e.miles
         WHERE e.entry_date >= ? AND e.entry_date < ?
           AND (e.vehicle_slug = ? OR e.vehicle IN ({placeholders}))
        """,
        (start, end, slug, *names),
    ).fetchone()
    trip_biz = float(trip_row["biz"] or 0) if trip_row else 0.0
    if yearly_biz <= 0 and trip_biz <= 0:
        return []
    # Drift threshold: 5% of the larger side.
    denom = max(abs(yearly_biz), abs(trip_biz), 1.0)
    drift = abs(yearly_biz - trip_biz) / denom
    if drift <= 0.05:
        return []
    return [
        HealthIssue(
            kind="yearly_row_drift",
            severity="info",
            title=(
                f"{ctx.year} yearly row ({yearly_biz:,.0f} mi) differs "
                f"from trip log ({trip_biz:,.0f} mi)"
            ),
            detail=(
                "Schedule C Part IV transcribes from the yearly row. "
                "Use the trip rollup as the source of truth, or leave "
                "the yearly row intact if you already filed."
            ),
            fix_url=f"/vehicles/{slug}?year={ctx.year}&fix=yearly_drift",
            meta={
                "yearly_business_miles": yearly_biz,
                "trip_business_miles": trip_biz,
            },
        ),
    ]
