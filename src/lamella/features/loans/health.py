# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Per-loan health assessment.

`assess()` is the single entry point every UI surface calls. Given a
loan row + loaded ledger entries + db connection + settings, it
returns a `LoanHealth` record: an ordered list of next actions plus
structured sub-reports that the detail page's panels consume
directly.

WP1 establishes the interface and fills in what it can without WP3
(coverage), WP4 (scaffolding check engine), WP7 (escrow flows), or
WP8 (anomalies) having landed yet. Those sub-reports default to
`None` or empty collections until their WPs plug in.

The function is pure with respect to `(loan, entries, conn, settings,
as_of)`: no ledger writes, no side effects, no randomness. Calling
it twice on the same inputs must return equal `LoanHealth` values.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Sequence

from beancount.core.data import Transaction

from lamella.features.loans.next_action_priorities import (
    priority_for,
    severity_rank,
    STABLE_KEY_FIELDS,
)


Severity = Literal["blocking", "attention", "info"]
SummaryBadge = Literal["ok", "attention", "blocking"]


@dataclass(frozen=True)
class _PauseLite:
    """Minimal pause record that satisfies coverage.build_schedule's
    duck-typed ``getattr(p, "start_date", None)`` contract. Adapter
    over the dict shape returned by ``read_loan_pauses``."""
    start_date: str
    end_date: str | None


@dataclass(frozen=True)
class NextAction:
    """One actionable item the UI renders as a card, chip, or banner."""

    kind: str
    severity: Severity
    title: str
    detail: str
    action_label: str
    method: Literal["GET", "POST"]
    endpoint: str
    payload: dict[str, Any] = field(default_factory=dict)
    priority: int = 500


@dataclass(frozen=True)
class FundingReport:
    is_funded: bool
    funding_date: date | None
    offset_account: str | None


@dataclass(frozen=True)
class AnchorFreshnessReport:
    anchor_count: int
    latest_anchor_date: date | None
    days_since_latest: int | None
    is_stale: bool


@dataclass(frozen=True)
class EscrowReport:
    is_configured: bool
    monthly_inflow: Decimal | None
    annual_inflow: Decimal | None
    annual_outflow_configured: Decimal | None
    projected_shortage: bool


@dataclass(frozen=True)
class ScaffoldingReport:
    """Placeholder shape WP4 fills in. WP1 ships an empty report."""

    issues: list = field(default_factory=list)  # list[scaffolding.Issue]

    @property
    def has_blockers(self) -> bool:
        return any(getattr(i, "severity", "") == "blocking" for i in self.issues)


# CoverageReport is defined in loans.coverage and re-exported here so
# older callers (and tests written before WP3 landed) can keep
# importing it from health. See coverage.py for the canonical shape.
from lamella.features.loans.coverage import CoverageReport  # noqa: E402


@dataclass(frozen=True)
class LoanHealth:
    loan_slug: str
    summary_badge: SummaryBadge
    next_actions: list[NextAction]
    scaffolding: ScaffoldingReport
    funding: FundingReport
    coverage: CoverageReport | None
    anchor_freshness: AnchorFreshnessReport
    escrow: EscrowReport | None
    anomalies: list  # list[anomalies.Anomaly]
    # WP12 — forbearance / pause windows, dicts from read_loan_pauses.
    pauses: list = field(default_factory=list)
    # WP5 — payment-group proposer + confirmed cache. Shape:
    # {"proposed": list[ProposedGroup], "confirmed": list[dict]}.
    # Keys are optional; templates use `.get(...)` guards.
    payment_groups: dict = field(default_factory=dict)


# --------------------------------------------------------------------- helpers


def _as_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _as_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _today(as_of: date | None) -> date:
    return as_of or datetime.now(timezone.utc).date()


# ------------------------------------------------------------------ sub-builders


def _build_funding_report(
    loan: dict, entries: Sequence[Any],
) -> FundingReport:
    """A loan is funded when a transaction tagged ``#lamella-loan-funding``
    with matching ``lamella-loan-slug`` meta exists in the ledger."""
    slug = loan.get("slug") or ""
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        tags = getattr(entry, "tags", None) or set()
        if "lamella-loan-funding" not in tags:
            continue
        meta = getattr(entry, "meta", None) or {}
        if meta.get("lamella-loan-slug") != slug:
            continue
        liability_path = loan.get("liability_account_path")
        offset: str | None = None
        for p in entry.postings:
            acct = getattr(p, "account", None)
            if acct and acct != liability_path:
                offset = acct
                break
        return FundingReport(
            is_funded=True,
            funding_date=_as_date(entry.date),
            offset_account=offset,
        )
    return FundingReport(is_funded=False, funding_date=None, offset_account=None)


def _build_anchor_freshness_report(
    loan: dict, conn: Any, as_of: date,
) -> AnchorFreshnessReport:
    slug = loan.get("slug")
    if conn is None or slug is None:
        return AnchorFreshnessReport(0, None, None, True)
    rows = conn.execute(
        "SELECT as_of_date FROM loan_balance_anchors "
        "WHERE loan_slug = ? ORDER BY as_of_date DESC",
        (slug,),
    ).fetchall()
    if not rows:
        return AnchorFreshnessReport(0, None, None, True)
    latest = _as_date(rows[0][0] if not isinstance(rows[0], dict) else rows[0]["as_of_date"])
    if latest is None:
        return AnchorFreshnessReport(len(rows), None, None, True)
    days = (as_of - latest).days
    # > 90 days without an update = stale. Statement cadences are
    # monthly, so a 90-day gap means the user skipped three.
    return AnchorFreshnessReport(
        anchor_count=len(rows),
        latest_anchor_date=latest,
        days_since_latest=days,
        is_stale=days > 90,
    )


def _build_escrow_report(loan: dict) -> EscrowReport | None:
    monthly = _as_decimal(loan.get("escrow_monthly"))
    if not monthly or monthly <= 0:
        # Loan doesn't have servicer-managed escrow configured —
        # whole panel is irrelevant. Returning None lets the layout
        # layer hide it without show_all=1.
        return None
    tax = _as_decimal(loan.get("property_tax_monthly")) or Decimal("0")
    insurance = _as_decimal(loan.get("insurance_monthly")) or Decimal("0")
    annual_in = monthly * 12
    annual_out = (tax + insurance) * 12
    shortage = bool(annual_out > 0 and annual_in < annual_out)
    return EscrowReport(
        is_configured=True,
        monthly_inflow=monthly,
        annual_inflow=annual_in,
        annual_outflow_configured=annual_out,
        projected_shortage=shortage,
    )


# ----------------------------------------------------------- next-action builder


def _build_next_actions(
    loan: dict,
    funding: FundingReport,
    anchor: AnchorFreshnessReport,
    escrow: EscrowReport | None,
    scaffolding: ScaffoldingReport,
    anomalies: list,
    coverage: CoverageReport | None,
) -> list[NextAction]:
    slug = loan.get("slug") or ""
    actions: list[NextAction] = []

    # Scaffolding blockers come first — funding can't succeed if
    # liability/offset accounts aren't open. WP1 ships with empty
    # scaffolding issues; WP4 extends.
    for issue in getattr(scaffolding, "issues", []):
        kind = f"scaffolding-{getattr(issue, 'kind', 'unknown')}"
        actions.append(NextAction(
            kind=kind,
            severity=getattr(issue, "severity", "attention"),
            title=getattr(issue, "message", "Scaffolding issue"),
            detail=getattr(issue, "message", ""),
            action_label="Fix" if getattr(issue, "can_autofix", False) else "Review",
            method="POST" if getattr(issue, "can_autofix", False) else "GET",
            endpoint=getattr(issue, "fix_endpoint", "") or f"/settings/loans/{slug}/edit",
            payload=getattr(issue, "fix_payload", {}) or {"path": getattr(issue, "path", "")},
            priority=priority_for(kind),
        ))

    if not funding.is_funded:
        principal = loan.get("original_principal") or "0"
        display = loan.get("display_name") or slug
        actions.append(NextAction(
            kind="fund-initial",
            severity="blocking",
            title="Post the initial funding transaction",
            detail=(
                f"This loan has principal {principal} but no funding block. "
                f"Pick an offset account (property cost-basis, opening "
                f"balances, or checking) to post the origination."
            ),
            action_label="Post funding",
            method="GET",
            endpoint=f"/settings/loans/{slug}#fund-initial-form",
            payload={"display_name": display},
            priority=priority_for("fund-initial"),
        ))

    if anchor.anchor_count == 0 and funding.is_funded:
        actions.append(NextAction(
            kind="add-anchor",
            severity="attention",
            title="Add a balance anchor",
            detail=(
                "No balance anchors on record. A monthly-statement "
                "balance pins the amortization model to reality — "
                "worth adding even when the model looks right."
            ),
            action_label="Add anchor",
            method="GET",
            endpoint=f"/settings/loans/{slug}",
            payload={},
            priority=priority_for("add-anchor"),
        ))
    elif anchor.is_stale and anchor.latest_anchor_date is not None:
        actions.append(NextAction(
            kind="stale-anchor",
            severity="attention",
            title="Refresh the balance anchor",
            detail=(
                f"Last anchor is {anchor.days_since_latest} days old. "
                f"The servicer's most recent statement balance would "
                f"bring the anchored-balance summary back in sync."
            ),
            action_label="Add anchor",
            method="GET",
            endpoint=f"/settings/loans/{slug}",
            payload={"last_anchor_date": anchor.latest_anchor_date.isoformat()},
            priority=priority_for("stale-anchor"),
        ))

    if escrow is not None and escrow.projected_shortage:
        actions.append(NextAction(
            kind="escrow-shortage-projected",
            severity="attention",
            title="Escrow inflow below projected outflow",
            detail=(
                f"Configured escrow {escrow.annual_inflow}/yr is below "
                f"configured tax+insurance {escrow.annual_outflow_configured}/yr. "
                f"Expect the servicer to raise the escrow requirement "
                f"at the next annual analysis."
            ),
            action_label="Review settings",
            method="GET",
            endpoint=f"/settings/loans/{slug}/edit",
            payload={},
            priority=priority_for("escrow-shortage-projected"),
        ))

    # WP8 anomalies — each gets a next-action entry. Anomaly authors
    # can attach a specific `recommended_action` (a NextAction); when
    # absent we synthesize a generic "Review" action with the
    # anomaly's own severity and kind.
    for an in anomalies:
        recommended = getattr(an, "recommended_action", None)
        if isinstance(recommended, NextAction):
            actions.append(recommended)
            continue
        kind = getattr(an, "kind", "unknown")
        actions.append(NextAction(
            kind="anomaly",
            severity=getattr(an, "severity", "attention"),
            title=f"Anomaly: {kind}",
            detail=getattr(an, "detail", ""),
            action_label="Review",
            method="GET",
            endpoint=f"/settings/loans/{slug}",
            payload={"anomaly_kind": kind},
            priority=priority_for("anomaly"),
        ))

    # WP3 coverage — missing-payment actions, with graceful degradation.
    # When `long_gap_detected` is True (≥3 consecutive missing), emit
    # ONE `long-payment-gap` item instead of N individual
    # missing-payment rows. Users with a loan mid-forbearance or with
    # a big chunk of un-imported history shouldn't see 10 blocking
    # actions — a single attention item explaining the gap is the
    # right UX until WP12 models real pause periods.
    if coverage is not None:
        if getattr(coverage, "long_gap_detected", False):
            start = getattr(coverage, "long_gap_start", None)
            end = getattr(coverage, "long_gap_end", None)
            count = getattr(coverage, "long_gap_count", 0)
            total_missing = getattr(coverage, "missing_count", count)
            if start and end:
                detail = (
                    f"No payments recorded for {count} consecutive month(s) "
                    f"from {start} to {end} "
                    f"(total missing in window: {total_missing}). "
                    f"If this loan was in forbearance / deferment, WP12 "
                    f"will let you record pause periods explicitly."
                )
            else:
                detail = (
                    f"{total_missing} expected payment(s) are missing "
                    f"from the ledger."
                )
            actions.append(NextAction(
                kind="long-payment-gap",
                severity="attention",
                title=f"Long gap in payments ({count} months)",
                detail=detail,
                action_label="Review",
                method="GET",
                endpoint=f"/settings/loans/{slug}",
                payload={
                    "long_gap_start": start.isoformat() if start else "",
                    "long_gap_end": end.isoformat() if end else "",
                    "count": count,
                },
                priority=priority_for("long-payment-gap"),
            ))
        else:
            for row in getattr(coverage, "rows", []):
                if getattr(row, "status", "") != "missing":
                    continue
                expected = getattr(row, "expected", None)
                if expected is None:
                    continue
                actions.append(NextAction(
                    kind="missing-payment",
                    severity="attention",
                    title=f"Missing payment for {expected.expected_date}",
                    detail=(
                        f"Expected payment #{expected.n} on {expected.expected_date} "
                        f"has no matching transaction in the ledger."
                    ),
                    action_label="Record it",
                    method="GET",
                    endpoint=f"/settings/loans/{slug}",
                    payload={"expected_date": expected.expected_date.isoformat()},
                    priority=priority_for("missing-payment"),
                ))

    return actions


def _sort_actions(actions: list[NextAction]) -> list[NextAction]:
    """Stable sort on (severity, priority, kind, stable_key, insertion_index).

    Per §3.1 of FEATURE_LOANS_IMPLEMENTATION.md. Using `enumerate`
    as terminal tie-breaker means same-input ordering is byte-equal
    across runs.
    """
    def key(pair: tuple[int, NextAction]) -> tuple:
        i, a = pair
        stable_field = STABLE_KEY_FIELDS.get(a.kind, "")
        stable_value = ""
        if stable_field:
            raw = a.payload.get(stable_field, "")
            stable_value = str(raw) if raw is not None else ""
        return (severity_rank(a.severity), a.priority, a.kind, stable_value, i)

    indexed = list(enumerate(actions))
    indexed.sort(key=key)
    return [a for _i, a in indexed]


def _summary_badge(actions: list[NextAction]) -> SummaryBadge:
    if any(a.severity == "blocking" for a in actions):
        return "blocking"
    if any(a.severity == "attention" for a in actions):
        return "attention"
    return "ok"


# ------------------------------------------------------------------------- main


# Sentinel for "please auto-compute this sub-report." Distinguishes
# "caller didn't pass one" from "caller passed None to explicitly
# disable." Tests use `coverage=None` / `scaffolding=None` to keep
# a sub-report empty; real callers omit the kwarg and get the
# auto-computed report.
_AUTO: Any = object()


def assess(
    loan: dict,
    entries: Sequence[Any],
    conn: Any,
    settings: Any,
    *,
    as_of: date | None = None,
    scaffolding: ScaffoldingReport | None = _AUTO,
    coverage: CoverageReport | None = _AUTO,
    anomalies: list | None = _AUTO,
) -> LoanHealth:
    """Compute a loan's current health.

    `entries`, `conn`, `settings` are pre-loaded by the caller —
    `assess` does not load the ledger or open DB connections. The
    `scaffolding`, `coverage`, `anomalies` keyword arguments accept
    pre-computed sub-reports from other WPs. Omitting a kwarg
    auto-computes that sub-report from the current inputs; passing
    `None` explicitly disables it (tests use this to isolate scope).

    Pure: same inputs always return equal outputs.
    """
    today = _today(as_of)
    if scaffolding is _AUTO:
        # Lazy-call the scaffolding check so route handlers get the
        # full health picture without manually composing sub-reports.
        from lamella.features.loans import scaffolding as _sf
        scaffolding = ScaffoldingReport(
            issues=_sf.check(loan, entries, conn, settings),
        )
    elif scaffolding is None:
        scaffolding = ScaffoldingReport(issues=[])

    if coverage is _AUTO:
        # Same pattern for coverage. coverage_for returns None when
        # the loan lacks enough terms to compute an expected schedule.
        from lamella.features.loans import coverage as _cov
        from lamella.features.loans.reader import read_loan_pauses

        # WP12: surface real forbearance / pause windows so paused
        # months don't get "missing" expected rows. Pauses are
        # filtered to this loan's slug before passing through; the
        # build_schedule contract takes a Sequence so a list of
        # dicts works (build_schedule reads ``start_date`` /
        # ``end_date`` via getattr(... or [key])).
        slug_for_pauses = loan.get("slug") or ""
        all_pauses = read_loan_pauses(entries) if slug_for_pauses else []
        my_pauses = [
            _PauseLite(p["start_date"], p.get("end_date"))
            for p in all_pauses
            if p["loan_slug"] == slug_for_pauses
        ]
        coverage = _cov.coverage_for(
            loan, entries, pauses=my_pauses, as_of=today,
        )

    if anomalies is _AUTO:
        # Same pattern for anomaly detection. Needs the directive
        # history for the drift-baseline reset logic, which is only
        # meaningful when we've been tracking this loan across edits.
        from lamella.features.loans import anomalies as _an
        from lamella.features.loans.coverage import extract_actuals
        from lamella.features.loans.reader import read_loan_directive_history

        slug = loan.get("slug") or ""
        history = read_loan_directive_history(entries, slug) if slug else []
        actuals = extract_actuals(loan, entries)
        anchor_rows: list[dict] = []
        if conn is not None and slug:
            try:
                anchor_rows = [
                    dict(r) if hasattr(r, "keys") else {"as_of_date": r[0], "balance": r[1]}
                    for r in conn.execute(
                        "SELECT as_of_date, balance FROM loan_balance_anchors "
                        "WHERE loan_slug = ? ORDER BY as_of_date DESC",
                        (slug,),
                    ).fetchall()
                ]
            except Exception:  # noqa: BLE001
                anchor_rows = []
        anomalies = _an.detect(
            loan, coverage, anchor_rows, actuals, history,
        )
    elif anomalies is None:
        anomalies = []

    funding = _build_funding_report(loan, entries)
    anchor = _build_anchor_freshness_report(loan, conn, today)
    escrow = _build_escrow_report(loan)

    raw_actions = _build_next_actions(
        loan=loan, funding=funding, anchor=anchor, escrow=escrow,
        scaffolding=scaffolding, anomalies=anomalies, coverage=coverage,
    )
    sorted_actions = _sort_actions(raw_actions)

    # WP12 — pauses already filtered to this loan in the coverage
    # branch above; if coverage=None we rebuild here for the panel.
    my_pauses: list = []
    if coverage is not None:
        # Cheap: re-read directly so the panel gets dicts (for
        # template iteration) rather than the _PauseLite adapters.
        from lamella.features.loans.reader import read_loan_pauses as _rlp
        slug_for_panel = loan.get("slug") or ""
        my_pauses = [
            p for p in _rlp(entries)
            if p["loan_slug"] == slug_for_panel
        ]

    # WP5 — payment groups. Read confirmed-group cache from SQLite
    # if the table exists (WP5's migration may predate a given
    # deployment of the health module); proposals are live-computed
    # only when the panel pass loads the detail page.
    payment_groups: dict = {"proposed": [], "confirmed": []}
    if conn is not None:
        try:
            slug_for_groups = loan.get("slug") or ""
            rows = conn.execute(
                "SELECT group_id, loan_slug, member_hashes, "
                "aggregate_amount, date_span_start, date_span_end, "
                "primary_hash, status FROM loan_payment_groups "
                "WHERE loan_slug = ? AND status = 'confirmed' "
                "ORDER BY date_span_start DESC",
                (slug_for_groups,),
            ).fetchall()
            payment_groups["confirmed"] = [
                dict(r) if hasattr(r, "keys") else r for r in rows
            ]
        except Exception:  # noqa: BLE001 — pre-WP5 DBs won't have the table
            pass

    return LoanHealth(
        loan_slug=loan.get("slug") or "",
        summary_badge=_summary_badge(sorted_actions),
        next_actions=sorted_actions,
        scaffolding=scaffolding,
        funding=funding,
        coverage=coverage,
        anchor_freshness=anchor,
        escrow=escrow,
        anomalies=list(anomalies),
        pauses=my_pauses,
        payment_groups=payment_groups,
    )
