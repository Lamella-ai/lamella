# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Setup-completeness check.

A hard gate before the dashboard opens to classification / matching /
AI. Each step has:

  - a short ``id`` (used in the URL fragment for the "fix me" link)
  - a human ``label`` (what the user sees)
  - ``required``: True for steps that block dashboard access, False
    for optional-but-strongly-encouraged steps
  - ``is_complete``: computed from current DB + ledger state
  - ``summary``: one-liner shown on the progress page
  - ``fix_url``: where to click to resolve the step
  - optional ``detail_rows``: per-row breakdown (e.g. each entity's
    entity_type status) rendered inline on the progress page

Rules per user vision:

1. **Entity list locked down.** At least 1 entity registered AND every
   registered entity has ``entity_type`` set. Without it the
   commingle-vs-intercompany resolver is blind.

2. **Accounts labeled.** Every account that has postings in the
   ledger has a row in ``accounts_meta`` with ``kind``,
   ``entity_slug`` set (institution + last_four soft-required).

3. **Chart scaffolded per entity.** Every entity whose tax_schedule
   is C/F/A (or slug==Personal) has its yaml-driven expense chart
   scaffolded — missing required categories block.

4. **Companion accounts opened.** Every labeled bank/card account
   has its kind-appropriate Interest / Bank:Fees / Bank:Cashback /
   OpeningBalances / Transfers:InFlight opened.

Optional steps (shown as "recommended" but don't block):

- SimpleFIN configured (env var set)
- Every account that'd be reachable via SimpleFIN has a mapping
- Vehicles registered + their expense accounts scaffolded
- Properties registered + asset accounts scaffolded
- Loans registered

Why this exists: the user's rule is "we can't classify into broken
or missing categories." The AI classifier needs a whitelist of
well-formed accounts to pick from. Every required step here closes
a hole the classifier could fall into.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class SetupStep:
    id: str
    label: str
    required: bool
    is_complete: bool
    summary: str
    fix_url: str = ""
    detail_rows: list[dict] = field(default_factory=list)


@dataclass
class SetupProgress:
    steps: list[SetupStep] = field(default_factory=list)

    @property
    def required_steps(self) -> list[SetupStep]:
        return [s for s in self.steps if s.required]

    @property
    def optional_steps(self) -> list[SetupStep]:
        return [s for s in self.steps if not s.required]

    @property
    def required_complete(self) -> bool:
        return all(s.is_complete for s in self.required_steps)

    @property
    def required_done(self) -> int:
        return sum(1 for s in self.required_steps if s.is_complete)

    @property
    def required_total(self) -> int:
        return len(self.required_steps)

    @property
    def optional_done(self) -> int:
        return sum(1 for s in self.optional_steps if s.is_complete)

    @property
    def optional_total(self) -> int:
        return len(self.optional_steps)


def _check_entities(conn: sqlite3.Connection) -> SetupStep:
    """Required: at least 1 entity AND every entity has entity_type set."""
    try:
        rows = conn.execute(
            "SELECT slug, entity_type FROM entities WHERE is_active = 1 "
            "ORDER BY slug"
        ).fetchall()
    except Exception:  # noqa: BLE001
        rows = []
    total = len(rows)
    missing = [r for r in rows if not (r["entity_type"] or "").strip()]
    details = [
        {
            "label": r["slug"],
            "status": "ok" if (r["entity_type"] or "").strip() else "missing",
            "value": r["entity_type"] or "(unset)",
        }
        for r in rows
    ]
    # Phase 4 of /setup/recovery: /setup/entities now has + Add
    # buttons in-page (?add=person / ?add=business open a modal).
    # The empty state used to redirect to /settings/entities — that
    # punt is gone; recovery surfaces never link out to /settings/*.
    if total == 0:
        summary = "No entities registered yet — add one to continue."
        is_complete = False
        fix_url = "/setup/entities?add=person"
    elif missing:
        summary = (
            f"{total} entit{'y' if total == 1 else 'ies'} registered; "
            f"{len(missing)} missing entity_type "
            f"({', '.join(r['slug'] for r in missing[:3])}"
            f"{'…' if len(missing) > 3 else ''})."
        )
        is_complete = False
        fix_url = "/setup/entities"
    else:
        summary = (
            f"{total} entit{'y' if total == 1 else 'ies'} registered, "
            f"all with entity_type set."
        )
        is_complete = True
        fix_url = "/setup/entities"
    return SetupStep(
        id="entities",
        label="Entities",
        required=True,
        is_complete=is_complete,
        summary=summary,
        fix_url=fix_url,
        detail_rows=details,
    )


def _check_account_labels(conn: sqlite3.Connection) -> SetupStep:
    """Required: every non-system account with postings has a
    labeled accounts_meta row (kind + entity_slug at minimum)."""
    try:
        rows = conn.execute(
            """
            SELECT account_path, kind, entity_slug, institution, last_four
              FROM accounts_meta
             WHERE closed_on IS NULL
             ORDER BY account_path
            """
        ).fetchall()
    except Exception:  # noqa: BLE001
        rows = []
    # Only require labels on accounts the user actually uses
    # (Assets/Liabilities for bank/card/loan). Skip Equity
    # OpeningBalances, Transfers:InFlight, etc.
    def _is_user_account(path: str) -> bool:
        if not path.startswith(("Assets:", "Liabilities:")):
            return False
        # System-ish buckets we don't need labeled
        skip_segments = {"Transfers", "FIXME", "OpeningBalances"}
        return not any(seg in skip_segments for seg in path.split(":"))

    user_rows = [r for r in rows if _is_user_account(r["account_path"])]
    unlabeled: list[dict] = []
    for r in user_rows:
        missing = []
        if not (r["kind"] or "").strip():
            missing.append("kind")
        if not (r["entity_slug"] or "").strip():
            missing.append("entity_slug")
        if missing:
            unlabeled.append({
                "label": r["account_path"],
                "status": "missing",
                "value": ", ".join(f"missing {m}" for m in missing),
            })
    details = unlabeled[:20]
    # Phase 4 of /setup/recovery: /setup/accounts now has an + Add
    # modal in-page (?add=account) for creating a one-off account
    # from scratch. The empty-state used to redirect to
    # /settings/accounts — that punt is gone; recovery surfaces
    # never link to /settings/*.
    if not user_rows:
        summary = (
            "No user-facing accounts registered yet — "
            "add one (or run SimpleFIN ingest) to continue."
        )
        is_complete = False
        fix_url = "/setup/accounts?add=account"
    elif unlabeled:
        summary = (
            f"{len(user_rows)} bank/card/loan account(s); "
            f"{len(unlabeled)} need kind + entity_slug."
        )
        is_complete = False
        fix_url = "/setup/accounts"
    else:
        summary = (
            f"All {len(user_rows)} bank/card/loan accounts have "
            f"kind + entity_slug set."
        )
        is_complete = True
        fix_url = "/setup/accounts"
    return SetupStep(
        id="account_labels",
        label="Account labels",
        required=True,
        is_complete=is_complete,
        summary=summary,
        fix_url=fix_url,
        detail_rows=details,
    )


def _check_charts_scaffolded(
    conn: sqlite3.Connection, entries: list | None,
) -> SetupStep:
    """Required: every entity with a tax_schedule has its expense
    chart scaffolded (required categories are open in the ledger).
    """
    from lamella.core.config import Settings
    from lamella.core.registry.service import (
        load_categories_yaml_for_entity, scaffold_paths_for_entity,
    )

    try:
        entity_rows = conn.execute(
            "SELECT slug, entity_type, tax_schedule FROM entities "
            "WHERE is_active = 1 ORDER BY slug"
        ).fetchall()
    except Exception:  # noqa: BLE001
        entity_rows = []
    if not entity_rows or entries is None:
        return SetupStep(
            id="charts",
            label="Expense charts",
            required=True,
            is_complete=False,
            summary="Can't check until entities are set up.",
            fix_url="/setup/entities",
        )
    settings = Settings()
    from lamella.features.setup.posting_counts import open_paths as _open_paths
    # Opens minus Closes — a chart account the user has Close'd must
    # flag as missing on the checklist, same as the setup_charts_page
    # handler does (§2-Class-A: checklist-green-page-red was the shape
    # when this predicate ignored Close directives).
    open_set = _open_paths(entries)
    details: list[dict] = []
    any_missing = False
    total_entities_covered = 0
    for row in entity_rows:
        slug = row["slug"]
        yaml_data = load_categories_yaml_for_entity(settings, row)
        if not yaml_data:
            continue  # entity doesn't map to a chart (no schedule / unknown type)
        total_entities_covered += 1
        candidates = scaffold_paths_for_entity(yaml_data, slug)
        missing = [c["path"] for c in candidates if c["path"] not in open_set]
        if missing:
            any_missing = True
            details.append({
                "label": slug,
                "status": "missing",
                "value": (
                    f"{len(missing)}/{len(candidates)} categories missing "
                    f"(first: {missing[0]})"
                ),
            })
        else:
            details.append({
                "label": slug,
                "status": "ok",
                "value": f"all {len(candidates)} categories open",
            })
    if total_entities_covered == 0:
        summary = (
            "No entities map to a Schedule C/F/A chart — nothing to scaffold."
        )
        is_complete = True  # nothing required
    elif any_missing:
        incomplete = [d for d in details if d["status"] == "missing"]
        summary = (
            f"{len(incomplete)}/{total_entities_covered} "
            f"entities have missing chart categories."
        )
        is_complete = False
    else:
        summary = (
            f"All {total_entities_covered} chart-bearing entities fully "
            f"scaffolded."
        )
        is_complete = True
    return SetupStep(
        id="charts",
        label="Expense chart scaffolding",
        required=True,
        is_complete=is_complete,
        summary=summary,
        fix_url="/setup/charts",
        detail_rows=details,
    )


def _check_companions(
    conn: sqlite3.Connection, entries: list | None,
) -> SetupStep:
    """Recommended (non-blocking): every labeled account has its
    kind-appropriate companion accounts (Interest / Bank:Fees /
    Bank:Cashback / OpeningBalances / Transfers:InFlight) opened.
    """
    from lamella.core.registry.companion_accounts import (
        companion_paths_for,
    )
    try:
        rows = conn.execute(
            "SELECT account_path, kind, entity_slug, institution "
            "FROM accounts_meta WHERE closed_on IS NULL "
            "AND kind IS NOT NULL AND entity_slug IS NOT NULL "
            "ORDER BY account_path"
        ).fetchall()
    except Exception:  # noqa: BLE001
        rows = []
    from lamella.features.setup.posting_counts import open_paths as _open_paths
    # Opens minus Closes — consistent with the action pages.
    open_set = _open_paths(entries or [])
    details: list[dict] = []
    any_missing = 0
    total_expected = 0
    for r in rows:
        companions = companion_paths_for(
            account_path=r["account_path"],
            kind=r["kind"],
            entity_slug=r["entity_slug"],
            institution=r["institution"],
        )
        missing = [cp.path for cp in companions if cp.path not in open_set]
        total_expected += len(companions)
        if missing:
            any_missing += len(missing)
            details.append({
                "label": r["account_path"],
                "status": "missing",
                "value": f"{len(missing)} missing (first: {missing[0]})",
            })
    if not rows:
        summary = "No labeled accounts to check yet."
        is_complete = True
    elif any_missing:
        summary = (
            f"{any_missing}/{total_expected} companion accounts missing "
            f"across {len(details)} labeled account(s)."
        )
        is_complete = False
    else:
        summary = (
            f"All {total_expected} companion accounts across "
            f"{len(rows)} labeled account(s) are open."
        )
        is_complete = True
    return SetupStep(
        id="companions",
        label="Companion accounts",
        required=False,
        is_complete=is_complete,
        summary=summary,
        fix_url="/setup/accounts",
        detail_rows=details[:20],
    )


def _check_vehicles(
    conn: sqlite3.Connection, entries: list | None,
) -> SetupStep:
    """Optional: every active vehicle has its canonical per-vehicle
    chart accounts open.

    Not required — a user with no vehicles doesn't need this — but
    if they DO have vehicles, every Fuel / Maintenance / Insurance /
    Registration / Tolls / Parking / CarWash charge needs a per-
    vehicle target account so the AI + mileage log can pair up
    cleanly. Without this, two same-make vehicles (2008 + 2009
    Fabrikam Suv) end up ambiguous on shared paths like
    ``Expenses:Personal:Custom:FabrikamSuvFuel``.
    """
    try:
        vehicle_rows = conn.execute(
            "SELECT slug, entity_slug FROM vehicles WHERE is_active = 1"
        ).fetchall()
    except Exception:  # noqa: BLE001
        vehicle_rows = []
    if not vehicle_rows:
        return SetupStep(
            id="vehicles",
            label="Vehicle chart accounts",
            required=False,
            is_complete=True,
            summary="No vehicles registered — nothing to scaffold.",
            fix_url="/setup/vehicles",
        )
    from lamella.features.vehicles.vehicle_companion import (
        vehicle_chart_paths_for,
    )
    from lamella.features.setup.posting_counts import open_paths as _open_paths
    # Opens minus Closes — a vehicle whose chart account was Close'd
    # must flag as incomplete, matching the setup_vehicles_page view.
    open_set = _open_paths(entries or [])
    details: list[dict] = []
    missing_total = 0
    for v in vehicle_rows:
        expected = vehicle_chart_paths_for(
            vehicle_slug=v["slug"], entity_slug=v["entity_slug"],
        )
        if not expected:
            details.append({
                "label": v["slug"],
                "status": "missing",
                "value": "no entity set on this vehicle",
            })
            missing_total += 1
            continue
        missing = [p.path for p in expected if p.path not in open_set]
        if missing:
            missing_total += len(missing)
            details.append({
                "label": v["slug"],
                "status": "missing",
                "value": f"{len(missing)}/{len(expected)} accounts missing",
            })
        else:
            details.append({
                "label": v["slug"],
                "status": "ok",
                "value": f"all {len(expected)} accounts open",
            })
    is_complete = missing_total == 0
    return SetupStep(
        id="vehicles",
        label="Vehicle chart accounts",
        # Required when the user has opted in (≥1 vehicle registered).
        # The app treats a half-scaffolded vehicle as broken state the
        # user must fix before using AI/bulk-op surfaces — matches the
        # rule "optionally choosing to use vehicles means you've
        # decided they're important."
        required=True,
        is_complete=is_complete,
        summary=(
            f"All {len(vehicle_rows)} vehicle(s) fully scaffolded."
            if is_complete
            else f"{missing_total} missing account(s) across "
                 f"{len(vehicle_rows)} vehicle(s)."
        ),
        fix_url="/setup/vehicles",
        detail_rows=details,
    )


def _check_properties(
    conn: sqlite3.Connection, entries: list | None,
) -> SetupStep:
    """Optional: every active property has its canonical per-property
    chart open (Assets:<Entity>:Property:<Slug> + Expenses:…:Tax,
    HOA, Insurance, Maintenance, Repairs, Utilities, MortgageInterest
    + rental extras if is_rental)."""
    try:
        rows = conn.execute(
            "SELECT slug, entity_slug, is_rental FROM properties "
            "WHERE is_active = 1"
        ).fetchall()
    except Exception:  # noqa: BLE001
        rows = []
    if not rows:
        return SetupStep(
            id="properties", label="Property chart accounts",
            required=False, is_complete=True,
            summary="No properties registered — nothing to scaffold.",
            fix_url="/setup/properties",
        )
    from lamella.features.properties.property_companion import (
        property_chart_paths_for,
    )
    from lamella.features.setup.posting_counts import open_paths as _open_paths
    # Opens minus Closes — parity with setup_properties_page.
    open_set = _open_paths(entries or [])
    missing_total = 0
    details: list[dict] = []
    for r in rows:
        expected = property_chart_paths_for(
            property_slug=r["slug"],
            entity_slug=r["entity_slug"],
            is_rental=bool(r["is_rental"]),
        )
        if not expected:
            missing_total += 1
            details.append({
                "label": r["slug"],
                "status": "missing",
                "value": "no entity set on this property",
            })
            continue
        missing = [p.path for p in expected if p.path not in open_set]
        if missing:
            missing_total += len(missing)
            details.append({
                "label": r["slug"],
                "status": "missing",
                "value": f"{len(missing)}/{len(expected)} missing",
            })
        else:
            details.append({
                "label": r["slug"],
                "status": "ok",
                "value": f"all {len(expected)} open",
            })
    return SetupStep(
        id="properties", label="Property chart accounts",
        # Required once the user has registered ≥1 property. Same
        # rationale as vehicles: opting in = making it important.
        required=True, is_complete=missing_total == 0,
        summary=(
            f"All {len(rows)} propert{'y' if len(rows) == 1 else 'ies'} scaffolded."
            if missing_total == 0
            else f"{missing_total} missing account(s) across {len(rows)} propert{'y' if len(rows) == 1 else 'ies'}."
        ),
        fix_url="/setup/properties",
        detail_rows=details,
    )


def _check_loans(conn: sqlite3.Connection) -> SetupStep:
    """Optional: each active loan has liability + interest + entity set."""
    try:
        rows = conn.execute(
            "SELECT slug, entity_slug, liability_account_path, "
            "       interest_account_path FROM loans WHERE is_active = 1"
        ).fetchall()
    except Exception:  # noqa: BLE001
        rows = []
    if not rows:
        return SetupStep(
            id="loans", label="Loan configuration",
            required=False, is_complete=True,
            summary="No loans registered.",
            fix_url="/setup/loans",
        )
    incomplete: list[dict] = []
    for r in rows:
        miss = []
        if not (r["entity_slug"] or "").strip():
            miss.append("entity")
        if not (r["liability_account_path"] or "").strip():
            miss.append("liability")
        if not (r["interest_account_path"] or "").strip():
            miss.append("interest")
        if miss:
            incomplete.append({
                "label": r["slug"],
                "status": "missing",
                "value": f"missing {', '.join(miss)}",
            })
    return SetupStep(
        id="loans", label="Loan configuration",
        # Required once ≥1 loan is registered — half-configured loans
        # produce ambiguous splits on payment classifications.
        required=True, is_complete=not incomplete,
        summary=(
            f"All {len(rows)} loan{'' if len(rows) == 1 else 's'} configured."
            if not incomplete
            else f"{len(incomplete)}/{len(rows)} loan(s) missing required fields."
        ),
        fix_url="/setup/loans",
        detail_rows=incomplete,
    )


def _check_import_applied(imports_dir: Path | None = None) -> SetupStep:
    """Optional: did the user import a prior ledger via /setup/import?

    Signal is presence of any ``.bean`` file in
    ``ledger_dir / "connector_imports/"`` — that directory is only
    ever populated by ``apply_import``. No SQLite tracking needed
    (CLAUDE.md rule: ledger is the source of truth; SQLite is cache).

    A fresh-scaffold user who never imports legitimately leaves this
    incomplete forever — that's why it's optional, not required. Its
    purpose is the post-import "✓" so the user sees the checklist
    visibly progress when they apply an import.

    ``imports_dir`` is passed through from
    :func:`compute_setup_progress` so tests against a tmp ledger don't
    fall back to the prod ``/ledger`` default in ``Settings()``."""
    if imports_dir is None:
        from lamella.core.config import Settings
        imports_dir = Settings().import_ledger_output_dir_resolved
    has_imports = (
        imports_dir.is_dir()
        and any(p.suffix == ".bean" for p in imports_dir.iterdir())
    ) if imports_dir.exists() else False
    return SetupStep(
        id="import_applied",
        label="Prior ledger imported",
        required=False,
        is_complete=has_imports,
        summary=(
            "Imported — historical transactions are part of the ledger."
            if has_imports
            else "Skip if starting fresh. /setup/import pulls in a prior "
                 "Beancount ledger (canonicalizing chart paths along the way)."
        ),
        fix_url="/setup/import",
    )


def _check_simplefin(conn: sqlite3.Connection | None = None) -> SetupStep:
    """Optional: SimpleFIN configured AND not user-dismissed.

    Phase 4 of /setup/recovery: routes to /setup/simplefin (the
    recovery wrapper), not bare /simplefin admin. A user who hits
    Skip on the recovery wrapper stamps a ``simplefin_dismissed_at``
    setting; we suppress this finding for 7 days from that
    timestamp so users who don't use SimpleFIN don't see a
    permanent nag.
    """
    from datetime import datetime, timedelta, timezone
    from lamella.core.config import Settings
    settings = Settings()
    configured = bool(
        settings.simplefin_access_url
        and settings.simplefin_access_url.get_secret_value()
    )

    dismissed_recently = False
    if not configured and conn is not None:
        try:
            row = conn.execute(
                "SELECT value FROM app_settings WHERE key = ?",
                ("simplefin_dismissed_at",),
            ).fetchone()
        except Exception:  # noqa: BLE001
            row = None
        raw = (row["value"] if row else "") or ""
        if raw:
            try:
                dismissed_at = datetime.fromisoformat(raw)
                # 7-day suppression window matches the wizard's
                # dismissed-IDs pattern.
                if datetime.now(timezone.utc) - dismissed_at < timedelta(days=7):
                    dismissed_recently = True
            except ValueError:
                pass

    if configured:
        summary = "Configured — new bank transactions flow in automatically."
        is_complete = True
    elif dismissed_recently:
        summary = (
            "Skipped by user — recovery suppresses this for a week. "
            "Visit /setup/simplefin to opt back in."
        )
        is_complete = True
    else:
        summary = "Not configured. Transactions must be imported manually."
        is_complete = False

    return SetupStep(
        id="simplefin",
        label="SimpleFIN sync",
        required=False,
        is_complete=is_complete,
        summary=summary,
        fix_url="/setup/simplefin",
    )


def compute_setup_progress(
    conn: sqlite3.Connection,
    entries: list | None,
    *,
    imports_dir: Path | None = None,
) -> SetupProgress:
    """Run every check, return a SetupProgress object the route +
    middleware can consume.

    ``imports_dir`` is the connector_imports directory used by
    :func:`_check_import_applied`. Pass it explicitly from route
    handlers (which have ``settings`` injected) so tests against a
    tmp ledger don't see the prod ``/ledger`` default."""
    steps: list[SetupStep] = []
    # Required
    steps.append(_check_entities(conn))
    steps.append(_check_account_labels(conn))
    steps.append(_check_charts_scaffolded(conn, entries))
    # Optional / recommended
    steps.append(_check_companions(conn, entries))
    steps.append(_check_vehicles(conn, entries))
    steps.append(_check_properties(conn, entries))
    steps.append(_check_loans(conn))
    steps.append(_check_schema_drift(conn, entries))
    steps.append(_check_legacy_paths(conn, entries))
    steps.append(_check_import_rewrite(conn, entries))
    steps.append(_check_import_applied(imports_dir))
    steps.append(_check_simplefin(conn))
    return SetupProgress(steps=steps)


def _check_schema_drift(
    conn: sqlite3.Connection, entries: list | None,
) -> SetupStep:
    """Phase 5 of /setup/recovery — surface SQLite + ledger schema
    drift. Both axes are blocker-severity, so this step is required;
    finding even one drift means the setup checklist isn't complete.

    The detector returns 0 findings on a fully-migrated install, in
    which case the step renders as complete with a forward-looking
    "schema is in sync" message — same shape as
    ``_check_legacy_paths``."""
    if not entries:
        return SetupStep(
            id="schema_drift",
            label="Schema drift",
            required=True,
            is_complete=True,
            summary="No ledger entries loaded yet.",
            fix_url="/setup/recovery/schema",
        )
    try:
        from lamella.features.recovery.findings import detect_schema_drift
        findings = detect_schema_drift(conn, entries)
    except Exception as exc:  # noqa: BLE001
        return SetupStep(
            id="schema_drift",
            label="Schema drift",
            required=True,
            is_complete=True,
            summary=f"detector unavailable: {exc}",
            fix_url=None,
        )

    n = len(findings)
    if n == 0:
        return SetupStep(
            id="schema_drift",
            label="Schema drift",
            required=True,
            is_complete=True,
            summary="SQLite + ledger versions in sync.",
            fix_url="/setup/recovery/schema",
        )

    detail_rows: list[dict] = []
    for f in findings:
        detail_rows.append({
            "status": "err",
            "label": f.target,
            "value": f.summary,
        })
    return SetupStep(
        id="schema_drift",
        label="Schema drift",
        required=True,
        is_complete=False,
        summary=(
            f"{n} drift finding{'s' if n != 1 else ''} — "
            "click through to review and apply."
        ),
        fix_url="/setup/recovery/schema",
        detail_rows=detail_rows,
    )


def _check_legacy_paths(
    conn: sqlite3.Connection, entries: list | None,
) -> SetupStep:
    """Phase 3 of /setup/recovery — surface non-canonical paths and
    link to the cleanup page where each one can be Closed or
    Move-and-closed individually.

    This step shares signal with ``_check_import_rewrite`` (both look
    at non-canonical chart shapes) but they're different in purpose:
    import_rewrite is a read-only report, legacy_paths is an
    actionable cleanup surface. Phase 6's repair-state work will
    consolidate; for Phase 3 they coexist.
    """
    if not entries:
        return SetupStep(
            id="legacy_paths",
            label="Legacy paths cleanup",
            required=False,
            is_complete=True,
            summary="No ledger entries loaded yet.",
            fix_url="/setup/legacy-paths",
        )
    try:
        from lamella.features.recovery.findings import detect_legacy_paths
        findings = detect_legacy_paths(conn, entries)
    except Exception as exc:  # noqa: BLE001
        return SetupStep(
            id="legacy_paths",
            label="Legacy paths cleanup",
            required=False,
            is_complete=True,
            summary=f"detector unavailable: {exc}",
            fix_url=None,
        )

    n = len(findings)
    if n == 0:
        return SetupStep(
            id="legacy_paths",
            label="Legacy paths cleanup",
            required=False,
            is_complete=True,
            summary="No legacy paths detected.",
            fix_url="/setup/legacy-paths",
        )

    detail_rows: list[dict] = []
    for f in findings[:20]:
        detail_rows.append({
            "status": "warn",
            "label": f.target,
            "value": f.summary,
        })
    return SetupStep(
        id="legacy_paths",
        label="Legacy paths cleanup",
        required=False,
        is_complete=False,
        summary=(
            f"{n} legacy path{'s' if n != 1 else ''} detected — "
            "click through to Close or Move &amp; close each."
        ),
        fix_url="/setup/legacy-paths",
        detail_rows=detail_rows,
    )


def _check_import_rewrite(
    conn: sqlite3.Connection, entries: list | None,
) -> SetupStep:
    """Optional: flag non-canonical chart paths a user may have
    imported from a prior setup. Detection-only for now — the full
    report at /setup/import-rewrite shows Custom:/short-bucket/
    unknown-entity paths with posting counts."""
    from lamella.features.setup.posting_counts import open_paths as _open_paths
    try:
        entity_slugs = {
            r["slug"] for r in conn.execute(
                "SELECT slug FROM entities WHERE is_active = 1"
            ).fetchall()
        }
    except Exception:  # noqa: BLE001
        entity_slugs = set()
    system_second = {
        "Transfers", "OpeningBalances", "FIXME", "Uncategorized",
        "Unattributed", "Clearing", "Retained", "DueFrom", "DueTo",
    }
    # Opens minus Closes — a Close'd non-canonical path isn't a
    # live problem any more; scoping to currently-open accounts
    # matches the setup_import_rewrite_page report.
    bad = 0
    for path in _open_paths(entries or []):
        parts = path.split(":")
        if len(parts) < 2:
            continue
        root = parts[0]
        if root not in ("Expenses", "Income", "Assets", "Liabilities"):
            continue
        second = parts[1]
        if second in system_second:
            continue
        if len(parts) == 2 and root == "Expenses":
            bad += 1
        elif len(parts) >= 3 and parts[2] == "Custom":
            bad += 1
        elif second not in entity_slugs and root != "Equity":
            bad += 1
    return SetupStep(
        id="import_rewrite", label="Chart-path conformance",
        required=False, is_complete=bad == 0,
        summary=(
            "No non-canonical chart paths detected."
            if bad == 0
            else f"{bad} open account(s) don't match the canonical "
                 f"Root:<Entity>:… shape. Review the report; "
                 f"migration is detection-only for now."
        ),
        fix_url="/setup/import-rewrite",
    )
