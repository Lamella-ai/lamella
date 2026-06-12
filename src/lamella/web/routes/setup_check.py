# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Setup completeness check: is everything configured right
BEFORE we ask the user to start classifying things?

Answers "which of my entities / accounts / vehicles / properties
/ loans / Paperless field mappings are incomplete or likely
wrong?" Each check surfaces specific, actionable issues with a
link to the setting page that fixes them.

Non-destructive. Queries only. Reports what's wrong; never
changes anything.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings

log = logging.getLogger(__name__)

router = APIRouter()


@dataclass
class CheckIssue:
    """A single actionable issue: what's wrong, why, where to go fix."""
    severity: str           # 'bad' | 'warn' | 'info'
    title: str
    detail: str = ""
    fix_url: str = ""
    fix_label: str = ""


@dataclass
class CheckSection:
    title: str
    summary: str = ""
    issues: list[CheckIssue] = field(default_factory=list)
    counts: list[tuple[str, str]] = field(default_factory=list)
    fix_url: str = ""
    fix_label: str = ""

    @property
    def health(self) -> str:
        if any(i.severity == "bad" for i in self.issues):
            return "bad"
        if any(i.severity == "warn" for i in self.issues):
            return "warn"
        return "ok"


# ------------------------------------------------------------------
# Per-section builders
# ------------------------------------------------------------------


def _ledger_open_accounts(entries) -> set[str]:
    """Collect account names that have an Open directive in the
    beancount files. These are the accounts the ledger accepts
    postings to."""
    from beancount.core import data as bdata
    out: set[str] = set()
    for e in entries:
        if isinstance(e, bdata.Open):
            out.add(e.account)
    return out


def _ledger_posting_accounts(entries) -> set[str]:
    """Accounts that actually have postings — these must all be in
    the Open set to be valid."""
    from beancount.core import data as bdata
    out: set[str] = set()
    for e in entries:
        if isinstance(e, bdata.Transaction):
            for p in e.postings or []:
                if p.account:
                    out.add(p.account)
    return out


def _entities_section(conn, entries) -> CheckSection:
    section = CheckSection(
        title="Entities",
        fix_url="/settings/entities",
        fix_label="Manage entities",
    )
    rows = conn.execute(
        "SELECT slug, display_name, is_active FROM entities "
        "WHERE COALESCE(is_active, 1) = 1 ORDER BY slug"
    ).fetchall()
    entity_names = [r["slug"] for r in rows]

    # Count txns per entity based on account prefix match.
    posting_accounts = _ledger_posting_accounts(entries)
    txns_per_entity = {name: 0 for name in entity_names}
    entity_prefixes = [f":{name}:" for name in entity_names]
    from beancount.core import data as bdata
    for e in entries:
        if not isinstance(e, bdata.Transaction):
            continue
        for p in e.postings or []:
            acct = p.account or ""
            for name, prefix in zip(entity_names, entity_prefixes):
                if prefix in f":{acct}:":
                    txns_per_entity[name] += 1
                    break

    active = sum(1 for n in entity_names if txns_per_entity[n] > 0)
    orphan = [n for n in entity_names if txns_per_entity[n] == 0]

    section.counts = [
        ("Registered entities", f"{len(entity_names):,}"),
        ("Active (have txns)", f"{active:,}"),
    ]
    section.summary = f"{active}/{len(entity_names)} active"

    if not entity_names:
        section.issues.append(CheckIssue(
            severity="bad",
            title="No entities registered",
            detail=(
                "Every transaction needs an entity binding — "
                "Personal, Acme, or a business name. Without at least "
                "one entity, classify can't produce correct "
                "Expenses:<Entity>:... account paths. Add your entities "
                "before classifying."
            ),
            fix_url="/settings/entities",
            fix_label="Add entities →",
        ))
    elif orphan:
        section.issues.append(CheckIssue(
            severity="warn",
            title=f"{len(orphan)} entities have zero transactions",
            detail=(
                f"Entities with no txns: {', '.join(orphan[:5])}"
                f"{'…' if len(orphan) > 5 else ''}. "
                "Either merge them into active entities, delete them, "
                "or start posting against them."
            ),
            fix_url="/settings/entities",
            fix_label="Review entities →",
        ))
    # Show an info line if more than half are active
    if entity_names and active == len(entity_names):
        section.issues.append(CheckIssue(
            severity="info",
            title="All entities have activity",
            detail="Good — every registered entity has at least one txn.",
        ))
    return section


def _accounts_section(conn, entries) -> CheckSection:
    """Ledger-layout hygiene: all posting accounts must be Open'd,
    and we check entity-first ordering per CLAUDE.md."""
    section = CheckSection(
        title="Accounts",
        fix_url="/settings/accounts",
        fix_label="Manage accounts",
    )
    opened = _ledger_open_accounts(entries)
    posting = _ledger_posting_accounts(entries)
    not_opened = sorted(posting - opened)
    unused_opens = sorted(opened - posting)

    # Entity-first hygiene (per CLAUDE.md): Expenses:Acme:Supplies
    # is correct; Expenses:Supplies:Acme is wrong. The guard in
    # startup discovery requires ≥20% entity-first — weaker hint
    # here just so the user sees the count.
    try:
        ent_rows = conn.execute("SELECT slug FROM entities").fetchall()
        entity_names = {r["slug"] for r in ent_rows}
    except sqlite3.Error:
        entity_names = set()
    entity_first_count = 0
    entity_non_first_count = 0
    for acct in posting:
        parts = acct.split(":")
        if len(parts) >= 2 and parts[1] in entity_names:
            entity_first_count += 1
        elif len(parts) >= 2 and any(p in entity_names for p in parts[2:]):
            entity_non_first_count += 1

    section.counts = [
        ("Open'd in ledger", f"{len(opened):,}"),
        ("Referenced in postings", f"{len(posting):,}"),
        ("Entity-first accounts", f"{entity_first_count:,}"),
    ]
    section.summary = f"{len(opened):,} opened · {len(posting):,} posted-to"

    if not_opened:
        # bean-check would be failing already if this is non-empty in
        # a real ledger. Surface loudly anyway.
        section.issues.append(CheckIssue(
            severity="bad",
            title=f"{len(not_opened)} accounts posted-to but not Open'd",
            detail=(
                f"First few: {', '.join(not_opened[:5])}"
                f"{'…' if len(not_opened) > 5 else ''}. "
                "bean-check should be failing on these; fix by adding "
                "Open directives (auto_accounts plugin creates them "
                "automatically for new accounts)."
            ),
            fix_url="/settings/accounts",
            fix_label="Open accounts →",
        ))
    if entity_non_first_count > 0:
        section.issues.append(CheckIssue(
            severity="warn",
            title=(
                f"{entity_non_first_count} accounts have the entity "
                f"NOT as the first segment"
            ),
            detail=(
                "The entity-first ordering (Expenses:Acme:Supplies, "
                "NOT Expenses:Supplies:Acme) is the project rule in "
                "CLAUDE.md. Accounts that violate it still work but "
                "report aggregation + classify whitelisting is noisier."
            ),
            fix_url="/settings/accounts",
            fix_label="Review layout →",
        ))
    if len(unused_opens) > 50:
        section.issues.append(CheckIssue(
            severity="info",
            title=f"{len(unused_opens)} Open'd accounts have no postings yet",
            detail=(
                "Not an error — just FYI. Scaffolded accounts awaiting "
                "first use."
            ),
        ))
    return section


def _vehicles_section(conn) -> CheckSection:
    section = CheckSection(
        title="Vehicles",
        fix_url="/vehicles",
        fix_label="Manage vehicles",
    )
    rows = conn.execute(
        "SELECT slug, display_name, entity_slug FROM vehicles "
        "WHERE COALESCE(is_active, 1) = 1 ORDER BY slug"
    ).fetchall()
    # `mileage_entries.vehicle` is populated with either slug or
    # display_name depending on the CSV source; we check both.
    vehicle_names: list[str] = []
    for r in rows:
        if r["display_name"]:
            vehicle_names.append(r["display_name"])
        elif r["slug"]:
            vehicle_names.append(r["slug"])

    # Entries per vehicle — joins by vehicle name (mileage_entries.vehicle).
    try:
        counts = conn.execute(
            "SELECT vehicle, COUNT(*) AS n FROM mileage_entries "
            "GROUP BY vehicle"
        ).fetchall()
    except sqlite3.Error:
        counts = []
    used_in_mileage = {r["vehicle"]: int(r["n"]) for r in counts}

    with_entries = [n for n in vehicle_names if used_in_mileage.get(n, 0) > 0]
    without_entries = [n for n in vehicle_names if used_in_mileage.get(n, 0) == 0]

    # Vehicles in mileage but NOT in registry (the common broken case).
    registered_set = set(vehicle_names)
    unregistered_in_mileage = [
        v for v in used_in_mileage.keys()
        if v and v not in registered_set
    ]

    section.counts = [
        ("Registered vehicles", f"{len(vehicle_names):,}"),
        ("With mileage entries", f"{len(with_entries):,}"),
    ]
    section.summary = (
        f"{len(with_entries)}/{len(vehicle_names)} with mileage"
        if vehicle_names else "No vehicles registered"
    )

    if not vehicle_names:
        section.issues.append(CheckIssue(
            severity="info",
            title="No vehicles registered",
            detail=(
                "If you don't track vehicle expenses per-vehicle this "
                "is fine. If you DO (mileage deduction, entity-specific "
                "fuel costs), register them so classify can pin Warehouse Club "
                "fuel charges to the right vehicle."
            ),
            fix_url="/vehicles",
            fix_label="Register vehicles →",
        ))
    if unregistered_in_mileage:
        section.issues.append(CheckIssue(
            severity="bad",
            title=(
                f"{len(unregistered_in_mileage)} vehicles in mileage "
                f"entries are NOT registered"
            ),
            detail=(
                f"Unregistered: {', '.join(unregistered_in_mileage[:5])}"
                f"{'…' if len(unregistered_in_mileage) > 5 else ''}. "
                "mileage_context_for_txn returns these as disambiguation "
                "hints, but classify can't map them to the right entity "
                "or account until they're registered."
            ),
            fix_url="/vehicles",
            fix_label="Register missing →",
        ))
    if without_entries:
        section.issues.append(CheckIssue(
            severity="warn",
            title=f"{len(without_entries)} registered vehicles have no mileage entries",
            detail=(
                f"No entries: {', '.join(without_entries[:5])}"
                f"{'…' if len(without_entries) > 5 else ''}. "
                "Either the mileage CSV hasn't been imported, or these "
                "vehicles aren't actually used."
            ),
        ))
    return section


def _properties_section(conn) -> CheckSection:
    section = CheckSection(
        title="Properties",
        fix_url="/settings/properties",
        fix_label="Manage properties",
    )
    try:
        rows = conn.execute(
            "SELECT slug, display_name, address FROM properties "
            "WHERE COALESCE(is_active, 1) = 1 ORDER BY slug"
        ).fetchall()
    except sqlite3.Error:
        rows = []
    total = len(rows)
    missing_address = [
        r["display_name"] or r["slug"]
        for r in rows if not (r["address"] or "").strip()
    ]
    try:
        valuations = conn.execute(
            "SELECT DISTINCT property_slug FROM property_valuations"
        ).fetchall()
        valued = {r["property_slug"] for r in valuations}
    except sqlite3.Error:
        valued = set()
    without_valuations = [
        r["display_name"] or r["slug"]
        for r in rows if r["slug"] not in valued
    ]

    section.counts = [("Registered properties", f"{total:,}")]
    if total:
        section.counts.append(("With valuations", f"{len(valued):,}"))
    section.summary = (
        f"{total} registered" if total else "No properties registered"
    )

    if total == 0:
        section.issues.append(CheckIssue(
            severity="info",
            title="No properties registered",
            detail=(
                "Only relevant if you have rental/investment property. "
                "Skip if not applicable."
            ),
        ))
        return section
    if missing_address:
        section.issues.append(CheckIssue(
            severity="warn",
            title=f"{len(missing_address)} properties have no address",
            detail=f"Missing: {', '.join(missing_address[:5])}",
            fix_url="/settings/properties",
            fix_label="Add addresses →",
        ))
    if without_valuations:
        section.issues.append(CheckIssue(
            severity="info",
            title=f"{len(without_valuations)} properties have no valuations",
            detail=(
                "Valuations drive Schedule E reporting. Add the initial "
                "purchase price + current fair market value."
            ),
        ))
    return section


def _loans_section(conn) -> CheckSection:
    section = CheckSection(
        title="Loans",
        fix_url="/settings/loans",
        fix_label="Manage loans",
    )
    try:
        rows = conn.execute(
            "SELECT slug, display_name FROM loans "
            "WHERE COALESCE(is_active, 1) = 1 ORDER BY slug"
        ).fetchall()
    except sqlite3.Error:
        rows = []
    total = len(rows)

    try:
        anchor_rows = conn.execute(
            "SELECT DISTINCT loan_slug FROM loan_balance_anchors"
        ).fetchall()
        with_anchor = {r["loan_slug"] for r in anchor_rows}
    except sqlite3.Error:
        with_anchor = set()
    no_anchor = [r["slug"] for r in rows if r["slug"] not in with_anchor]

    section.counts = [("Registered loans", f"{total:,}")]
    section.summary = f"{total} registered" if total else "No loans registered"
    if total == 0:
        section.issues.append(CheckIssue(
            severity="info",
            title="No loans registered",
            detail=(
                "Only relevant if you track mortgages, auto loans, or "
                "lines of credit. Skip if not applicable."
            ),
        ))
        return section
    if no_anchor:
        section.issues.append(CheckIssue(
            severity="warn",
            title=f"{len(no_anchor)} loans have no balance anchor",
            detail=(
                f"Anchorless: {', '.join(no_anchor[:5])}. Without an "
                "anchor we can't compute accurate principal vs. interest "
                "splits."
            ),
            fix_url="/settings/loans",
            fix_label="Add anchors →",
        ))
    return section


def _paperless_fields_section(conn, settings) -> CheckSection:
    section = CheckSection(
        title="Paperless field mapping",
        fix_url="/settings/paperless-fields",
        fix_label="Manage field mapping",
    )
    if not settings.paperless_configured:
        section.summary = "Paperless not configured"
        section.issues.append(CheckIssue(
            severity="info",
            title="Paperless not configured",
            detail=(
                "Paperless provides receipt data for classify context "
                "and verify-and-writeback. Set PAPERLESS_URL + "
                "PAPERLESS_API_TOKEN to enable."
            ),
        ))
        return section
    try:
        rows = conn.execute(
            "SELECT paperless_field_id, paperless_field_name, canonical_role "
            "FROM paperless_field_map"
        ).fetchall()
    except sqlite3.Error:
        rows = []
    total = len(rows)
    ignored = sum(1 for r in rows if r["canonical_role"] == "ignore")
    mapped = total - ignored
    canonical_roles = {
        r["canonical_role"] for r in rows if r["canonical_role"] != "ignore"
    }
    # Only `total` is truly required: Paperless has no built-in monetary
    # field so without a custom-field mapping the matcher can't amount-match.
    # `vendor` and `receipt_date` have reliable built-in fallbacks — the
    # matcher uses Paperless's `correspondent` field for vendor (see
    # receipts.txn_matcher stage 6) and `created` date when `receipt_date`
    # is NULL (see the `receipt_date IS NULL AND created_date ...` clauses).
    # So flag those as info, not as a blocker.
    required = {"total"}
    soft = {"vendor", "receipt_date"}
    missing_required = required - canonical_roles
    missing_soft = soft - canonical_roles

    section.counts = [
        ("Paperless fields known", f"{total:,}"),
        ("Mapped to a canonical role", f"{mapped:,}"),
        ("Ignored / unmapped", f"{ignored:,}"),
    ]
    section.summary = f"{mapped}/{total} mapped"
    if total == 0:
        section.issues.append(CheckIssue(
            severity="warn",
            title="No Paperless fields synced yet",
            detail=(
                "Run a Paperless sync or open the field mapping page "
                "to pull the custom fields from your Paperless instance."
            ),
            fix_url="/settings/paperless-fields",
            fix_label="Sync fields →",
        ))
    if missing_required:
        section.issues.append(CheckIssue(
            severity="bad",
            title=(
                f"Missing canonical role mapping: "
                f"{', '.join(sorted(missing_required))}"
            ),
            detail=(
                "Paperless has no built-in monetary field, so without a "
                "custom field mapped to 'total' the matcher can't match "
                "receipts to transactions by amount."
            ),
            fix_url="/settings/paperless-fields",
            fix_label="Map canonical roles →",
        ))
    if missing_soft:
        section.issues.append(CheckIssue(
            severity="info",
            title=(
                f"Optional canonical roles unmapped: "
                f"{', '.join(sorted(missing_soft))}"
            ),
            detail=(
                "Not required — the matcher already uses Paperless's "
                "built-in `correspondent` as vendor fallback and `created` "
                "date when no custom receipt_date is set. Mapping custom "
                "fields for these only helps if your Paperless instance "
                "stores them more reliably than the built-ins."
            ),
            fix_url="/settings/paperless-fields",
            fix_label="Map canonical roles →",
        ))
    return section


def _rules_section(conn) -> CheckSection:
    section = CheckSection(
        title="Classification rules",
        fix_url="/rules",
        fix_label="Manage rules",
    )
    total = 0
    by_source: list[sqlite3.Row] = []
    try:
        total = int(
            conn.execute(
                "SELECT COUNT(*) AS n FROM classification_rules"
            ).fetchone()["n"] or 0
        )
        by_source = conn.execute(
            "SELECT created_by, COUNT(*) AS n FROM classification_rules "
            "GROUP BY created_by"
        ).fetchall()
    except sqlite3.Error:
        pass
    section.counts = [("Total rules", f"{total:,}")]
    user_rules = next(
        (int(r["n"]) for r in by_source if r["created_by"] == "user"), 0,
    )
    ai_rules = next(
        (int(r["n"]) for r in by_source if r["created_by"] == "ai"), 0,
    )
    section.counts.append(("User-created", f"{user_rules:,}"))
    section.counts.append(("AI-learned", f"{ai_rules:,}"))
    section.summary = f"{total:,} rules · {user_rules:,} user, {ai_rules:,} AI"
    if total == 0:
        section.issues.append(CheckIssue(
            severity="info",
            title="No classification rules yet",
            detail=(
                "Rules are optional — the AI classifier works without "
                "them. But high-confidence user rules (e.g., merchant X "
                "always goes to account Y) auto-apply without calling "
                "the AI, saving tokens. Rules emerge naturally as you "
                "accept AI suggestions."
            ),
        ))
    return section


# ------------------------------------------------------------------
# Route
# ------------------------------------------------------------------


@router.get("/setup/check", response_class=HTMLResponse)
def setup_check_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Setup completeness check. Validates each piece of
    user-configurable state against the ledger reality, surfaces
    actionable issues with fix links."""
    try:
        entries = reader.load().entries
    except Exception as exc:  # noqa: BLE001
        entries = []
        log.warning("setup-check couldn't load ledger: %s", exc)
    sections = [
        _entities_section(conn, entries),
        _accounts_section(conn, entries),
        _vehicles_section(conn),
        _properties_section(conn),
        _loans_section(conn),
        _paperless_fields_section(conn, settings),
        _rules_section(conn),
    ]
    ok_count = sum(1 for s in sections if s.health == "ok")
    warn_count = sum(1 for s in sections if s.health == "warn")
    bad_count = sum(1 for s in sections if s.health == "bad")
    overall = (
        "ok" if bad_count == 0 and warn_count == 0
        else ("bad" if bad_count else "warn")
    )
    return request.app.state.templates.TemplateResponse(
        request, "setup_check.html",
        {
            "sections": sections,
            "ok_count": ok_count,
            "warn_count": warn_count,
            "bad_count": bad_count,
            "overall": overall,
        },
    )
