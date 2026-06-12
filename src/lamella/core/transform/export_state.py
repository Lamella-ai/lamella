# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Export SQLite state to the ledger as custom directives.

Runs against a populated SQLite DB that predates some / all of the
reconstruct directives (loans, properties, projects, fuel_log,
mileage_trip_meta, account/entity classify_context, audit_dismissals,
notes, plus the vehicle family). Reads each state table and appends
the matching ``custom "…"`` directive to connector_config.bean when
the ledger doesn't already carry an entry for that primary key.

Idempotent. Safe to re-run.

Usage:
    python -m lamella.core.transform.export_state           # dry-run counts
    python -m lamella.core.transform.export_state --apply   # write
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from beancount import loader
from beancount.core.data import Custom

log = logging.getLogger(__name__)


@dataclass
class ExportReport:
    section: str
    rows: int = 0
    written: int = 0
    skipped_existing: int = 0
    errors: list[str] = field(default_factory=list)


def _existing_keys(entries: list, directive_type: str, key_fn: Callable[[Any], Any]) -> set:
    out: set = set()
    for entry in entries:
        if isinstance(entry, Custom) and entry.type == directive_type:
            k = key_fn(entry)
            if k is not None:
                out.add(k)
    return out


def _arg(entry: Any, i: int) -> Any:
    from lamella.core.transform.custom_directive import custom_arg
    return custom_arg(entry, i)


def _export_loans(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.loans.writer import append_loan, append_loan_balance_anchor
    report = ExportReport(section="loans")
    have = _existing_keys(entries, "loan", lambda e: _arg(e, 0))
    for row in conn.execute("SELECT * FROM loans"):
        r = dict(row)
        report.rows += 1
        if r["slug"] in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_loan(
                connector_config=connector_config, main_bean=main_bean,
                slug=r["slug"], display_name=r.get("display_name"),
                loan_type=r.get("loan_type") or "other",
                entity_slug=r.get("entity_slug"),
                institution=r.get("institution"),
                original_principal=r.get("original_principal") or "0",
                funded_date=r.get("funded_date"),
                first_payment_date=r.get("first_payment_date"),
                payment_due_day=r.get("payment_due_day"),
                term_months=r.get("term_months"),
                interest_rate_apr=r.get("interest_rate_apr"),
                monthly_payment_estimate=r.get("monthly_payment_estimate"),
                escrow_monthly=r.get("escrow_monthly"),
                property_tax_monthly=r.get("property_tax_monthly"),
                insurance_monthly=r.get("insurance_monthly"),
                liability_account_path=r.get("liability_account_path"),
                interest_account_path=r.get("interest_account_path"),
                escrow_account_path=r.get("escrow_account_path"),
                simplefin_account_id=r.get("simplefin_account_id"),
                property_slug=r.get("property_slug"),
                payoff_date=r.get("payoff_date"),
                payoff_amount=r.get("payoff_amount"),
                is_active=bool(r.get("is_active", 1)),
                notes=r.get("notes"),
            )
            report.written += 1
        except Exception as exc:  # noqa: BLE001
            report.errors.append(f"loan {r['slug']!r}: {exc}")

    have_anchor = _existing_keys(
        entries, "loan-balance-anchor",
        lambda e: (_arg(e, 0), e.date.isoformat()),
    )
    for row in conn.execute(
        "SELECT loan_slug, as_of_date, balance, source, notes FROM loan_balance_anchors"
    ):
        r = dict(row)
        report.rows += 1
        key = (r["loan_slug"], str(r["as_of_date"]))
        if key in have_anchor:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_loan_balance_anchor(
                connector_config=connector_config, main_bean=main_bean,
                loan_slug=r["loan_slug"], as_of_date=r["as_of_date"],
                balance=r["balance"], source=r.get("source"),
                notes=r.get("notes"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"anchor {key}: {exc}")
    return report


def _export_properties(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.properties.writer import (
        append_property, append_property_valuation,
    )
    report = ExportReport(section="properties")
    have = _existing_keys(entries, "property", lambda e: _arg(e, 0))
    for row in conn.execute("SELECT * FROM properties"):
        r = dict(row)
        report.rows += 1
        if r["slug"] in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_property(
                connector_config=connector_config, main_bean=main_bean,
                slug=r["slug"], display_name=r.get("display_name"),
                property_type=r.get("property_type") or "other",
                entity_slug=r.get("entity_slug"),
                address=r.get("address"), city=r.get("city"),
                state=r.get("state"), postal_code=r.get("postal_code"),
                purchase_date=r.get("purchase_date"),
                purchase_price=r.get("purchase_price"),
                closing_costs=r.get("closing_costs"),
                asset_account_path=r.get("asset_account_path"),
                sale_date=r.get("sale_date"),
                sale_price=r.get("sale_price"),
                is_primary_residence=bool(r.get("is_primary_residence", 0)),
                is_rental=bool(r.get("is_rental", 0)),
                is_active=bool(r.get("is_active", 1)),
                notes=r.get("notes"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"property {r['slug']!r}: {exc}")

    have_val = _existing_keys(
        entries, "property-valuation",
        lambda e: (_arg(e, 0), e.date.isoformat()),
    )
    try:
        val_rows = list(conn.execute(
            "SELECT property_slug, as_of_date, value, source, notes FROM property_valuations"
        ))
    except sqlite3.OperationalError:
        val_rows = []
    for row in val_rows:
        r = dict(row)
        report.rows += 1
        key = (r["property_slug"], str(r["as_of_date"]))
        if key in have_val:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_property_valuation(
                connector_config=connector_config, main_bean=main_bean,
                property_slug=r["property_slug"], as_of_date=r["as_of_date"],
                value=r["value"], source=r.get("source"), notes=r.get("notes"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"valuation {key}: {exc}")
    return report


def _export_projects(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    import json as _json
    from lamella.features.projects.writer import append_project
    report = ExportReport(section="projects")
    have = _existing_keys(entries, "project", lambda e: _arg(e, 0))
    for row in conn.execute("SELECT * FROM projects"):
        r = dict(row)
        report.rows += 1
        if r["slug"] in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        merchants: list[str] = []
        raw = r.get("expected_merchants")
        if raw:
            try:
                merchants = list(_json.loads(raw))
            except (ValueError, TypeError):
                merchants = []
        try:
            append_project(
                connector_config=connector_config, main_bean=main_bean,
                slug=r["slug"], display_name=r.get("display_name") or r["slug"],
                start_date=r.get("start_date"),
                entity_slug=r.get("entity_slug"),
                property_slug=r.get("property_slug"),
                project_type=r.get("project_type"),
                end_date=r.get("end_date"),
                budget_amount=r.get("budget_amount"),
                expected_merchants=merchants,
                previous_project_slug=r.get("previous_project_slug"),
                is_active=bool(r.get("is_active", 1)),
                closed_at=r.get("closed_at"),
                description=r.get("description"),
                notes=r.get("notes"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"project {r['slug']!r}: {exc}")
    return report


def _export_vehicles(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.vehicles.writer import append_vehicle
    report = ExportReport(section="vehicles")
    have = _existing_keys(entries, "vehicle", lambda e: _arg(e, 0))
    for row in conn.execute("SELECT * FROM vehicles"):
        r = dict(row)
        report.rows += 1
        if r["slug"] in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_vehicle(
                connector_config=connector_config, main_bean=main_bean,
                slug=r["slug"], display_name=r.get("display_name"),
                year=r.get("year"), make=r.get("make"), model=r.get("model"),
                vin=r.get("vin"), license_plate=r.get("license_plate"),
                entity_slug=r.get("entity_slug"),
                purchase_date=(r.get("purchase_date") or None),
                purchase_price=r.get("purchase_price"),
                purchase_fees=r.get("purchase_fees"),
                asset_account=r.get("asset_account_path"),
                gvwr_lbs=r.get("gvwr_lbs"),
                placed_in_service=(r.get("placed_in_service_date") or None),
                fuel_type=r.get("fuel_type"),
                notes=r.get("notes"),
                is_active=bool(r.get("is_active", 1)),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"vehicle {r['slug']!r}: {exc}")
    return report


def _export_vehicle_yearly_mileage(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.vehicles.writer import append_vehicle_yearly_mileage
    report = ExportReport(section="vehicle_yearly_mileage")
    have = _existing_keys(
        entries, "vehicle-yearly-mileage",
        lambda e: (_arg(e, 0), _arg(e, 1)),
    )
    try:
        rows = list(conn.execute("SELECT * FROM vehicle_yearly_mileage"))
    except sqlite3.OperationalError:
        return report
    for row in rows:
        r = dict(row)
        report.rows += 1
        key = (r["vehicle_slug"], int(r["year"]))
        if key in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_vehicle_yearly_mileage(
                connector_config=connector_config, main_bean=main_bean,
                slug=r["vehicle_slug"], year=int(r["year"]),
                start_mileage=r.get("start_mileage"),
                end_mileage=r.get("end_mileage"),
                business_miles=r.get("business_miles"),
                commuting_miles=r.get("commuting_miles"),
                personal_miles=r.get("personal_miles"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"yearly {key}: {exc}")
    return report


def _export_vehicle_valuations(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.vehicles.writer import append_vehicle_valuation
    from datetime import date as _date
    report = ExportReport(section="vehicle_valuations")
    have = _existing_keys(
        entries, "vehicle-valuation",
        lambda e: (_arg(e, 0), str(_arg(e, 1))),
    )
    try:
        rows = list(conn.execute("SELECT * FROM vehicle_valuations"))
    except sqlite3.OperationalError:
        return report
    for row in rows:
        r = dict(row)
        report.rows += 1
        key = (r["vehicle_slug"], str(r["as_of_date"]))
        if key in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            as_of = _date.fromisoformat(str(r["as_of_date"])[:10])
            append_vehicle_valuation(
                connector_config=connector_config, main_bean=main_bean,
                slug=r["vehicle_slug"], as_of_date=as_of,
                value=r["value"], source=r.get("source"),
                notes=r.get("notes"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"valuation {key}: {exc}")
    return report


def _export_vehicle_elections(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.vehicles.writer import append_vehicle_election
    report = ExportReport(section="vehicle_elections")
    have = _existing_keys(
        entries, "vehicle-election",
        lambda e: (_arg(e, 0), _arg(e, 1)),
    )
    try:
        rows = list(conn.execute("SELECT * FROM vehicle_elections"))
    except sqlite3.OperationalError:
        return report
    for row in rows:
        r = dict(row)
        report.rows += 1
        key = (r["vehicle_slug"], int(r["tax_year"]))
        if key in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_vehicle_election(
                connector_config=connector_config, main_bean=main_bean,
                slug=r["vehicle_slug"], tax_year=int(r["tax_year"]),
                depreciation_method=r.get("depreciation_method"),
                section_179_amount=r.get("section_179_amount"),
                bonus_depreciation_amount=r.get("bonus_depreciation_amount"),
                basis_at_placed_in_service=r.get("basis_at_placed_in_service"),
                business_use_pct_override=r.get("business_use_pct_override"),
                listed_property_qualified=r.get("listed_property_qualified"),
                notes=r.get("notes"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"election {key}: {exc}")
    return report


def _export_vehicle_credits(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.vehicles.writer import append_vehicle_credit
    report = ExportReport(section="vehicle_credits")
    have = _existing_keys(
        entries, "vehicle-credit",
        lambda e: (_arg(e, 0), _arg(e, 1), _arg(e, 2)),
    )
    try:
        rows = list(conn.execute("SELECT * FROM vehicle_credits"))
    except sqlite3.OperationalError:
        return report
    for row in rows:
        r = dict(row)
        report.rows += 1
        key = (r["vehicle_slug"], int(r["tax_year"]), r["credit_label"])
        if key in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_vehicle_credit(
                connector_config=connector_config, main_bean=main_bean,
                slug=r["vehicle_slug"], tax_year=int(r["tax_year"]),
                credit_label=r["credit_label"],
                amount=r.get("amount"), status=r.get("status"),
                notes=r.get("notes"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"credit {key}: {exc}")
    return report


def _export_vehicle_renewals(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.vehicles.writer import append_vehicle_renewal
    from datetime import date as _date
    report = ExportReport(section="vehicle_renewals")
    have = _existing_keys(
        entries, "vehicle-renewal",
        lambda e: (_arg(e, 0), _arg(e, 1), str(_arg(e, 2))),
    )
    try:
        rows = list(conn.execute("SELECT * FROM vehicle_renewals"))
    except sqlite3.OperationalError:
        return report
    for row in rows:
        r = dict(row)
        report.rows += 1
        key = (r["vehicle_slug"], r["renewal_kind"], str(r["due_date"]))
        if key in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            due = _date.fromisoformat(str(r["due_date"])[:10])
            last = None
            if r.get("last_completed"):
                last = _date.fromisoformat(str(r["last_completed"])[:10])
            append_vehicle_renewal(
                connector_config=connector_config, main_bean=main_bean,
                slug=r["vehicle_slug"], renewal_kind=r["renewal_kind"],
                due_date=due,
                cadence_months=r.get("cadence_months"),
                last_completed=last,
                notes=r.get("notes"),
                is_active=bool(r.get("is_active", 1)),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"renewal {key}: {exc}")
    return report


def _export_vehicle_trip_templates(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.vehicles.writer import append_vehicle_trip_template
    report = ExportReport(section="vehicle_trip_templates")
    have = _existing_keys(
        entries, "vehicle-trip-template", lambda e: _arg(e, 0),
    )
    try:
        rows = list(conn.execute("SELECT * FROM vehicle_trip_templates"))
    except sqlite3.OperationalError:
        return report
    for row in rows:
        r = dict(row)
        report.rows += 1
        if r["slug"] in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_vehicle_trip_template(
                connector_config=connector_config, main_bean=main_bean,
                slug=r["slug"], display_name=r["display_name"],
                vehicle_slug=r.get("vehicle_slug"),
                entity=r.get("entity"),
                default_from=r.get("default_from"),
                default_to=r.get("default_to"),
                default_purpose=r.get("default_purpose"),
                default_miles=r.get("default_miles"),
                default_category=r.get("default_category"),
                is_round_trip=bool(r.get("is_round_trip", 0)),
                is_active=bool(r.get("is_active", 1)),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"template {r['slug']!r}: {exc}")
    return report


def _export_fuel_log(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.vehicles.fuel_writer import append_fuel_entry
    report = ExportReport(section="fuel_log")
    have = _existing_keys(
        entries, "vehicle-fuel-entry",
        lambda e: (_arg(e, 0), e.date.isoformat()),
    )
    try:
        rows = list(conn.execute("SELECT * FROM vehicle_fuel_log"))
    except sqlite3.OperationalError:
        return report
    for row in rows:
        r = dict(row)
        report.rows += 1
        key = (r["vehicle_slug"], str(r["as_of_date"]))
        if key in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_fuel_entry(
                connector_config=connector_config, main_bean=main_bean,
                vehicle_slug=r["vehicle_slug"], as_of_date=r["as_of_date"],
                quantity=r["quantity"], unit=r.get("unit") or "gallon",
                fuel_type=r.get("fuel_type") or "gasoline",
                as_of_time=r.get("as_of_time"),
                cost_cents=r.get("cost_cents"),
                odometer=r.get("odometer"),
                location=r.get("location"),
                paperless_id=r.get("paperless_id"),
                notes=r.get("notes"),
                source=r.get("source") or "manual",
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"fuel {key}: {exc}")
    return report


def _export_trip_meta(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.mileage.trip_meta_writer import append_trip_meta
    report = ExportReport(section="mileage_trip_meta")
    have = _existing_keys(
        entries, "mileage-trip-meta",
        lambda e: (e.date.isoformat(), _arg(e, 0), _arg(e, 1)),
    )
    try:
        rows = list(conn.execute("SELECT * FROM mileage_trip_meta"))
    except sqlite3.OperationalError:
        return report
    for row in rows:
        r = dict(row)
        report.rows += 1
        key = (str(r["entry_date"]), r["vehicle"], float(r["miles"]))
        if key in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_trip_meta(
                connector_config=connector_config, main_bean=main_bean,
                entry_date=r["entry_date"], vehicle=r["vehicle"],
                miles=float(r["miles"]),
                business_miles=r.get("business_miles"),
                personal_miles=r.get("personal_miles"),
                commuting_miles=r.get("commuting_miles"),
                category=r.get("category"),
                purpose_parsed=r.get("purpose_parsed"),
                entity_parsed=r.get("entity_parsed"),
                auto_from_ai=bool(r.get("auto_from_ai", 0)),
                free_text=r.get("free_text"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"trip-meta {key}: {exc}")
    return report


def _export_classify_context(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.core.transform.steps.step14_classify_context import (
        append_account_description, append_entity_context,
    )
    report = ExportReport(section="classify_context")
    have_acct = _existing_keys(entries, "account-description", lambda e: _arg(e, 0))
    have_ent = _existing_keys(entries, "entity-context", lambda e: _arg(e, 0))
    for row in conn.execute("SELECT account_path, description FROM account_classify_context"):
        r = dict(row)
        report.rows += 1
        if r["account_path"] in have_acct:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_account_description(
                connector_config=connector_config, main_bean=main_bean,
                account_path=r["account_path"], description=r["description"],
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"account-desc {r['account_path']!r}: {exc}")
    for row in conn.execute(
        "SELECT slug, classify_context FROM entities WHERE classify_context IS NOT NULL AND classify_context != ''"
    ):
        r = dict(row)
        report.rows += 1
        if r["slug"] in have_ent:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_entity_context(
                connector_config=connector_config, main_bean=main_bean,
                entity_slug=r["slug"], context=r["classify_context"],
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"entity-context {r['slug']!r}: {exc}")
    return report


def _export_audit_dismissals(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.core.transform.steps.step15_audit_dismissals import (
        append_audit_dismissed,
    )
    report = ExportReport(section="audit_dismissals")
    have = _existing_keys(entries, "audit-dismissed", lambda e: _arg(e, 0))
    try:
        rows = list(conn.execute("SELECT * FROM audit_dismissals"))
    except sqlite3.OperationalError:
        return report
    cols = [r[1] for r in conn.execute("PRAGMA table_info(audit_dismissals)")]
    fp_col = "fingerprint" if "fingerprint" in cols else (cols[0] if cols else None)
    if fp_col is None:
        return report
    for row in rows:
        r = dict(row)
        fp = r.get(fp_col)
        if not fp:
            continue
        report.rows += 1
        if fp in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_audit_dismissed(
                connector_config=connector_config, main_bean=main_bean,
                fingerprint=str(fp),
                reason=r.get("reason"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"audit-dismissed {fp}: {exc}")
    return report


def _export_account_kinds(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    """Emit `custom "account-kind"` directives for every accounts_meta
    row whose current kind differs from what discovery's heuristic
    would infer. Pure heuristic matches are skipped — they'll get
    re-inferred on boot, so stamping them would be noise.
    """
    from lamella.core.registry.discovery import _infer_account_kind
    from lamella.core.registry.kind_writer import append_account_kind
    report = ExportReport(section="account_kinds")
    have = _existing_keys(entries, "account-kind", lambda e: _arg(e, 0))
    try:
        rows = list(conn.execute(
            "SELECT account_path, kind FROM accounts_meta WHERE kind IS NOT NULL"
        ))
    except sqlite3.OperationalError:
        return report
    for row in rows:
        r = dict(row)
        inferred = _infer_account_kind(r["account_path"])
        if r["kind"] == inferred:
            # Discovery would re-derive the same value — no directive needed.
            continue
        report.rows += 1
        if r["account_path"] in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_account_kind(
                connector_config=connector_config, main_bean=main_bean,
                account_path=r["account_path"], kind=r["kind"],
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"account-kind {r['account_path']!r}: {exc}")
    return report


def _export_balance_anchors(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.dashboard.balances.writer import append_balance_anchor
    report = ExportReport(section="balance_anchors")
    have = _existing_keys(
        entries, "balance-anchor",
        lambda e: (_arg(e, 0), e.date.isoformat()),
    )
    try:
        rows = list(conn.execute(
            "SELECT account_path, as_of_date, balance, currency, source, notes "
            "FROM account_balance_anchors"
        ))
    except sqlite3.OperationalError:
        return report
    for row in rows:
        r = dict(row)
        report.rows += 1
        key = (r["account_path"], str(r["as_of_date"]))
        if key in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            append_balance_anchor(
                connector_config=connector_config, main_bean=main_bean,
                account_path=r["account_path"],
                as_of_date=r["as_of_date"],
                balance=r["balance"],
                currency=r.get("currency") or "USD",
                source=r.get("source"),
                notes=r.get("notes"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"balance-anchor {key}: {exc}")
    return report


def _export_notes(
    conn: sqlite3.Connection, entries: list, *, apply: bool,
    connector_config: Path, main_bean: Path,
) -> ExportReport:
    from lamella.features.notes.writer import append_note
    report = ExportReport(section="notes")
    have = _existing_keys(entries, "note", lambda e: _arg(e, 0))
    cols = [r[1] for r in conn.execute("PRAGMA table_info(notes)")]
    col_list = ", ".join(cols)
    for row in conn.execute(f"SELECT {col_list} FROM notes"):
        r = dict(zip(cols, row))
        report.rows += 1
        note_id = r.get("id")
        if note_id in have:
            report.skipped_existing += 1
            continue
        if not apply:
            report.written += 1
            continue
        try:
            keywords_raw = r.get("keywords") or ""
            keywords = [k for k in keywords_raw.split(",") if k.strip()]
            append_note(
                connector_config=connector_config, main_bean=main_bean,
                note_id=int(note_id),
                body=r.get("body") or "",
                captured_at=r.get("captured_at"),
                merchant_hint=r.get("merchant_hint"),
                entity_hint=r.get("entity_hint"),
                active_from=r.get("active_from"),
                active_to=r.get("active_to"),
                keywords=keywords,
                card_override=(bool(r.get("card_override"))
                               if r.get("card_override") is not None else None),
                status=r.get("status"),
                resolved_txn=r.get("resolved_txn"),
                resolved_receipt=r.get("resolved_receipt"),
            )
            report.written += 1
        except Exception as exc:
            report.errors.append(f"note {note_id}: {exc}")
    return report


ALL_SECTIONS = (
    ("loans", _export_loans),
    ("properties", _export_properties),
    ("projects", _export_projects),
    ("vehicles", _export_vehicles),
    ("vehicle_yearly_mileage", _export_vehicle_yearly_mileage),
    ("vehicle_valuations", _export_vehicle_valuations),
    ("vehicle_elections", _export_vehicle_elections),
    ("vehicle_credits", _export_vehicle_credits),
    ("vehicle_renewals", _export_vehicle_renewals),
    ("vehicle_trip_templates", _export_vehicle_trip_templates),
    ("fuel_log", _export_fuel_log),
    ("mileage_trip_meta", _export_trip_meta),
    ("classify_context", _export_classify_context),
    ("audit_dismissals", _export_audit_dismissals),
    ("account_kinds", _export_account_kinds),
    ("balance_anchors", _export_balance_anchors),
    ("notes", _export_notes),
)


def run_export(
    conn: sqlite3.Connection, main_bean: Path, connector_config: Path,
    *, apply: bool, sections: tuple[str, ...] | None = None,
) -> list[ExportReport]:
    from lamella.utils._legacy_meta import normalize_entries
    entries, _errors, _opts = loader.load_file(str(main_bean))
    entries = normalize_entries(entries)
    reports: list[ExportReport] = []
    selected = set(sections) if sections else set(name for name, _ in ALL_SECTIONS)
    for name, fn in ALL_SECTIONS:
        if name not in selected:
            continue
        try:
            report = fn(
                conn, entries, apply=apply,
                connector_config=connector_config, main_bean=main_bean,
            )
        except Exception as exc:
            log.exception("export section %s failed: %s", name, exc)
            report = ExportReport(section=name, errors=[f"section crashed: {exc}"])
        reports.append(report)
        # Reload entries after apply so later sections don't re-emit
        # directives we just wrote.
        if apply and report.written:
            entries, _, _ = loader.load_file(str(main_bean))
            entries = normalize_entries(entries)
    return reports


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description=(
            "Export current SQLite state rows (loans, properties, projects, "
            "vehicles, fuel_log, mileage_trip_meta, classify_context, "
            "audit_dismissals, notes) to ledger custom directives. Idempotent."
        )
    )
    parser.add_argument("--apply", action="store_true", help="Write changes (default dry-run).")
    parser.add_argument(
        "--section", action="append",
        help="Limit export to named section(s). Repeatable. Omit for all.",
    )
    args = parser.parse_args(argv)

    from lamella.core.config import Settings
    from lamella.core.db import connect, migrate

    settings = Settings()
    conn = connect(settings.db_path)
    migrate(conn)

    sections = tuple(args.section) if args.section else None
    reports = run_export(
        conn=conn,
        main_bean=settings.ledger_main,
        connector_config=settings.connector_config_path,
        apply=args.apply,
        sections=sections,
    )
    action = "WOULD write" if not args.apply else "wrote"
    total_written = 0
    total_rows = 0
    total_errors = 0
    for r in reports:
        total_written += r.written
        total_rows += r.rows
        total_errors += len(r.errors)
        print(
            f"{r.section:20s}  rows={r.rows:5d}  {action}={r.written:5d}  "
            f"skipped_existing={r.skipped_existing:5d}  errors={len(r.errors)}"
        )
        for e in r.errors[:5]:
            print(f"    ! {e}")
    print(
        f"\nTOTAL  rows={total_rows}  {action}={total_written}  errors={total_errors}"
    )
    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
