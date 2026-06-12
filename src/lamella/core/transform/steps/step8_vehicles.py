# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 8: vehicle state reconstruction.

Rebuilds every vehicle-related state table from the ledger's custom
directives (see vehicles/writer.py for the writing side and
vehicles/reader.py for the parsing side).

State tables handled by this step:
  - vehicles
  - vehicle_yearly_mileage
  - vehicle_valuations
  - vehicle_elections
  - vehicle_credits
  - vehicle_renewals
  - vehicle_trip_templates
  - mileage_trip_meta.attributed_entity (column-level state)
  - vehicle_disposals (content-addressed by lamella-disposal-id on
    connector_overrides.bean transactions, NOT a custom directive)

Fuel log + data-health cache are cache tables and stay out of this
reconstruct pass — they're regenerated naturally or simply empty.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from beancount.core.data import Transaction

from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy
from lamella.features.vehicles.reader import (
    read_mileage_attributions,
    read_vehicle_credits,
    read_vehicle_elections,
    read_vehicle_renewals,
    read_vehicle_trip_templates,
    read_vehicle_valuations,
    read_vehicle_yearly_mileage,
    read_vehicles,
)

log = logging.getLogger(__name__)


@register(
    "step8:vehicles",
    state_tables=[
        "vehicles",
        "vehicle_yearly_mileage",
        "vehicle_valuations",
        "vehicle_elections",
        "vehicle_credits",
        "vehicle_renewals",
        "vehicle_trip_templates",
        "vehicle_disposals",
    ],
)
def reconstruct_vehicles(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    written = 0
    notes: list[str] = []

    # 1. vehicles
    vehicles = read_vehicles(entries)
    for v in vehicles:
        cursor = conn.execute(
            """
            INSERT INTO vehicles
                (slug, display_name, year, make, model, vin, license_plate,
                 entity_slug, purchase_date, purchase_price, purchase_fees,
                 asset_account_path, gvwr_lbs, placed_in_service_date,
                 fuel_type, notes, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (slug) DO UPDATE SET
                display_name            = excluded.display_name,
                year                    = excluded.year,
                make                    = excluded.make,
                model                   = excluded.model,
                vin                     = excluded.vin,
                license_plate           = excluded.license_plate,
                entity_slug             = excluded.entity_slug,
                purchase_date           = excluded.purchase_date,
                purchase_price          = excluded.purchase_price,
                purchase_fees           = excluded.purchase_fees,
                asset_account_path      = excluded.asset_account_path,
                gvwr_lbs                = excluded.gvwr_lbs,
                placed_in_service_date  = excluded.placed_in_service_date,
                fuel_type               = excluded.fuel_type,
                notes                   = excluded.notes,
                is_active               = excluded.is_active
            """,
            (
                v["slug"], v["display_name"], v["year"], v["make"], v["model"],
                v["vin"], v["license_plate"], v["entity_slug"],
                v["purchase_date"], v["purchase_price"], v["purchase_fees"],
                v["asset_account_path"], v["gvwr_lbs"],
                v["placed_in_service_date"], v["fuel_type"],
                v["notes"], v["is_active"],
            ),
        )
        if cursor.rowcount:
            written += 1
    notes.append(f"{len(vehicles)} vehicles")

    # 2. vehicle_yearly_mileage
    yearly = read_vehicle_yearly_mileage(entries)
    for y in yearly:
        conn.execute(
            """
            INSERT INTO vehicle_yearly_mileage
                (vehicle_slug, year, start_mileage, end_mileage,
                 business_miles, commuting_miles, personal_miles)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (vehicle_slug, year) DO UPDATE SET
                start_mileage   = excluded.start_mileage,
                end_mileage     = excluded.end_mileage,
                business_miles  = excluded.business_miles,
                commuting_miles = excluded.commuting_miles,
                personal_miles  = excluded.personal_miles
            """,
            (
                y["vehicle_slug"], y["year"], y["start_mileage"],
                y["end_mileage"], y["business_miles"], y["commuting_miles"],
                y["personal_miles"],
            ),
        )
        written += 1
    notes.append(f"{len(yearly)} yearly rows")

    # 3. vehicle_valuations
    valuations = read_vehicle_valuations(entries)
    for val in valuations:
        conn.execute(
            """
            INSERT INTO vehicle_valuations
                (vehicle_slug, as_of_date, value, source, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (vehicle_slug, as_of_date) DO UPDATE SET
                value  = excluded.value,
                source = excluded.source,
                notes  = excluded.notes
            """,
            (
                val["vehicle_slug"], val["as_of_date"],
                val["value"], val["source"], val["notes"],
            ),
        )
        written += 1
    notes.append(f"{len(valuations)} valuations")

    # 4. vehicle_elections
    elections = read_vehicle_elections(entries)
    for el in elections:
        conn.execute(
            """
            INSERT INTO vehicle_elections
                (vehicle_slug, tax_year, depreciation_method,
                 section_179_amount, bonus_depreciation_amount,
                 basis_at_placed_in_service, business_use_pct_override,
                 listed_property_qualified, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (vehicle_slug, tax_year) DO UPDATE SET
                depreciation_method         = excluded.depreciation_method,
                section_179_amount          = excluded.section_179_amount,
                bonus_depreciation_amount   = excluded.bonus_depreciation_amount,
                basis_at_placed_in_service  = excluded.basis_at_placed_in_service,
                business_use_pct_override   = excluded.business_use_pct_override,
                listed_property_qualified   = excluded.listed_property_qualified,
                notes                       = excluded.notes
            """,
            (
                el["vehicle_slug"], el["tax_year"],
                el["depreciation_method"], el["section_179_amount"],
                el["bonus_depreciation_amount"], el["basis_at_placed_in_service"],
                el["business_use_pct_override"], el["listed_property_qualified"],
                el["notes"],
            ),
        )
        written += 1
    notes.append(f"{len(elections)} elections")

    # 5. vehicle_credits — content-addressed; INSERT OR IGNORE on a
    # natural key (slug, year, label).
    credits = read_vehicle_credits(entries)
    for c in credits:
        existing = conn.execute(
            "SELECT id FROM vehicle_credits "
            "WHERE vehicle_slug = ? AND tax_year = ? AND credit_label = ?",
            (c["vehicle_slug"], c["tax_year"], c["credit_label"]),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE vehicle_credits SET amount = ?, status = ?, notes = ? "
                "WHERE id = ?",
                (c["amount"], c["status"], c["notes"], existing["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO vehicle_credits "
                "(vehicle_slug, tax_year, credit_label, amount, status, notes) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    c["vehicle_slug"], c["tax_year"], c["credit_label"],
                    c["amount"], c["status"], c["notes"],
                ),
            )
            written += 1
    notes.append(f"{len(credits)} credits")

    # 6. vehicle_renewals — natural key (slug, kind, due_date).
    renewals = read_vehicle_renewals(entries)
    for r in renewals:
        existing = conn.execute(
            "SELECT id FROM vehicle_renewals "
            "WHERE vehicle_slug = ? AND renewal_kind = ? AND due_date = ?",
            (r["vehicle_slug"], r["renewal_kind"], r["due_date"]),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE vehicle_renewals SET cadence_months = ?, "
                "last_completed = ?, notes = ?, is_active = ? WHERE id = ?",
                (
                    r["cadence_months"], r["last_completed"], r["notes"],
                    r["is_active"], existing["id"],
                ),
            )
        else:
            conn.execute(
                "INSERT INTO vehicle_renewals "
                "(vehicle_slug, renewal_kind, due_date, cadence_months, "
                " last_completed, notes, is_active) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    r["vehicle_slug"], r["renewal_kind"], r["due_date"],
                    r["cadence_months"], r["last_completed"], r["notes"],
                    r["is_active"],
                ),
            )
            written += 1
    notes.append(f"{len(renewals)} renewals")

    # 7. vehicle_trip_templates
    templates = read_vehicle_trip_templates(entries)
    for t in templates:
        conn.execute(
            """
            INSERT INTO vehicle_trip_templates
                (slug, display_name, vehicle_slug, entity,
                 default_from, default_to, default_purpose,
                 default_miles, default_category,
                 is_round_trip, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (slug) DO UPDATE SET
                display_name     = excluded.display_name,
                vehicle_slug     = excluded.vehicle_slug,
                entity           = excluded.entity,
                default_from     = excluded.default_from,
                default_to       = excluded.default_to,
                default_purpose  = excluded.default_purpose,
                default_miles    = excluded.default_miles,
                default_category = excluded.default_category,
                is_round_trip    = excluded.is_round_trip,
                is_active        = excluded.is_active
            """,
            (
                t["slug"], t["display_name"], t["vehicle_slug"], t["entity"],
                t["default_from"], t["default_to"], t["default_purpose"],
                t["default_miles"], t["default_category"],
                t["is_round_trip"], t["is_active"],
            ),
        )
        written += 1
    notes.append(f"{len(templates)} templates")

    # 8. mileage_trip_meta.attributed_entity — column-level state
    attributions = read_mileage_attributions(entries)
    for a in attributions:
        conn.execute(
            """
            INSERT INTO mileage_trip_meta
                (entry_date, vehicle, miles, attributed_entity)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (entry_date, vehicle, miles) DO UPDATE SET
                attributed_entity = excluded.attributed_entity
            """,
            (a["entry_date"], a["vehicle"], a["miles"], a["attributed_entity"]),
        )
        written += 1
    notes.append(f"{len(attributions)} attributions")

    # 9. vehicle_disposals — content-addressed by `lamella-disposal-id`.
    # Read from `connector_overrides.bean` transactions tagged
    # #lamella-vehicle-disposal. Pair revokes with originals.
    disposal_txns = [
        e for e in entries
        if isinstance(e, Transaction)
           and "lamella-vehicle-disposal" in (e.tags or set())
    ]
    disposals_by_id: dict[str, dict] = {}
    revokes_by_target: dict[str, str] = {}
    for t in disposal_txns:
        meta = t.meta or {}
        did = meta.get("lamella-disposal-id")
        if not did:
            continue
        revokes = meta.get("lamella-disposal-revokes")
        disposals_by_id[str(did)] = {
            "disposal_id": str(did),
            "vehicle_slug": meta.get("lamella-disposal-vehicle"),
            "disposal_date": t.date.isoformat(),
            "disposal_type": meta.get("lamella-disposal-type") or "sale",
            "revokes_disposal_id": str(revokes) if revokes else None,
            "notes": meta.get("lamella-disposal-notes"),
        }
        if revokes:
            revokes_by_target[str(revokes)] = str(did)
    for did, d in disposals_by_id.items():
        revoked_by = revokes_by_target.get(did)
        conn.execute(
            """
            INSERT INTO vehicle_disposals
                (disposal_id, vehicle_slug, disposal_date, disposal_type,
                 revokes_disposal_id, revoked_by_disposal_id, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (disposal_id) DO UPDATE SET
                vehicle_slug           = excluded.vehicle_slug,
                disposal_date          = excluded.disposal_date,
                disposal_type          = excluded.disposal_type,
                revokes_disposal_id    = excluded.revokes_disposal_id,
                revoked_by_disposal_id = excluded.revoked_by_disposal_id,
                notes                  = excluded.notes
            """,
            (
                d["disposal_id"], d["vehicle_slug"], d["disposal_date"],
                d["disposal_type"], d["revokes_disposal_id"], revoked_by,
                d["notes"],
            ),
        )
        written += 1
    notes.append(f"{len(disposals_by_id)} disposals")

    return ReconstructReport(
        pass_name="step8:vehicles",
        rows_written=written,
        notes=notes,
    )


# Register cache-table + state-table policies.
for table in (
    "vehicles", "vehicle_yearly_mileage", "vehicle_valuations",
    "vehicle_elections", "vehicle_credits", "vehicle_renewals",
    "vehicle_trip_templates", "vehicle_disposals",
):
    register_policy(
        TablePolicy(table=table, kind="state", primary_key=("slug",))
    )
for table in (
    "vehicle_fuel_log", "vehicle_data_health_cache",
    "vehicle_breaking_change_seen", "user_ui_state",
):
    register_policy(
        TablePolicy(table=table, kind="cache", primary_key=())
    )
