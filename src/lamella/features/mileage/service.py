# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date as date_t
from pathlib import Path
from typing import Iterable

from lamella.features.mileage.csv_store import (
    MileageCsvError,
    MileageCsvStore,
    MileageRow,
)

log = logging.getLogger(__name__)


def _normalize_vehicle_token(text: str | None) -> str:
    # Lowercase, strip every non-alphanumeric char. Lets a human-written
    # "2008 Work SUV" collide with a Beancount-shaped slug like
    # "V2008WorkSUV" after we also peel the leading V prefix.
    if not text:
        return ""
    return "".join(c for c in text.lower() if c.isalnum())


def _slug_norm_keys(slug: str | None) -> list[str]:
    # A Beancount account segment has to start with an uppercase letter,
    # so "2008WorkSUV" isn't legal and users prefix "V" (or similar).
    # Produce both the raw normalized form and one with a single-letter
    # prefix stripped so bootstrap matching covers either convention.
    if not slug:
        return []
    norm = _normalize_vehicle_token(slug)
    keys = {norm}
    if len(norm) > 1 and norm[0].isalpha() and norm[1].isdigit():
        keys.add(norm[1:])
    return [k for k in keys if k]


class MileageValidationError(ValueError):
    """Surface-level validation failure (bad odometer delta, unknown entity).
    Raised by the service so routes can render a review-style error card."""


class MileageImportError(ValueError):
    """Raised by import_rows when a batch can't be validated."""


@dataclass(frozen=True)
class YearlySummaryRow:
    vehicle: str
    entity: str
    miles: float
    deduction_usd: float
    business_miles: float = 0.0
    commuting_miles: float = 0.0
    personal_miles: float = 0.0


@dataclass(frozen=True)
class ImportPreviewRow:
    """One candidate row in a pending import preview."""
    line_no: int
    entry_date: date_t | None
    entry_time: str | None
    vehicle: str | None
    odometer_start: int | None
    odometer_end: int | None
    miles: float | None
    description: str | None
    error: str | None = None
    conflict: str | None = None
    personal_miles: float | None = None
    business_miles: float | None = None
    commuting_miles: float | None = None
    category: str | None = None


@dataclass(frozen=True)
class ImportResult:
    batch_id: int
    rows_written: int
    rows_skipped: int
    conflicts: int
    messages: list[str]


class MileageService:
    """High-level mileage operations: validate, persist, derive,
    summarize. As of migration 032, mileage_entries is the primary
    store — the CSV (if configured) is a daily backup written from
    the DB, never read as truth except for a one-shot legacy import
    when the table is empty on first startup after the migration."""

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        csv_path: Path | None = None,
    ):
        self.conn = conn
        self.csv_path = csv_path
        self.store = MileageCsvStore(csv_path) if csv_path is not None else None

    # ---- Cache / startup ----------------------------------------------

    def refresh_cache(self) -> int:
        """Back-compat shim. Pre-032 this rebuilt mileage_entries from
        the CSV on mtime change; now the DB is primary so this just
        returns the row count. Preserved for callers that still
        invoke it on startup."""
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM mileage_entries"
        ).fetchone()
        return int(row["n"]) if row else 0

    def bootstrap_from_csv_if_empty(self) -> int:
        """If mileage_entries is empty AND the configured CSV has
        rows, import those rows with source='csv_legacy'. Returns
        number of rows imported. Called once at startup so a fresh
        install with only a CSV on disk still comes up with data."""
        if self.store is None:
            return 0
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM mileage_entries"
        ).fetchone()
        if row and int(row["n"]) > 0:
            return 0
        try:
            rows, warnings = self.store.read_all(strict=False)
        except MileageCsvError as exc:
            log.warning("vehicles.csv bootstrap parse failed: %s", exc)
            return 0
        for w in warnings:
            log.warning("vehicles.csv bootstrap: %s", w)

        for r in rows:
            self.conn.execute(
                """
                INSERT INTO mileage_entries
                    (entry_date, vehicle, odometer_start, odometer_end,
                     miles, purpose, entity, from_loc, to_loc, notes,
                     source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'csv_legacy')
                """,
                (
                    r.entry_date.isoformat(), r.vehicle,
                    r.odometer_start, r.odometer_end, float(r.miles),
                    r.purpose, r.entity, r.from_loc, r.to_loc, r.notes,
                ),
            )
        self.link_unlinked_entries()
        return len(rows)

    def link_unlinked_entries(self) -> int:
        """Backfill mileage_entries.vehicle_slug for rows whose CSV-derived
        display name plausibly matches a registered vehicle slug. Also
        populates vehicles.display_name from the first matching CSV name
        when the column is NULL. Idempotent — safe to call on every boot.

        Beancount account segments must start with an uppercase letter, so
        users commonly prefix a year-first vehicle name with a single
        letter (``V2008WorkSUV``). The CSV meanwhile keeps the human
        form (``2008 Work SUV``). Normalizing to [a-z0-9] and
        stripping a leading single-letter prefix lets the two conventions
        collide cleanly without an explicit mapping file.
        """
        # Map (norm-key, entity_slug) → slug so two entities owning
        # vehicles with the same display name don't collide. NULL
        # entity_slug (personal vehicle) keeps its None bucket so a
        # personal CSV row isn't pulled into a business vehicle.
        slug_by_norm: dict[tuple[str, str | None], str] = {}
        for v in self.conn.execute(
            "SELECT slug, display_name, entity_slug FROM vehicles"
        ).fetchall():
            slug = v["slug"]
            display = v["display_name"]
            ent = v["entity_slug"]
            for key in _slug_norm_keys(slug):
                slug_by_norm.setdefault((key, ent), slug)
            if display:
                for key in _slug_norm_keys(display):
                    slug_by_norm.setdefault((key, ent), slug)
        if not slug_by_norm:
            return 0

        unlinked = self.conn.execute(
            "SELECT DISTINCT vehicle, entity FROM mileage_entries "
            "WHERE (vehicle_slug IS NULL OR vehicle_slug = '') AND vehicle IS NOT NULL"
        ).fetchall()
        linked = 0
        linked_display: dict[str, str] = {}
        for row in unlinked:
            display = row["vehicle"]
            entry_entity = row["entity"]
            norm = _normalize_vehicle_token(display)
            slug = slug_by_norm.get((norm, entry_entity))
            if not slug:
                # No same-entity vehicle matches. Refuse to backfill —
                # would cross the entity boundary and silently attribute
                # this mileage to another entity's vehicle.
                continue
            self.conn.execute(
                "UPDATE mileage_entries SET vehicle_slug = ? "
                "WHERE vehicle = ? AND entity = ? "
                "AND (vehicle_slug IS NULL OR vehicle_slug = '')",
                (slug, display, entry_entity),
            )
            linked += self.conn.execute(
                "SELECT changes() AS n"
            ).fetchone()["n"]
            linked_display.setdefault(slug, display)

        for slug, display in linked_display.items():
            self.conn.execute(
                "UPDATE vehicles SET display_name = ? "
                "WHERE slug = ? AND (display_name IS NULL OR display_name = '')",
                (display, slug),
            )
        return linked

    # ---- Reads ---------------------------------------------------------

    def list_entries(
        self, *, year: int | None = None, limit: int = 200,
        offset: int = 0, vehicle: str | None = None,
        fix: str | None = None,
    ) -> list[MileageRow]:
        """List mileage entries with optional filters.

        ``vehicle`` uses the tolerant predicate (matches either slug
        or display name). ``fix`` filters to rows matching a specific
        data-health issue so the detail-page "missing splits" link
        opens the right subset:

          - 'splits'   — trips where mileage_trip_meta has no
                         business/commuting/personal recorded.
          - 'purpose'  — trips with empty purpose AND from_loc AND
                         to_loc AND notes AND miles > 0. A 0-mile
                         maintenance day whose ``notes`` is "oil
                         change" is NOT in this set — notes on its own
                         substantiates. 0-mile "no trips today"
                         markers are also excluded; they contribute 0
                         to the deduction so IRS substantiation rules
                         don't apply.
          - 'orphan'   — trips whose vehicle_slug is NULL and whose
                         ``vehicle`` string doesn't match any
                         registered slug or display_name.
        """
        clauses: list[str] = []
        params: list = []
        joins = ""
        if year is not None:
            clauses.append("e.entry_date >= ? AND e.entry_date < ?")
            params.extend([f"{year:04d}-01-01", f"{year + 1:04d}-01-01"])
        if vehicle:
            clauses.append(
                "(e.vehicle = ? OR e.vehicle_slug = ? OR e.vehicle IN "
                "(SELECT display_name FROM vehicles WHERE slug = ?))"
            )
            params.extend([vehicle, vehicle, vehicle])
        if fix == "splits":
            joins += (
                " LEFT JOIN mileage_trip_meta m "
                "   ON m.entry_date = e.entry_date "
                "  AND m.vehicle = e.vehicle "
                "  AND m.miles = e.miles"
            )
            clauses.append(
                "(m.business_miles IS NULL "
                "AND m.commuting_miles IS NULL "
                "AND m.personal_miles IS NULL)"
            )
        elif fix == "purpose":
            # 0-mile entries are "no trips today" markers (or maintenance
            # days where notes substantiate). They contribute 0 to the
            # deduction, so IRS substantiation rules don't apply and we
            # don't want to flag them as missing.
            clauses.append(
                "COALESCE(e.miles, 0) > 0 "
                "AND COALESCE(e.purpose, '') = '' "
                "AND COALESCE(e.from_loc, '') = '' "
                "AND COALESCE(e.to_loc, '') = '' "
                "AND COALESCE(e.notes, '') = ''"
            )
        elif fix == "orphan":
            clauses.append(
                "e.vehicle_slug IS NULL AND e.vehicle NOT IN ("
                "  SELECT slug FROM vehicles "
                "    UNION "
                "  SELECT display_name FROM vehicles WHERE display_name IS NOT NULL"
                ")"
            )
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])
        rows = self.conn.execute(
            f"""
            SELECT e.id, e.entry_date, e.entry_time, e.vehicle, e.vehicle_slug,
                   e.odometer_start, e.odometer_end, e.miles,
                   e.purpose, e.entity, e.from_loc, e.to_loc, e.notes
              FROM mileage_entries e
              {joins}
              {where}
          ORDER BY e.entry_date DESC, COALESCE(e.entry_time, '') DESC, e.id DESC
             LIMIT ? OFFSET ?
            """,
            tuple(params),
        ).fetchall()
        return [_row_to_mileage(r) for r in rows]

    def count_entries(
        self, *, year: int | None = None, vehicle: str | None = None,
        fix: str | None = None,
    ) -> int:
        """Companion to `list_entries` for pagination. Same filter
        semantics; returns the unbounded row count."""
        clauses: list[str] = []
        params: list = []
        joins = ""
        if year is not None:
            clauses.append("e.entry_date >= ? AND e.entry_date < ?")
            params.extend([f"{year:04d}-01-01", f"{year + 1:04d}-01-01"])
        if vehicle:
            clauses.append(
                "(e.vehicle = ? OR e.vehicle_slug = ? OR e.vehicle IN "
                "(SELECT display_name FROM vehicles WHERE slug = ?))"
            )
            params.extend([vehicle, vehicle, vehicle])
        if fix == "splits":
            joins += (
                " LEFT JOIN mileage_trip_meta m "
                "   ON m.entry_date = e.entry_date "
                "  AND m.vehicle = e.vehicle "
                "  AND m.miles = e.miles"
            )
            clauses.append(
                "(m.business_miles IS NULL "
                "AND m.commuting_miles IS NULL "
                "AND m.personal_miles IS NULL)"
            )
        elif fix == "purpose":
            # 0-mile entries are "no trips today" markers (or maintenance
            # days where notes substantiate). They contribute 0 to the
            # deduction, so IRS substantiation rules don't apply and we
            # don't want to flag them as missing.
            clauses.append(
                "COALESCE(e.miles, 0) > 0 "
                "AND COALESCE(e.purpose, '') = '' "
                "AND COALESCE(e.from_loc, '') = '' "
                "AND COALESCE(e.to_loc, '') = '' "
                "AND COALESCE(e.notes, '') = ''"
            )
        elif fix == "orphan":
            clauses.append(
                "e.vehicle_slug IS NULL AND e.vehicle NOT IN ("
                "  SELECT slug FROM vehicles "
                "    UNION "
                "  SELECT display_name FROM vehicles WHERE display_name IS NOT NULL"
                ")"
            )
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        row = self.conn.execute(
            f"SELECT COUNT(*) AS n FROM mileage_entries e {joins} {where}",
            tuple(params),
        ).fetchone()
        return int(row["n"]) if row else 0

    def entry_by_id(self, entry_id: int) -> dict | None:
        """Return a single mileage row + its trip_meta sidecar, if
        any. Used by the edit form to pre-fill both halves."""
        row = self.conn.execute(
            """
            SELECT id, entry_date, entry_time, vehicle, vehicle_slug,
                   odometer_start, odometer_end, miles,
                   purpose, entity, from_loc, to_loc, notes,
                   purpose_category
              FROM mileage_entries WHERE id = ?
            """,
            (int(entry_id),),
        ).fetchone()
        if row is None:
            return None
        out = dict(row)
        meta = self.conn.execute(
            "SELECT business_miles, commuting_miles, personal_miles, "
            "category, free_text "
            "FROM mileage_trip_meta "
            "WHERE entry_date = ? AND vehicle = ? AND miles = ?",
            (out["entry_date"], out["vehicle"], float(out["miles"] or 0)),
        ).fetchone()
        out["meta"] = dict(meta) if meta else None
        return out

    def yearly_summary(
        self, year: int, *, rate_per_mile: float,
    ) -> list[YearlySummaryRow]:
        """Aggregate a year's mileage into (vehicle, entity) buckets
        with per-entry rate lookup from the `mileage_rates` table.
        ``rate_per_mile`` is a FALLBACK only (empty rate table / trip
        predates the earliest row). Per-entry lookup means IRS
        mid-year rate changes are honored naturally.

        Business/commuting/personal split comes from mileage_trip_meta
        via the (entry_date, vehicle, miles) key — matches the tolerant
        predicate used elsewhere. When no split is recorded, the row's
        business/commuting/personal stay at 0; the Phase 2 data-health
        banner flags that explicitly. Deduction = business miles × rate
        (Phase 2 narrowing) — commuting and personal are never
        deductible under Schedule C Part IV. Trips without a recorded
        split contribute 0 to the deduction; the banner surfaces
        those so the user can record splits and get a non-zero figure.
        """
        start = f"{year:04d}-01-01"
        end = f"{year + 1:04d}-01-01"
        rows = self.conn.execute(
            """
            SELECT e.entry_date, e.vehicle, e.entity, e.miles,
                   m.business_miles,
                   m.commuting_miles,
                   m.personal_miles
              FROM mileage_entries e
              LEFT JOIN mileage_trip_meta m
                     ON m.entry_date = e.entry_date
                    AND m.vehicle = e.vehicle
                    AND m.miles = e.miles
             WHERE e.entry_date >= ? AND e.entry_date < ?
          ORDER BY e.vehicle, e.entity, e.entry_date
            """,
            (start, end),
        ).fetchall()
        agg: dict[tuple[str, str], dict] = {}
        for r in rows:
            miles = float(r["miles"] or 0)
            if miles <= 0:
                continue
            try:
                d = date_t.fromisoformat(str(r["entry_date"])[:10])
            except ValueError:
                continue
            rate = self.rate_for_date(d, fallback=rate_per_mile)
            key = (r["vehicle"], r["entity"])
            bucket = agg.setdefault(
                key,
                {
                    "miles": 0.0,
                    "business_miles": 0.0,
                    "commuting_miles": 0.0,
                    "personal_miles": 0.0,
                    "deduction_usd": 0.0,
                },
            )
            bucket["miles"] += miles
            biz = float(r["business_miles"] or 0)
            com = float(r["commuting_miles"] or 0)
            per = float(r["personal_miles"] or 0)
            bucket["business_miles"] += biz
            bucket["commuting_miles"] += com
            bucket["personal_miles"] += per
            # Phase 2: deduction = business miles only. Commuting and
            # personal are never deductible. When no split is recorded
            # (biz=0), the trip contributes 0 here; the data-health
            # banner on /vehicles surfaces the missing split so the
            # user knows why the number looks low.
            bucket["deduction_usd"] += round(biz * rate, 4)
        return [
            YearlySummaryRow(
                vehicle=vehicle,
                entity=entity,
                miles=b["miles"],
                deduction_usd=round(b["deduction_usd"], 2),
                business_miles=round(b["business_miles"], 2),
                commuting_miles=round(b["commuting_miles"], 2),
                personal_miles=round(b["personal_miles"], 2),
            )
            for (vehicle, entity), b in sorted(agg.items())
        ]

    def latest_for_vehicle(self, vehicle: str) -> MileageRow | None:
        row = self.conn.execute(
            """
            SELECT id, entry_date, entry_time, vehicle, vehicle_slug,
                   odometer_start, odometer_end, miles,
                   purpose, entity, from_loc, to_loc, notes
              FROM mileage_entries
             WHERE vehicle = ?
          ORDER BY entry_date DESC,
                   COALESCE(entry_time, '') DESC,
                   id DESC
             LIMIT 1
            """,
            (vehicle,),
        ).fetchone()
        return _row_to_mileage(row) if row else None

    def last_odometer_for(self, vehicle: str) -> dict | None:
        """Return {'odometer': N, 'entry_date': 'YYYY-MM-DD'} for the
        most recent odometer_end recorded against this vehicle, or
        None. Used by the mileage form to prefill start odometer.
        Ordering: most recent date, then highest odometer_end for
        within-day ties (odometers only grow)."""
        row = self.conn.execute(
            """
            SELECT entry_date, odometer_end
              FROM mileage_entries
             WHERE vehicle = ? AND odometer_end IS NOT NULL
          ORDER BY entry_date DESC,
                   COALESCE(entry_time, '') DESC,
                   odometer_end DESC
             LIMIT 1
            """,
            (vehicle,),
        ).fetchone()
        if row is None or row["odometer_end"] is None:
            return None
        return {
            "odometer": int(row["odometer_end"]),
            "entry_date": row["entry_date"],
        }

    def vehicles(self) -> list[str]:
        """Distinct vehicle names used in the mileage log. Historical
        signal only; the UI drives the dropdown off the registry."""
        rows = self.conn.execute(
            "SELECT DISTINCT vehicle FROM mileage_entries ORDER BY vehicle"
        ).fetchall()
        return [r["vehicle"] for r in rows]

    # ---- Trip meta (splits + provenance) -------------------------------

    def upsert_trip_meta(
        self,
        *,
        entry_date: date_t,
        vehicle: str,
        miles: float,
        business_miles: float | None = None,
        personal_miles: float | None = None,
        commuting_miles: float | None = None,
        category: str | None = None,
        free_text: str | None = None,
        auto_from_ai: bool = False,
        connector_config_path: Path | None = None,
        main_bean_path: Path | None = None,
    ) -> None:
        """Attach business/commuting/personal split + provenance to a
        trip. Sidecar keyed on (entry_date, vehicle, miles) so it
        survives reorganizations of mileage_entries. ``category`` is
        the enum {'business','commuting','personal','mixed',None} —
        populated when the user picked a simplified radio; NULL when
        numbers were typed directly.

        When ``connector_config_path`` + ``main_bean_path`` are supplied,
        the split is also emitted as a `custom "mileage-trip-meta"`
        directive so a DB wipe can rebuild it. Callers that don't pass
        them (unit tests, pre-031 code paths) keep the DB-only behavior.
        """
        if (
            business_miles is None
            and personal_miles is None
            and commuting_miles is None
            and category is None
            and not free_text
        ):
            return
        if category is not None and category not in {
            "business", "commuting", "personal", "mixed",
        }:
            raise MileageValidationError(
                f"invalid category {category!r}"
            )
        self.conn.execute(
            """
            INSERT INTO mileage_trip_meta
                (entry_date, vehicle, miles, business_miles,
                 commuting_miles, personal_miles, category,
                 free_text, auto_from_ai)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (entry_date, vehicle, miles) DO UPDATE SET
                business_miles  = excluded.business_miles,
                commuting_miles = excluded.commuting_miles,
                personal_miles  = excluded.personal_miles,
                category        = excluded.category,
                free_text       = COALESCE(excluded.free_text, free_text),
                auto_from_ai    = excluded.auto_from_ai
            """,
            (
                entry_date.isoformat(),
                vehicle,
                float(miles),
                float(business_miles) if business_miles is not None else None,
                float(commuting_miles) if commuting_miles is not None else None,
                float(personal_miles) if personal_miles is not None else None,
                category,
                free_text,
                1 if auto_from_ai else 0,
            ),
        )
        # Mirror the split to the ledger so a DB wipe can rebuild it.
        if connector_config_path is not None and main_bean_path is not None:
            try:
                from lamella.features.mileage.trip_meta_writer import append_trip_meta
                append_trip_meta(
                    connector_config=connector_config_path,
                    main_bean=main_bean_path,
                    entry_date=entry_date,
                    vehicle=vehicle,
                    miles=float(miles),
                    business_miles=business_miles,
                    personal_miles=personal_miles,
                    commuting_miles=commuting_miles,
                    category=category,
                    auto_from_ai=auto_from_ai,
                    free_text=free_text,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "mileage-trip-meta directive write failed for %s %s: %s",
                    vehicle, entry_date, exc,
                )

    def trip_meta_for(
        self, *, entry_date: date_t, vehicle: str, miles: float,
    ) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM mileage_trip_meta "
            "WHERE entry_date = ? AND vehicle = ? AND miles = ?",
            (entry_date.isoformat(), vehicle, float(miles)),
        ).fetchone()
        return dict(row) if row else None

    # ---- Writes --------------------------------------------------------

    def add_entry(
        self,
        *,
        entry_date: date_t,
        vehicle: str,
        entity: str,
        miles: float | None = None,
        odometer_start: int | None = None,
        odometer_end: int | None = None,
        entry_time: str | None = None,
        vehicle_slug: str | None = None,
        purpose: str | None = None,
        from_loc: str | None = None,
        to_loc: str | None = None,
        notes: str | None = None,
        known_entities: Iterable[str] | None = None,
        business_miles: float | None = None,
        personal_miles: float | None = None,
        commuting_miles: float | None = None,
        category: str | None = None,
        free_text: str | None = None,
        source: str = "manual",
        import_batch_id: int | None = None,
    ) -> MileageRow:
        vehicle = (vehicle or "").strip()
        entity = (entity or "").strip()
        if not vehicle:
            raise MileageValidationError("vehicle is required")
        if not entity:
            raise MileageValidationError("entity is required")
        if known_entities is not None:
            allowed = {e.strip() for e in known_entities if e and e.strip()}
            if allowed and entity not in allowed:
                raise MileageValidationError(
                    f"entity {entity!r} is not in the known entity list"
                )

        # An explicit odometer_start from the caller is the user's
        # ground truth for "what the odometer read when I got in the
        # car." Pre-044 this was auto-filled from the most recent
        # entry's end odometer, which silently produced thousands of
        # phantom miles when there had been a gap since the last log.
        # Now: odometer_start is accepted; we only fall back to the
        # auto-lookup when the caller didn't supply one AND didn't
        # supply miles directly.

        if (
            odometer_end is not None
            and miles is not None
            and odometer_start is None
        ):
            raise MileageValidationError(
                "provide either (odometer_start + odometer_end) OR miles, not both"
            )

        odo_start: int | None = None
        odo_end: int | None = None
        derived_miles: float

        if odometer_start is not None and odometer_end is not None:
            if int(odometer_end) < int(odometer_start):
                raise MileageValidationError(
                    f"end odometer ({odometer_end}) must be >= start "
                    f"odometer ({odometer_start})"
                )
            odo_start = int(odometer_start)
            odo_end = int(odometer_end)
            derived_miles = float(odo_end - odo_start)
        elif odometer_end is not None:
            # Legacy fallback: no explicit start — use the last
            # recorded odometer for this vehicle. Kept for the
            # contemporaneous-logging case where the user only
            # enters the current reading.
            prior = self.latest_for_vehicle(vehicle)
            if prior is None or prior.odometer_end is None:
                raise MileageValidationError(
                    f"no prior odometer reading for {vehicle!r}; "
                    "enter start odometer or miles directly for the first trip"
                )
            delta = int(odometer_end) - int(prior.odometer_end)
            if delta <= 0:
                raise MileageValidationError(
                    f"odometer must be greater than prior reading "
                    f"({prior.odometer_end}); got {odometer_end}"
                )
            odo_start = int(prior.odometer_end)
            odo_end = int(odometer_end)
            derived_miles = float(delta)
        elif miles is not None:
            if float(miles) <= 0:
                raise MileageValidationError("miles must be positive")
            derived_miles = float(miles)
        else:
            raise MileageValidationError(
                "either (odometer_start + odometer_end) or odometer_end or miles is required"
            )

        if category is not None and category not in {
            "business", "commuting", "personal", "mixed",
        }:
            raise MileageValidationError(
                f"invalid category {category!r}"
            )

        cursor = self.conn.execute(
            """
            INSERT INTO mileage_entries
                (entry_date, entry_time, vehicle, vehicle_slug,
                 odometer_start, odometer_end, miles,
                 purpose, entity, from_loc, to_loc, notes,
                 purpose_category,
                 source, import_batch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry_date.isoformat(),
                entry_time,
                vehicle,
                vehicle_slug,
                odo_start,
                odo_end,
                derived_miles,
                (purpose or None),
                entity,
                (from_loc or None),
                (to_loc or None),
                (notes or None),
                category,
                source,
                import_batch_id,
            ),
        )
        new_id = int(cursor.lastrowid)

        if (
            business_miles is not None
            or personal_miles is not None
            or commuting_miles is not None
            or category is not None
            or free_text
        ):
            try:
                self.upsert_trip_meta(
                    entry_date=entry_date,
                    vehicle=vehicle,
                    miles=derived_miles,
                    business_miles=business_miles,
                    commuting_miles=commuting_miles,
                    personal_miles=personal_miles,
                    category=category,
                    free_text=free_text,
                    auto_from_ai=False,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("trip_meta upsert failed: %s", exc)

        # Opportunistic CSV backup write — failure is non-fatal.
        try:
            self.backup_to_csv()
        except Exception as exc:  # noqa: BLE001
            log.warning("mileage CSV backup failed: %s", exc)

        # Back-fill audit — if this new row is dated meaningfully in
        # the past (user catching up on old paper logs), note the
        # date so the audit page can surface same-day classified
        # txns for re-review.
        try:
            from lamella.features.mileage.backfill_audit import (
                record_backfill,
            )
            record_backfill(self.conn, entry_date=entry_date)
        except Exception as exc:  # noqa: BLE001
            log.warning("backfill audit record failed: %s", exc)

        return MileageRow(
            entry_date=entry_date,
            vehicle=vehicle,
            odometer_start=odo_start,
            odometer_end=odo_end,
            miles=derived_miles,
            purpose=(purpose or None),
            entity=entity,
            from_loc=(from_loc or None),
            to_loc=(to_loc or None),
            notes=(notes or None),
            csv_row_index=new_id,  # repurposed — carries DB id now
        )

    def update_entry(
        self,
        entry_id: int,
        *,
        entry_date: date_t,
        vehicle: str,
        entity: str,
        miles: float,
        odometer_start: int | None = None,
        odometer_end: int | None = None,
        entry_time: str | None = None,
        vehicle_slug: str | None = None,
        purpose: str | None = None,
        from_loc: str | None = None,
        to_loc: str | None = None,
        notes: str | None = None,
        business_miles: float | None = None,
        personal_miles: float | None = None,
        commuting_miles: float | None = None,
        category: str | None = None,
        known_entities: Iterable[str] | None = None,
    ) -> MileageRow:
        """Edit an existing mileage row.

        Unlike ``add_entry``, this does NOT validate odometer deltas
        against the last-recorded reading — edits may legitimately
        correct a bad odometer, and the data-health panel surfaces
        non-monotonic sequences separately. ``miles`` is passed
        through verbatim so 0-mile maintenance days (oil change, tire
        rotation) can keep miles=0 with notes substantiating.

        Updates the sidecar `mileage_trip_meta` when any of
        business / commuting / personal / category is provided. When
        all four are None, the sidecar is left alone — blanking out a
        previously-recorded split would require an explicit ``clear``
        operation (not needed for Phase 5A)."""
        if not vehicle or not entity:
            raise MileageValidationError("vehicle and entity are required")
        if known_entities is not None:
            allowed = {e.strip() for e in known_entities if e and e.strip()}
            if allowed and entity not in allowed:
                raise MileageValidationError(
                    f"entity {entity!r} is not in the known entity list"
                )
        if category is not None and category not in {
            "business", "commuting", "personal", "mixed",
        }:
            raise MileageValidationError(f"invalid category {category!r}")

        # Stash the prior entry_date so the backfill-audit re-aggregate
        # at the end also refreshes the old date when the edit moved
        # the row to a different day.
        prior_row = self.conn.execute(
            "SELECT entry_date FROM mileage_entries WHERE id = ?",
            (int(entry_id),),
        ).fetchone()
        prior_date_iso: str | None = None
        if prior_row is not None:
            raw = prior_row["entry_date"]
            prior_date_iso = str(raw)[:10] if raw else None

        self.conn.execute(
            """
            UPDATE mileage_entries SET
                entry_date = ?, entry_time = ?, vehicle = ?, vehicle_slug = ?,
                odometer_start = ?, odometer_end = ?, miles = ?,
                purpose = ?, entity = ?, from_loc = ?, to_loc = ?, notes = ?,
                purpose_category = COALESCE(?, purpose_category)
              WHERE id = ?
            """,
            (
                entry_date.isoformat(),
                entry_time,
                vehicle,
                vehicle_slug,
                odometer_start,
                odometer_end,
                float(miles),
                purpose or None,
                entity,
                from_loc or None,
                to_loc or None,
                notes or None,
                category,
                int(entry_id),
            ),
        )
        if (
            business_miles is not None
            or commuting_miles is not None
            or personal_miles is not None
            or category is not None
        ):
            try:
                self.upsert_trip_meta(
                    entry_date=entry_date,
                    vehicle=vehicle,
                    miles=float(miles),
                    business_miles=business_miles,
                    commuting_miles=commuting_miles,
                    personal_miles=personal_miles,
                    category=category,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("trip_meta upsert on edit failed: %s", exc)
        try:
            self.backup_to_csv()
        except Exception as exc:  # noqa: BLE001
            log.warning("mileage CSV backup after edit failed: %s", exc)

        # Back-fill audit — refresh the new entry_date AND the prior
        # entry_date if the edit moved the row between days. If the
        # old date no longer has any back-filled rows, record_backfill
        # purges its stale audit entry.
        try:
            from lamella.features.mileage.backfill_audit import (
                record_backfill,
            )
            record_backfill(self.conn, entry_date=entry_date)
            if prior_date_iso and prior_date_iso != entry_date.isoformat():
                record_backfill(self.conn, entry_date=prior_date_iso)
        except Exception as exc:  # noqa: BLE001
            log.warning("backfill audit refresh on edit failed: %s", exc)

        return MileageRow(
            entry_date=entry_date,
            vehicle=vehicle,
            odometer_start=odometer_start,
            odometer_end=odometer_end,
            miles=float(miles),
            purpose=purpose or None,
            entity=entity,
            from_loc=from_loc or None,
            to_loc=to_loc or None,
            notes=notes or None,
            csv_row_index=int(entry_id),
        )

    def delete_entry(self, entry_id: int) -> bool:
        """Delete a mileage row by DB id. Returns True if a row was
        removed. Kept cheap because entries are independent; referential
        mileage_trip_meta survives by content match."""
        prior = self.conn.execute(
            "SELECT entry_date FROM mileage_entries WHERE id = ?",
            (int(entry_id),),
        ).fetchone()
        prior_date_iso: str | None = None
        if prior is not None:
            raw = prior["entry_date"]
            prior_date_iso = str(raw)[:10] if raw else None

        cur = self.conn.execute(
            "DELETE FROM mileage_entries WHERE id = ?", (int(entry_id),),
        )
        removed = cur.rowcount and cur.rowcount > 0
        if removed:
            try:
                self.backup_to_csv()
            except Exception as exc:  # noqa: BLE001
                log.warning("mileage CSV backup after delete failed: %s", exc)
            if prior_date_iso:
                try:
                    from lamella.features.mileage.backfill_audit import (
                        record_backfill,
                    )
                    record_backfill(self.conn, entry_date=prior_date_iso)
                except Exception as exc:  # noqa: BLE001
                    log.warning("backfill audit refresh on delete failed: %s", exc)
        return bool(removed)

    # ---- CSV backup ----------------------------------------------------

    def backup_to_csv(self) -> Path | None:
        """Rewrite the configured CSV file to mirror the current DB
        contents. No-op when no csv_path is configured. Atomic — a
        crash in the middle leaves the previous backup intact."""
        if self.store is None or self.csv_path is None:
            return None
        rows = self.conn.execute(
            """
            SELECT id, entry_date, vehicle, odometer_start, odometer_end,
                   miles, purpose, entity, from_loc, to_loc, notes
              FROM mileage_entries
          ORDER BY entry_date ASC, COALESCE(entry_time, '') ASC, id ASC
            """,
        ).fetchall()
        mileage_rows: list[MileageRow] = []
        for idx, r in enumerate(rows):
            try:
                ed = date_t.fromisoformat(str(r["entry_date"])[:10])
            except ValueError:
                continue
            mileage_rows.append(MileageRow(
                entry_date=ed,
                vehicle=r["vehicle"],
                odometer_start=r["odometer_start"],
                odometer_end=r["odometer_end"],
                miles=float(r["miles"] or 0),
                purpose=r["purpose"],
                entity=r["entity"],
                from_loc=r["from_loc"],
                to_loc=r["to_loc"],
                notes=r["notes"],
                csv_row_index=idx,
            ))
        self.store.rewrite(mileage_rows)
        return self.csv_path

    # ---- Import --------------------------------------------------------

    def create_import_batch(
        self,
        *,
        vehicle_slug: str | None,
        source_filename: str | None,
        source_format: str,
        notes: str | None = None,
    ) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO mileage_imports
                (vehicle_slug, source_filename, source_format, notes)
            VALUES (?, ?, ?, ?)
            """,
            (vehicle_slug, source_filename, source_format, notes),
        )
        return int(cur.lastrowid)

    def finalize_import_batch(
        self, batch_id: int, *,
        row_count: int, skipped_count: int, conflict_count: int,
    ) -> None:
        self.conn.execute(
            """
            UPDATE mileage_imports
               SET row_count = ?, skipped_count = ?, conflict_count = ?
             WHERE id = ?
            """,
            (row_count, skipped_count, conflict_count, batch_id),
        )

    def list_import_batches(self, *, limit: int = 50) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT id, imported_at, vehicle_slug, source_filename,
                   source_format, row_count, skipped_count, conflict_count, notes
              FROM mileage_imports
          ORDER BY imported_at DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def delete_import_batch(self, batch_id: int) -> int:
        """Delete every row tagged with this import batch, plus the
        batch record itself. Returns rows deleted. Useful for undoing
        a bad import."""
        cur = self.conn.execute(
            "DELETE FROM mileage_entries WHERE import_batch_id = ?",
            (int(batch_id),),
        )
        deleted = int(cur.rowcount or 0)
        self.conn.execute(
            "DELETE FROM mileage_imports WHERE id = ?", (int(batch_id),),
        )
        if deleted:
            try:
                self.backup_to_csv()
            except Exception as exc:  # noqa: BLE001
                log.warning("mileage CSV backup after batch-undo failed: %s", exc)
            try:
                from lamella.features.mileage.backfill_audit import (
                    rebuild_mileage_backfill_audit,
                )
                rebuild_mileage_backfill_audit(self.conn)
            except Exception as exc:  # noqa: BLE001
                log.warning("backfill audit rebuild after batch-undo failed: %s", exc)
        return deleted

    def write_import_rows(
        self,
        *,
        batch_id: int,
        vehicle: str,
        vehicle_slug: str | None,
        entity: str,
        rows: list[ImportPreviewRow],
    ) -> ImportResult:
        """Insert preview rows into mileage_entries. Rows with an
        ``error`` are skipped; rows with a ``conflict`` are still
        written but counted as conflicts so the user sees the count.
        Duplicate protection: if a row with the same vehicle +
        entry_date + entry_time + miles + odometer_end already
        exists, skip it rather than double-inserting."""
        written = 0
        skipped = 0
        conflicts = 0
        messages: list[str] = []
        for row in rows:
            if row.error is not None:
                skipped += 1
                messages.append(f"line {row.line_no}: {row.error}")
                continue
            if row.entry_date is None or row.miles is None or row.miles < 0:
                skipped += 1
                messages.append(
                    f"line {row.line_no}: missing date or negative miles"
                )
                continue
            # miles == 0 is a legitimate "no trips today" marker;
            # keep it.

            if self._duplicate_exists(
                entry_date=row.entry_date,
                entry_time=row.entry_time,
                vehicle=vehicle,
                miles=float(row.miles),
                odometer_end=row.odometer_end,
            ):
                skipped += 1
                messages.append(
                    f"line {row.line_no}: duplicate — already recorded"
                )
                continue

            if row.conflict is not None:
                conflicts += 1
                messages.append(f"line {row.line_no}: {row.conflict}")

            self.conn.execute(
                """
                INSERT INTO mileage_entries
                    (entry_date, entry_time, vehicle, vehicle_slug,
                     odometer_start, odometer_end, miles,
                     purpose, entity, notes,
                     purpose_category,
                     source, import_batch_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.entry_date.isoformat(),
                    row.entry_time,
                    vehicle,
                    vehicle_slug,
                    row.odometer_start,
                    row.odometer_end,
                    float(row.miles),
                    row.description or None,
                    entity,
                    None,
                    row.category,
                    "import",
                    batch_id,
                ),
            )
            written += 1
            # Forward business/commuting/personal splits + category
            # to the sidecar.
            if (
                row.personal_miles is not None
                or row.business_miles is not None
                or row.commuting_miles is not None
                or row.category is not None
            ):
                try:
                    self.upsert_trip_meta(
                        entry_date=row.entry_date,
                        vehicle=vehicle,
                        miles=float(row.miles),
                        business_miles=row.business_miles,
                        commuting_miles=row.commuting_miles,
                        personal_miles=row.personal_miles,
                        category=row.category,
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning("import trip_meta upsert failed: %s", exc)

        self.finalize_import_batch(
            batch_id,
            row_count=written, skipped_count=skipped, conflict_count=conflicts,
        )
        if written:
            try:
                self.backup_to_csv()
            except Exception as exc:  # noqa: BLE001
                log.warning("mileage CSV backup after import failed: %s", exc)
            # Bulk import — the cheapest correct refresh is a full
            # rebuild. Imports typically touch many dates at once so
            # per-row `record_backfill` would be N SQLs for little
            # benefit vs one aggregate.
            try:
                from lamella.features.mileage.backfill_audit import (
                    rebuild_mileage_backfill_audit,
                )
                rebuild_mileage_backfill_audit(self.conn)
            except Exception as exc:  # noqa: BLE001
                log.warning("backfill audit rebuild after import failed: %s", exc)
        return ImportResult(
            batch_id=batch_id,
            rows_written=written,
            rows_skipped=skipped,
            conflicts=conflicts,
            messages=messages,
        )

    def _duplicate_exists(
        self, *,
        entry_date: date_t,
        entry_time: str | None,
        vehicle: str,
        miles: float,
        odometer_end: int | None,
    ) -> bool:
        # Same date + vehicle + same miles (to 0.1 mi) is a duplicate.
        # If an odometer end is supplied, require that to match too —
        # otherwise two real back-to-back trips with identical mileage
        # would collide.
        rows = self.conn.execute(
            """
            SELECT odometer_end, entry_time, miles
              FROM mileage_entries
             WHERE entry_date = ? AND vehicle = ?
            """,
            (entry_date.isoformat(), vehicle),
        ).fetchall()
        for r in rows:
            if abs(float(r["miles"] or 0) - float(miles)) > 0.05:
                continue
            if odometer_end is not None and r["odometer_end"] is not None:
                if int(r["odometer_end"]) != int(odometer_end):
                    continue
            if entry_time and r["entry_time"] and entry_time != r["entry_time"]:
                continue
            return True
        return False

    # ---- Rates ---------------------------------------------------------

    def rate_for_date(
        self, trip_date: date_t, *, fallback: float = 0.67,
    ) -> float:
        """Look up the IRS standard mileage rate effective on
        ``trip_date`` from ``mileage_rates``. Falls back to the
        caller-supplied value when no row covers the date."""
        try:
            row = self.conn.execute(
                "SELECT rate_per_mile FROM mileage_rates "
                "WHERE effective_from <= ? "
                "ORDER BY effective_from DESC LIMIT 1",
                (trip_date.isoformat(),),
            ).fetchone()
        except sqlite3.Error:
            row = None
        if row is None or row["rate_per_mile"] is None:
            return fallback
        try:
            return float(row["rate_per_mile"])
        except (TypeError, ValueError):
            return fallback

    def list_rates(self) -> list[dict]:
        try:
            rows = self.conn.execute(
                "SELECT id, effective_from, rate_per_mile, notes "
                "FROM mileage_rates ORDER BY effective_from DESC"
            ).fetchall()
        except sqlite3.Error:
            return []
        return [dict(r) for r in rows]

    def upsert_rate(
        self, *, effective_from: str, rate_per_mile: float,
        notes: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO mileage_rates (effective_from, rate_per_mile, notes)
            VALUES (?, ?, ?)
            ON CONFLICT (effective_from) DO UPDATE SET
                rate_per_mile = excluded.rate_per_mile,
                notes = excluded.notes
            """,
            (effective_from, rate_per_mile, notes),
        )

    def delete_rate(self, rate_id: int) -> None:
        self.conn.execute("DELETE FROM mileage_rates WHERE id = ?", (rate_id,))


def _row_to_mileage(row: sqlite3.Row) -> MileageRow:
    raw_date = row["entry_date"]
    if isinstance(raw_date, str):
        entry_date = date_t.fromisoformat(raw_date[:10])
    else:
        entry_date = raw_date
    # MileageRow's legacy csv_row_index field now carries the DB id.
    entry_id = row["id"] if "id" in row.keys() else -1
    return MileageRow(
        entry_date=entry_date,
        vehicle=row["vehicle"],
        odometer_start=row["odometer_start"],
        odometer_end=row["odometer_end"],
        miles=float(row["miles"]),
        purpose=row["purpose"],
        entity=row["entity"],
        from_loc=row["from_loc"],
        to_loc=row["to_loc"],
        notes=row["notes"],
        csv_row_index=int(entry_id) if entry_id is not None else -1,
    )


def group_by_vehicle(rows: Iterable[MileageRow]) -> dict[str, list[MileageRow]]:
    groups: dict[str, list[MileageRow]] = defaultdict(list)
    for r in rows:
        groups[r.vehicle].append(r)
    return groups
