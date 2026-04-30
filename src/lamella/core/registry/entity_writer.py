# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for the entity registry — ``custom "entity"`` directives in
connector_config.bean so the registry round-trips through the ledger.

Before this, the ``entities`` table was SQLite-only: display_name,
entity_type, tax_schedule, start_date, ceased_date, notes — all lost
on DB wipe. The reconstruct fallback left the app without the
registry that drives commingle-vs-intercompany logic, scaffold
routing, and everything downstream.

Directive shape:

    2020-01-01 custom "entity" "ZetaGen"
      lamella-display-name:  "Zeta Gen"
      lamella-entity-type:   "llc"
      lamella-tax-schedule:  "C"
      lamella-start-date:    "2016-03-15"
      lamella-notes:         "Widget merchandise business"
      lamella-modified-at:   "2026-04-24T02:15:00-06:00"

Each write rewrites the entity's block — previous directives for the
same slug are stripped on append so the file stays as one block per
entity. Reconstruct reads every block and upserts.

Date pinning: we use 2000-01-01 as the directive date unless the
entity has a real start_date, since bean-check doesn't care about
custom-directive dates and a pre-ledger date avoids "invalid
reference" issues.
"""
from __future__ import annotations

import logging
import re
from datetime import UTC, date, datetime
from pathlib import Path

from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    ensure_include_in_main,
    run_bean_check_vs_baseline,
)

log = logging.getLogger(__name__)


def _q(s: str | None) -> str:
    """Quote-escape a value for a Beancount string literal."""
    if s is None:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _strip_existing_entity_blocks(text: str, entity_slug: str) -> str:
    """Remove any existing ``custom "entity" "<slug>"`` block from the
    file so the writer stays one-block-per-slug."""
    # Match a block: `<date> custom "entity" "<slug>"` plus any
    # indented continuation lines (metadata).
    pattern = re.compile(
        r'(?:^|\n)(\d{4}-\d{2}-\d{2})\s+custom\s+"entity"\s+"'
        + re.escape(entity_slug)
        + r'"\s*\n(?:[ \t]+[^\n]*\n)*',
        re.MULTILINE,
    )
    return pattern.sub("\n", text)


def append_entity_directive(
    *,
    connector_config: Path,
    main_bean: Path,
    entity_slug: str,
    display_name: str | None = None,
    entity_type: str | None = None,
    tax_schedule: str | None = None,
    start_date: str | date | None = None,
    ceased_date: str | date | None = None,
    notes: str | None = None,
    is_active: bool | None = None,
    run_check: bool = True,
) -> None:
    """Write (or rewrite) the ``custom "entity"`` block for
    ``entity_slug``. Atomic: snapshot + bean-check + restore on
    failure.

    Writing empty/None fields just omits the corresponding metadata
    line — so reconstruct's reader can tell "unset" from "set to
    empty-string" (we treat both as unset; reconstruct won't overwrite
    an existing DB value with None from a missing directive key).
    """
    if not entity_slug:
        raise ValueError("entity_slug required")
    if not main_bean.exists():
        raise FileNotFoundError(f"main.bean not found at {main_bean}")

    backup_main = main_bean.read_bytes()
    config_existed = connector_config.exists()
    backup_config = connector_config.read_bytes() if config_existed else None

    baseline_output = ""
    if run_check:
        _, baseline_output = capture_bean_check(main_bean)

    # Ensure the file exists + is included from main.bean.
    if not config_existed:
        connector_config.parent.mkdir(parents=True, exist_ok=True)
        connector_config.write_text(
            "; connector_config.bean — configuration state written by Lamella.\n"
            "; Paperless field-role mappings and UI-persisted settings live here.\n"
            "; Do not hand-edit; use the /settings pages.\n\n",
            encoding="utf-8",
        )
    ensure_include_in_main(main_bean, connector_config)

    current = connector_config.read_text(encoding="utf-8")
    stripped = _strip_existing_entity_blocks(current, entity_slug)

    # Build the new block.
    when = "2000-01-01"
    if isinstance(start_date, date):
        when = start_date.isoformat()
    elif isinstance(start_date, str) and start_date.strip():
        when = start_date.strip()[:10]
    modified_at = datetime.now(UTC).isoformat(timespec="seconds")
    lines = [
        f'{when} custom "entity" "{_q(entity_slug)}"',
    ]
    if display_name and display_name.strip():
        lines.append(f'  lamella-display-name: "{_q(display_name.strip())}"')
    if entity_type and entity_type.strip():
        lines.append(f'  lamella-entity-type: "{_q(entity_type.strip())}"')
    if tax_schedule and tax_schedule.strip():
        lines.append(f'  lamella-tax-schedule: "{_q(tax_schedule.strip())}"')
    if start_date:
        sd = start_date.isoformat() if isinstance(start_date, date) else str(start_date)[:10]
        lines.append(f'  lamella-start-date: "{sd}"')
    if ceased_date:
        cd = ceased_date.isoformat() if isinstance(ceased_date, date) else str(ceased_date)[:10]
        lines.append(f'  lamella-ceased-date: "{cd}"')
    if notes and notes.strip():
        lines.append(f'  lamella-notes: "{_q(notes.strip())}"')
    if is_active is not None:
        lines.append(f'  lamella-is-active: "{1 if is_active else 0}"')
    lines.append(f'  lamella-modified-at: "{modified_at}"')
    block = "\n" + "\n".join(lines) + "\n"

    new_text = stripped.rstrip() + "\n" + block
    try:
        connector_config.write_text(new_text, encoding="utf-8")
        if run_check:
            run_bean_check_vs_baseline(main_bean, baseline_output)
    except BeanCheckError:
        main_bean.write_bytes(backup_main)
        if backup_config is None:
            connector_config.unlink(missing_ok=True)
        else:
            connector_config.write_bytes(backup_config)
        raise


def append_entity_deleted(
    *,
    connector_config: Path,
    main_bean: Path,
    entity_slug: str,
    run_check: bool = True,
) -> None:
    """Append a ``custom "entity-deleted"`` tombstone directive.

    Phase 1.4. The §7 #7 shape: when the user deletes an entity from
    the DB, the next boot's ``discover_entity_slugs`` walks every
    ``Open`` directive and re-creates a row for any slug that still
    appears in the ledger as segment 1. Without an explicit tombstone,
    delete cannot be made permanent — the entity resurrects on every
    boot until the user also closes and physically removes every
    ``Open Expenses:<slug>:*`` directive.

    The tombstone is the established prior-art pattern in this repo
    (see ``classification-rule-revoked``, ``recurring-revoked``,
    ``loan-deleted``, ``property-deleted``, ``loan-pause-revoked``).
    Once written, the reader loop drops the slug from any rebuild and
    ``discover_entity_slugs`` filters it out of boot-time auto-seed.

    Mirrors :func:`append_property_deleted` and
    :func:`append_loan_deleted` in shape — the tombstone carries no
    metadata beyond the slug arg + directive date, since the only
    thing the reader needs to know is "this slug is gone." Re-creating
    the same slug after deletion requires explicit user action that
    issues a fresh ``custom "entity"`` directive — but the existing
    reader is one-way (tombstone-then-create stays tombstoned within
    a single load), matching the "delete means delete" contract from
    Phase 1.4 decision A.
    """
    from lamella.core.transform.custom_directive import (
        append_custom_directive,
    )
    if not entity_slug:
        raise ValueError("entity_slug required")
    if not main_bean.exists():
        raise FileNotFoundError(f"main.bean not found at {main_bean}")
    today = datetime.now(UTC).date()
    append_custom_directive(
        target=connector_config,
        main_bean=main_bean,
        header=(
            "; connector_config.bean — configuration state written by Lamella.\n"
            "; Paperless field-role mappings and UI-persisted settings live here.\n"
            "; Do not hand-edit; use the /settings pages.\n\n"
        ),
        directive_date=today,
        directive_type="entity-deleted",
        args=[entity_slug],
        meta={"lamella-deleted-at": datetime.now(UTC).isoformat(timespec="seconds")},
        run_check=run_check,
    )


def read_deleted_entity_slugs(entries) -> set[str]:
    """Return the set of entity slugs whose latest directive in load
    order is a ``custom "entity-deleted"`` tombstone.

    Used by both:
    - :func:`read_entity_directives` to drop tombstoned rows from the
      reconstruct rebuild;
    - ``registry.discovery.discover_entity_slugs`` to filter slugs out
      of the boot-time auto-seed pass.

    A re-creation flow (a ``custom "entity"`` directive after the
    tombstone) is intentionally NOT honored at this layer — matching
    the established ``property-deleted`` / ``loan-deleted`` reader
    pattern. Once tombstoned within a load, stays tombstoned. If the
    user wants the slug back, they re-create with a different slug or
    a deliberate revoke flow we can build separately.
    """
    from beancount.core.data import Custom
    deleted: set[str] = set()
    for e in entries:
        if not isinstance(e, Custom):
            continue
        if e.type != "entity-deleted":
            continue
        if not e.values:
            continue
        raw = e.values[0]
        slug = raw.value if hasattr(raw, "value") else raw
        slug = str(slug).strip()
        if slug:
            deleted.add(slug)
    return deleted


def read_entity_directives(entries) -> list[dict]:
    """Parse every ``custom "entity"`` directive out of ``entries``,
    dropping any whose slug appears in a later ``entity-deleted``
    tombstone.

    Returns a list of dicts ready for SQL upsert — fields mirror the
    ``entities`` table columns.
    """
    from beancount.core.data import Custom
    deleted = read_deleted_entity_slugs(entries)
    out: list[dict] = []
    for e in entries:
        if not isinstance(e, Custom):
            continue
        if e.type != "entity":
            continue
        if not e.values:
            continue
        # First value is the slug (string literal).
        raw = e.values[0]
        slug = raw.value if hasattr(raw, "value") else raw
        slug = str(slug).strip()
        if not slug:
            continue
        if slug in deleted:
            continue
        meta = e.meta or {}
        is_active_raw = _meta_get(meta, "lamella-is-active")
        is_active: int | None = None
        if is_active_raw is not None:
            is_active = 1 if is_active_raw.strip() in ("1", "true", "True", "yes") else 0
        row = {
            "slug": slug,
            "display_name": _meta_get(meta, "lamella-display-name"),
            "entity_type": _meta_get(meta, "lamella-entity-type"),
            "tax_schedule": _meta_get(meta, "lamella-tax-schedule"),
            "start_date": _meta_get(meta, "lamella-start-date"),
            "ceased_date": _meta_get(meta, "lamella-ceased-date"),
            "notes": _meta_get(meta, "lamella-notes"),
            "is_active": is_active,
            "_directive_date": e.date.isoformat() if hasattr(e.date, "isoformat") else str(e.date),
        }
        out.append(row)
    return out


def _meta_get(meta: dict, key: str) -> str | None:
    """Strip surrounding quotes from a string meta value; return None
    when the key is absent or empty."""
    if key not in meta:
        return None
    v = meta[key]
    if hasattr(v, "value"):
        v = v.value
    if v is None:
        return None
    s = str(v).strip()
    return s or None
