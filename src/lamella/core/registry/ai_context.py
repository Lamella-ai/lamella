# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Build registry context strings for AI prompts.

Called by ai/classify.py, ai/notes.py, and ai/match.py before rendering
their Jinja prompt templates. Turns the registry tables into a compact
human-readable preamble the model can use to resolve "gas for my
Work SUV at Warehouse Club on the Citi card" into concrete account paths.
"""
from __future__ import annotations

import sqlite3


def registry_preamble(conn: sqlite3.Connection) -> str:
    """Return a multi-line string suitable for embedding directly in
    an AI prompt. Empty string if nothing useful is known yet."""
    entities = conn.execute(
        "SELECT slug, display_name, entity_type, tax_schedule "
        "FROM entities WHERE is_active = 1 ORDER BY sort_order, display_name"
    ).fetchall()
    vehicles = conn.execute(
        "SELECT slug, display_name, year, make, model "
        "FROM vehicles WHERE is_active = 1 ORDER BY year DESC, display_name"
    ).fetchall()
    accounts = conn.execute(
        "SELECT account_path, display_name, kind, last_four, institution "
        "FROM accounts_meta WHERE is_active = 1 AND closed_on IS NULL "
        "AND kind IN ('checking','savings','credit_card','line_of_credit','loan','brokerage') "
        "ORDER BY display_name LIMIT 40"
    ).fetchall()

    if not entities and not vehicles and not accounts:
        return ""

    lines: list[str] = ["Registry context:"]
    if entities:
        lines.append("Entities (use the slug as the Expenses:<slug>:… prefix):")
        for e in entities:
            bits = [f"`{e['slug']}`"]
            if e["display_name"]:
                bits.append(f"= {e['display_name']}")
            if e["entity_type"]:
                bits.append(f"({e['entity_type']})")
            if e["tax_schedule"]:
                bits.append(f"Schedule {e['tax_schedule']}")
            lines.append("- " + " ".join(bits))
    if vehicles:
        lines.append("Vehicles (use as Expenses:Vehicles:<slug>:…):")
        for v in vehicles:
            desc = f"`{v['slug']}`"
            if v["display_name"]:
                desc += f" = {v['display_name']}"
            elif v["year"] or v["make"] or v["model"]:
                parts = [str(v["year"] or ""), v["make"] or "", v["model"] or ""]
                desc += f" = {' '.join(p for p in parts if p).strip()}"
            lines.append("- " + desc)
    if accounts:
        lines.append("Known accounts (map merchant hints like 'Citi card' to these):")
        for a in accounts:
            tail = []
            if a["institution"]:
                tail.append(a["institution"])
            if a["last_four"]:
                tail.append(f"****{a['last_four']}")
            if a["kind"]:
                tail.append(a["kind"])
            suffix = f" ({' · '.join(tail)})" if tail else ""
            lines.append(f"- {a['display_name']}{suffix} → `{a['account_path']}`")
    return "\n".join(lines) + "\n"
