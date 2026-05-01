# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 21: accounts_meta extended-field reconstruct.

Reads ``custom "account-meta" <account>`` directives and upserts
display_name / institution / last_four / entity_slug /
simplefin_account_id / notes into accounts_meta.

Companion to step_kind (which handles the ``kind`` column via
``custom "account-kind"``). The two steps cover every user-edited
field on accounts_meta; anything else on that table is either
ledger-derived (seeded_from_ledger, closed_on) or system-computed
(opened_on).
"""
from __future__ import annotations

import logging

from lamella.core.registry.account_meta_writer import (
    read_account_meta_directives,
)
from lamella.core.registry.discovery import _short_name_for_path
from lamella.core.transform.reconstruct import ReconstructReport, register

log = logging.getLogger(__name__)


@register(
    "step21:account-meta",
    state_tables=["accounts_meta"],
)
def reconstruct_account_meta(conn, entries) -> ReconstructReport:
    directives = read_account_meta_directives(entries)
    if not directives:
        return ReconstructReport(
            pass_name="step21:account-meta",
            notes=["no account-meta directives"],
        )
    inserted = 0
    updated = 0
    for d in directives:
        account_path = d["account_path"]
        existed = conn.execute(
            "SELECT 1 FROM accounts_meta WHERE account_path = ?",
            (account_path,),
        ).fetchone()

        directive_display_name = d.get("display_name")
        if existed:
            # Preserve every existing column unless the directive
            # supplies a non-null replacement. We pass the raw
            # (possibly None) directive values to COALESCE so missing
            # fields don't clobber user-set ones.
            conn.execute(
                """
                UPDATE accounts_meta SET
                    display_name         = COALESCE(?, display_name),
                    institution          = COALESCE(?, institution),
                    last_four            = COALESCE(?, last_four),
                    entity_slug          = COALESCE(?, entity_slug),
                    simplefin_account_id = COALESCE(?, simplefin_account_id),
                    notes                = COALESCE(?, notes)
                WHERE account_path = ?
                """,
                (
                    directive_display_name,
                    d.get("institution"),
                    d.get("last_four"),
                    d.get("entity_slug"),
                    d.get("simplefin_account_id"),
                    d.get("notes"),
                    account_path,
                ),
            )
            updated += 1
        else:
            # `display_name` is NOT NULL in the schema. The writer
            # omits `lamella-display-name` when the user hasn't set one
            # (see `account_meta_writer.append_account_meta`), so on a
            # fresh seed (e.g. /setup/import) the directive can carry
            # no display_name. Fall back to the same heuristic
            # `seed_accounts_meta` uses for unlabeled accounts. The
            # fallback only fires on first insert; an existing row's
            # user-set name is preserved by the UPDATE branch above.
            display_name = directive_display_name or _short_name_for_path(account_path)
            conn.execute(
                """
                INSERT INTO accounts_meta
                    (account_path, display_name, institution, last_four,
                     entity_slug, simplefin_account_id, notes,
                     seeded_from_ledger, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
                """,
                (
                    account_path,
                    display_name,
                    d.get("institution"),
                    d.get("last_four"),
                    d.get("entity_slug"),
                    d.get("simplefin_account_id"),
                    d.get("notes"),
                ),
            )
            inserted += 1
    return ReconstructReport(
        pass_name="step21:account-meta",
        rows_written=inserted + updated,
        notes=[
            f"reconstructed {len(directives)} account-meta directive(s): "
            f"{inserted} inserted, {updated} updated",
        ],
    )
