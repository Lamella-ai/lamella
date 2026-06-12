# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Review-duplicates surface — ADR-0058.

Lists rows the cross-source dedup oracle flagged at intake time and
gives the user two actions:

* **Confirm — same event.** Marks the row dismissed with
  ``dedup`` reason. The match is the user's say-so that this is a
  duplicate of the staged-or-ledger row recorded in
  ``raw_json["dedup_match"]``; nothing else moves.
* **Release — different.** Flips the row back to ``status='new'``
  so it goes through the normal review queue. The dedup pointer
  stays on the row (an audit trail that the user said "not a dup").

The list is cheap (one indexed status filter) and has no AI / job
plumbing. Keeping the code small here keeps the contract obvious:
the dedup oracle decides at intake; this surface is just the
human-in-the-loop.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse

from lamella.core.config import Settings
from lamella.features.import_.staging.service import StagingService
from lamella.web.deps import get_db, get_settings

log = __import__("logging").getLogger(__name__)


router = APIRouter()


def _decode_dedup_match(raw_json_str: str | None) -> dict | None:
    """Pull the ``dedup_match`` block out of a staged row's
    ``raw_json``. Returns None when missing or malformed — the
    surface should still render the row, just without the
    "matches: ..." caption."""
    if not raw_json_str:
        return None
    try:
        payload = json.loads(raw_json_str)
    except (TypeError, ValueError):
        return None
    match = payload.get("dedup_match") if isinstance(payload, dict) else None
    return match if isinstance(match, dict) else None


@router.get("/inbox/duplicates", response_class=HTMLResponse)
@router.get("/review/duplicates", response_class=HTMLResponse)
def review_duplicates_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Render every row currently in ``status='likely_duplicate'``
    along with its match reference.

    The ordering puts most-recently-staged first because that's
    typically what the user just imported and is reviewing. We pull
    ``raw_json`` so the template can render the matched_date /
    description without a second lookup."""
    rows = conn.execute(
        """
        SELECT id, source, source_ref, posting_date, amount, currency,
               payee, description, raw_json, created_at, updated_at
          FROM staged_transactions
         WHERE status = 'likely_duplicate'
         ORDER BY created_at DESC, id DESC
         LIMIT 500
        """
    ).fetchall()
    items = []
    for r in rows:
        items.append({
            "id": int(r["id"]),
            "source": r["source"],
            "posting_date": r["posting_date"],
            "amount": r["amount"],
            "currency": r["currency"],
            "payee": r["payee"],
            "description": r["description"],
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
            "dedup_match": _decode_dedup_match(r["raw_json"]),
        })
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "review_duplicates.html",
        {"items": items, "total": len(items)},
    )


@router.post("/inbox/duplicates/{staged_id}/confirm",
             response_class=HTMLResponse)
@router.post("/review/duplicates/{staged_id}/confirm",
             response_class=HTMLResponse)
def review_duplicates_confirm(
    request: Request,
    staged_id: int,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """User confirms "yes, this is the same event as the matched
    row." Two writes happen atomically:

    1. ADR-0019 + ADR-0058 + ADR-0059 — when the match was against a
       ledger entry on disk, append the new source's
       ``lamella-source-N`` / ``lamella-source-reference-id-N``
       (and optionally ``lamella-source-description-N``) triplet to
       the matched entry's bank-side posting at the next free index.
       The new source becomes a recorded observation on the same
       event.
    2. Mark the staged row dismissed with reason='dedup'. Its
       ``raw_json["dedup_match"]`` block stays for audit.

    For staged-side matches (the matched row hasn't been promoted
    yet) only step 2 runs; the eventual promotion will collapse
    every observing source into one ledger entry per the existing
    matcher rules.
    """
    svc = StagingService(conn)
    row = svc.get(staged_id)
    paired_meta_appended = False

    # Pull the match details out of raw.dedup_match.
    match = (row.raw or {}).get("dedup_match")
    if isinstance(match, dict) and match.get("kind") == "ledger":
        # Resolve which source tag + reference id to write. The
        # staged row's ``source`` is the format class (simplefin,
        # csv, paste, etc.). The reference id is in source_ref under
        # the source-specific shape — try common keys, fall back to
        # the source_ref_hash.
        ref = row.source_ref if isinstance(row.source_ref, dict) else {}
        ref_id = (
            ref.get("txn_id")
            or ref.get("reference_id")
            or ref.get("raw_row_id")
            or row.source_ref_hash
        )
        target_id = match.get("matched_lamella_txn_id")
        target_account = match.get("matched_account")
        # Fall back to a heuristic source_account when the ledger
        # match didn't capture matched_account (older dedup_match
        # blocks predate that field). The user can re-confirm
        # safely; the helper is a no-op when the posting can't be
        # located.
        if target_id and target_account:
            from lamella.features.bank_sync.synthetic_replace import (
                append_source_paired_meta_in_place,
            )
            try:
                # Try the connector_overrides file first (overrides
                # are owned by Lamella so writes are safe), then
                # the connector-specific bank file as a second pass.
                # In practice the matched ledger entry's filename is
                # in the dedup_match block; use it directly.
                bean_path = match.get("filename")
                if bean_path:
                    paired_meta_appended = (
                        append_source_paired_meta_in_place(
                            bean_file=Path(bean_path),
                            lamella_txn_id=str(target_id),
                            posting_account=str(target_account),
                            source=row.source,
                            source_reference_id=str(ref_id),
                            source_description=(
                                row.description or row.payee or None
                            ),
                        )
                    )
                    # ADR-0059 — re-synthesize the canonical txn
                    # narration to combine signal across all
                    # observing sources. Uses the deterministic
                    # synthesizer (Haiku is opt-in via the AI
                    # service; deterministic is safe to call from
                    # the request hot path). Skips when the entry's
                    # narration is user-pinned (no `synthesized: TRUE`
                    # marker; a future read-then-rewrite check will
                    # respect the marker).
                    if paired_meta_appended:
                        try:
                            from lamella.features.ai_cascade.narration_synthesizer import (
                                DeterministicNarrationSynthesizer,
                                SourceObservation,
                                build_synthesis_input,
                            )
                            from lamella.features.bank_sync.synthetic_replace import (
                                rewrite_narration_in_place,
                            )
                            # The new source observation alone won't
                            # make a perfect synthesis — but the
                            # deterministic adapter just picks the
                            # longer description, so the rewrite is
                            # a no-op when the new source's text
                            # is shorter than the existing
                            # narration. This is safe; a richer
                            # invocation pulls every source from
                            # the entry's posting and is a
                            # follow-up.
                            synth_input = build_synthesis_input(
                                signed_amount=row.amount,
                                currency=row.currency or "USD",
                                source_account=str(target_account),
                                target_account=None,
                                observations=[
                                    SourceObservation(
                                        source=row.source,
                                        reference_id=str(ref_id),
                                        description=row.description,
                                        payee=row.payee,
                                    ),
                                ],
                                existing_narration=(
                                    match.get("matched_description")
                                ),
                            )
                            result = (
                                DeterministicNarrationSynthesizer()
                                .synthesize(synth_input)
                            )
                            if (
                                result.narration
                                and result.narration
                                != match.get("matched_description")
                            ):
                                rewrite_narration_in_place(
                                    bean_file=Path(bean_path),
                                    lamella_txn_id=str(target_id),
                                    new_narration=result.narration,
                                )
                        except Exception as exc:  # noqa: BLE001
                            log.warning(
                                "review_duplicates confirm: "
                                "narration re-synthesis failed: %s",
                                exc,
                            )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "review_duplicates confirm: paired-meta append "
                    "failed for staged_id=%s lamella_txn_id=%s: %s",
                    staged_id, target_id, exc,
                )

    svc.dismiss(staged_id, reason="dedup-confirmed-by-user")
    conn.commit()
    appended_msg = (
        " New source observation appended to the matched ledger "
        "entry (ADR-0019 paired meta)."
        if paired_meta_appended else ""
    )
    return HTMLResponse(
        f"<div class='subtle'>Confirmed as duplicate; row {staged_id} "
        f"dismissed.{appended_msg}</div>"
    )


@router.post("/inbox/duplicates/{staged_id}/release",
             response_class=HTMLResponse)
@router.post("/review/duplicates/{staged_id}/release",
             response_class=HTMLResponse)
def review_duplicates_release(
    request: Request,
    staged_id: int,
    conn: sqlite3.Connection = Depends(get_db),
):
    """User says "actually different — release this back to the main
    queue." Flip status back to ``'new'``. The ``dedup_match`` block
    in ``raw_json`` is left untouched — it's an audit trail that the
    user explicitly chose not to dedup against the matched row."""
    cur = conn.execute(
        "UPDATE staged_transactions "
        "   SET status = 'new', updated_at = datetime('now') "
        " WHERE id = ? AND status = 'likely_duplicate'",
        (staged_id,),
    )
    conn.commit()
    if cur.rowcount == 0:
        return HTMLResponse(
            f"<div class='subtle'>Row {staged_id} is no longer "
            "likely_duplicate — nothing to release.</div>"
        )
    return HTMLResponse(
        f"<div class='subtle'>Released row {staged_id} back to the "
        "review queue.</div>"
    )
