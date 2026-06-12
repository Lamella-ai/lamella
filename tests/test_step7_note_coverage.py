# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Audit that the current notes model is capture-only, which step 7
relies on to classify the table as ephemeral. If this test fails,
someone added a note-resolution path and the audit needs to be
redone — see docstring in
``lamella.core.transform.steps.step7_note_coverage``.
"""
from __future__ import annotations

import pathlib
import re


def test_no_code_writes_resolved_state_on_notes():
    """No code path writes to notes.resolved_txn, notes.resolved_receipt,
    or sets notes.status = 'resolved'. If a new code path adds any of
    these, step 7's ephemeral classification has to be revisited
    (notes become partially state)."""
    src = pathlib.Path(__file__).resolve().parent.parent / "src" / "lamella"

    # Concrete write patterns that would indicate a resolution path.
    # The `[^;]{0,200}` caps the match so an INSERT/UPDATE into notes
    # a hundred lines above an unrelated SELECT reading resolved_txn
    # doesn't produce a false positive.
    forbidden = [
        re.compile(
            r"UPDATE\s+notes\s+SET[^;]{0,200}resolved_txn\s*=",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"UPDATE\s+notes\s+SET[^;]{0,200}resolved_receipt\s*=",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"UPDATE\s+notes\s+SET\s+status\s*=\s*['\"]resolved['\"]",
            re.IGNORECASE,
        ),
        re.compile(
            r"INSERT\s+INTO\s+notes[^;]{0,200}resolved_txn\s*[,)]",
            re.IGNORECASE | re.DOTALL,
        ),
    ]
    violations: list[str] = []
    for path in src.rglob("*.py"):
        # Need multi-line match so the cross-line SQL catches too.
        text = path.read_text(encoding="utf-8")
        for rx in forbidden:
            m = rx.search(text)
            if m:
                violations.append(f"{path.name}: {m.group(0)[:120]}")
    assert not violations, (
        "Found writes to notes resolution state. Step 7 assumes "
        "notes are capture-only; if resolution is now being written, "
        "update transform/steps/step7_note_coverage.py with a proper "
        "writer + reconstruct pass. Violations: " + "; ".join(violations)
    )


def test_notes_service_has_no_resolve_method():
    """The NoteService class is the single entry point for mutations.
    It must not grow a resolve()-style method without step 7 being
    revised."""
    from lamella.features.notes.service import NoteService

    forbidden_names = {"resolve", "mark_resolved", "attach_to_txn", "link_to_receipt"}
    attrs = {name for name in dir(NoteService) if not name.startswith("_")}
    assert not (attrs & forbidden_names), (
        f"NoteService grew a resolution method: "
        f"{attrs & forbidden_names}. Revise step 7."
    )
