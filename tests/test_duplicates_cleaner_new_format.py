# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 7b — duplicates/cleaner.py understands both legacy and the
new posting-level paired source meta when scanning .bean text for
SimpleFIN ids. Without this, dropping the SimpleFIN writer's
dual-emit would silently break duplicate cleanup for any
post-Phase-7 entry."""
from __future__ import annotations

from lamella.features.data_integrity.cleaner import (
    _aliases_insertion_index,
    _extract_sfid_from_block,
    _inject_aliases_into_block,
)


def _block(text: str) -> list[str]:
    """Helper: split a multi-line string into the per-line list the
    cleaner walks. Preserves trailing newlines so list-edits round-trip
    cleanly."""
    return [ln + "\n" for ln in text.splitlines()]


# ---------------------------------------------------------------------------
# Extraction — new format
# ---------------------------------------------------------------------------

def test_extract_sfid_finds_new_paired_source_format():
    block = _block(
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-txn-id: "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '    lamella-source-0: "simplefin"\n'
        '    lamella-source-reference-id-0: "TRN-NEW-1"\n'
        '  Expenses:Acme:Supplies  42.17 USD'
    )
    assert _extract_sfid_from_block(block) == "TRN-NEW-1"


def test_extract_sfid_finds_simplefin_at_higher_index():
    """Cross-source dedup: posting carries a CSV source at -0 and the
    SimpleFIN one was appended at -1. The cleaner must still find it."""
    block = _block(
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-txn-id: "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '    lamella-source-0: "csv"\n'
        '    lamella-source-reference-id-0: "CSV-99"\n'
        '    lamella-source-1: "simplefin"\n'
        '    lamella-source-reference-id-1: "TRN-X"\n'
        '  Expenses:Acme:Supplies  42.17 USD'
    )
    assert _extract_sfid_from_block(block) == "TRN-X"


def test_extract_sfid_legacy_format_still_works():
    block = _block(
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-LEGACY"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD'
    )
    assert _extract_sfid_from_block(block) == "TRN-LEGACY"


def test_extract_sfid_returns_none_when_no_simplefin_provenance():
    block = _block(
        '2026-04-15 * "Manual Entry" ""\n'
        '  lamella-txn-id: "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"\n'
        '  Liabilities:Acme:Card  -10.00 USD\n'
        '  Expenses:Acme:Misc  10.00 USD'
    )
    assert _extract_sfid_from_block(block) is None


def test_extract_sfid_ignores_csv_only_source_pair():
    """A posting carrying only ``csv`` provenance shouldn't surface as
    a SimpleFIN id — the cleaner targets SimpleFIN duplicates
    specifically."""
    block = _block(
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-txn-id: "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '    lamella-source-0: "csv"\n'
        '    lamella-source-reference-id-0: "CSV-99"\n'
        '  Expenses:Acme:Supplies  42.17 USD'
    )
    assert _extract_sfid_from_block(block) is None


# ---------------------------------------------------------------------------
# Aliases insertion — new format
# ---------------------------------------------------------------------------

def test_aliases_inject_anchors_under_lineage_when_no_legacy_sfid():
    """New-format entry: no `lamella-simplefin-id` line to anchor
    against, so aliases land right under `lamella-txn-id`."""
    block = _block(
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-txn-id: "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '    lamella-source-0: "simplefin"\n'
        '    lamella-source-reference-id-0: "TRN-PRIMARY"\n'
        '  Expenses:Acme:Supplies  42.17 USD'
    )
    out = _inject_aliases_into_block(block, {"TRN-DUP-1", "TRN-DUP-2"})
    body = "".join(out)
    assert 'lamella-simplefin-aliases: "TRN-DUP-1 TRN-DUP-2"' in body
    # Anchored under the lineage line, not the posting source pair.
    txn_id_line_idx = next(
        i for i, ln in enumerate(out) if "lamella-txn-id" in ln
    )
    aliases_line_idx = next(
        i for i, ln in enumerate(out) if "lamella-simplefin-aliases" in ln
    )
    assert aliases_line_idx == txn_id_line_idx + 1


def test_aliases_inject_anchors_under_legacy_sfid_when_present():
    """Legacy on-disk entry: `lamella-simplefin-id` is the anchor."""
    block = _block(
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-PRIMARY"\n'
        '  lamella-txn-id: "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD'
    )
    out = _inject_aliases_into_block(block, {"TRN-DUP"})
    sfid_line_idx = next(
        i for i, ln in enumerate(out) if "lamella-simplefin-id" in ln
    )
    aliases_line_idx = next(
        i for i, ln in enumerate(out) if "lamella-simplefin-aliases" in ln
    )
    assert aliases_line_idx == sfid_line_idx + 1


def test_aliases_inject_idempotent_on_existing_aliases_line():
    """Running the inject helper twice should not duplicate aliases —
    it merges + dedupes + sorts."""
    block = _block(
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-txn-id: "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"\n'
        '  lamella-simplefin-aliases: "TRN-A TRN-B"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '    lamella-source-0: "simplefin"\n'
        '    lamella-source-reference-id-0: "TRN-PRIMARY"\n'
        '  Expenses:Acme:Supplies  42.17 USD'
    )
    out = _inject_aliases_into_block(block, {"TRN-A", "TRN-C"})
    body = "".join(out)
    # Existing TRN-A + TRN-B kept, new TRN-C added, sorted, deduped.
    assert 'lamella-simplefin-aliases: "TRN-A TRN-B TRN-C"' in body
    # Only one aliases line in the block.
    aliases_count = sum(
        1 for ln in out if "lamella-simplefin-aliases" in ln
    )
    assert aliases_count == 1


def test_aliases_insertion_index_falls_back_to_header():
    """Pre-Lamella content with no lineage and no SFID line — the
    aliases land right under the date header."""
    block = _block(
        '2026-04-15 * "Hand-edited" ""\n'
        '  Liabilities:Acme:Card  -10.00 USD\n'
        '  Expenses:Acme:Misc  10.00 USD'
    )
    insert_after, _indent = _aliases_insertion_index(block)
    assert insert_after == 0
