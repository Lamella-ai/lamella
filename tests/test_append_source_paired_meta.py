# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0058 confirm-as-dup writer + ADR-0019 paired meta append.

When the user clicks "Confirm — same event" on /review/duplicates
for a ledger-side match, the new source's
``lamella-source-N`` / ``lamella-source-reference-id-N`` (and
optionally ``lamella-source-description-N``) triplet is appended to
the matched ledger entry's bank-side posting at the next free index.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.features.bank_sync.synthetic_replace import (
    append_source_paired_meta_in_place,
)


def _write(tmp: Path, content: str) -> Path:
    p = tmp / "main.bean"
    p.write_text(content, encoding="utf-8")
    return p


def test_append_to_posting_with_existing_pair_uses_next_index(tmp_path):
    """An existing source-0 pair should remain; the new source lands
    at -1."""
    bean = _write(
        tmp_path,
        '2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-A"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '    lamella-source-0: "simplefin"\n'
        '    lamella-source-reference-id-0: "TRN-AAA"\n'
        '  Expenses:Food      12.50 USD\n',
    )
    ok = append_source_paired_meta_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-EVENT-A",
        posting_account="Liabilities:Card",
        source="csv",
        source_reference_id="ROW-42",
        source_description="Coffee Shop — Decaf",
    )
    assert ok is True
    text = bean.read_text(encoding="utf-8")
    # Existing pair untouched.
    assert 'lamella-source-0: "simplefin"' in text
    assert 'lamella-source-reference-id-0: "TRN-AAA"' in text
    # New triplet at index 1.
    assert 'lamella-source-1: "csv"' in text
    assert 'lamella-source-reference-id-1: "ROW-42"' in text
    assert (
        'lamella-source-description-1: "Coffee Shop — Decaf"'
    ) in text


def test_append_when_no_prior_source_meta_uses_index_0(tmp_path):
    """When the matched posting has no source pair yet (e.g.
    user-typed ledger entry pre-Lamella), the first observation
    lands at index 0."""
    bean = _write(
        tmp_path,
        '2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-B"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '  Expenses:Food      12.50 USD\n',
    )
    ok = append_source_paired_meta_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-EVENT-B",
        posting_account="Liabilities:Card",
        source="simplefin",
        source_reference_id="TRN-FIRST",
    )
    assert ok is True
    text = bean.read_text(encoding="utf-8")
    assert 'lamella-source-0: "simplefin"' in text
    assert 'lamella-source-reference-id-0: "TRN-FIRST"' in text


def test_append_skips_description_when_not_supplied(tmp_path):
    """ADR-0059 — description is optional. When the staging row has
    no description text, no -description-N line appears."""
    bean = _write(
        tmp_path,
        '2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-C"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '    lamella-source-0: "simplefin"\n'
        '    lamella-source-reference-id-0: "TRN-AAA"\n'
        '  Expenses:Food      12.50 USD\n',
    )
    append_source_paired_meta_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-EVENT-C",
        posting_account="Liabilities:Card",
        source="csv",
        source_reference_id="ROW-42",
    )
    text = bean.read_text(encoding="utf-8")
    assert 'lamella-source-description-1' not in text


def test_append_finds_block_amongst_many_transactions(tmp_path):
    """The walker scans for the matching txn-id, not the first
    transaction in the file."""
    bean = _write(
        tmp_path,
        '2026-04-10 * "Other" "Other event"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-OTHER-AAA"\n'
        '  Assets:Bank      -5.00 USD\n'
        '  Expenses:Food     5.00 USD\n'
        '\n'
        '2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-D"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '  Expenses:Food      12.50 USD\n',
    )
    ok = append_source_paired_meta_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-EVENT-D",
        posting_account="Liabilities:Card",
        source="paste",
        source_reference_id="PASTE-99",
    )
    assert ok is True
    text = bean.read_text(encoding="utf-8")
    assert 'lamella-source-0: "paste"' in text
    # Other txn untouched.
    other_block_start = text.find("OTHER-AAA")
    other_block_end = text.find("EVENT-D")
    other_slice = text[other_block_start:other_block_end]
    assert "lamella-source-" not in other_slice


def test_append_returns_false_when_txn_id_not_found(tmp_path):
    bean = _write(
        tmp_path,
        '2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-E"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '  Expenses:Food      12.50 USD\n',
    )
    pre = bean.read_text(encoding="utf-8")
    ok = append_source_paired_meta_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-DOES-NOT-EXIST",
        posting_account="Liabilities:Card",
        source="csv",
        source_reference_id="ROW-1",
    )
    assert ok is False
    assert bean.read_text(encoding="utf-8") == pre


def test_append_returns_false_when_posting_account_missing(tmp_path):
    """The txn-id matches but no posting carries the requested
    account — defensive no-op rather than write-the-wrong-leg."""
    bean = _write(
        tmp_path,
        '2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-F"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '  Expenses:Food      12.50 USD\n',
    )
    pre = bean.read_text(encoding="utf-8")
    ok = append_source_paired_meta_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-EVENT-F",
        posting_account="Assets:NonExistent",
        source="csv",
        source_reference_id="ROW-1",
    )
    assert ok is False
    assert bean.read_text(encoding="utf-8") == pre


def test_rewrite_narration_in_place_replaces_header_narration(tmp_path):
    """ADR-0059 — the txn-level narration is rewritable in place,
    keyed on lamella-txn-id. Adds the synthesized marker the first
    time it's invoked."""
    from lamella.features.bank_sync.synthetic_replace import (
        rewrite_narration_in_place,
    )
    bean = _write(
        tmp_path,
        '2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-N"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '  Expenses:Food      12.50 USD\n',
    )
    ok = rewrite_narration_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-EVENT-N",
        new_narration="Coffee Shop — Decaf and a scone",
    )
    assert ok is True
    text = bean.read_text(encoding="utf-8")
    # Old narration replaced.
    assert '"Decaf"\n' not in text
    assert '"Coffee Shop — Decaf and a scone"' in text
    # Payee preserved (the rewrite only touches the narration
    # string, not the optional payee that precedes it).
    assert '"Coffee Shop"' in text
    # Marker added.
    assert "lamella-narration-synthesized: TRUE" in text


def test_rewrite_narration_idempotent_marker(tmp_path):
    """A second rewrite must not duplicate the synthesized marker
    even when it changes the narration text again."""
    from lamella.features.bank_sync.synthetic_replace import (
        rewrite_narration_in_place,
    )
    bean = _write(
        tmp_path,
        '2026-04-15 * "Coffee Shop" "v1"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-N2"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '  Expenses:Food      12.50 USD\n',
    )
    rewrite_narration_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-EVENT-N2",
        new_narration="v2",
    )
    rewrite_narration_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-EVENT-N2",
        new_narration="v3",
    )
    text = bean.read_text(encoding="utf-8")
    assert text.count("lamella-narration-synthesized: TRUE") == 1
    assert '"v3"' in text


def test_rewrite_narration_returns_false_for_unknown_txn_id(tmp_path):
    from lamella.features.bank_sync.synthetic_replace import (
        rewrite_narration_in_place,
    )
    bean = _write(
        tmp_path,
        '2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-N3"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '  Expenses:Food      12.50 USD\n',
    )
    pre = bean.read_text(encoding="utf-8")
    ok = rewrite_narration_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-DOES-NOT-EXIST",
        new_narration="anything",
    )
    assert ok is False
    assert bean.read_text(encoding="utf-8") == pre


def test_rewrite_narration_refuses_multiline_input(tmp_path):
    """Beancount narrations are single-line; the writer refuses to
    embed a newline (which would produce malformed bean output)."""
    from lamella.features.bank_sync.synthetic_replace import (
        rewrite_narration_in_place,
    )
    bean = _write(
        tmp_path,
        '2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-N4"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '  Expenses:Food      12.50 USD\n',
    )
    pre = bean.read_text(encoding="utf-8")
    ok = rewrite_narration_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-EVENT-N4",
        new_narration="line one\nline two",
    )
    assert ok is False
    assert bean.read_text(encoding="utf-8") == pre


def test_quotes_and_backslashes_escaped_in_appended_values(tmp_path):
    bean = _write(
        tmp_path,
        '2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-G"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '  Expenses:Food      12.50 USD\n',
    )
    append_source_paired_meta_in_place(
        bean_file=bean,
        lamella_txn_id="0190f000-0000-7000-8000-EVENT-G",
        posting_account="Liabilities:Card",
        source="csv",
        source_reference_id='id-with-"quote"',
        source_description='Joe\'s "Coffee" \\Co.',
    )
    text = bean.read_text(encoding="utf-8")
    assert (
        'lamella-source-reference-id-0: "id-with-\\"quote\\""'
    ) in text
    assert (
        'lamella-source-description-0: '
        '"Joe\'s \\"Coffee\\" \\\\Co."'
    ) in text
