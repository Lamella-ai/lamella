# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from lamella.core.transform.key_rename import (
    META_KEY_RENAMES,
    TAG_RENAMES,
    rewrite_text,
)


def test_rewrites_known_metadata_keys():
    src = (
        '2026-04-15 * "ACME" "thing"\n'
        '  simplefin-id: "sf-1"\n'
        '  ai-classified: TRUE\n'
        '  memo: "lunch"\n'
        '  Liabilities:X  -10.00 USD\n'
        '  Expenses:Y  10.00 USD\n'
    )
    out, edits = rewrite_text(src)
    assert 'lamella-simplefin-id: "sf-1"' in out
    assert 'lamella-ai-classified: TRUE' in out
    assert 'lamella-import-memo: "lunch"' in out
    # Original un-prefixed keys gone.
    assert 'simplefin-id:' not in out.replace('lamella-simplefin-id', '')
    assert edits == 3


def test_rewrites_tags():
    src = '2026-04-15 * "Fix" "narration" #connector-override\n'
    out, edits = rewrite_text(src)
    assert '#lamella-override' in out
    assert '#connector-override' not in out
    assert edits == 1


def test_idempotent_on_already_renamed_content():
    src = (
        '2026-04-15 * "ACME" "thing" #lamella-override\n'
        '  lamella-simplefin-id: "sf-1"\n'
        '  lamella-ai-classified: TRUE\n'
    )
    out, edits = rewrite_text(src)
    assert out == src
    assert edits == 0


def test_preserves_indentation_and_surrounding_lines():
    src = (
        '; a comment we should not touch\n'
        '2026-04-15 * "payee" "narr"\n'
        '    override-of: "deadbeef"\n'
        '    Assets:A  1.00 USD\n'
        '    Expenses:B  -1.00 USD\n'
    )
    out, _ = rewrite_text(src)
    assert '    lamella-override-of: "deadbeef"\n' in out
    assert '; a comment we should not touch\n' in out
    assert 'Assets:A  1.00 USD' in out


def test_does_not_rewrite_unknown_keys():
    src = '  paypal-txn-id: "pp-7"\n  historical-source: "x"\n'
    out, edits = rewrite_text(src)
    assert out == src
    assert edits == 0


def test_tag_boundary_does_not_overreach():
    # A tag whose name extends beyond a known tag prefix must not be
    # shortened mid-word. `#connector-override-v2` (fictional) stays put.
    src = '2026-04-15 * "x" "y" #connector-override-v2\n'
    out, _ = rewrite_text(src)
    assert '#connector-override-v2' in out
    assert '#lamella-override-v2' not in out


def test_mapping_is_complete_for_expected_generic_keys():
    # Guards against dropping entries in META_KEY_RENAMES by accident.
    for required in (
        "memo", "txn-id", "simplefin-id", "override-of",
        "vehicle", "entity", "miles",
        "paperless-id", "ai-classified",
    ):
        assert required in META_KEY_RENAMES


def test_all_bcg_targets_are_prefixed():
    for new in META_KEY_RENAMES.values():
        assert new.startswith("lamella-")
    for new in TAG_RENAMES.values():
        assert new.startswith("#lamella-")
