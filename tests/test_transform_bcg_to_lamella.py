# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the one-shot ``bcg-`` → ``lamella-`` rewrite transform."""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.transform.bcg_to_lamella import rewrite_text


def test_rewrites_meta_keys():
    src = (
        '2026-04-15 * "lunch"\n'
        '  bcg-simplefin-id: "sf-1"\n'
        '  bcg-ai-classified: TRUE\n'
        '  Assets:Bank   -10.00 USD\n'
        '    bcg-rule-id: "r-7"\n'
        '  Expenses:Food  10.00 USD\n'
    )
    out, n = rewrite_text(src)
    assert n == 3
    assert "lamella-simplefin-id" in out
    assert "lamella-ai-classified" in out
    assert "lamella-rule-id" in out
    assert "bcg-" not in out


def test_rewrites_tags():
    src = '2026-04-15 * "x" "y" #bcg-override #bcg-loan-funding #user-tag\n'
    out, n = rewrite_text(src)
    assert n == 2
    assert "#lamella-override" in out
    assert "#lamella-loan-funding" in out
    assert "#user-tag" in out
    assert "#bcg-" not in out


def test_rewrites_custom_directive_type():
    src = '2026-01-01 custom "bcg-ledger-version" "1"\n'
    out, n = rewrite_text(src)
    assert n == 1
    assert 'custom "lamella-ledger-version" "1"' in out


def test_idempotent_on_already_lamella():
    src = (
        '2026-01-01 custom "lamella-ledger-version" "1"\n'
        '2026-04-15 * "x" #lamella-override\n'
        '  lamella-simplefin-id: "sf-1"\n'
        '  Assets:Bank   1.00 USD\n'
    )
    out, n = rewrite_text(src)
    assert n == 0
    assert out == src


def test_does_not_touch_user_keys():
    """Bare metadata keys without the bcg- prefix (memo, simplefin-id,
    etc.) come from user-authored content and the rewrite must not
    touch them. The previous transform (key_rename.py) handles that
    pre-prefix migration; this one is bcg- → lamella- only."""
    src = (
        '2026-04-15 * "lunch"\n'
        '  simplefin-id: "user-sf"\n'
        '  memo: "from the bank export"\n'
        '  Assets:Bank   -10.00 USD\n'
        '  Expenses:Food  10.00 USD\n'
    )
    out, n = rewrite_text(src)
    assert n == 0
    assert out == src


def test_preserves_indentation_and_value_formatting():
    src = (
        '2026-04-15 * "lunch"\n'
        '       bcg-simplefin-id:        "sf-1"\n'
        '  Assets:Bank   -10.00 USD\n'
    )
    out, n = rewrite_text(src)
    assert n == 1
    assert '       lamella-simplefin-id:        "sf-1"' in out


def test_does_not_match_bcg_inside_other_words():
    """Substrings like ``some-bcg-word:`` shouldn't be rewritten —
    the regex anchors on whitespace before the ``bcg-`` prefix."""
    src = (
        '2026-04-15 * "lunch"\n'
        '  some-bcg-word: "value"\n'
        '  Assets:Bank   -10.00 USD\n'
    )
    out, n = rewrite_text(src)
    # Meta-key regex requires ``bcg-`` to follow the indent; the
    # ``some-`` prefix means no match.
    assert n == 0
    assert "some-bcg-word" in out
