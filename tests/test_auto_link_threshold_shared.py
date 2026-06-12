# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0063 §2: AUTO_LINK_THRESHOLD must be a single shared constant.

If anyone redefines the threshold in another module, the forward
and reverse auto-link paths can diverge — the failure mode this
ADR was written to prevent. This test guards that.
"""
from __future__ import annotations

import re
from pathlib import Path

import lamella.features.receipts.auto_match as auto_match_mod
import lamella.features.receipts.scorer as scorer_mod
from lamella.features.receipts.scorer import (
    AUTO_LINK_THRESHOLD,
    REVIEW_THRESHOLD,
    Scorer,
)


def test_threshold_value_is_090():
    assert AUTO_LINK_THRESHOLD == 0.90


def test_review_threshold_value_is_060():
    assert REVIEW_THRESHOLD == 0.60


def test_scorer_class_attribute_matches_module_constant():
    """Test code asserts on Scorer.AUTO_LINK_THRESHOLD as the single
    public surface; the class constant MUST be the same object as
    the module-level constant."""
    assert Scorer.AUTO_LINK_THRESHOLD == AUTO_LINK_THRESHOLD
    assert Scorer.REVIEW_THRESHOLD == REVIEW_THRESHOLD


def test_auto_match_re_exports_same_constant():
    """The forward sweep imports AUTO_LINK_THRESHOLD as its default
    threshold; it must be the same numeric value as the scorer's
    constant. (It re-exports via assignment so they're equal but
    distinct module attributes.)"""
    assert auto_match_mod.AUTO_LINK_THRESHOLD == AUTO_LINK_THRESHOLD
    assert auto_match_mod.REVIEW_THRESHOLD == REVIEW_THRESHOLD


def test_no_other_module_redefines_threshold_constant():
    """Grep test: scan every .py file under src/ for a line that
    looks like `AUTO_LINK_THRESHOLD = <number>`. The only legitimate
    definition is inside scorer.py — every other module must import
    the constant rather than redefine it.

    auto_match.py is allowed because it re-exports via
    ``AUTO_LINK_THRESHOLD = _SCORER_AUTO_LINK_THRESHOLD`` (it carries
    the imported name on the RHS, not a numeric literal).
    """
    src_root = Path(__file__).resolve().parent.parent / "src" / "lamella"
    bad: list[str] = []
    # Match `AUTO_LINK_THRESHOLD = 0.90` style assignments where the
    # RHS is a numeric literal — re-exports like
    # ``AUTO_LINK_THRESHOLD = _SCORER_AUTO_LINK_THRESHOLD`` are
    # allowed because they're not duplicating the value.
    pat = re.compile(
        r"^AUTO_LINK_THRESHOLD\s*[:=].*?(\d+(?:\.\d+)?)",
    )
    for py in src_root.rglob("*.py"):
        # Skip the legitimate definition site.
        if py.name == "scorer.py":
            continue
        try:
            text = py.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for line_num, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            m = pat.match(stripped)
            if m is not None:
                bad.append(f"{py}:{line_num}: {stripped}")
    assert bad == [], (
        "AUTO_LINK_THRESHOLD must only be defined in scorer.py — "
        "found duplicate definitions:\n  " + "\n  ".join(bad)
    )


def test_forward_sweep_default_threshold_is_shared_constant():
    """sweep_recent's min_score default uses the shared constant."""
    import inspect

    from lamella.features.receipts.auto_match import sweep_recent
    sig = inspect.signature(sweep_recent)
    assert sig.parameters["min_score"].default == AUTO_LINK_THRESHOLD


def test_reverse_sweep_uses_same_threshold():
    """auto_link_unlinked_documents references AUTO_LINK_THRESHOLD
    directly (not a copy); changing the scorer's threshold updates
    the reverse direction without further edits."""
    from lamella.features.receipts import auto_match
    src = Path(auto_match.__file__).read_text()
    # The reverse function compares against AUTO_LINK_THRESHOLD by
    # name (not a hardcoded 0.90 numeric literal).
    assert "if top.score < AUTO_LINK_THRESHOLD" in src, (
        "auto_link_unlinked_documents must compare against the named "
        "AUTO_LINK_THRESHOLD constant, not a numeric literal"
    )
