# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Static-asset guards for the classify popover + account picker.

Upstream lamella#10: the account-picker suggest popup shipped with no
CSS at all, so it rendered in-flow and overflowed the classify
popover. These tests pin the contracts that made that bug silent:
every class the popup partial emits must be styled, and the popover
width hardcoded in txnact_popover.js must match the CSS.
"""
from __future__ import annotations

import re
from pathlib import Path

STATIC = Path(__file__).parent.parent / "src" / "lamella" / "web" / "static"


def test_account_picker_popup_classes_are_styled():
    css = (STATIC / "components.css").read_text()
    # Classes emitted by partials/_account_picker_popup.html and the
    # account_picker macro. An unstyled popup is the lamella#10 bug.
    for cls in (
        ".acct-picker-popup",
        ".acct-picker-list",
        ".acct-picker-row",
        ".acct-picker-row__path",
        ".acct-picker-empty",
        ".acct-picker-filter-strip",
        ".acct-picker-clear-filter",
        ".acct-picker-more",
        ".acct-picker-prefill-badge",
    ):
        assert cls in css, f"{cls} has no CSS rule in components.css"
    # The popup must be out-of-flow — in-flow growth is what pushed
    # content through the popover's edge.
    popup_rule = css.split(".acct-picker-popup {", 1)[1].split("}", 1)[0]
    assert "position: absolute" in popup_rule
    # account_picker.js flips the popup above the input near the
    # viewport bottom; the attribute needs a matching rule.
    assert '.acct-picker-popup[data-flip="up"]' in css


def test_txnact_popover_width_synced_between_css_and_js():
    css = (STATIC / "components.css").read_text()
    js = (STATIC / "txnact_popover.js").read_text()
    css_m = re.search(
        r"\.txnact__popover\s*{[^}]*width:\s*min\((\d+)rem", css,
    )
    js_m = re.search(r"POPOVER_W\s*=\s*(\d+)\s*\*\s*16", js)
    assert css_m, "couldn't find .txnact__popover width in components.css"
    assert js_m, "couldn't find POPOVER_W in txnact_popover.js"
    assert css_m.group(1) == js_m.group(1), (
        "txnact popover width drifted: CSS says "
        f"{css_m.group(1)}rem, JS positions with {js_m.group(1)}rem"
    )
