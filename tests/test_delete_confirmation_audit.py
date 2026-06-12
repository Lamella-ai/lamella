# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Template audit: every destructive form gets a confirmation modal.

Upstream lamella#4 found a hard-delete that fired with no working
confirmation. This sweep enforces the rule going forward: any <form>
whose action looks destructive (delete / remove / purge / revoke /
destroy) must carry the site-wide confirm-modal contract —
``data-confirm`` on the form, ``hx-confirm`` (routed through the same
modal by the htmx shim), or a ``data-confirm`` submit button inside
the form (what B.btn's ``confirm=`` renders; read via ev.submitter).

Native browser dialogs (onsubmit="return confirm(...)") are also
rejected — they bypass the modal styling, the custom button label,
and the type-to-confirm strong mode.
"""
from __future__ import annotations

import re
from pathlib import Path

TEMPLATES = Path(__file__).parent.parent / "src" / "lamella" / "web" / "templates"

# Form actions that are destructive enough to require a confirm.
DESTRUCTIVE_ACTION = re.compile(r"(delete|remove|purge|revoke|destroy|clear)", re.I)

# Deliberate exceptions — actions that pattern-match but aren't
# destructive. Each entry needs a justification.
ALLOWLIST = {
    # "No, classify normally" choice button — clears a UI hint marker,
    # part of an explicit two-option prompt that IS the confirmation.
    "/review/staged/{{ item.staged_id }}/clear-synthetic-marker",
}

FORM_RE = re.compile(r"<form\b[^>]*>", re.S)
ACTION_RE = re.compile(r'action="([^"]*)"')


def _destructive_forms():
    for path in sorted(TEMPLATES.rglob("*.html")):
        text = path.read_text()
        for m in FORM_RE.finditer(text):
            tag = m.group(0)
            am = ACTION_RE.search(tag)
            if not am:
                continue
            action = am.group(1)
            if not DESTRUCTIVE_ACTION.search(action):
                continue
            if action in ALLOWLIST:
                continue
            end = text.find("</form>", m.end())
            body = text[m.end() : end] if end != -1 else ""
            line = text[: m.start()].count("\n") + 1
            yield path.relative_to(TEMPLATES), line, action, tag, body


def test_every_destructive_form_has_a_confirm():
    missing = []
    for rel, line, action, tag, body in _destructive_forms():
        ok = (
            "data-confirm" in tag
            or "hx-confirm" in tag
            or "data-confirm" in body  # rendered by B.btn(confirm=...)
            or "confirm=" in body      # macro arg, pre-render
        )
        if not ok:
            missing.append(f"{rel}:{line} action={action}")
    assert not missing, (
        "destructive form(s) without a confirmation modal "
        "(add data-confirm to the form, or confirm= on the submit "
        "button):\n  " + "\n  ".join(missing)
    )


def test_no_native_confirm_dialogs():
    """onsubmit/onclick window.confirm bypasses the site modal."""
    offenders = []
    for path in sorted(TEMPLATES.rglob("*.html")):
        for i, line in enumerate(path.read_text().splitlines(), start=1):
            if re.search(r"on(submit|click)\s*=\s*\"[^\"]*\bconfirm\(", line):
                offenders.append(f"{path.relative_to(TEMPLATES)}:{i}")
    assert not offenders, (
        "native confirm() dialog(s) — use data-confirm so the "
        "site-wide modal handles it:\n  " + "\n  ".join(offenders)
    )
