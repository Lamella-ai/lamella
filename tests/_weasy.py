# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Test helper: import weasyprint, skip the test if either the import
itself or a smoke render fails (which happens on dev boxes that don't
have GTK/pango/cairo). The Linux Docker runtime ships these, so CI
should always exercise the real path."""

from __future__ import annotations

import pytest


def require_weasyprint():
    try:
        import weasyprint  # noqa: F401
    except (ImportError, OSError) as exc:
        pytest.skip(f"WeasyPrint unavailable: {exc}")
    # Smoke render to catch missing native libs that import succeeds against
    # but render fails for.
    try:
        weasyprint.HTML(string="<p>x</p>").write_pdf()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"WeasyPrint render unavailable: {exc}")
