# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
from importlib import import_module
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

log = logging.getLogger(__name__)


PDF_TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


_env: Environment | None = None


def template_env() -> Environment:
    """Lazy Jinja env for PDF templates. Kept separate from the FastAPI app
    env so the PDF templates can use {% extends %} chains without colliding
    with the UI templates."""
    global _env
    if _env is None:
        _env = Environment(
            loader=FileSystemLoader(str(PDF_TEMPLATE_DIR)),
            autoescape=select_autoescape(["html", "xml"]),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        _env.filters["money"] = _money
        _env.filters["money_signed"] = _money_signed
    return _env


def _money(value) -> str:
    try:
        from decimal import Decimal

        return f"{Decimal(value):,.2f}"
    except Exception:  # noqa: BLE001
        return str(value)


def _money_signed(value) -> str:
    from decimal import Decimal

    d = Decimal(value)
    sign = "-" if d < 0 else ""
    return f"{sign}{abs(d):,.2f}"


def render_html(template_name: str, **context: Any) -> str:
    return template_env().get_template(template_name).render(**context)


class PDFRenderingUnavailable(RuntimeError):
    """Raised when WeasyPrint cannot import or render. The route layer
    catches this and returns a 503 with a clear message rather than 500."""


def render_pdf(html: str, *, base_url: Path | None = None) -> bytes:
    """Render ``html`` to PDF bytes via WeasyPrint. WeasyPrint is imported
    lazily so that environments without the native libs can still import
    this module (and the route returns a clean 503 when called)."""
    try:
        weasyprint = import_module("weasyprint")
    except OSError as exc:  # native libs missing
        raise PDFRenderingUnavailable(
            f"WeasyPrint native libs unavailable: {exc}"
        ) from exc
    except Exception as exc:  # noqa: BLE001 — surface anything else
        raise PDFRenderingUnavailable(f"WeasyPrint import failed: {exc}") from exc
    base = str(base_url) if base_url is not None else str(PDF_TEMPLATE_DIR)
    try:
        doc = weasyprint.HTML(string=html, base_url=base)
        return doc.write_pdf()
    except OSError as exc:
        raise PDFRenderingUnavailable(
            f"WeasyPrint render failed (missing native lib): {exc}"
        ) from exc
