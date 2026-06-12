# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""HTMX response conventions — single source of truth for the
partial-on-HX-Request contract used across the app's route layer.

Why this module exists
----------------------

The project ships a ~500-line custom HTMX-compatible shim
(``static/htmx.min.js``) that implements only a focused subset of
htmx.org's feature surface. In particular it does **not** support
``hx-select``, and its ``hx-swap`` mode check is exact-string match —
``"outerHTML show:window:top"`` falls through to the default
``innerHTML`` branch. Both quirks share the same failure mode: an
HTMX-driven request that hits an endpoint returning the *full page*
(a Jinja template extending ``base.html``) ends up with the entire
sidebar / topbar / footer swapped into the target element. Users
see the layout nested inside itself.

The fix is uniform: every endpoint that's a possible HTMX swap target
must return a *partial* (a fragment that does NOT extend
``base.html``) when the ``HX-Request`` header is set, and the regular
full-page template otherwise. The helpers here codify that contract
so callers don't have to re-derive it.

Convention
----------

For an endpoint that's targeted by ``hx-post`` / ``hx-get`` /
``hx-delete`` from a template:

1. **Read endpoints** (``GET``) that drive a swap should call
   :func:`render` with both a ``full`` and a ``partial`` template.
   The partial must be self-contained markup wrapped in the element
   that ``hx-target`` selects (e.g. ``<div id="staged-list">…</div>``).
   The full template includes the partial inside its ``{% block
   content %}`` so the markup is identical on either path.

2. **Write endpoints** (``POST`` / ``DELETE``) follow the action with
   either:

   * :func:`redirect` — the standard "go look at the list now" outcome.
     For HTMX it returns a ``204`` plus ``HX-Redirect`` header (the
     shim does a full client-side nav, no swap); for vanilla form
     submits it returns a regular ``303``. Either way the user's
     filter/error context is preserved through the redirect URL.
   * :func:`empty` — the row vanished, return nothing so the form's
     ``outerHTML`` swap leaves a hole. Pair with a ``302`` for vanilla
     form submits if you want them redirected to a list page.
   * :func:`error_fragment` — the action failed and you want a small
     inline error to render inside the row's slot. Use this rather
     than redirecting on error: a 30x error redirect sent to an HTMX
     target re-triggers the nesting bug.

3. **Don't** use ``hx-select`` on the calling template — the shim
   ignores it. If you find yourself reaching for it, the right move
   is to make the response *be* the partial in the first place.

4. **Don't** put modifiers on ``hx-swap`` (e.g. ``show:``, ``scroll:``,
   ``settle:``). The shim's mode check is exact-string and silently
   falls back to ``innerHTML``. Just use the bare mode.

See ``src/lamella/routes/staging_review.py`` and ``card.py`` for the
two reference implementations that shaped these helpers.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import quote

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response


__all__ = [
    "is_htmx",
    "render",
    "redirect",
    "empty",
    "error_fragment",
]


def is_htmx(request: Request) -> bool:
    """``True`` when the request originated from an HTMX swap.

    Reads the ``HX-Request`` header (case-insensitive value match
    against ``"true"``) — the shim sends this on every swap-driving
    fetch, and the header survives same-origin 30x redirects so the
    flag stays accurate across the full POST→303→GET chain."""
    return request.headers.get("hx-request", "").lower() == "true"


def render(
    request: Request,
    *,
    full: str,
    partial: str,
    context: dict[str, Any] | None = None,
):
    """Render ``partial`` for HTMX swaps, ``full`` otherwise.

    Both templates receive the same context dict, so the partial
    is just an ``{% include %}`` away from being reused inside the
    full template. The full template extends ``base.html``; the
    partial is the swappable wrapper element with no page chrome.

    Caller responsibility: ensure the partial's outermost element
    matches the ``hx-target`` on the calling template, since
    ``hx-swap="outerHTML"`` replaces that element wholesale."""
    templates = request.app.state.templates
    template = partial if is_htmx(request) else full
    return templates.TemplateResponse(
        request, template, context or {},
    )


def redirect(
    request: Request,
    url: str,
    *,
    error: str | None = None,
    message: str | None = None,
    status_code: int = 303,
):
    """Build an HTMX-safe redirect.

    For HTMX, returns ``204 No Content`` with ``HX-Redirect`` set so
    the shim does a client-side ``window.location.assign(...)``. For
    vanilla form submits returns a regular 30x redirect.

    Either path appends ``?error=…`` / ``?message=…`` to the target
    URL when given. Pre-existing query strings are preserved (we
    pick the right separator)."""
    target = url
    qs_parts: list[str] = []
    if error:
        qs_parts.append(f"error={quote(error)}")
    if message:
        qs_parts.append(f"message={quote(message)}")
    if qs_parts:
        sep = "&" if "?" in target else "?"
        target = f"{target}{sep}{'&'.join(qs_parts)}"

    if is_htmx(request):
        return Response(
            status_code=204,
            headers={"HX-Redirect": target},
        )
    return RedirectResponse(target, status_code=status_code)


def empty() -> HTMLResponse:
    """Return a 200 with an empty body.

    Used when the user's action removed the row and the form's
    ``outerHTML`` swap should leave nothing in its place. Equivalent
    to telling HTMX "swap in the empty string"."""
    return HTMLResponse("")


def error_fragment(
    html: str,
    *,
    status_code: int = 400,
) -> HTMLResponse:
    """Return a small inline error fragment for HTMX to swap.

    Use this in place of a redirect-on-error from an action endpoint
    that's targeted by an HTMX form: the redirect chain otherwise
    fetches the full list page and outerHTML-swaps it into the row,
    nesting the layout. The fragment should preserve the row's
    ``id``/``class`` so subsequent swaps from sibling actions still
    locate their target."""
    return HTMLResponse(html, status_code=status_code)
