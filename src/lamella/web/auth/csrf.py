# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""CSRF protection — double-submit cookie pattern, ADR-0050.

A CSRF token is generated per session and exposed two ways on every
request:

  * As a cookie (so a same-origin browser can read it via JavaScript
    if needed for HTMX cross-cuts).
  * As `request.state.csrf` for templates and as a response header
    `X-CSRF-Token` for HTMX responses.

State-changing requests (POST / PUT / DELETE / PATCH) must include the
token either as a hidden form input named `_csrf` or as the
`X-CSRF-Token` request header. The submitted value is constant-time
compared against the session row's stored token. GET requests bypass.

The login route bypasses CSRF (no session yet); it relies on
SameSite=Lax cookie semantics + the credential check itself.
"""

from __future__ import annotations

from fastapi import Request

from lamella.web.auth.passwords import constant_time_compare


HEADER_NAME = "X-CSRF-Token"
FORM_FIELD_NAME = "_csrf"

# Routes that bypass CSRF even when authenticated. Login is intrinsic
# (no session). Logout is NOT here — destructive routes always require
# CSRF.
BYPASS_PATHS = frozenset({"/login"})


def is_safe_method(method: str) -> bool:
    return method.upper() in ("GET", "HEAD", "OPTIONS")


async def extract_submitted_token(request: Request) -> str:
    """Pulls the token from header first, then from form body.

    Critical: when we fall through to reading the form, we must call
    ``await request.body()`` first so Starlette caches the raw body
    on the Request object. Without that, ``await request.form()``
    here consumes the ASGI receive stream — and the downstream
    FastAPI handler's ``Form(...)`` parsing sees an empty body and
    raises 422 "field required" for every form field. Caching via
    body() is idempotent (the cached value is reused on every
    subsequent .form() / .body() / .json() call), so this is the
    safe shape.
    """
    header = request.headers.get(HEADER_NAME, "")
    if header:
        return header
    ct = request.headers.get("content-type", "")
    if "application/x-www-form-urlencoded" in ct or "multipart/form-data" in ct:
        try:
            await request.body()  # caches request._body
            form = await request.form()
        except Exception:
            return ""
        value = form.get(FORM_FIELD_NAME, "")
        if isinstance(value, str):
            return value
    return ""


def check_token(submitted: str, expected: str) -> bool:
    if not submitted or not expected:
        return False
    return constant_time_compare(submitted, expected)
