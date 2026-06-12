# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Auth + security middleware — ADR-0050.

Single ASGI middleware that:

  1. Resolves request.state.user / request.state.tenant from a session
     cookie (or injects ANONYMOUS_OWNER when auth is disabled).
  2. Enforces the bypass list and HTMX-aware unauth redirects.
  3. Validates CSRF tokens on state-changing requests.
  4. Adds security response headers everywhere.

It runs BEFORE the existing setup_gate middleware in create_app so an
unauthenticated request never reaches a setup-detection branch and
never sees ledger state on its way to /login.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import Iterable
from urllib.parse import quote, urlencode

from fastapi import FastAPI
from fastapi.responses import JSONResponse, RedirectResponse, Response

from lamella.web.auth import csrf as csrf_mod
from lamella.web.auth.events import (
    EVENT_CSRF_REJECTED,
    record_event,
)
from lamella.web.auth.sessions import (
    COOKIE_INSECURE,
    COOKIE_SECURE,
    CSRF_COOKIE_INSECURE,
    CSRF_COOKIE_SECURE,
    load_session,
    touch_session,
    unsign_session_id,
)
from lamella.web.auth.user import (
    ANONYMOUS_OWNER,
    ANONYMOUS_TENANT,
    Tenant,
    User,
)


log = logging.getLogger("lamella.auth.middleware")


# Always-allowed paths — even when auth is on, these never require a
# session. The login routes are intrinsic; static + healthz are
# operational. See ADR-0050 §12.
BYPASS_PREFIXES: tuple[str, ...] = ("/static",)
BYPASS_PATHS: frozenset[str] = frozenset({
    "/healthz",
    "/readyz",
    "/login",
    "/favicon.ico",
})


def _is_bypass(path: str) -> bool:
    if path in BYPASS_PATHS:
        return True
    for p in BYPASS_PREFIXES:
        if path == p or path.startswith(p + "/"):
            return True
    return False


def _is_https(request) -> bool:
    """Detect HTTPS via X-Forwarded-Proto first (proxy case), then fall
    back to the request's own scheme (direct-connection case).

    The middleware uses this to decide:
      - whether to set Secure-flagged cookies under their __Host- name
      - whether to emit Strict-Transport-Security
    """
    fwd = request.headers.get("x-forwarded-proto", "").lower()
    if fwd == "https":
        return True
    if fwd == "http":
        return False
    return (request.url.scheme or "").lower() == "https"


def _wants_html(request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept or "*/*" in accept or accept == ""


def _is_htmx(request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"


def _is_api_path(path: str) -> bool:
    """Routes under /api or any path that looks like an AJAX surface."""
    return path.startswith("/api/") or path == "/api"


def _unauth_response(request, login_url: str) -> Response:
    """Pick the right shape of unauth response per request type."""
    path = request.url.path
    if _is_htmx(request):
        # HTMX clients honor HX-Redirect by issuing a top-level
        # navigation; the body is irrelevant.
        resp = Response(status_code=200)
        resp.headers["HX-Redirect"] = login_url
        return resp
    if _is_api_path(path) or not _wants_html(request):
        return JSONResponse(
            {"detail": "authentication required"}, status_code=401
        )
    return RedirectResponse(login_url, status_code=302)


def _login_url_for(request) -> str:
    next_path = request.url.path
    if request.url.query:
        next_path = f"{next_path}?{request.url.query}"
    return f"/login?{urlencode({'next': next_path})}"


def _security_headers(response: Response, *, https: bool, path: str) -> None:
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault(
        "Referrer-Policy", "strict-origin-when-cross-origin"
    )
    if https:
        response.headers.setdefault(
            "Strict-Transport-Security",
            "max-age=31536000; includeSubDomains",
        )
    if path == "/login" or path.startswith("/account/"):
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; img-src 'self' data:; "
            "style-src 'self' 'unsafe-inline'; script-src 'self'; "
            "frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
        )


def install(app: FastAPI) -> None:
    """Mount the auth + security middleware on the app. Called from
    create_app() before any other middleware so unauthenticated
    requests never see route-level state."""

    @app.middleware("http")
    async def auth_security_middleware(request, call_next):
        path = request.url.path
        https = _is_https(request)

        # ----- Resolve user / tenant -----
        db: sqlite3.Connection | None = getattr(request.app.state, "db", None)
        settings = request.app.state.settings
        session_secret: str | None = getattr(
            request.app.state, "auth_session_secret", None
        )
        auth_required = (
            db is not None
            and session_secret is not None
            and _users_exist(db)
        )

        request.state.user = ANONYMOUS_OWNER
        request.state.tenant = ANONYMOUS_TENANT
        request.state.csrf = ""

        cookie_name = COOKIE_SECURE if https else COOKIE_INSECURE
        csrf_cookie_name = CSRF_COOKIE_SECURE if https else CSRF_COOKIE_INSECURE

        signed_cookie = request.cookies.get(cookie_name)
        active_session = None
        if signed_cookie and session_secret and db is not None:
            session_id = unsign_session_id(signed_cookie, session_secret)
            if session_id:
                active_session = load_session(db, session_id)

        if active_session is not None and db is not None:
            user_row = db.execute(
                "SELECT id, username, account_id, role FROM users WHERE id = ?",
                (active_session.user_id,),
            ).fetchone()
            if user_row is not None:
                request.state.user = User(
                    id=user_row["id"],
                    username=user_row["username"],
                    account_id=user_row["account_id"],
                    role=user_row["role"] or "owner",
                )
                tenant_row = db.execute(
                    "SELECT id, name FROM accounts WHERE id = ?",
                    (user_row["account_id"],),
                ).fetchone()
                if tenant_row is not None:
                    request.state.tenant = Tenant(
                        id=tenant_row["id"], name=tenant_row["name"]
                    )
                request.state.csrf = active_session.csrf_token
                touch_session(db, active_session.session_id)

        # ----- Enforce auth gate -----
        if auth_required and active_session is None and not _is_bypass(path):
            return _unauth_response(request, _login_url_for(request))

        # ----- Enforce CSRF on state-changing requests -----
        if (
            auth_required
            and active_session is not None
            and not csrf_mod.is_safe_method(request.method)
            and path not in csrf_mod.BYPASS_PATHS
        ):
            submitted = await csrf_mod.extract_submitted_token(request)
            if not csrf_mod.check_token(submitted, active_session.csrf_token):
                if db is not None:
                    record_event(
                        db,
                        event_type=EVENT_CSRF_REJECTED,
                        user_id=active_session.user_id,
                        account_id=active_session.account_id,
                        success=False,
                        request=request,
                        detail=f"path={path}",
                    )
                return JSONResponse(
                    {"detail": "CSRF token invalid or missing"},
                    status_code=403,
                )

        # ----- Pass through -----
        response = await call_next(request)

        # ----- Add security headers + refresh CSRF cookie -----
        _security_headers(response, https=https, path=path)
        if active_session is not None:
            # Refresh the CSRF cookie on every authenticated response so
            # forms rendered by HTMX swaps without a top-level page reload
            # still see a current value.
            cookie_kwargs = dict(
                key=csrf_cookie_name,
                value=active_session.csrf_token,
                max_age=settings.auth_session_days * 86400,
                httponly=False,  # readable by JS for HTMX header injection
                samesite="lax",
                secure=https,
                path="/",
            )
            response.set_cookie(**cookie_kwargs)
            response.headers.setdefault(
                "X-CSRF-Token", active_session.csrf_token
            )

        return response


def _users_exist(db: sqlite3.Connection) -> bool:
    """Cached-friendly check; auth becomes 'on' the moment the first
    user row exists. Bootstrapping inserts that row, so by the time
    a request lands the gate is already live."""
    try:
        row = db.execute("SELECT 1 FROM users LIMIT 1").fetchone()
    except sqlite3.Error:
        return False
    return row is not None


__all__: Iterable[str] = ("install",)


# Used by JSON detail responses; importing here keeps the call site flat.
def _json(payload: dict, status_code: int) -> JSONResponse:
    return JSONResponse(payload, status_code=status_code)


# Re-exposed for tests.
def __json_dumps(d: dict) -> str:
    return json.dumps(d, separators=(",", ":"))
