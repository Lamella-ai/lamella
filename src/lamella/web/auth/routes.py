# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Auth routes — /login, /logout, /account/password — ADR-0050.

The login route bypasses CSRF (no session yet) and the auth gate
(it's the gate). Logout requires CSRF. Password change requires CSRF
and current-password re-verification, then revokes every other
session for the user (defense against the "did somebody change my
password while I was logged in" scenario).
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from lamella.core.config import Settings
from lamella.web.auth import csrf as csrf_mod
from lamella.web.auth.dependencies import current_user
from lamella.web.auth.events import (
    EVENT_LOGIN_FAILURE,
    EVENT_LOGIN_SUCCESS,
    EVENT_LOCKOUT,
    EVENT_LOGOUT,
    EVENT_PASSWORD_CHANGE,
    record_event,
)
from lamella.web.auth.lockout import (
    is_locked,
    register_failure,
    register_success,
)
from lamella.web.auth.passwords import (
    DUMMY_HASH,
    hash_password,
    needs_rehash,
    verify_password,
)
from lamella.web.auth.sessions import (
    COOKIE_INSECURE,
    COOKIE_SECURE,
    create_session,
    revoke_all_for_user,
    revoke_session,
    sign_session_id,
    unsign_session_id,
)
from lamella.web.auth.user import User
from lamella.web.deps import get_db, get_settings


log = logging.getLogger("lamella.auth.routes")

router = APIRouter()


GENERIC_LOGIN_ERROR = "Invalid username or password."
LOCKED_ERROR = "This account is temporarily locked. Try again later."


def _is_https(request: Request) -> bool:
    fwd = request.headers.get("x-forwarded-proto", "").lower()
    if fwd == "https":
        return True
    if fwd == "http":
        return False
    return (request.url.scheme or "").lower() == "https"


def _safe_next(raw: str | None) -> str:
    """Validate the post-login `next` redirect target — local paths only.
    Defends against open-redirect attacks where /login?next=https://evil
    would bounce the freshly-authenticated user to an attacker site."""
    if not raw:
        return "/"
    if not raw.startswith("/"):
        return "/"
    if raw.startswith("//"):
        # Protocol-relative URL — treat as external.
        return "/"
    if "\n" in raw or "\r" in raw:
        return "/"
    return raw


def _render_login(
    request: Request,
    *,
    next_url: str = "/",
    error: str | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "auth/login.html",
        {
            "next": next_url,
            "error": error,
            "page_title": "Sign in",
        },
        status_code=status_code,
    )


@router.get("/login", response_class=HTMLResponse)
async def login_get(
    request: Request,
    next: str = "/",  # noqa: A002 — intentional FastAPI query param name
):
    return _render_login(request, next_url=_safe_next(next))


@router.post("/login")
async def login_post(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    next: str = Form("/"),  # noqa: A002
    db: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Verify credentials, issue session, set signed cookie, redirect."""
    next_url = _safe_next(next)
    username = (username or "").strip()
    password = password or ""

    # Fetch the user row OR fall through to the dummy-hash path so the
    # unknown-user response time matches the wrong-password response time.
    row = None
    if username:
        row = db.execute(
            "SELECT id, account_id, password_hash, role, locked_until "
            "FROM users WHERE username = ? AND account_id = 1",
            (username,),
        ).fetchone()

    if row is None:
        # Unknown user. Verify against the dummy hash to keep the
        # branch timing matched, then fail uniformly.
        verify_password(DUMMY_HASH, password)
        record_event(
            db,
            event_type=EVENT_LOGIN_FAILURE,
            user_id=None,
            account_id=1,
            success=False,
            request=request,
            detail="unknown_user",
        )
        return _render_login(
            request, next_url=next_url, error=GENERIC_LOGIN_ERROR, status_code=200
        )

    user_id = row["id"]
    account_id = row["account_id"]

    # Lockout check happens BEFORE password verify so a locked account
    # cannot leak password-correctness via timing on subsequent attempts.
    if is_locked(db, user_id):
        record_event(
            db,
            event_type=EVENT_LOGIN_FAILURE,
            user_id=user_id,
            account_id=account_id,
            success=False,
            request=request,
            detail="locked",
        )
        return _render_login(
            request, next_url=next_url, error=LOCKED_ERROR, status_code=200
        )

    if not verify_password(row["password_hash"], password):
        tripped = register_failure(
            db,
            user_id,
            threshold=settings.auth_lockout_threshold,
            window_minutes=settings.auth_lockout_window_minutes,
            duration_minutes=settings.auth_lockout_duration_minutes,
        )
        record_event(
            db,
            event_type=EVENT_LOGIN_FAILURE,
            user_id=user_id,
            account_id=account_id,
            success=False,
            request=request,
            detail="bad_password",
        )
        if tripped:
            record_event(
                db,
                event_type=EVENT_LOCKOUT,
                user_id=user_id,
                account_id=account_id,
                success=True,
                request=request,
                detail=(
                    f"threshold={settings.auth_lockout_threshold} "
                    f"duration_minutes={settings.auth_lockout_duration_minutes}"
                ),
            )
        return _render_login(
            request, next_url=next_url, error=GENERIC_LOGIN_ERROR, status_code=200
        )

    # Successful auth. Reset lockout state, opportunistically rehash if
    # parameters strengthened, mint a fresh session, set the cookie.
    register_success(db, user_id)
    if needs_rehash(row["password_hash"]):
        try:
            new_hash = hash_password(password)
            db.execute(
                "UPDATE users SET password_hash = ?, password_changed_at = "
                "strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?",
                (new_hash, user_id),
            )
            db.commit()
        except sqlite3.Error:
            # Rehash is opportunistic; failure to upgrade does not
            # block login.
            pass

    https = _is_https(request)
    session_secret: str = request.app.state.auth_session_secret
    session = create_session(
        db,
        user_id=user_id,
        account_id=account_id,
        expires_in_days=settings.auth_session_days,
        ua=request.headers.get("user-agent", ""),
        ip=request.client.host if request.client else None,
    )
    record_event(
        db,
        event_type=EVENT_LOGIN_SUCCESS,
        user_id=user_id,
        account_id=account_id,
        success=True,
        request=request,
    )
    cookie_name = COOKIE_SECURE if https else COOKIE_INSECURE

    response = RedirectResponse(next_url, status_code=303)
    response.set_cookie(
        key=cookie_name,
        value=sign_session_id(session.session_id, session_secret),
        max_age=settings.auth_session_days * 86400,
        httponly=True,
        samesite="lax",
        secure=https,
        path="/",
    )
    return response


@router.post("/logout")
async def logout_post(
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
    user: User = Depends(current_user),
):
    """Revoke the current session server-side and clear cookies."""
    https = _is_https(request)
    cookie_name = COOKIE_SECURE if https else COOKIE_INSECURE
    signed = request.cookies.get(cookie_name)
    if signed:
        session_secret: str = request.app.state.auth_session_secret
        sid = unsign_session_id(signed, session_secret)
        if sid:
            revoke_session(db, sid)
    record_event(
        db,
        event_type=EVENT_LOGOUT,
        user_id=user.id,
        account_id=user.account_id,
        success=True,
        request=request,
    )
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(cookie_name, path="/")
    return response


@router.get("/account/password", response_class=HTMLResponse)
async def password_form_get(
    request: Request,
    user: User = Depends(current_user),
):
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "auth/password.html",
        {
            "page_title": "Change password",
            "user": user,
            "error": None,
            "ok": False,
        },
    )


@router.post("/account/password")
async def password_form_post(
    request: Request,
    current_password: str = Form(""),
    new_password: str = Form(""),
    confirm_password: str = Form(""),
    db: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
    user: User = Depends(current_user),
):
    templates = request.app.state.templates

    def _render(error: str | None = None, ok: bool = False, status_code: int = 200):
        return templates.TemplateResponse(
            request,
            "auth/password.html",
            {
                "page_title": "Change password",
                "user": user,
                "error": error,
                "ok": ok,
            },
            status_code=status_code,
        )

    row = db.execute(
        "SELECT password_hash FROM users WHERE id = ?", (user.id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="user not found")

    if not verify_password(row["password_hash"], current_password or ""):
        record_event(
            db,
            event_type=EVENT_PASSWORD_CHANGE,
            user_id=user.id,
            account_id=user.account_id,
            success=False,
            request=request,
            detail="bad_current_password",
        )
        return _render(error="Current password is incorrect.", status_code=200)

    if not new_password or len(new_password) < 8:
        return _render(error="New password must be at least 8 characters.", status_code=200)
    if new_password != confirm_password:
        return _render(error="New password and confirmation must match.", status_code=200)

    new_hash = hash_password(new_password)
    db.execute(
        "UPDATE users SET password_hash = ?, password_changed_at = "
        "strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?",
        (new_hash, user.id),
    )
    db.commit()

    # Revoke every other session for the user. We deliberately revoke
    # the current session too and re-issue a fresh one so the cookie
    # rotates on a privilege event.
    revoke_all_for_user(db, user.id)
    record_event(
        db,
        event_type=EVENT_PASSWORD_CHANGE,
        user_id=user.id,
        account_id=user.account_id,
        success=True,
        request=request,
    )

    https = _is_https(request)
    session_secret: str = request.app.state.auth_session_secret
    session = create_session(
        db,
        user_id=user.id,
        account_id=user.account_id,
        expires_in_days=settings.auth_session_days,
        ua=request.headers.get("user-agent", ""),
        ip=request.client.host if request.client else None,
    )
    cookie_name = COOKIE_SECURE if https else COOKIE_INSECURE
    response = _render(ok=True)
    response.set_cookie(
        key=cookie_name,
        value=sign_session_id(session.session_id, session_secret),
        max_age=settings.auth_session_days * 86400,
        httponly=True,
        samesite="lax",
        secure=https,
        path="/",
    )
    return response
