# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""FastAPI dependency-injection helpers — ADR-0050.

Routes consume `current_user` / `current_tenant` rather than reading
from request.state directly. Today both resolve from middleware-
populated state; SaaS-day extends them (e.g. role checks) without
touching call sites.
"""

from __future__ import annotations

from fastapi import Depends, HTTPException, Request

from lamella.web.auth.user import User, Tenant


def current_user(request: Request) -> User:
    user = getattr(request.state, "user", None)
    if user is None:
        # Should not happen — middleware injects ANONYMOUS_OWNER when
        # auth is off. A None here means the middleware never ran.
        raise HTTPException(status_code=500, detail="auth middleware not installed")
    return user


def current_tenant(request: Request) -> Tenant:
    tenant = getattr(request.state, "tenant", None)
    if tenant is None:
        raise HTTPException(status_code=500, detail="auth middleware not installed")
    return tenant


def require_owner(user: User = Depends(current_user)) -> User:
    """Today every authenticated user is an owner. SaaS-day this gate
    starts to do work; today it's a no-op type-narrower so call sites
    can be written future-proof."""
    if not user.is_owner:
        raise HTTPException(status_code=403, detail="owner role required")
    return user
