# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""User and Tenant value objects exposed via request.state.

Single-tenant mode resolves both to fixed values:
  request.state.user = User(id=1, username="admin", account_id=1, role="owner")
  request.state.tenant = Tenant(id=1, name="local")

SaaS-day extends User to carry richer fields (email, mfa_enabled, etc.)
without changing the dependency-injection shape that routes consume.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Tenant:
    id: int
    name: str


@dataclass(frozen=True)
class User:
    id: int
    username: str
    account_id: int
    role: str = "owner"

    @property
    def is_owner(self) -> bool:
        return self.role == "owner"


# Sentinel injected when auth is disabled. Routes that need to know
# "auth was not configured" can check `user is ANONYMOUS_OWNER`.
ANONYMOUS_OWNER = User(id=0, username="admin", account_id=1, role="owner")
ANONYMOUS_TENANT = Tenant(id=1, name="local")
