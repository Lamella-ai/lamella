# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Authentication package — ADR-0050.

Optional, opt-in via env var; financial-grade defaults; SaaS-ready
shape. Routes consume `request.state.user` and `request.state.tenant`;
the middleware injects them whether auth is configured or not.
"""

from lamella.web.auth.user import User, Tenant

__all__ = ["User", "Tenant"]
