# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""uid/pwd compatibility shim — runs at package import before
anything else, so it MUST stay side-effect-only and cheap.

Problem: containerized runtimes are commonly invoked as an
arbitrary uid (set via ``PUID`` / ``PGID`` env vars on
self-hosted images, or by an orchestrator's pod-security policy)
that has no entry in ``/etc/passwd``. When any Python library
does::

    os.path.expanduser("~")       # → pwd.getpwuid(geteuid())
    pwd.getpwuid(os.geteuid())    # directly
    getpass.getuser()             # → pwd.getpwuid fallback

…it raises ``KeyError: 'getpwuid(): uid not found: N'`` and
crashes. ``sentence-transformers`` + ``huggingface_hub`` hit this
during model download on the first vector-index build, which
made the feature unusable for any deploy that runs the container
as a uid the image doesn't know about.

Setting ``HOME``/``HF_HOME``/``TRANSFORMERS_CACHE`` in the
Dockerfile *usually* prevents the lookup, but (a) some libraries
call ``pwd.getpwuid`` directly regardless of env and (b) users on
older images don't have the env set at all. So instead of relying
on env alone, we monkey-patch ``pwd.getpwuid`` at package import
to return a fake entry when the uid is missing.

This is safe because nothing in this codebase actually cares
about the real passwd data — all we need is a struct with a
home directory that ``os.path.expanduser`` can return.
"""
from __future__ import annotations

import os


def _set_default_env() -> None:
    """Give HF/transformers a stable cache dir even when HOME is
    weird. These defaults match the Dockerfile; unset means we're
    running dev / tests and the real HOME is fine."""
    defaults = {
        "HOME": os.environ.get("HOME") or "/app",
        "HF_HOME": "/data/huggingface",
        "TRANSFORMERS_CACHE": "/data/huggingface",
        "SENTENCE_TRANSFORMERS_HOME": "/data/huggingface",
        "XDG_CACHE_HOME": "/data/cache",
    }
    # Only fill HOME if it really is missing. For the HF stack we
    # leave existing values untouched — user may have chosen a
    # different cache location — but fill them when absent.
    if not os.environ.get("HOME"):
        os.environ["HOME"] = defaults["HOME"]
    for key in ("HF_HOME", "TRANSFORMERS_CACHE", "SENTENCE_TRANSFORMERS_HOME", "XDG_CACHE_HOME"):
        if not os.environ.get(key):
            os.environ[key] = defaults[key]


def _patch_pwd() -> None:
    """Wrap ``pwd.getpwuid`` so unknown uids get a synthetic entry
    instead of ``KeyError``. On Windows the ``pwd`` module doesn't
    exist; skip silently."""
    try:
        import pwd  # noqa: I001 — POSIX only
    except ImportError:
        return

    # Guard against double-patching (tests, re-imports).
    if getattr(pwd.getpwuid, "_beancount_ai_patched", False):
        return

    _real_getpwuid = pwd.getpwuid
    home = os.environ.get("HOME") or "/tmp"

    def _safe_getpwuid(uid):  # type: ignore[no-untyped-def]
        try:
            return _real_getpwuid(uid)
        except KeyError:
            # Build a struct_passwd-like tuple. pwd.struct_passwd
            # is named-tuple: (pw_name, pw_passwd, pw_uid, pw_gid,
            # pw_gecos, pw_dir, pw_shell).
            return pwd.struct_passwd((
                f"uid{uid}",
                "x",
                int(uid),
                int(uid),
                "",
                home,
                "/sbin/nologin",
            ))

    _safe_getpwuid._beancount_ai_patched = True  # type: ignore[attr-defined]
    pwd.getpwuid = _safe_getpwuid  # type: ignore[assignment]


def install() -> None:
    _set_default_env()
    _patch_pwd()
