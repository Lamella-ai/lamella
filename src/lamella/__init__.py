# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Top-level package — exposes the installed version string.

The very first thing we do is install the uid/pwd compatibility
shim so ``sentence-transformers`` and ``huggingface_hub`` don't
crash with ``getpwuid(): uid not found: N`` on container runtimes
where the invoking uid isn't in ``/etc/passwd`` (e.g. PUID/PGID
env-var overrides, orchestrator pod-security policies). See
``_uid_compat`` for the full story.
"""
from lamella.utils import _uid_compat as _uid_compat

_uid_compat.install()

from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    __version__ = _pkg_version("lamella")
except PackageNotFoundError:
    # Dev-tree fallback when the distribution isn't installed.
    __version__ = "0.0.0-dev"
