# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Lamella /setup/recovery surface — drift detection + heal actions.

The recovery module is the post-install equivalent of the wizard:
where the wizard scaffolds an install from nothing, recovery
detects when an existing install has drifted from what the current
code expects (legacy paths, schema gaps, missing scaffolds, orphaned
references) and offers per-finding heal actions.

Module layout (per SETUP_IMPLEMENTATION.md Phase 3+):

  - models.py    — Finding dataclass, HealResult, make_finding_id
  - snapshot.py  — with_bean_snapshot context manager
  - findings/    — pure detector functions
  - heal/        — fix actions consuming Findings inside snapshot envelopes

Detectors are pure: ``(conn, entries) -> tuple[Finding, ...]``.
Heal actions take one Finding plus the env they need
(``conn, settings, reader, finding``) and return a HealResult.
Per-finding atomicity in Phase 3 — bulk apply is Phase 6's job.
"""
