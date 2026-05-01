# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Projects: named cross-cutting classification contexts.

See FUTURE.md "Phase: Projects" for the full design.
"""
from lamella.features.projects.service import (
    Project,
    ProjectService,
    active_projects_for_txn,
)

__all__ = ["Project", "ProjectService", "active_projects_for_txn"]
