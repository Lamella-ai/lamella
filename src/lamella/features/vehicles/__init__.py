# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Vehicle-specific service and health-check modules.

Sibling to `mileage/` — where `mileage/` is about trip logging and
per-year rollups, `vehicles/` carries everything keyed on the vehicle
row itself (identity, elections, disposals, fuel log, data-health
panel, allocation, templates, etc.). Phase 2 lands the health-check
registry; later phases plug in additional check functions and writer
modules.
"""
