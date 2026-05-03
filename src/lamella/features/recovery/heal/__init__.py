# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Recovery heal actions. Each consumes one Finding and applies
its proposed_fix inside a ``with_bean_snapshot`` envelope. Failures
restore the declared file set and raise; the route handler turns
that into a user-visible error message."""
from lamella.features.recovery.heal.legacy_paths import heal_legacy_path
from lamella.features.recovery.heal.schema_drift import heal_schema_drift

__all__ = ["heal_legacy_path", "heal_schema_drift"]
