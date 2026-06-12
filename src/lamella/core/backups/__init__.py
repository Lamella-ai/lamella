# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from lamella.core.backups.sqlite_dump import BackupResult, run_backup

__all__ = ["BackupResult", "run_backup"]
