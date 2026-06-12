# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from lamella.adapters.paperless.client import PaperlessClient, PaperlessError
from lamella.adapters.paperless.schemas import CustomField, Document

__all__ = ["CustomField", "Document", "PaperlessClient", "PaperlessError"]
