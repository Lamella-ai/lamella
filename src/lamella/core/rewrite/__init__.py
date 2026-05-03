# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""In-place ledger rewrites.

The canonical flow for categorization is to edit the source .bean
file directly — no override layer, no accumulated correction
blocks. This module owns the file surgery: locate the target
posting, replace the account path, validate with bean-check,
roll back on any failure.

NEXTGEN Phase E3 (rule mining) is shipped as a batch rewrite;
this package supplies the per-txn equivalent.
"""
