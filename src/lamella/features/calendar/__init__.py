# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Calendar feature — a cross-cutting day-by-day review lens over
ledger transactions, notes, mileage, and Paperless documents.

The calendar is NOT a new source of data. Every item surfaced on a
day view links out to whatever edit/detail page already handles it.
All day-level review state is mirrored to the ledger (custom
"day-review" directives) so the reconstruct-from-ledger guarantee
holds.
"""
