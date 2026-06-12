# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 7: spreadsheet import with AI-assisted column mapping.

This package absorbs importer_bundle/importers/ into the Connector. The
per-source ingesters (`sources/`) are ported verbatim; the classifier and
transfer detector are ported; `emit` is patched to write to Connector-owned
`connector_imports/` instead of Cowork's `historical/`. The genuinely new
pieces are `mapping` (AI column proposal) and `sources/generic` (user-
confirmed column mapping -> raw_rows).
"""
