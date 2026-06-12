# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Bootstrap package: first-run detection, scaffold, import, markers.

Implements the canonical ledger layout defined in
`docs/specs/LEDGER_LAYOUT.md`. Submodules:

- `markers` — removal-marker writer and parser (§7.4).
- `scaffold` — "start fresh" ledger creation (§8.3).        [planned]
- `detection` — first-run + structural-emptiness check (§8.1–8.2). [planned]
- `import_ledger` — three-bucket import flow (§9).           [planned]
"""
