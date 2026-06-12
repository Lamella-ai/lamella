# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Registry layer: entities, accounts, vehicles + humanization.

The registry is how the UI stops showing raw Beancount paths. Every
display of an account or entity passes through `alias.alias_for()`,
which reads the user-curated registry and falls back to a heuristic
pretty-formatter when metadata is missing.

Discovery is adaptive — `discovery.sync_from_ledger()` walks the
ledger's Open directives on boot and inserts new slugs it hasn't seen
before. Users never type a slug that already exists; they just label
it.
"""
