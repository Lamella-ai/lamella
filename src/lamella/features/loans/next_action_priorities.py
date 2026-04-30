# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Priority table for loan NextAction sort order.

One integer per kind, smaller = sorted first within a severity bucket.
Lives in its own module so all tests, UI, and logic agree on order.

The full sort key used by health.py is a 5-tuple:
    (severity_rank, priority, kind, stable_key, insertion_index)

Why five levels, not three:

1. `severity_rank` — blocking/attention/info buckets. Non-negotiable
   ordering; a missing-funding blocker must never sort below a
   cosmetic info item.
2. `priority` — within a severity, rank by "what does the user
   actually need to do first" (scaffolding before funding before
   missing payments, etc).
3. `kind` — when two actions share severity AND priority (common
   with un-tabled kinds defaulting to 500), group like-with-like
   lexicographically so the UI renders a natural batch.
4. `stable_key` — multiple actions of the SAME kind
   (e.g. three missing-payment rows for Jan/Feb/Mar) need a
   sub-kind tiebreak. STABLE_KEY_FIELDS picks the right payload
   field per kind; ISO dates sort lexicographically.
5. `insertion_index` — final terminal tiebreaker. Guarantees
   deterministic ordering even when every other key ties, without
   forcing every action producer to supply a unique stable_key.
   This is what makes the output byte-equal across runs.

Removing any of these levels reintroduces UI flicker between page
reloads because Python's sort is only stable within the current
call, not across calls with differently-ordered inputs.
"""
from __future__ import annotations


NEXT_ACTION_PRIORITIES: dict[str, int] = {
    # Blocking — scaffolding / funding must be resolved before
    # anything else makes sense.
    "scaffolding-open-missing":          10,
    "scaffolding-open-date-too-late":    11,
    "fund-initial":                      20,
    "scaffolding-escrow-path-missing":   30,
    "scaffolding-tax-path-missing":      31,
    "scaffolding-insurance-path-missing":32,

    # Attention — routine work the user should be aware of.
    "record-payment":                    100,
    "missing-payment":                   110,
    "add-anchor":                        120,
    "stale-anchor":                      121,
    "anomaly":                           130,
    "escrow-shortage-projected":         140,
    "sustained-overflow":                150,
    "long-payment-gap":                  160,
    "dense-window":                      170,

    # Info — purely advisory.
    "scaffolding-property-slug-dangling": 200,
    "scaffolding-simplefin-stale":        210,
    "info":                               900,
}


SEVERITY_RANK: dict[str, int] = {
    "blocking":  0,
    "attention": 1,
    "info":      2,
}


STABLE_KEY_FIELDS: dict[str, str] = {
    # Pulls one field out of NextAction.payload to disambiguate
    # multiple actions of the same kind. ISO-formatted dates work
    # because they sort lexicographically.
    "missing-payment":                     "expected_date",
    "record-payment":                      "txn_hash",
    "scaffolding-open-missing":            "path",
    "scaffolding-open-date-too-late":      "path",
    "scaffolding-escrow-path-missing":     "path",
    "scaffolding-tax-path-missing":        "path",
    "scaffolding-insurance-path-missing":  "path",
    "scaffolding-property-slug-dangling":  "property_slug",
    "stale-anchor":                        "last_anchor_date",
    "anomaly":                             "anomaly_kind",
}


def priority_for(kind: str) -> int:
    return NEXT_ACTION_PRIORITIES.get(kind, 500)


def severity_rank(severity: str) -> int:
    return SEVERITY_RANK.get(severity, 99)
