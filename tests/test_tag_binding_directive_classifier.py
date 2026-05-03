# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0065 — classifier routes the new tag-binding directive types.

Verifies that:
* ``lamella-tag-binding`` is in OWNED_CUSTOM_TARGETS and routes to connector_config.bean
* ``lamella-tag-binding-revoked`` is in OWNED_CUSTOM_TARGETS and routes to connector_config.bean
* Both are in OWNED_CUSTOM_TYPES
* The directive vocabulary constants are exported from directive_types.py
"""
from __future__ import annotations

from lamella.core.bootstrap.classifier import (
    OWNED_CUSTOM_TARGETS,
    OWNED_CUSTOM_TYPES,
)
from lamella.features.receipts.directive_types import (
    DIRECTIVE_TAG_BINDING_NEW,
    DIRECTIVE_TAG_BINDING_REVOKED_NEW,
    DIRECTIVE_TYPES_ALL_TAG_BINDING,
    DIRECTIVE_TYPES_TAG_BINDING,
    DIRECTIVE_TYPES_TAG_BINDING_REVOKED,
)


def test_binding_directive_in_owned_custom_targets():
    """lamella-tag-binding is routed to connector_config.bean."""
    assert DIRECTIVE_TAG_BINDING_NEW in OWNED_CUSTOM_TARGETS
    assert OWNED_CUSTOM_TARGETS[DIRECTIVE_TAG_BINDING_NEW] == "connector_config.bean"


def test_binding_revoked_directive_in_owned_custom_targets():
    """lamella-tag-binding-revoked is routed to connector_config.bean."""
    assert DIRECTIVE_TAG_BINDING_REVOKED_NEW in OWNED_CUSTOM_TARGETS
    assert OWNED_CUSTOM_TARGETS[DIRECTIVE_TAG_BINDING_REVOKED_NEW] == "connector_config.bean"


def test_both_directive_types_in_owned_custom_types():
    """Both binding directive types are in OWNED_CUSTOM_TYPES."""
    assert DIRECTIVE_TAG_BINDING_NEW in OWNED_CUSTOM_TYPES
    assert DIRECTIVE_TAG_BINDING_REVOKED_NEW in OWNED_CUSTOM_TYPES


def test_directive_type_constants():
    """Vocabulary constants have expected values."""
    assert DIRECTIVE_TAG_BINDING_NEW == "lamella-tag-binding"
    assert DIRECTIVE_TAG_BINDING_REVOKED_NEW == "lamella-tag-binding-revoked"


def test_directive_types_tuples():
    """Tuple constants contain expected members."""
    assert DIRECTIVE_TAG_BINDING_NEW in DIRECTIVE_TYPES_TAG_BINDING
    assert DIRECTIVE_TAG_BINDING_REVOKED_NEW in DIRECTIVE_TYPES_TAG_BINDING_REVOKED
    assert DIRECTIVE_TAG_BINDING_NEW in DIRECTIVE_TYPES_ALL_TAG_BINDING
    assert DIRECTIVE_TAG_BINDING_REVOKED_NEW in DIRECTIVE_TYPES_ALL_TAG_BINDING


def test_directive_types_all_tag_binding_is_union():
    """DIRECTIVE_TYPES_ALL_TAG_BINDING is the union of TAG_BINDING + REVOKED."""
    expected = set(DIRECTIVE_TYPES_TAG_BINDING) | set(DIRECTIVE_TYPES_TAG_BINDING_REVOKED)
    assert set(DIRECTIVE_TYPES_ALL_TAG_BINDING) == expected
