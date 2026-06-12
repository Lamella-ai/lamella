# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0064 — namespace helper round-trips and idempotency.

Pure-function tests covering ``lamella_namespace.{canonical_name,
legacy_name, is_lamella_name, to_canonical, name_variants}``."""
from __future__ import annotations

import pytest

from lamella.features.paperless_bridge import lamella_namespace as ns


class TestPrefixConstants:
    def test_canonical_prefix_is_colon(self):
        assert ns.LAMELLA_NAMESPACE_PREFIX_NEW == "Lamella:"

    def test_legacy_prefix_is_underscore(self):
        assert ns.LAMELLA_NAMESPACE_PREFIX_LEGACY == "Lamella_"


class TestCanonicalName:
    def test_simple_suffix(self):
        assert ns.canonical_name("Vendor") == "Lamella:Vendor"

    def test_camel_case_suffix(self):
        assert ns.canonical_name("AwaitingExtraction") == "Lamella:AwaitingExtraction"

    def test_empty_suffix_yields_just_prefix(self):
        # Edge case: empty suffix gives bare prefix. Useful for tests
        # of "is anything namespaced" rather than for real callers.
        assert ns.canonical_name("") == "Lamella:"


class TestLegacyName:
    def test_simple_suffix(self):
        assert ns.legacy_name("Vendor") == "Lamella_Vendor"

    def test_camel_case_suffix(self):
        assert ns.legacy_name("AwaitingExtraction") == "Lamella_AwaitingExtraction"


class TestIsLamellaName:
    @pytest.mark.parametrize("name", [
        "Lamella:Vendor",
        "Lamella:AwaitingExtraction",
        "Lamella_Vendor",
        "Lamella_AwaitingExtraction",
        "Lamella:",
        "Lamella_",
    ])
    def test_recognized_namespaces(self, name):
        assert ns.is_lamella_name(name) is True

    @pytest.mark.parametrize("name", [
        "vendor",
        "Vendor",
        "lamella:vendor",  # lowercase doesn't qualify
        "lamella_vendor",
        "L:Vendor",
        "Lamella",  # no separator
        "",
    ])
    def test_rejects_non_namespaced(self, name):
        assert ns.is_lamella_name(name) is False


class TestToCanonical:
    def test_legacy_to_canonical(self):
        assert ns.to_canonical("Lamella_Vendor") == "Lamella:Vendor"

    def test_already_canonical_is_pass_through(self):
        assert ns.to_canonical("Lamella:Vendor") == "Lamella:Vendor"

    def test_idempotent(self):
        # to_canonical(to_canonical(x)) == to_canonical(x) for all x
        for name in [
            "Lamella_Vendor",
            "Lamella:Vendor",
            "vendor",
            "Lamella_AwaitingExtraction",
        ]:
            once = ns.to_canonical(name)
            twice = ns.to_canonical(once)
            assert once == twice, (
                f"to_canonical not idempotent for {name!r}: "
                f"{once!r} -> {twice!r}"
            )

    def test_non_lamella_pass_through(self):
        # A plain user-owned name is returned verbatim — to_canonical
        # is safe to call on any string.
        assert ns.to_canonical("vendor") == "vendor"
        assert ns.to_canonical("custom_field_name") == "custom_field_name"


class TestNameVariants:
    def test_returns_canonical_first_legacy_second(self):
        canonical, legacy = ns.name_variants("Vendor")
        assert canonical == "Lamella:Vendor"
        assert legacy == "Lamella_Vendor"

    def test_for_workflow_tag(self):
        canonical, legacy = ns.name_variants("AwaitingExtraction")
        assert canonical == "Lamella:AwaitingExtraction"
        assert legacy == "Lamella_AwaitingExtraction"


class TestCanonicalConstants:
    def test_workflow_tag_constants_use_colon(self):
        assert ns.TAG_AWAITING_EXTRACTION == "Lamella:AwaitingExtraction"
        assert ns.TAG_EXTRACTED == "Lamella:Extracted"
        assert ns.TAG_NEEDS_REVIEW == "Lamella:NeedsReview"
        assert ns.TAG_DATE_ANOMALY == "Lamella:DateAnomaly"
        assert ns.TAG_LINKED == "Lamella:Linked"

    def test_all_workflow_tags_in_order(self):
        assert ns.ALL_WORKFLOW_TAGS == (
            "Lamella:AwaitingExtraction",
            "Lamella:Extracted",
            "Lamella:NeedsReview",
            "Lamella:DateAnomaly",
            "Lamella:Linked",
        )

    def test_writeback_field_constants_use_colon(self):
        assert ns.FIELD_ENTITY == "Lamella:Entity"
        assert ns.FIELD_CATEGORY == "Lamella:Category"
        assert ns.FIELD_TXN == "Lamella:TXN"
        assert ns.FIELD_ACCOUNT == "Lamella:Account"

    def test_all_writeback_fields_in_order(self):
        assert ns.ALL_WRITEBACK_FIELDS == (
            "Lamella:Entity",
            "Lamella:Category",
            "Lamella:TXN",
            "Lamella:Account",
        )

    def test_legacy_workflow_tags_match_canonical_count(self):
        # Migration uses these to find rename candidates; the count
        # MUST equal the canonical count or the migration drops a tag.
        assert len(ns._LEGACY_WORKFLOW_TAGS) == len(ns.ALL_WORKFLOW_TAGS)
        for legacy, canonical in zip(
            ns._LEGACY_WORKFLOW_TAGS, ns.ALL_WORKFLOW_TAGS,
        ):
            assert legacy.startswith("Lamella_")
            assert canonical.startswith("Lamella:")
            assert ns.to_canonical(legacy) == canonical

    def test_legacy_writeback_fields_match_canonical_count(self):
        assert len(ns._LEGACY_WRITEBACK_FIELDS) == len(
            ns.ALL_WRITEBACK_FIELDS
        )
        for legacy, canonical in zip(
            ns._LEGACY_WRITEBACK_FIELDS, ns.ALL_WRITEBACK_FIELDS,
        ):
            assert legacy.startswith("Lamella_")
            assert canonical.startswith("Lamella:")
            assert ns.to_canonical(legacy) == canonical
