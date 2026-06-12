# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0059 narration synthesizer port + deterministic adapter +
Haiku fallback semantics."""
from __future__ import annotations

from decimal import Decimal

import pytest

from lamella.features.ai_cascade.narration_synthesizer import (
    DeterministicNarrationSynthesizer,
    HaikuNarrationSynthesizer,
    LAMELLA_NARRATION_SYNTHESIZED_KEY,
    SourceObservation,
    SynthesisInput,
    SynthesisResult,
    build_synthesis_input,
)


def _input(*, observations, **kw):
    return build_synthesis_input(
        signed_amount=Decimal("-12.50"),
        currency="USD",
        source_account="Liabilities:Card",
        target_account="Expenses:Food",
        observations=observations,
        **kw,
    )


class TestDeterministicSynthesizer:
    """No AI calls — pure logic on the inputs."""

    def test_picks_longest_description(self):
        synth = DeterministicNarrationSynthesizer()
        result = synth.synthesize(_input(observations=[
            SourceObservation(
                source="simplefin",
                reference_id="TRN-1",
                description="POS DEBIT",
                payee="ACME",
            ),
            SourceObservation(
                source="csv",
                reference_id="ROW-2",
                description="Acme Coffee — Decaf and a scone",
                payee="ACME",
            ),
        ]))
        assert isinstance(result, SynthesisResult)
        assert result.narration == "Acme Coffee — Decaf and a scone"
        assert "deterministic" in result.rationale

    def test_falls_back_to_payee_when_no_descriptions(self):
        synth = DeterministicNarrationSynthesizer()
        result = synth.synthesize(_input(observations=[
            SourceObservation(
                source="simplefin",
                reference_id="TRN-1",
                description=None,
                payee="ACME COFFEE",
            ),
        ]))
        assert result.narration == "ACME COFFEE"
        assert "payee" in result.rationale

    def test_placeholder_when_no_signal(self):
        """No description, no payee → placeholder line the user can
        recognize and overwrite. Doesn't crash."""
        synth = DeterministicNarrationSynthesizer()
        result = synth.synthesize(_input(observations=[
            SourceObservation(
                source="simplefin",
                reference_id="TRN-1",
                description=None,
                payee=None,
            ),
        ]))
        assert result.narration == "(no narration)"

    def test_empty_observations_still_produces_result(self):
        synth = DeterministicNarrationSynthesizer()
        result = synth.synthesize(_input(observations=[]))
        assert isinstance(result, SynthesisResult)
        assert result.narration  # never empty

    def test_skips_empty_strings_in_descriptions(self):
        """An observation with description='' must not be picked as
        the longest — it'd produce an empty narration."""
        synth = DeterministicNarrationSynthesizer()
        result = synth.synthesize(_input(observations=[
            SourceObservation(
                source="simplefin",
                reference_id="TRN-1",
                description="",
                payee="ACME",
            ),
            SourceObservation(
                source="csv",
                reference_id="ROW-2",
                description="A coffee",
                payee="ACME",
            ),
        ]))
        assert result.narration == "A coffee"


class TestHaikuSynthesizerFallback:
    """When the AI service is None / disabled / errors, Haiku
    adapter falls through to deterministic so the promotion path
    never hard-blocks on AI."""

    def test_none_ai_service_falls_back(self):
        synth = HaikuNarrationSynthesizer(ai_service=None)
        result = synth.synthesize(_input(observations=[
            SourceObservation(
                source="csv",
                reference_id="ROW-1",
                description="Coffee Shop",
                payee="Coffee Shop",
            ),
        ]))
        assert result.narration == "Coffee Shop"
        assert "deterministic" in result.rationale

    def test_disabled_ai_service_falls_back(self):
        class Stub:
            enabled = False
        synth = HaikuNarrationSynthesizer(ai_service=Stub())
        result = synth.synthesize(_input(observations=[
            SourceObservation(
                source="csv",
                reference_id="ROW-1",
                description="Coffee Shop",
                payee="Coffee Shop",
            ),
        ]))
        assert result.narration == "Coffee Shop"

    def test_client_returning_none_falls_back(self):
        """Some configurations have ai.enabled but new_client()
        returns None (no API key). Must not raise."""
        class StubAI:
            enabled = True
            def new_client(self):
                return None
            def model_for(self, key):
                return "haiku"
        synth = HaikuNarrationSynthesizer(ai_service=StubAI())
        result = synth.synthesize(_input(observations=[
            SourceObservation(
                source="csv",
                reference_id="ROW-1",
                description="Coffee Shop",
                payee=None,
            ),
        ]))
        assert result.narration == "Coffee Shop"


class TestExports:
    """Marker key exported as a module constant so writers can
    import it without re-typing the string."""

    def test_marker_key_constant(self):
        assert (
            LAMELLA_NARRATION_SYNTHESIZED_KEY
            == "lamella-narration-synthesized"
        )
