# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware
# financial intelligence
# https://lamella.ai

"""Regression: when ``_resolve_account_path`` can't map a staged
row to a backing account, the AI classify path must degrade
gracefully (fall through with ``source_account=None``) rather than
hard-aborting with "Can't resolve the card/account for this row."

Reported scenarios (AJ, 2026-04-29 — 2026-05-02):

  * Reboot rows whose ``source_ref`` only carries {file, lineno}
    surfaced "No confident proposal — Can't resolve the card/account
    for this row" in the Ask AI modal. The classifier never even
    tried.
  * The unified ``/api/txn/{ref}/ask-ai`` endpoint silently returned
    None on the same shape; the modal rendered as "no confident
    proposal" with no clue to the operator that the resolver was
    the actual blocker.

The fix:

  * ``_maybe_ai_classify(source_account: str | None)`` accepts None.
  * Both call sites (``staging_review.ask_ai_modal`` worker and
    ``api_txn._run_staged_ask_ai``) log a warning and fall through.
  * The classifier widens to cross-entity scope when entity is
    None — the existing ``intercompany_flag`` mitigation catches
    high-confidence cross-entity picks at the gate.

This test fixture pins the contract at the function-signature
level. End-to-end behaviour is exercised in the higher-level
classify integration tests once the OpenRouter mocks are in place.
"""
from __future__ import annotations

import inspect


class TestMaybeAiClassifyAcceptsOptionalSource:
    """``_maybe_ai_classify`` MUST accept ``source_account=None`` so
    the staged callers can fall through when the resolver fails."""

    def test_source_account_is_optional(self):
        from lamella.features.bank_sync.ingest import SimpleFINIngest
        sig = inspect.signature(SimpleFINIngest._maybe_ai_classify)
        assert "source_account" in sig.parameters, (
            "_maybe_ai_classify must keep its source_account kwarg"
        )
        # The annotation must include None / Optional so the type
        # checker (and human readers) know the no-source path is
        # explicitly supported, not an oversight.
        annot = sig.parameters["source_account"].annotation
        annot_str = str(annot)
        assert "None" in annot_str or "Optional" in annot_str, (
            f"source_account annotation must be optional; got "
            f"{annot_str!r}. Hard-aborting on a None hint silently "
            "kills the AI for any row whose backing account couldn't "
            "be mapped (reboot rows, account-meta orphans)."
        )


class TestRunStagedAskAiSoftFallback:
    """``_run_staged_ask_ai`` MUST NOT short-circuit on a None
    source_account — it should log + continue. Otherwise the unified
    /api/txn/{ref}/ask-ai endpoint silently returns None and the
    modal renders "no confident proposal" with no diagnostic for
    the operator."""

    def test_no_explicit_short_circuit_on_resolver_none(self):
        # Source-level guard: the historical hard-abort `return None`
        # was tripped by `if not source_account: return None`. We
        # don't keep that pattern any more.
        import lamella.web.routes.api_txn as api_txn_mod
        src = inspect.getsource(api_txn_mod._run_staged_ask_ai)
        # Crude but effective: the first bit of the function used
        # to be `if not source_account: return None`. After the fix
        # we either drop that line or keep `return None` strictly
        # behind a logged warning. The simplest signature: there's
        # at least one log.warning call referring to source_account
        # in this function's body.
        assert "log.warning" in src, (
            "_run_staged_ask_ai must surface the resolver miss "
            "as a logged warning, not a silent return-None"
        )
        # And the early `return None` directly after the resolver
        # call is gone — the function must reach the SimpleFIN
        # ingest call below.
        # Pattern check: find the resolver call and confirm there's
        # NO `return None` between it and the next blank line.
        lines = src.splitlines()
        in_block = False
        for i, line in enumerate(lines):
            if "_resolve_account_path" in line and "from lamella" not in line:
                in_block = True
                continue
            if in_block:
                stripped = line.strip()
                if stripped.startswith("return None"):
                    raise AssertionError(
                        "_run_staged_ask_ai still has an early "
                        "return None after _resolve_account_path; the "
                        "fix should warn-and-continue instead."
                    )
                # End of the immediate block.
                if stripped.startswith("posted_epoch"):
                    break


class TestStagedAskAiModalSoftFallback:
    """Mirror guard for the legacy staging_review.ask_ai_modal
    worker — same hard-abort had to be converted to log+continue."""

    def test_no_failure_emit_for_resolver_miss(self):
        import lamella.web.routes.staging_review as sr_mod
        # Find the inner _worker function in
        # staged_review_ask_ai_modal. We grep the file source — the
        # historical "Can't resolve the card/account" failure emit
        # must be gone (or changed to outcome="info").
        src = inspect.getsource(sr_mod)
        # The historical exact phrase "needs a source-account hint"
        # was the failure-emit text. After the fix, the same place
        # logs a warning + emits outcome="info", or drops the message
        # entirely. Either way, the failure-emit pairing must be
        # gone.
        bad_pair = (
            'needs a source-account hint',
            'outcome="failure"',
        )
        # Look for both phrases on adjacent lines — historical shape.
        idx = src.find(bad_pair[0])
        if idx == -1:
            return  # already removed
        window = src[idx:idx + 400]
        assert bad_pair[1] not in window, (
            "staging_review.ask_ai_modal worker still hard-aborts "
            "with outcome='failure' when the resolver returns None; "
            "the fix should log a warning and fall through with "
            "source_account=None."
        )

    def test_no_proposal_after_ai_invoked_emits_info_not_failure(self):
        """When the AI ran successfully but returned no confident proposal
        (inline is None or inline.proposal is None), the worker must emit
        outcome='info' rather than outcome='failure'.

        The 'failure' badge implies a system error; an AI confidence miss
        is a normal operating condition and must be surfaced as info — matching
        the /api/txn/{ref}/ask-ai endpoint UX that shows 'No confident proposal'
        without a failure badge.

        Source-level guard: confirm the worker uses ai_invoked tracking so
        the info/failure distinction is explicit, and that 'outcome="failure"'
        does not immediately follow the ai_invoked=True assignment."""
        import lamella.web.routes.staging_review as sr_mod

        src = inspect.getsource(sr_mod)
        # The fix must use ai_invoked tracking — if this is absent,
        # the distinction between "AI ran and declined" vs "AI never ran"
        # cannot be made correctly.
        assert "ai_invoked" in src, (
            "staging_review ask_ai_modal worker must use an ai_invoked "
            "flag to distinguish 'AI ran but no proposal' (info) from "
            "'pre-flight prevented AI call' (failure)."
        )
        # The no-proposal emit block must use a variable for outcome
        # (not a hardcoded "failure") — check that the block near
        # "No confident proposal" does NOT hard-code outcome="failure".
        no_proposal_idx = src.find("No confident proposal")
        assert no_proposal_idx != -1, (
            "Expected 'No confident proposal' message in worker; "
            "text may have changed — update this test to match."
        )
        # Within 200 chars of the no-proposal message, there should be
        # no hardcoded outcome="failure" (it must use the variable).
        window = src[max(0, no_proposal_idx - 50):no_proposal_idx + 200]
        assert 'outcome="failure"' not in window, (
            "The no-proposal emit near 'No confident proposal' still "
            "hard-codes outcome='failure'. It must use the ai_invoked "
            "variable so that a normal AI confidence-miss is shown as "
            "info, not failure."
        )
