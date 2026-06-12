# Contributing to Lamella

Thank you for your interest in contributing to Lamella! This document
explains how to contribute and what we expect from contributors.

## License of contributions

Lamella is licensed under the Apache License, Version 2.0. By submitting
a contribution (pull request, patch, code, documentation, etc.), you
agree that:

1. Your contribution is licensed under the Apache License, Version 2.0,
   the same license as the rest of the project.
2. You have the right to submit the contribution under that license
   (i.e., you wrote it yourself, or you have permission from the
   copyright holder).
3. You understand that your contribution is public and may be
   redistributed under the project's license.

This is consistent with the Apache 2.0 license's contribution clause
(Section 5), which states that contributions are submitted under the
license terms unless explicitly stated otherwise.

## Developer Certificate of Origin

For now, contributions are accepted under the Apache 2.0 license terms
without a separate Contributor License Agreement (CLA). We may
introduce a CLA in the future if the project's governance needs change
(for example, if we want the option to dual-license or relicense). If
that happens, we will request that contributors sign the CLA going
forward; we will not retroactively require it for past contributions.

If you would prefer to assert authorship explicitly, you may add a
"Signed-off-by" line to your commits per the Developer Certificate of
Origin (https://developercertificate.org/):

    git commit -s -m "Your commit message"

This is appreciated but not required.

## How to contribute

Lamella is a self-hosted, single-user, single-container app. The
fastest path to a working dev environment is:

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e .[dev]
uvicorn lamella.main:app --reload
pytest
```

See `README.md` for the full quick start, environment variables, and
volume layout.

### Architectural decisions live in ADRs

Lamella's design is captured as Architecture Decision Records under
`docs/adr/`. They are load-bearing. Every non-trivial PR is expected
to either (a) follow the relevant ADRs, or (b) supersede an ADR with
a new one (`Status: Supersedes NNNN`). Start with `docs/adr/index.md`
for the table of contents and `docs/core/PROJECT_CHARTER.md` /
`docs/core/PRODUCT_VISION.md` for project-level intent. The
`/adr-check` slash command (under `.claude/commands/`) walks an agent
through pointing at an existing ADR or writing a new one before
starting a feature.

Per-feature current state lives in `docs/features/<slug>.md` (auto-
derivable from code via `/feature-blueprint`). Read those before
proposing a structural change.

`CLAUDE.md` (and the per-directory `CLAUDE.md` files that auto-load
when you work in those subtrees) is the routing layer that points at
the right ADR / spec / feature blueprint for the kind of work you're
doing.

Conventions:

- Run `pytest` and `ruff` before submitting.
- Keep commits focused; prefer multiple small commits over one large
  one when the changes are logically separable.
- New endpoints that drive HTMX swaps must return partials (see the
  HTMX section in `CLAUDE.md`).
- Long-running operations must run as background jobs with a progress
  modal, not synchronous POSTs.

## Code of conduct

Be respectful. Assume good faith. Don't be a jerk.

## Trademark

Please note that contributing code does not grant you any rights to the
Lamella trademark. See TRADEMARK.md for our trademark policy.

## Questions

For general questions: hello@lamella.ai. For trademark questions:
legal@lamella.ai.
