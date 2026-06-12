# ADR-0028: UI Stack Lock. HTMX + Vanilla CSS, No JS Frameworks, No Preprocessors

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** CLAUDE.md §"HTMX endpoints return partials, not full pages", `src/lamella/web/static/htmx.min.js`, `src/lamella/web/templates/`, `.claude-flow/` tooling tree (excluded from compliance scans)

## Context

Lamella's frontend is intentionally minimal: server-rendered Jinja2
templates, HTMX-style attributes for dynamic swap behavior, and
vanilla CSS. This is a deliberate choice for a single-user, self-hosted
tool where build complexity has no payoff.

JavaScript framework ecosystems (React, Vue, Svelte, Alpine) introduce
build pipelines, `node_modules` trees, bundler config, and client-side
routing that all require ongoing maintenance. CSS preprocessors
(Tailwind, Sass, PostCSS) add compilation steps that break the "edit a
file and see the result" development loop. None of these tools address
a real problem Lamella has.

Without a written decision, new contributors will reach for familiar
tools from their day jobs. Each addition is small in isolation; the
cumulative drift produces a stack that nobody wanted and everybody
maintains.

The Phase 7 violation scan confirmed zero current violations. No
forbidden deps, no `package.json`, no preprocessor files. This ADR
locks the stack against future drift.

## Decision

The UI stack is locked to:

- **Templates:** Jinja2 in `src/lamella/web/templates/`
- **Interactivity:** HTMX-style attributes via the custom shim at
  `src/lamella/web/static/htmx.min.js`
- **CSS:** Vanilla CSS in `src/lamella/web/static/`; CSS custom properties
  for theming
- **JS:** Vanilla JS controllers in `src/lamella/web/static/`; mounted via
  `document.addEventListener('htmx:load', ...)` so they survive HTMX
  swaps

No additions from the following categories are permitted:

- JS frameworks: React, Vue, Svelte, Alpine, Stimulus, Lit, or any
  component framework
- CSS preprocessors or utility-class systems: Tailwind, Sass, Less,
  PostCSS plugins, styled-components
- JS build tools: webpack, vite, esbuild, parcel, rollup, Babel
- A `package.json` at any path in the repository (its existence implies
  a JS build step)

The custom HTMX shim (`htmx.min.js`) is the only JS framework in the
stack. Replacing it with upstream htmx.org requires a new ADR. The
shim has deliberate behavioral differences that callers depend on.

If a genuine need emerges that the current stack cannot address, that
need must be articulated in a new ADR that supersedes this one. "It
would be cleaner" is not sufficient justification.

## Consequences

### Positive
- No build step. A developer can edit `src/lamella/web/static/app.css` and
  reload the browser with no intermediate step.
- Dependency surface is limited to Python packages. Security audits
  cover one ecosystem.
- `pyproject.toml` and `requirements*.txt` are the complete dependency
  inventory. No hidden `node_modules` tree.

### Negative / Costs
- Some UI patterns (complex data tables, drag-and-drop, rich text
  editing) are harder to build without a component framework. Those
  patterns should either be avoided or solved with a targeted
  vanilla-JS controller.
- The custom HTMX shim must be maintained in-tree. If the shim falls
  behind a needed htmx.org feature, that feature cannot be used until
  the shim is updated or an ADR is filed.

### Mitigations
- Vanilla JS controllers mounted via `htmx:load` events have a clear
  lifecycle and can be arbitrarily complex. The constraint is on
  frameworks, not on JS itself.
- The shim is small enough (< 1000 lines) to audit and patch in-tree
  without external tooling.

## Compliance

How `/adr-check` detects violations:

- **Forbidden Python deps:** grep `pyproject.toml` and
  `requirements*.txt` for `react`, `vue`, `svelte`, `alpine`,
  `tailwind`, `sass`, `less`, `webpack`, `vite`, `esbuild`, `parcel`,
  `babel`. Any hit is a violation.
- **package.json existence:** `find /home/aj/projects/lamella -name package.json
  -not -path '*/node_modules/*' -not -path '*/.claude-flow/*'`.
  Any result inside the lamella source tree is a violation. Tooling
  trees outside `src/`, `tests/`, `docs/`, and the repo root (e.g.
  `.claude-flow/`, `.github/`, transient `node_modules/`) are
  excluded; their presence does not imply a JS build step for the
  Lamella app itself.
- **Non-shim JS library in static:** `find src/lamella/web/static/ -name '*.js'`.
  Files other than `htmx.min.js` and project-owned controllers are
  flagged for manual review.

## References

- CLAUDE.md §"HTMX endpoints return partials, not full pages"
- `src/lamella/web/static/htmx.min.js`: custom HTMX shim
- `src/lamella/web/routes/_htmx.py`: server-side partial helpers
- `src/lamella/web/templates/base.html`: base template layout
