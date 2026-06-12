# ADR-0032: Component Library. One Jinja Macro per Action, Reused Everywhere

- **Status:** Accepted (Amended 2026-04-29)
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0005](0005-htmx-endpoints-return-partials.md), [ADR-0011](0011-autocomplete-everywhere.md), `src/lamella/templates/_components/`, `docs/core/UI_PATTERNS.md`

## Context

The app has over a dozen surfaces that show transactions: the review queue,
card view, search results, AI suggestion panel, audit list, and detail pages.
Each originally grew its own inline HTML for classify, ask-AI, accept, and
dismiss buttons. The SVG paths, CSS classes, HTMX attributes, and aria labels
diverge independently as features change. A fix to the classify button's
aria-label has to be applied in five files to take effect everywhere.

`src/lamella/templates/_components/_txn_actions.html` (`T.actions`) was created
to address this for the core four actions. It shows the pattern works: one
macro call renders the entire action cluster with correct HTMX wiring and aria
attributes. But the pattern stops there. Other recurring actions (link receipt,
undo override, promote staging row, revoke rule) are still defined inline at
the call site.

`src/lamella/templates/_components/_icons.html` already supplies every icon
through Lucide-style macros. The icon library proves that single-source SVG
distribution works across the whole template tree; the same principle must
extend to the interactive elements that wrap those icons.

The Phase 7 violation scan found 5 inline action buttons in partials/ outside
_components/.

## Decision

Every recurring UI action MUST be expressed as a single Jinja macro inside
`src/lamella/templates/_components/`. The macro owns the button text, icon,
color class, HTMX attributes, and accessibility attributes. Call sites pass
only per-call context: the target identifier, an optional return URL, and an
optional label override. The macro never accepts a `variant` or `color`
argument. Visual style is the macro's responsibility, not the caller's.

Specific obligations:

1. The canonical action macros are:
   - `T.actions(ref, ...)`: the existing cluster in `_txn_actions.html`
     (classify + ask-AI + ignore); this is the reference for the pattern.
   - `B.btn(label, ...)`: the existing general button in `buttons.html`
     for non-action-specific uses.
   - New macros for receipt link, undo, promote, revoke, and any future
     recurring action follow the same file-per-concern layout under
     `_components/`.
2. Inline `<button>` HTML for a named action (classify, ask-AI, link,
   undo, promote) outside `_components/` is a violation.
3. The icon set is `_components/_icons.html` exclusively. Inline SVG paths
   outside `_icons.html` are a violation; so is any import from an external
   icon font or CDN sprite.
4. New actions follow the same pattern: write the macro first, reference it
   from every surface, never copy-paste the HTML.

## Consequences

### Positive
- A single edit to a macro propagates to every surface. Fixing an aria
  attribute, adding a keyboard shortcut, or changing the HTMX swap strategy
  is a one-file change with guaranteed full coverage.
- Reviews are faster: a reviewer seeing `{{ T.actions(ref=...) }}` knows
  the action cluster is correct without reading the generated HTML.
- The icon library pattern (`_icons.html`) already works at scale. Extending
  it to interactive elements follows the same mental model the team already has.

### Negative / Costs
- Macros that accept too many arguments become hard to read. The discipline
  (no visual-style arguments) must be enforced in review; the template engine
  cannot enforce it.
- Existing inline button HTML across non-`_components/` templates is migration
  debt. Until each surface migrates, two representations of the same action
  coexist.

### Mitigations
- The reference implementation at `_components/_txn_actions.html` shows the
  full pattern including HTMX wiring, popover form, and aria labels.
- `docs/core/UI_PATTERNS.md` is the cookbook; macro signatures are documented
  there. New contributors read one file to understand every action surface.
- Migration is tracked per-surface in the feature blueprint Remaining Tasks
  sections. The queue, card, and search surfaces are first.

## Compliance

- Grep `src/lamella/templates/` for `<button` inside forms whose `action` or
  `hx-post` contains `/classify`, `/ask-ai`, `/dismiss`, `/link`, `/undo`,
  `/promote`, `/revoke`. Every match outside `_components/` is a violation.
- Grep `src/lamella/templates/` for inline SVG `<path` content outside
  `_components/_icons.html`. Each match is a candidate violation (confirm
  it is not a data viz or one-off illustration before flagging).
- PR review gate: new templates importing `_components/` patterns are
  compliant; new templates with raw `<button>` HTML for named actions are not.

## References

- `src/lamella/templates/_components/_txn_actions.html`: reference implementation
- `src/lamella/templates/_components/_icons.html`: icon library
- `src/lamella/templates/_components/buttons.html`: general `btn` / `icon_btn` macros
- [ADR-0005](0005-htmx-endpoints-return-partials.md): HTMX partials; components rendered via partials
- [ADR-0011](0011-autocomplete-everywhere.md): account picker macro as prior art
- `docs/core/UI_PATTERNS.md`: macro signatures cookbook

## Amendment 2026-04-29: Group/workflow form carve-out

The `T.actions` macro is the canonical pattern for per-transaction action
buttons (classify, link receipt, dismiss, etc.). It does NOT cover inline
`<form>` blocks that carry group/workflow context via hidden inputs
(`source`, `rule`, `scope`, `group_id`, `workflow_id`). These forms remain
inline by design. Extending T.actions to accept them would require a
parameter explosion that costs more than the inconsistency saves.

For reference, the macro's current signature accepts only
`ref`, `compact`, `return_url`, `proposal`, and `undoable_txn_id`
(see `src/lamella/web/templates/_components/_txn_actions.html`). Anything
else a caller needs to thread into the POST body has nowhere to live.

### Carve-out scope (frozen list)

The following 7 inline form blocks (across 3 template files) are exempt
from T.actions migration as of v0.3.0:

- `src/lamella/web/templates/partials/_ask_ai_result.html:113`: Ask-AI Accept-proposal form (hidden `source` for staged mode)
- `src/lamella/web/templates/partials/_ask_ai_result.html:151`: Ask-AI Manual-classify form (hidden `source` for staged mode)
- `src/lamella/web/templates/partials/_staged_list.html:348`: group-band Apply-to-all classify-group form (hidden `source` = filter source)
- `src/lamella/web/templates/partials/_staged_list.html:363`: group-band single-row accept-proposed form (hidden `source` = filter source)
- `src/lamella/web/templates/partials/_staged_list.html:413`: group-bulk manual classify-group form (hidden `source` = filter source)
- `src/lamella/web/templates/partials/_staged_list.html:491`: single-row group manual classify form (hidden `source` = filter source)
- `src/lamella/web/templates/setup_import.html:131`: `/setup/import/apply` workflow form (hidden `source` = import source slug)

New action buttons MUST go through T.actions. New group/workflow forms MUST
match the existing inline pattern in these 7 files. Adding to the carve-out
list requires a follow-up amendment.
