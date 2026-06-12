# ADR-0035: Dense Data Readability. Tables, Numbers, Eye-Strain

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0034](0034-wcag-2.2-aa-accessibility.md), [ADR-0041](0041-display-names-everywhere.md), `src/lamella/static/components.css`

## Context

Lamella surfaces a lot of numbers and accounts side by side. The review queue
can show 50+ rows at once. The search results page lists transactions with
dates, payees, amounts, and account paths. The budget page shows multi-row
tables of categories with actuals versus limits. The import page shows
raw-CSV columns alongside mapped Beancount accounts.

On these surfaces, three visual problems recur:

1. **Row tracking.** In a uniform table, the eye loses which row it was on
   after any horizontal eye movement. Zebra striping reduces this friction
   at zero layout cost.
2. **Column misalignment.** Amounts rendered in a proportional font do not
   align. The digit "1" occupies less width than "8". Scanning for the
   larger amount requires reading rather than glancing.
   `font-variant-numeric: tabular-nums` fixes this in one CSS declaration.
3. **Negative misidentification.** A red amount with no minus sign or
   parentheses can be missed entirely in a monochrome print, low-brightness
   screen, or colorblind rendering. ADR-0034 forbids color-only meaning
   carriers; this ADR specifies the concrete replacement for financial negatives.

Prose readability also degrades on very long lines. The dashboard, notes, and
description fields render prose text. Lines exceeding ~75 characters force
wider horizontal eye movement and increase fatigue.

The Phase 7 violation scan found 127 raw `<table>` elements outside the
canonical macro, the largest single violation count. The `data.table`
macro must exist before these can be migrated.

## Decision

Tables and numbers in Lamella follow a consistent set of visual conventions.
These conventions are implemented via inline CSS on `<td>` and `<th>` elements
and via CSS utility classes in `src/lamella/static/components.css`. The
canonical pattern is **inline `font-variant-numeric: tabular-nums` on money
columns plus right-alignment via a `td`/`th` class**. A `_components/data.html`
table macro was originally proposed but was never built; the inline approach is
in use across approximately 35 template sites and fully meets this ADR's intent.

Specific obligations:

1. **Zebra striping.** Tables with more than 5 data rows MUST use alternating
   row background colors. The stripe color MUST meet ≥ 3:1 contrast against
   the base row background (per ADR-0034 UI component contrast). Stark
   color alternation (e.g., white / dark gray) is forbidden; the stripe is
   subtle enough not to draw the eye away from the cell content.
2. **Tabular numbers.** Any column whose cells contain currency amounts,
   integer counts, or percentages MUST apply `font-variant-numeric: tabular-nums`
   to that column. The column MUST be right-aligned. These are two separate
   declarations; neither alone is sufficient. Both MAY be applied inline on
   the `<th>`/`<td>` element or via a shared CSS class.
3. **Negative amounts.** A negative currency amount MUST carry an explicit
   minus sign (`−`) or be parenthesized `(100.00)`. It MAY additionally be
   colored red, but color is not the primary signal.
4. **Currency symbol placement.** The currency symbol (e.g., `$`) appears on
   the first data row of a column and on any total/subtotal row. It is
   optional on interior rows for visual rhythm. When omitted from interior
   rows, the column alignment is sufficient for the user to understand the
   unit.
5. **Sticky headers.** Any `<table>` that overflows its container vertically
   MUST apply `position: sticky; top: 0` to its `<thead>` row. Without this,
   column labels scroll out of view on long lists.
6. **Row hover.** The hover state on a table row MUST be visually distinct
   from the zebra stripe. A user hovering row 7 (which has the stripe
   background) must still see a different background on hover.
7. **Prose line width.** Prose content (notes, descriptions, narrations as
   full text) MUST be constrained to `max-width: 75ch`. This applies to any
   `<p>`, prose `<div>`, or text block that wraps to multiple lines.
   `line-height` MUST be ≥ 1.5 in prose regions.
8. **Account paths in user surfaces.** Long raw account paths (e.g.,
   `Expenses:EntityA:Office:Supplies:Paper`) are displayed using a display
   name per ADR-0041 when one exists. In settings and debug pages where the
   raw path is meaningful, use a monospace font.

## Consequences

### Positive
- Amount columns align digit-for-digit across all rows. Scanning for the
  largest expense in a list is a visual task, not a reading task.
- Negatives are unambiguous in print, on colorblind renders, and on
  low-brightness screens. The minus sign or parenthesis is always present.
- Zebra striping and sticky headers are applied consistently through shared
  CSS classes; the compliance grep catches any table that omits them.

### Negative / Costs
- Applying `tabular-nums` to a font that does not support the OpenType
  feature has no visible effect. The system font stack must be verified for
  tabular-nums support on the target OS.
- Without a macro, each template author is responsible for applying the
  correct class/style themselves; the constraint is enforced by grep rather
  than compile-time macro usage.

### Mitigations
- The font stack in `src/lamella/static/app.css` uses system fonts that
  support tabular numerals on all major platforms (system-ui covers this on
  macOS, Windows, and Linux with modern font renderers).
- Lighthouse audit covers contrast of the zebra stripe; the ADR-0034 audit
  process applies here too.

## Compliance

- Grep `src/lamella/templates/` for `<th` and `<td` elements that contain
  currency amounts and do NOT carry `font-variant-numeric: tabular-nums`
  (inline or via a class that sets it). Each such column is a violation.
- Grep `src/lamella/templates/` for money-column headers/cells that are
  not right-aligned (look for absence of `text-right`, `text-end`, or
  equivalent). Right-alignment and `tabular-nums` are both required.
- Manual review: open the review queue and the budget page. Tab to the table;
  verify: sticky header stays on scroll, hover state is distinct from zebra
  stripe, amounts right-align, negatives show `−` or `()`.

## References

- `src/lamella/static/components.css`: zebra stripe, tabular-nums, sticky header CSS
- [ADR-0034](0034-wcag-2.2-aa-accessibility.md): contrast requirements apply to zebra colors
- [ADR-0041](0041-display-names-everywhere.md): display names used in table cells
