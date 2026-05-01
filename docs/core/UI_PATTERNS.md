---
audience: agents implementing UI surfaces
read-cost-target: 250 lines
authority: implementation cookbook (informative)
cross-refs: docs/adr/0032-component-library-per-action.md, docs/adr/0033-per-concern-api-endpoints.md, docs/adr/0034-wcag-2.2-aa-accessibility.md, docs/adr/0035-dense-data-readability.md, docs/adr/0036-instant-feedback-100ms.md, docs/adr/0037-no-full-page-reloads-preserve-scroll.md, docs/adr/0038-toast-vs-modal-usage-rules.md, docs/adr/0039-htmx-swap-failure-modes-first-class.md
---

# UI Patterns Cookbook

Implementation reference for the UI ADRs. The ADRs set the rules; this
document shows what the rules look like in code.

## Component macros (per ADR-0032)

Jinja macros live under `src/lamella/templates/_components/`. Import them at
the top of any template that needs them. Never duplicate button or table HTML
inline; use the macro.

```jinja
{% import "_components/buttons.html"      as B %}
{% import "_components/_modal.html"       as M %}
{% import "_components/data.html"         as D %}
{% import "_components/feedback.html"     as F %}
{% import "_components/_txn_actions.html" as T %}
{% import "_components/account_picker.html" as AP %}
```

### `B.btn`: generic button / link (per ADR-0032)

```jinja
{# button variants: default | primary | warn | danger | ghost #}
{{ B.btn('Save', type='submit', variant='primary') }}
{{ B.btn('Cancel', href='/back', variant='ghost') }}
{{ B.btn('Delete', variant='danger',
         confirm='Delete this account?',
         confirm_token='DELETE') }}
{{ B.btn('Sync now', icon=icon.refresh(), href='/simplefin') }}
```

Rendered shape: `<button class="btn btn-{variant}">` or `<a class="btn btn-{variant}">` when `href` is set. The `data-confirm` attribute hooks into the site-wide confirm handler in `base.html`; no per-page JS needed.

### `B.icon_btn`: icon-only button with aria-label

```jinja
{{ B.icon_btn(icon.close(), label='Close', variant='ghost') }}
```

Always supply `label`. It becomes `aria-label` and `title`. Omitting it is an a11y violation per ADR-0034.

### `T.actions`: transaction action cluster (per ADR-0032)

Renders the classify / Ask AI / ignore buttons for any transaction row.

```jinja
{# staged row #}
{{ T.actions(ref="staged:" ~ row.staged_id) }}

{# ledger row — compact for dense tables #}
{{ T.actions(ref="ledger:" ~ row.txn_hash, compact=True) }}

{# with prefilled proposal and return URL #}
{{ T.actions(ref="staged:" ~ row.staged_id,
             proposal=row.proposal,
             return_url=request.url.path) }}
```

The cluster opens inline popover forms; no route-level modal scaffolding needed. Ask-AI uses `hx-target="body" hx-swap="beforeend"` to overlay the job modal on the current page.

### `account_picker`: account autocomplete (per ADR-0011)

```jinja
{% from "_components/account_picker.html" import account_picker %}

{# blank — no prefill, popup ranks on focus #}
{{ account_picker(name='target_account') }}

{# confirmed source — no badge shown #}
{{ account_picker(name='target_account', value=proposal.account,
                  prefill_reason='rule-applied') }}

{# heuristic guess — badge + X-clear shown #}
{{ account_picker(name='target_account', value=guess,
                  prefill_reason='ai-suggestion') }}
```

The macro enforces the prefill discipline at render time: a non-empty
`value` without `edit_row=true` and without a `prefill_reason` raises a
`TemplateError`. Do not suppress it; the discipline prevents silent
missing-badge bugs.

The JS controller at `static/account_picker.js` mounts via event
delegation on `document.body` and survives HTMX swaps without re-init.

### `M.modal`: generic centered dialog (per ADR-0038)

```jinja
{% call M.modal(title='Confirm action', id='confirm-delete', open=true, size='sm') %}
  <p>This cannot be undone. Are you sure?</p>
  {% call M.actions() %}
    <button class="btn btn-ghost"
            onclick="this.closest('.modal-backdrop').classList.remove('is-open')">Cancel</button>
    <button class="btn btn-danger" form="delete-form" type="submit">Delete</button>
  {% endcall %}
{% endcall %}
```

Click-outside, Esc-to-close, and focus trapping are wired in `base.html` site-wide. The macro only renders the markup.

For confirm modals on irrevocable actions, use `data-confirm` on `B.btn` instead; the site-wide confirm handler shows a modal automatically.

### `F.banner`: inline banners (per ADR-0038)

```jinja
{# tones: info | warn | err | ok #}
{% call F.banner(tone='warn', title='Ledger has parse warnings') %}
  <ul>{% for e in errors %}<li>{{ e }}</li>{% endfor %}</ul>
{% endcall %}
```

Banners are for page-level alerts. Toast notifications (transient, auto-dismiss) are emitted via `HX-Trigger` headers; see "Toast pattern" below.

### `F.loading` and `D.progress_bar`: feedback during ops (per ADR-0036)

```jinja
{# inline spinner while partial loads #}
{{ F.loading('Fetching accounts …') }}

{# progress bar — ratio 0.0..1.0; tone auto-derived from ratio #}
{{ D.progress_bar(0.74, label='74%') }}
{{ D.progress_bar(1.05, label='105%', tone='err') }}
```

For long-running jobs, use the job modal pattern (see below), not an
inline spinner that blocks the page.

## Job modal pattern (per ADR-0006 + ADR-0036)

Any handler that calls AI, hits an external API, or iterates 50+ items
MUST run as a background job and return the `_job_modal.html` partial.

**Route:**
```python
from lamella.jobs import JobContext

async def my_handler(request: Request):
    def work(ctx: JobContext):
        ctx.set_total(len(items))
        for item in items:
            ctx.raise_if_cancelled()
            # ... process ...
            ctx.advance(1)
            ctx.emit(f"Processed {item.name}")
        ctx.emit("Done", outcome="success")

    job_id = request.app.state.job_runner.submit(work)
    return request.app.state.templates.TemplateResponse(
        request, "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": str(request.url)},
    )
```

**Form trigger:**
```html
<form hx-post="/my-action"
      hx-target="body"
      hx-swap="beforeend">
  ...
</form>
```

The modal overlays the current page. The browser polls `/jobs/{id}/partial`
every 1 s, shows progress bar + event log + Cancel button. When the job
reaches a terminal state, it sends `HX-Trigger: lamella:job-terminal` and
the polling loop self-terminates.

## Color palette (per ADR-0034 + ADR-0035)

CSS custom properties from `static/shell.css`. Both light and dark themes
are defined; reference tokens, not raw hex values.

| Token | Light value | Use |
|---|---|---|
| `--fg` | `#0f172a` | Primary text |
| `--fg-soft` | `#1f2937` | Secondary headings |
| `--muted` | `#64748b` | De-emphasized text; WCAG AA against `--bg` |
| `--muted-soft` | `#94a3b8` | Placeholders only; do NOT use for readable text |
| `--bg` | `#f7f8fa` | Page background |
| `--bg-elev` | `#ffffff` | Card / panel surface |
| `--panel-2` | `#f3f5f8` | Alternating table rows (zebra) |
| `--border` | `#e4e7ec` | Dividers |
| `--accent` | `#2563eb` | Interactive elements |
| `--ok` | `#15803d` | Success / positive |
| `--ok-soft` | `#dcfce7` | Success backgrounds |
| `--warn` | `#b45309` | Warning foreground |
| `--warn-soft` | `#fef3c7` | Warning backgrounds |
| `--err` | `#b91c1c` | Error foreground |
| `--err-soft` | `#fee2e2` | Error backgrounds |

All foreground tokens meet WCAG 2.2 AA (4.5:1) against their paired
background tokens on both themes. Verify new pairings with a contrast
checker before merging.

## Table macro: `D.account_path` + zebra rows (per ADR-0035)

No full table macro exists yet (TARGET, not yet implemented). Use these
patterns manually until one lands:

```html
<table class="data-table">
  <thead>
    <tr>
      <th>Date</th>
      <th class="num">Amount</th>  {# .num applies tabular-nums #}
      <th>Account</th>
    </tr>
  </thead>
  <tbody>
    {% for row in rows %}
    <tr class="{% if loop.index is even %}row-alt{% endif %}">
      <td>{{ row.date }}</td>
      <td class="num">{{ D.money(row.amount) }}</td>
      <td>{{ D.account_path(row.account, last_n=2) }}</td>
    </tr>
    {% endfor %}
  </tbody>
</table>
```

`D.account_path` collapses long paths: `Expenses:Acme:Office:Supplies` → `Office:Supplies` with the full path as `title`. `D.money` applies `.num` (tabular-nums) automatically and colors positive/negative amounts.

Sticky headers: add `class="data-table data-table--sticky-head"` and wrap in a scrollable container.

## HTMX patterns

### Per-concern endpoint convention (per ADR-0033)

Endpoints are grouped by the concern they serve, not by page:

```
GET  /api/accounts/suggest          → account picker suggestions partial
POST /api/txn/{ref}/classify        → classify action
POST /api/txn/{ref}/dismiss         → dismiss/ignore action
POST /api/txn/{ref}/ask-ai          → AI job submission
GET  /api/jobs/{id}/partial         → job progress partial (polled)
```

Each endpoint returns a self-contained partial. The calling component
knows which partial shape to expect from that endpoint; no page-specific
wrapper needed.

### Instant feedback: disable + indicator (per ADR-0036)

```html
<form hx-post="/api/txn/{{ ref }}/classify"
      hx-target="closest tr"
      hx-swap="outerHTML">
  <button type="submit"
          hx-disabled-elt="this"
          hx-indicator="closest .row">Apply</button>
</form>
```

`hx-disabled-elt="this"` grays the button within one frame of the click.
`hx-indicator="closest .row"` adds `.htmx-request` to the row so a CSS
spinner can appear while the request is in flight.

### No full reloads: row swap + OOB counter (per ADR-0037)

```html
<tr id="row-{{ txn.id }}"
    hx-post="/api/txn/staged:{{ txn.id }}/classify"
    hx-target="closest tr"
    hx-swap="outerHTML">
  ...
</tr>
```

Cross-cutting counts (queue badge in sidebar, review-queue header) update
via out-of-band swaps in the response:

```html
<!-- in the partial returned by classify -->
<tr id="row-{{ txn.id }}">...updated row...</tr>
<span id="queue-count" hx-swap-oob="true">{{ new_count }}</span>
```

The OOB element replaces the element with the same `id` anywhere in the
DOM without touching the primary swap target.

### Routing from write endpoints (per ADR-0037)

```python
from lamella.routes._htmx import redirect, empty, error_fragment

# Success — navigate to the list page
return redirect(request, "/review", message="Transaction classified")

# Success — row deleted, nothing to put back
return empty()

# Failure — show inline error inside the row slot
return error_fragment(
    f'<tr id="row-{txn_id}" class="row row--err">'
    f'  <td colspan="5">{html.escape(str(exc))}</td>'
    f'</tr>'
)
```

Never use `RedirectResponse` from an HTMX-targeted handler. A 30x response
on a swap-driving request follows the redirect, fetches the full list page,
and outerHTML-swaps it into the row, nesting the layout inside itself.

### Swap failure modes (per ADR-0039)

The server returns 4xx with `error_fragment`. The client handles
`htmx:responseError` globally in `base.html`:

```js
document.body.addEventListener("htmx:responseError", function (e) {
  // emit a toast from the response body or status
  showToast(e.detail.xhr.responseText || "An error occurred", "err");
});
```

Do not swallow errors silently. Every failed swap must produce either an
inline error fragment or a toast.

### Toast pattern (per ADR-0038)

Toasts fire via `HX-Trigger` response headers; no full swap needed:

```python
# In a route handler, after a successful background action:
return Response(
    status_code=204,
    headers={
        "HX-Trigger": json.dumps({
            "lamella:toast": {"message": "Settings saved", "tone": "ok"}
        }),
    },
)
```

The global handler in `base.html` listens for `lamella:toast` and renders
a dismissing overlay. Do not return toast HTML in the swap body; that
locks the toast to the swap target's position.

## Accessibility checklist (per ADR-0034)

Review this before merging any new page or component.

- [ ] All interactive elements are `<button>` or `<a>`, not `<div onclick>` or `<span onclick>`
- [ ] Every `<button>` has visible text OR an `aria-label` (icon-only buttons use `B.icon_btn`)
- [ ] Focus order follows visual reading order; no focus traps outside modals
- [ ] Modals set `role="dialog"` and `aria-modal="true"` (already in `M.modal`)
- [ ] Account pickers set `aria-autocomplete="list"` and `aria-expanded` (already in `account_picker`)
- [ ] Color is not the sole carrier of meaning (pair color with icon or text label)
- [ ] `--muted` (not `--muted-soft`) is the minimum token for readable text
- [ ] No `outline: none` without a `focus-visible` replacement (use `--ring` token)
- [ ] `font-variant-numeric: tabular-nums` on all numeric columns (use `.num` class)
- [ ] Alt text on all `<img>` elements; decorative icons carry `aria-hidden="true"`
- [ ] Contrast verified for any new color pairing (tool: polypane or browser devtools)

## Anti-patterns (forbidden)

| Anti-pattern | Why forbidden | Correct alternative |
|---|---|---|
| `<div onclick>` or `<span onclick>` for actions | Not keyboard-accessible; ADR-0034 | `<button type="button">` |
| Inline button HTML for actions covered by a macro | Diverges from standard; ADR-0032 | `B.btn`, `T.actions`, `B.icon_btn` |
| `RedirectResponse` from HTMX-targeted handlers | Causes layout nesting bug; ADR-0037 | `_htmx.redirect(request, url)` |
| `<select>` for account/entity/vehicle/project lists | Unusable at ledger scale; ADR-0011 | `account_picker` macro + `aria-autocomplete` |
| `outline: none` without focus-visible replacement | Fails keyboard a11y; ADR-0034 | `outline: none; box-shadow: var(--ring)` on `:focus-visible` |
| Color as sole carrier of meaning | Fails color-blind users; ADR-0034 | Add icon or text label alongside color |
| `hx-select` in templates | Shim ignores it silently; CLAUDE.md | Make the response be the partial |
| `hx-swap` with modifiers (e.g. `outerHTML settle:100ms`) | Shim treats as `innerHTML`; CLAUDE.md | Use bare `hx-swap="outerHTML"` |
| Synchronous handler for AI/external API/50+ items | Indistinguishable from hung process; ADR-0006 | `job_runner.submit(...)` + job modal |
| Returning full `base.html` template to HTMX swap | Nests layout inside target element; ADR-0005 | `_htmx.render(request, full=..., partial=...)` |
