# ADR-0047: Settings is a dashboard, not an editor

- **Status:** Accepted
- **Date:** 2026-04-27
- **Related:** [ADR-0011](0011-autocomplete-everywhere.md), [ADR-0048](0048-url-singular-vs-plural.md)

## Context

Early in development the `/settings` page held inline forms for every
configurable surface: entities, accounts, paperless field maps, mileage
rates, AI cost cap, all on one screen. As the configurable surface grew
to 87 sub-pages this became unmaintainable: the landing template ballooned,
state mutations leaked into multiple POST handlers on the same path, and
new contributors couldn't find where a given setting actually lived.

The `/settings` template today is mostly a tile grid linking to focused
sub-pages, but nothing prevents drift back to the inline-forms shape. As
the project preps for public release we want the boundary to be explicit
so it doesn't have to be re-derived in PR review.

## Decision

The `/settings` landing page is a **navigation dashboard**. It links to
focused sub-surfaces and holds zero stateful behavior of its own.

1. **No inline forms on `/settings` landing.** The `/settings` route
   serves a template that is a tile grid (with optional banner / status
   sections). Forms with `method="post"` to `/settings` itself are
   forbidden; the only acceptable POST on the landing path is a session
   action like dismiss-banner that does not touch user data.

2. **Every config CRUD lives at its own route.** Either at
   `/settings/<area>` (e.g. `/settings/accounts`, `/settings/backups`)
   or at a promoted top-level path (e.g. `/loans`, `/entities`,
   `/properties`) per ADR-0048. The route owns its template, its POST
   handlers, and its state.

3. **New settings tiles get added to the dashboard, never inline.**
   When a new configurable area lands, the workflow is: build the
   sub-page first, then add a tile to the `/settings` template
   pointing at it. The dashboard is purely additive.

4. **Promotion is allowed.** When a "setting" turns out to be a
   first-class user concept (loans, properties, entities), promote it
   to a top-level route and keep the `/settings/<area>` path as a 301
   alias so old bookmarks resolve. The settings dashboard tile points
   at the new canonical URL.

## Consequences

- **Easier navigation.** Settings becomes a known shape: tile grid,
  hop into focused page, do the thing, hop back.
- **Templates stay small.** The settings template doesn't grow with
  every new feature; only the tile list does.
- **Promotion is non-destructive.** Anything that grows beyond
  "settings" can move out without breaking links.
- **Bigger up-front cost** for new settings: you can't just slap a
  form on the landing page anymore. That's the desired bar.

## Migration plan

The current `/settings` template is already mostly a tile grid; the
ADR makes it explicit. Audit pass on landing:

- Verify no `<form method="post" action="/settings"…>` outside the
  banner-dismiss case.
- Verify each tile points at a path with its own template + handler.
- 301-redirect any `/settings/foo` path that was already promoted.

When a new tile category appears (e.g. AI cost cap), the route +
template come first, the tile second.

## Alternatives considered

1. **Discourse-style filterable single-page settings**: every
   configurable item visible with inline edit + search. Rich for
   power users, but a much bigger build with state-management
   complexity (optimistic updates, undo, conflict on concurrent
   tabs). Deferred as a possible v4 enhancement; the dashboard is
   v3's public-release shape.

2. **Keep the current flexible state** (no rule against inline
   forms). Rejected: drift back to one-page-everything is the
   exact failure mode we're walling off.
