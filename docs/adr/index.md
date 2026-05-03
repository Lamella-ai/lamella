# Architecture Decision Records

This directory holds Lamella's Architecture Decision Records. An
ADR is a one-page record of a decision that's load-bearing enough
that someone six months from now will wonder why we did it the way
we did. The format is defined in `.claude/commands/adr-check.md`;
the helper for finding existing ADRs is `scripts/adr_check.py`.

ADRs are immutable once accepted. Updating a decision means writing
a new ADR that supersedes the old one and editing the old one's
status line to point at the successor.

## Index

| #    | Date       | Status   | Title                                                                    |
|------|------------|----------|--------------------------------------------------------------------------|
| 0001 | 2026-04-26 | Accepted | Beancount ledger is the single source of truth                           |
| 0002 | 2026-04-26 | Accepted | In-place .bean rewrites are the default; overrides are the fallback     |
| 0003 | 2026-04-26 | Accepted | `lamella-*` is the metadata namespace; legacy prefixes are read-compat   |
| 0004 | 2026-04-26 | Accepted | Bean-check after every write                                             |
| 0005 | 2026-04-26 | Accepted | HTMX endpoints return partials, not full pages                           |
| 0006 | 2026-04-26 | Accepted | Long-running operations run as background jobs                           |
| 0007 | 2026-04-26 | Accepted | Entity-first account hierarchy                                           |
| 0008 | 2026-04-26 | Accepted | Dedup unconditionally                                                    |
| 0009 | 2026-04-26 | Accepted | Card binding is a starting hypothesis, not ground truth                  |
| 0010 | 2026-04-26 | Accepted | Rules are signals, not commands                                          |
| 0011 | 2026-04-26 | Accepted | Autocomplete everywhere for ledger-derived lists                         |
| 0012 | 2026-04-26 | Accepted | Multi-agent work dispatches via ruflo MCP, not Task                      |
| 0013 | 2026-04-26 | Accepted | Workers verify their own commits                                         |
| 0014 | 2026-04-26 | Accepted | Memory, write everything, list to recall                                 |
| 0015 | 2026-04-26 | Accepted | Reconstruct capability is a shipping gate                                |
| 0016 | 2026-04-26 | Accepted | Paperless writeback policy                                               |
| 0017 | 2026-04-26 | Accepted | Example and placeholder data policy                                      |
| 0018 | 2026-04-26 | Accepted | Classification is intentionally slow                                     |
| 0019 | 2026-04-26 | Accepted | Transaction identity reads must use `identity.py` helpers                |
| 0020 | 2026-04-26 | Accepted | Adapter pattern for external data sources                                |
| 0021 | 2026-04-27 | Accepted | Configuration reads go through the Settings service                      |
| 0022 | 2026-04-27 | Accepted | Money is `Decimal`, never `float`                                        |
| 0023 | 2026-04-27 | Accepted | Datetimes are TZ-aware UTC at rest, user-local at display                |
| 0024 | 2026-04-27 | Accepted | Tests hit real SQLite and real Beancount fixtures                        |
| 0025 | 2026-04-27 | Accepted | Logs identify entities, never expose values                              |
| 0026 | 2026-04-27 | Accepted | Migrations are forward-only, append-only                                 |
| 0027 | 2026-04-27 | Accepted | External HTTP calls use tenacity + 30s timeout                           |
| 0028 | 2026-04-27 | Accepted | UI stack lock: HTMX + vanilla CSS, no JS frameworks                      |
| 0029 | 2026-04-27 | Accepted | SQL is parameterized; subprocess args are list-form                      |
| 0030 | 2026-04-27 | Accepted | File operations validate paths against allowed roots (project-wide)      |
| 0031 | 2026-04-27 | Accepted | Slugs are immutable per-parent identifiers                               |
| 0032 | 2026-04-27 | Accepted | Component library: one Jinja macro per action, reused everywhere         |
| 0033 | 2026-04-27 | Accepted | Per-concern API endpoints: one URL per action across all surfaces        |
| 0034 | 2026-04-27 | Accepted | Accessibility: WCAG 2.2 AA minimum                                       |
| 0035 | 2026-04-27 | Accepted | Dense data readability: tables, numbers, eye-strain                      |
| 0036 | 2026-04-27 | Accepted | Every user action acknowledges within 100ms                              |
| 0037 | 2026-04-27 | Accepted | In-page actions do not reload the page or disturb scroll position        |
| 0038 | 2026-04-27 | Accepted | Toasts for transient feedback; modals only for confirmation or long-form |
| 0039 | 2026-04-27 | Accepted | HTMX swap failure modes are first-class                                  |
| 0040 | 2026-04-27 | Accepted | Source code is organized by concern type, not flat under `src/lamella/`  |
| 0041 | 2026-04-27 | Accepted | Display names everywhere; account paths are implementation detail        |
| 0042 | 2026-04-27 | Accepted | Chart of Accounts Standard                                               |
| 0043 | 2026-04-27 | Accepted | Unclassified bank data is staged via `custom` directives, not FIXME      |
| 0043b| 2026-04-29 | Accepted | Staged-txn directive shape: frozen spec, migration landed in v0.3.1     |
| 0044 | 2026-04-27 | Accepted | Paperless writeback uses `Lamella_`-prefixed custom fields               |
| 0045 | 2026-04-27 | Accepted | Beancount account segments must start with `[A-Z]`                       |
| 0046 | 2026-04-27 | Accepted | Synthetic transfer counterparts: replaceable placeholders                |
| 0047 | 2026-04-27 | Accepted | Settings is a dashboard, not an editor                                   |
| 0048 | 2026-04-27 | Accepted | URL singular vs plural conventions                                       |
| 0049 | 2026-04-27 | Accepted | Form validation + save-before-side-effect                                |
| 0050 | 2026-04-27 | Accepted | Optional authentication with financial-grade defaults                    |
| 0051 | 2026-04-29 | Accepted | Display-layer slug normalization: internal slugs never leak into UI     |
| 0052 | 2026-04-29 | Accepted | Versioning policy: when the app version bumps                            |
| 0053 | 2026-04-29 | Accepted | Paperless read precedence + integration self-test                        |
| 0054 | 2026-04-29 | Accepted | Linked-txn hypothesis is ground truth in receipt verify                  |
| 0055 | 2026-04-29 | Accepted | AI prompts must be generalized, never overfit to a specific failure      |
| 0056 | 2026-04-28 | Accepted | Receipt-linking is a pre-classification affordance on every txn list     |
| 0057 | 2026-04-29 | Accepted | Reboot is a round-trip ETL — extract, transform, re-emit, validate       |
| 0058 | 2026-04-29 | Accepted | Cross-source intake-time deduplication                                   |
| 0059 | 2026-04-29 | Accepted | Per-source description preservation + AI-synthesized canonical narration |
| 0060 | 2026-04-29 | Accepted | Imported-file archive + per-file source identity                         |
| 0061 | 2026-05-02 | Accepted | Documents abstraction and ledger v4 cutover                              |
| 0062 | 2026-05-02 | Accepted | Tag-driven workflow engine for Paperless documents                       |
| 0063 | 2026-05-02 | Accepted | Bidirectional document ↔ transaction matching                            |
| 0064 | 2026-05-02 | Accepted | Paperless namespace uses colon separator (`Lamella:X`)                   |
| 0065 | 2026-05-02 | Accepted | User-defined tag→action bindings with ledger source-of-truth             |

## Adding a new ADR

1. Run `python3 scripts/adr_check.py <topic>` to confirm an
   existing record doesn't already cover it.
2. Use the next number printed by `python3 scripts/adr_check.py --next`.
3. Copy the template from `.claude/commands/adr-check.md`.
4. File path: `docs/adr/NNNN-<short-slug>.md`.
5. Append a row to the table above.

## Companion docs

- `docs/specs/CHART_OF_ACCOUNTS.md`: companion to ADR-0042 (publishable spec)
- `docs/core/UI_PATTERNS.md`: companion to ADR-0032/0034/0035/0036/0037/0038/0039
- `docs/core/UI_LANGUAGE.md`: companion to ADR-0041 (renderer functions + translation table)
