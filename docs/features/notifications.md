---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0005-htmx-endpoints-return-partials.md, docs/adr/0020-adapter-pattern-for-external-data-sources.md
last-derived-from-code: 2026-04-26
---
# Notifications

## Summary

Notification dispatch (ntfy / Pushover) for digest + ad-hoc alerts; per-channel delivery audit.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET | `/notifications` | `notifications_page` | `src/lamella/web/routes/notifications.py:104` |
| POST | `/notifications/test` | `notifications_test` | `src/lamella/web/routes/notifications.py:116` |
| POST | `/notifications/{row_id}/resend` | `notifications_resend` | `src/lamella/web/routes/notifications.py:152` |

## Owned templates

- `src/lamella/web/templates/notifications.html`

## Owned source files

- `src/lamella/adapters/ntfy/client.py`
- `src/lamella/adapters/pushover/client.py`
- `src/lamella/features/notifications/digests.py`
- `src/lamella/features/notifications/dispatcher.py`

## Owned tests

- `tests/test_notify_digest.py`
- `tests/test_notify_dispatcher.py`
- `tests/test_notify_ntfy.py`
- `tests/test_notify_pushover.py`
- `tests/test_simplefin_notify_hook.py`

## ADR compliance

- ADR-0020: `Notifier` ABC in `notify/base.py` is the adapter port; ntfy and
  Pushover are concrete adapters. Adding a channel means adding a new
  `Notifier` subclass, no dispatcher changes required.
- ADR-0008: Dedup enforced in `Dispatcher._recently_delivered` via a 24-hour
  window keyed on `dedup_key`. Deduped attempts still log a row so the audit
  table is honest.
- ADR-0006: No long-running handler, notifications are dispatched inline from
  the APScheduler job (`_run_weekly_digest` at 09:00 daily) or from the ingest
  path. Neither blocks an HTTP handler.

## Current state


### Compliant ADRs
- ADR-0020: `Notifier` ABC in `notify/base.py` is the adapter port; ntfy and
  Pushover are concrete adapters. Adding a channel means adding a new
  `Notifier` subclass, no dispatcher changes required.
- ADR-0008: Dedup enforced in `Dispatcher._recently_delivered` via a 24-hour
  window keyed on `dedup_key`. Deduped attempts still log a row so the audit
  table is honest.
- ADR-0006: No long-running handler, notifications are dispatched inline from
  the APScheduler job (`_run_weekly_digest` at 09:00 daily) or from the ingest
  path. Neither blocks an HTTP handler.

### Known violations
- ADR-0005: `POST /notifications/test` and `POST /notifications/{id}/resend`
  return full-page `TemplateResponse("notifications.html", ...)` even when
  `HX-Request` is set. No partial template exists. The page re-renders correctly
  but the swap target receives a full shell.
- ADR-0015 (reconstruct): Digest "already sent this week" state comes from the
  `notifications` table (cache). The digest's `dedup_key` is
  `digest:<ISO-YYYY-WW>`, so the Dispatcher's 24-hour window actually suppresses
  re-sends within a day, not the full week. A wiped SQLite means the digest
  could re-send on the next scheduler fire. This is acceptable for a cache
  (digest is not user-configured state), but worth noting.
- No email channel exists. Two channels today: ntfy and Pushover.

## Known gaps

- ADR-0005: `POST /notifications/test` and `POST /notifications/{id}/resend`
  return full-page `TemplateResponse("notifications.html", ...)` even when
  `HX-Request` is set. No partial template exists. The page re-renders correctly
  but the swap target receives a full shell.
- ADR-0015 (reconstruct): Digest "already sent this week" state comes from the
  `notifications` table (cache). The digest's `dedup_key` is
  `digest:<ISO-YYYY-WW>`, so the Dispatcher's 24-hour window actually suppresses
  re-sends within a day, not the full week. A wiped SQLite means the digest
  could re-send on the next scheduler fire. This is acceptable for a cache
  (digest is not user-configured state), but worth noting.
- No email channel exists. Two channels today: ntfy and Pushover.

## Remaining tasks

- Add partial template `partials/_notification_result.html` for HTMX swaps on
  test/resend actions (fixes ADR-0005 violation).
- Consider a `digest_dedup_window = 6 * 24 * 3600` override in the weekly job
  so a wiped DB does not re-send the same week's digest more than once.
- Wire Pushover priority mapping: URGENT → Pushover priority 1, WARN → 0,
  INFO → -1 (currently unverified against live Pushover spec).
