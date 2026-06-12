# ADR-0023: Datetimes Are TZ-Aware UTC At Rest, User-Local At Display

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** `CLAUDE.md`, `src/lamella/config.py` (`app_tz` field), `src/lamella/rewrite/txn_inplace.py`, `src/lamella/routes/note.py`

## Context

Naive datetimes (`datetime.now()`, `datetime.utcnow()`) carry no timezone
information. When the host timezone differs from the user's timezone, common
in Docker on a cloud host, naive "local time" snapshots are silently wrong.
Tax-year boundary computations (Dec 31 → Jan 1 rollover) done in UTC can
misattribute transactions to the wrong tax year for users in UTC-offset zones.

Several existing writers call `datetime.now()` with no argument. The Phase 7
violation scan found 37 instances (34 `datetime.now()` and 3 deprecated
`datetime.utcnow()`). This ADR closes that path. The `app_tz` field in
`Settings` (IANA tz name) carries the user's local timezone; it is the only
approved source for display conversions.

## Decision

All production code MUST use TZ-aware datetimes. Naive datetimes are forbidden.

Specific obligations:

- `datetime.now()` with no argument MUST NOT appear in production code.
  Use `datetime.now(UTC)` or `datetime.now(timezone.utc)`.
- `datetime.utcnow()` MUST NOT appear. It is deprecated in Python 3.12
  and returns a naive datetime despite the name.
- `datetime.fromtimestamp(x)` without an explicit `tz` argument MUST NOT
  appear in production code.
- Date-only values (no time component) MUST use `datetime.date`, not
  `datetime.datetime`.
- SQLite stores datetimes as ISO-8601 UTC strings
  (`2026-04-27T14:30:00+00:00`) or epoch integers. Never store a
  local-time string without a UTC offset.
- Backdated entries (notes, mileage trips) MUST carry both `event_date`
  (the user-stated date, `date` type) and `captured_at` (the wall-clock
  UTC moment of capture, TZ-aware `datetime`).
- Tax-year boundary computations MUST convert to `settings.app_tz`
  before evaluating Dec 31 / Jan 1 boundaries.
- Display layer converts UTC datetimes to `settings.app_tz` at render
  time, never at storage time.

## Consequences

### Positive
- Tax-year boundaries are correct for all UTC-offset timezones.
- Audit trails (`captured_at`) record the true wall-clock moment
  regardless of host timezone drift.
- SQLite strings sort correctly as UTC ISO-8601 without a parsing step.

### Negative / Costs
- Existing `datetime.now()` call sites (37 per Phase 7 scan) require
  remediation.
- `datetime.utcnow()` is still common in older Python idioms; reviewers
  must catch it.

### Mitigations
- AST scan catches `datetime.now()` with no args and `datetime.utcnow()`
  before they merge.
- `from datetime import UTC` (Python 3.11+) is the approved import;
  `from datetime import timezone; timezone.utc` is the fallback for
  compatibility.

## Compliance

AST scan for:
- `datetime\.now\(\)` (no argument): violation.
- `datetime\.utcnow\(\)`: violation.
- `datetime\.fromtimestamp\([^,)]+\)` (no `tz=` kwarg): violation.

Manual check: SQLite columns storing datetime values should contain
`+00:00` or `Z` suffix. Bare local-time strings without offset are violations.

## References

- CLAUDE.md § "Non-negotiable architectural rules"
- `src/lamella/config.py`: `app_tz` field (IANA timezone name)
- `src/lamella/routes/note.py`, `rewrite/txn_inplace.py`,
  `vehicles/writer.py`: known `datetime.now()` call sites
- Python 3.12 docs: `datetime.utcnow()` deprecated since 3.12
- `docs/features/mileage.md`: `captured_at` convention for backdated entries
