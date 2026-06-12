# ADR-0025: Logs Identify Entities, Never Expose Values

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** `CLAUDE.md`, `audits/pii-leak/2026-04-26.md`, [ADR-0017](0017-example-data-policy.md)

## Context

Lamella is a single-user financial system. Every transaction carries a
payee name, an amount, a narration, and often a card's last-four digits.
Log records flow to container stdout, which is collected by the host's
log viewer, crash-report tools, and any syslog forwarder the operator
adds. A log line like `classified $123.45 at Acme Plumbing` exposes a
financial amount and a merchant name to every destination the operator
has configured, not just the local dev terminal.

The distinction is: the user viewing their own data in the UI owns that
data and can see values. Logs and telemetry go to systems the user may
not fully control, and may be shared with support or stored long-term.
Values in logs are PII; identifiers are not.

The `/pii-leak-check` baseline at `audits/pii-leak/2026-04-26.md`
records 4 confirmed violations (3 in `paperless/client.py`, 1 each in
`setup_wizard.py` and `step2_classification_rules.py`). This ADR defines
the rule going forward.

## Decision

Log statements MUST identify transactions and entities by opaque
identifiers. They MUST NOT include monetary amounts, payee strings,
narration text, card last-four digits, email addresses, or phone numbers.

Specific obligations:

- `logger.info(f"...{amount}...")` is forbidden when `amount` carries
  a real monetary value. Log the `txn_id` instead.
- `logger.*(f"...{payee}...")` and `logger.*(f"...{narration}...")`
  are forbidden. Log the `txn_id` and `account_path`.
- Acceptable pattern:
  `logger.info("classified txn=%s account=%s", txn_id, account_path)`
- Forbidden pattern:
  `logger.info("classified $%s at %s", amount, payee)`
- Error messages surfaced to the user UI MAY include amounts and payees.
  The user owns their own data and the message stays in-browser.
- Error messages that flow to the application log (Python `logger.*`
  calls, `print(...)` to stdout) MUST NOT include values.
- AI-decision audit fields stored in `ai_decisions` (SQLite) MAY store
  identifiers. They MUST NOT store raw payee or amount values from the
  transaction if those fields would also appear in log output.
- New `/pii-leak-check` violations detected via the regex scan are
  blocking. They prevent merge until resolved.

## Consequences

### Positive
- Log records are safe to share in support tickets and operator dashboards.
- Operator syslog forwarders cannot accidentally collect financial PII.
- Compliance posture: logs satisfy basic PII minimization without
  a scrubbing pipeline.

### Negative / Costs
- Debugging classifier behavior requires cross-referencing `txn_id`
  against the ledger rather than reading the log directly.
- Existing log call sites that include amounts or payees must be
  remediated (4 confirmed sites tracked in `audits/pii-leak/2026-04-26.md`).

### Mitigations
- Developer debug logging at `DEBUG` level MAY be more verbose in
  local development, but MUST be gated on a `DEBUG`-level logger so
  it does not appear in default `INFO` production output.
- The `txn_id` (UUIDv7) is the bridge between a log line and the
  full transaction context in the UI. Operators follow the id to the
  transaction detail page.

## Compliance

`/pii-leak-check` (baseline: `audits/pii-leak/2026-04-26.md`).

Regex patterns applied to `logger.|log.|print(` call sites:
- Variable references matching `amount|balance|payee|narration|
  last_four|email|phone` inside f-strings or `%`-format args are
  flagged.
- New violations vs. the baseline are blocking.
- Manual review required for any new logger call that formats
  transaction-level data.

## References

- CLAUDE.md § "Non-negotiable architectural rules"
- `audits/pii-leak/2026-04-26.md`: current baseline (4 confirmed violations)
- [ADR-0017](0017-example-data-policy.md): example data policy (same PII discipline for code)
- `src/lamella/ai/classify.py`: classifier log call sites (verify in module)
- `src/lamella/simplefin/ingest.py`: ingest log call sites (verify in module)
