# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0057 §3 — typed-envelope Beancount serializer.

Round-trip half of the reboot ETL contract. Takes the typed
envelope produced by ``reboot._typed_meta_list`` /
``_typed_meta_value`` (and the surrounding date / payee /
narration carried as ``staged_transactions`` columns) and emits a
Beancount-syntax transaction that honors LEDGER_LAYOUT.md §6.3
type rules:

* booleans → bare ``TRUE`` / ``FALSE``
* dates → bare ``YYYY-MM-DD``
* amounts → bare ``<n> <ccy>``
* decimals / integers → bare numbers
* strings → double-quoted

The serializer is the gate for the round-trip property the ADR
specifies:

    serialize(parse(serialize(parse(text)))) == serialize(parse(text))

That is — when run on already-clean Beancount input, two extract /
serialize cycles must produce byte-identical output. A regression
in the serializer that drops a meta line, mis-types a value, or
reorders postings would break this property and CI catches it.
"""
from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any


__all__ = [
    "serialize_envelope",
    "serialize_meta_value",
]


def _q(value: str) -> str:
    """Escape backslash + double-quote for a Beancount string
    literal. Mirrors writer._q."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def serialize_meta_value(envelope: dict[str, Any]) -> str:
    """Render one typed-envelope meta value as the Beancount
    literal a parser will read back as the same Python value.

    The envelope shape is ``{"type": <tag>, "value": <jsonable>}``
    produced by ``reboot._typed_meta_value``. The serializer is the
    inverse: feed the envelope, get a Beancount literal.
    """
    type_ = envelope.get("type")
    value = envelope.get("value")
    if type_ == "boolean":
        return "TRUE" if value else "FALSE"
    if type_ == "integer":
        return str(int(value))
    if type_ == "decimal":
        # ``value`` is the canonical string already (the extract
        # layer stringifies Decimal). Pass through verbatim — the
        # source string preserves the user's precision intent
        # (e.g. ``2.50`` vs ``2.5``).
        return str(value)
    if type_ == "date":
        # ISO 8601 → bare YYYY-MM-DD. Beancount's date literal is
        # the same format.
        return str(value)
    if type_ == "amount":
        if not isinstance(value, dict):
            return f'"{_q(str(value))}"'
        number = value.get("number") or "0"
        currency = value.get("currency") or "USD"
        return f"{number} {currency}"
    if type_ == "string":
        return f'"{_q(str(value))}"'
    # unknown / fallback — emit as quoted string so the file
    # still parses; round-trip property test catches regressions.
    return f'"{_q(str(value))}"'


def _serialize_meta_lines(
    meta: list[dict[str, Any]] | None,
    *,
    indent: str,
) -> list[str]:
    """Serialize a list of {key, type, value} envelopes as
    indented meta lines, in the order they were captured."""
    if not meta:
        return []
    out: list[str] = []
    for item in meta:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if not isinstance(key, str):
            continue
        rendered = serialize_meta_value(item)
        out.append(f"{indent}{key}: {rendered}\n")
    return out


def _serialize_cost(cost: dict[str, Any] | None) -> str:
    """Render a captured cost dict back to ``{<n> <ccy>}`` (or
    ``{<n> <ccy>, <date>}`` when a lot date is present, etc.).

    Beancount accepts a few cost shapes; we pick the one that
    matches whatever fields the extract layer captured. When no
    fields are present we return an empty string so the caller can
    skip the cost portion."""
    if not cost:
        return ""
    parts: list[str] = []
    if "number" in cost:
        parts.append(str(cost["number"]))
    elif "number_per" in cost:
        parts.append(str(cost["number_per"]))
    if "currency" in cost:
        parts.append(str(cost["currency"]))
    inner = " ".join(parts)
    extras: list[str] = []
    if "date" in cost:
        extras.append(str(cost["date"]))
    if "label" in cost:
        extras.append(f'"{_q(str(cost["label"]))}"')
    if extras:
        inner = inner + ", " + ", ".join(extras)
    return f" {{{inner}}}" if inner else ""


def _serialize_price(price: dict[str, Any] | None) -> str:
    """Render a captured price dict back to ``@ <n> <ccy>``."""
    if not price:
        return ""
    number = price.get("number")
    currency = price.get("currency")
    if number is None or currency is None:
        return ""
    return f" @ {number} {currency}"


def _format_amount(amount: str | None, currency: str | None) -> str:
    """Posting amount formatting: ``<n> <ccy>`` with the number
    passed through verbatim from the extract (preserves the user's
    decimal precision intent)."""
    if amount is None or currency is None:
        return ""
    return f"{amount} {currency}"


def serialize_envelope(
    *,
    date: str | datetime.date,
    payee: str | None,
    narration: str | None,
    envelope: dict[str, Any],
    posting_indent: str = "  ",
    meta_indent: str = "    ",
) -> str:
    """Render the typed envelope back to Beancount transaction
    text. Output ends with a trailing newline. Format mirrors the
    canonical writer output:

        YYYY-MM-DD <flag> ["payee"] "narration" [#tag] [^link]
          [txn-meta-key]: <value>
          <Account>  <amount> <ccy> [{cost}] [@ price]
            [posting-meta-key]: <value>
          ...

    The envelope dict is the shape ``reboot.py`` writes to
    ``staged_transactions.raw_json`` (``flag``, ``tags``,
    ``links``, ``txn_meta``, ``postings``). ``date``, ``payee``,
    ``narration`` come from the staging row's dedicated columns
    so the serializer doesn't need to also encode them in the
    envelope."""
    if isinstance(date, datetime.date):
        date_str = date.isoformat()
    else:
        date_str = str(date)[:10]

    flag = (envelope.get("flag") or "*").strip() or "*"
    tags = envelope.get("tags") or []
    links = envelope.get("links") or []
    txn_meta = envelope.get("txn_meta") or []
    postings = envelope.get("postings") or []

    header_parts: list[str] = [f"{date_str} {flag}"]
    if payee:
        header_parts.append(f'"{_q(str(payee))}"')
    header_parts.append(f'"{_q(str(narration or ""))}"')
    for t in tags:
        if isinstance(t, str) and t:
            header_parts.append(f"#{t}")
    for ln in links:
        if isinstance(ln, str) and ln:
            header_parts.append(f"^{ln}")
    out_lines: list[str] = [" ".join(header_parts) + "\n"]
    out_lines.extend(_serialize_meta_lines(
        txn_meta, indent=posting_indent,
    ))
    for p in postings:
        if not isinstance(p, dict):
            continue
        account = p.get("account")
        if not account:
            continue
        amt_text = _format_amount(p.get("amount"), p.get("currency"))
        cost_text = _serialize_cost(p.get("cost"))
        price_text = _serialize_price(p.get("price"))
        leading_flag = (p.get("flag") or "").strip()
        flag_prefix = f"{leading_flag} " if leading_flag else ""
        if amt_text:
            line = (
                f"{posting_indent}{flag_prefix}{account}"
                f"  {amt_text}{cost_text}{price_text}\n"
            )
        else:
            line = f"{posting_indent}{flag_prefix}{account}\n"
        out_lines.append(line)
        out_lines.extend(_serialize_meta_lines(
            p.get("meta"), indent=meta_indent,
        ))
    return "".join(out_lines)
