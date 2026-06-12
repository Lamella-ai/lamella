# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Vector index over resolved transactions + user corrections.

NEXTGEN Phase H. Replaces the substring-match recency window in
``similar_transactions`` with semantic similarity across the full
resolved history, weighted so user corrections outrank original
classifications and recent entries outrank old ones.

Design choices (load-bearing — the user explicitly prescribed
these, don't quietly adjust):

* **Local embeddings only.** No OpenAI embeddings API, no
  network dependency at classification time. ``sentence-transformers``
  is a soft dependency: if it's not installed and the flag is on,
  the module logs a warning and callers fall back to the substring
  path. Tests inject a deterministic fake embedder.
* **Corrections weighted higher than ledger rows.** When the user
  overrode a past AI classification (``ai_decisions.user_corrected=1``),
  the correction's text is embedded at weight
  ``DEFAULT_CORRECTION_WEIGHT`` so it outranks the plain ledger
  entry at retrieval time. This is the most important part given
  Phase E will surface lots of corrections.
* **Rebuild on ledger invalidation.** The query path checks the
  current ledger signature against the stored build signature; if
  stale, a rebuild runs before the query completes. Stale index is
  worse than a missing index because it returns plausible-looking
  wrong answers.
* **Cache, not state.** `staged_transactions` is cache;
  `txn_embeddings` is cache. If the DB is wiped, rebuild from
  ledger + ai_decisions. No reconstruct pass registered.

The query-returned ``SimilarTxn``-shaped records slot into the
existing classify prompt without any prompt changes.
"""
from __future__ import annotations

import logging
import math
import struct
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Callable, Iterable, Sequence

from beancount.core.data import Transaction

from lamella.features.ai_cascade.context import SimilarTxn
from lamella.core.beancount_io.txn_hash import txn_hash as compute_txn_hash

log = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_MODEL",
    "DEFAULT_CORRECTION_WEIGHT",
    "DEFAULT_RECENCY_HALF_LIFE_DAYS",
    "VectorIndex",
    "VectorUnavailable",
    "similar_transactions_via_vector",
]


def _ledger_part(sig: str) -> str:
    """Return just the ``len:max_date`` prefix of a ledger_signature.

    Signature format is ``N:YYYY-MM-DD:c<n>:l<id>``. The first two
    segments encode the ledger state (txn count + latest date);
    the trailing ``c…:l…`` encode the correction count and last
    correction id. Comparing just the ledger part tells us
    whether the ledger changed."""
    if not sig:
        return ""
    parts = sig.split(":")
    if len(parts) < 2:
        return sig
    return f"{parts[0]}:{parts[1]}"


def _corrections_part(sig: str) -> str:
    if not sig:
        return ""
    parts = sig.split(":")
    if len(parts) < 3:
        return ""
    return ":".join(parts[2:])


DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_CORRECTION_WEIGHT = 2.0
DEFAULT_RECENCY_HALF_LIFE_DAYS = 365   # a year-old match is worth ½ as much


EmbedFn = Callable[[Sequence[str]], Sequence[Sequence[float]]]


class VectorUnavailable(Exception):
    """sentence-transformers isn't installed (or failed to load).
    Callers should fall back to the substring path."""


# ------------------------------------------------------------------
# Embedding backend — lazy soft-import.
# ------------------------------------------------------------------


def _default_embed_fn(model_name: str) -> EmbedFn:
    """Returns an ``embed(texts) -> vectors`` callable backed by
    ``sentence-transformers``. Raises VectorUnavailable when the
    library is not installed."""
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore
    except ImportError as exc:
        raise VectorUnavailable(
            "sentence-transformers not installed. Run "
            "`uv add sentence-transformers` (or pip install) to enable "
            "Phase H vector search."
        ) from exc

    model = SentenceTransformer(model_name)

    def _embed(texts: Sequence[str]) -> Sequence[Sequence[float]]:
        import numpy as np
        vectors = model.encode(
            list(texts),
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return np.asarray(vectors, dtype="float32").tolist()

    return _embed


# ------------------------------------------------------------------
# Storage helpers — raw float32 buffers in SQLite BLOB.
# ------------------------------------------------------------------


def _pack(vec: Sequence[float]) -> bytes:
    return struct.pack(f"{len(vec)}f", *[float(v) for v in vec])


def _unpack(buf: bytes) -> tuple[float, ...]:
    n = len(buf) // 4
    return struct.unpack(f"{n}f", buf)


def _cosine(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine similarity. Assumes both vectors are already L2-normalized
    (which sentence-transformers.encode(..., normalize_embeddings=True)
    guarantees)."""
    n = min(len(a), len(b))
    return sum(a[i] * b[i] for i in range(n))


# ------------------------------------------------------------------
# Input text composition — the string that gets embedded.
# ------------------------------------------------------------------


def _compose_ledger_text(
    *, payee: str | None, narration: str | None, target_account: str | None,
) -> str:
    """The thing we embed for a ledger txn. Putting the target
    account into the embedding text anchors the vector to its
    classification — when querying with a merchant string, related
    merchants that classify to the same category cluster together."""
    bits = []
    if payee:
        bits.append(payee.strip())
    if narration:
        bits.append(narration.strip())
    text = " ".join(bits).strip()
    if target_account:
        text = f"{text} -> {target_account}" if text else target_account
    return text


def _compose_correction_text(
    *, merchant_text: str, corrected_account: str,
) -> str:
    """A user correction embedded at its corrected form. The
    original AI guess is discarded — we want the index to retrieve
    the *right* answer."""
    return f"{merchant_text} -> {corrected_account}".strip()


# ------------------------------------------------------------------
# Recency decay — older matches still contribute but worth less.
# ------------------------------------------------------------------


def _recency_factor(
    entry_date: date, reference_date: date,
    *, half_life_days: int = DEFAULT_RECENCY_HALF_LIFE_DAYS,
) -> float:
    if half_life_days <= 0:
        return 1.0
    delta = abs((reference_date - entry_date).days)
    return 0.5 ** (delta / half_life_days)


# ------------------------------------------------------------------
# The index.
# ------------------------------------------------------------------


@dataclass(frozen=True)
class VectorMatch:
    """One match from a vector query, before conversion to SimilarTxn."""
    source: str                  # 'ledger' | 'correction'
    merchant_text: str
    target_account: str
    posting_date: date
    amount: Decimal
    similarity: float            # raw cosine
    recency_factor: float
    weight: float                # 1.0 for ledger, > 1.0 for corrections
    score: float                 # similarity * recency * weight


class VectorIndex:
    """Build + query the vector index.

    Every public method is safe to call with vector search disabled
    — the caller guards via the ``ai_vector_search_enabled`` setting.
    When enabled but sentence-transformers can't load, methods raise
    ``VectorUnavailable`` so the caller can fall back cleanly.
    """

    def __init__(
        self,
        conn,
        *,
        model_name: str = DEFAULT_MODEL,
        embed_fn: EmbedFn | None = None,
        correction_weight: float = DEFAULT_CORRECTION_WEIGHT,
    ):
        self.conn = conn
        self.model_name = model_name
        self.correction_weight = correction_weight
        self._embed_fn: EmbedFn | None = embed_fn  # None → lazy default

    # -- lazy embed backend --------------------------------------------

    def _embed(self) -> EmbedFn:
        if self._embed_fn is None:
            self._embed_fn = _default_embed_fn(self.model_name)
        return self._embed_fn

    def embed_one(self, text: str) -> Sequence[float]:
        """Embed a single string. Used by queries."""
        return self._embed()([text])[0]

    # -- build ---------------------------------------------------------

    def build(
        self,
        *,
        entries: Iterable,
        ai_decisions: "DecisionsLog | None" = None,
        ledger_signature: str = "",
        force: bool = False,
        trigger: str = "classify",
    ) -> dict[str, int]:
        """Build (or incrementally refresh) the index.

        When ``force`` is False, skips the rebuild if the stored
        ledger_signature matches the supplied one — the index is
        already up to date. When it runs, upserts one row per
        resolved ledger txn and one row per user_corrected=1
        ai_decisions entry.
        """
        stored_signature = ""
        if not force:
            row = self.conn.execute(
                "SELECT ledger_signature FROM txn_embeddings_build "
                "WHERE source = 'ledger' AND model_name = ?",
                (self.model_name,),
            ).fetchone()
            if row:
                stored_signature = row["ledger_signature"] or ""
            if (
                row and stored_signature == ledger_signature
                and ledger_signature
            ):
                return {"ledger_added": 0, "corrections_added": 0, "skipped_stale": 0}

        # Fast-path re-embed: ledger part of signature unchanged
        # but corrections-part moved → skip ledger iteration.
        skip_ledger_pass = (
            not force
            and _ledger_part(stored_signature) == _ledger_part(ledger_signature)
            and _ledger_part(ledger_signature)
            and _corrections_part(stored_signature) != _corrections_part(ledger_signature)
        )

        # Progress tracking. Insert a 'building' run-row at start;
        # flip to 'complete' at the end (or 'error' on exception).
        # The /status page reads this to show spinners / disable
        # rebuild buttons while a build is in flight.
        run_id: int | None = None
        try:
            cursor = self.conn.execute(
                "INSERT INTO vector_index_runs (state, trigger) "
                "VALUES ('building', ?)",
                (trigger,),
            )
            run_id = int(cursor.lastrowid)
        except Exception:  # noqa: BLE001 — migration 030 might not be applied
            run_id = None

        try:
            return self._build_body(
                entries=entries, ai_decisions=ai_decisions,
                ledger_signature=ledger_signature,
                skip_ledger_pass=skip_ledger_pass,
                stats={"ledger_added": 0, "corrections_added": 0},
                run_id=run_id,
            )
        except Exception as exc:
            if run_id is not None:
                try:
                    self.conn.execute(
                        "UPDATE vector_index_runs "
                        "SET state = 'error', finished_at = datetime('now'), "
                        "    error_message = ? "
                        "WHERE id = ? AND state = 'building'",
                        (str(exc)[:500], run_id),
                    )
                except Exception:  # noqa: BLE001
                    pass
            raise

    def _build_body(
        self,
        *,
        entries,
        ai_decisions,
        ledger_signature: str,
        skip_ledger_pass: bool,
        stats: dict,
        run_id: int | None,
    ) -> dict[str, int]:

        # ---- ledger side --------------------------------------------
        to_embed: list[tuple[str, str, str, str, str, str]] = []
        # (identity, merchant_text, target_account, posting_date, amount, file/lineno_ref)
        file_line_refs: list[tuple[str | None, int | None]] = []
        if skip_ledger_pass:
            # Fast path — ledger unchanged since last build. Skip
            # re-embedding every ledger row and jump straight to
            # the corrections side.
            log.info(
                "vector index fast-path: ledger unchanged, "
                "re-embedding corrections only"
            )
        iterable_entries = [] if skip_ledger_pass else entries
        for entry in iterable_entries:
            if not isinstance(entry, Transaction):
                continue
            target = _first_resolved_target(entry)
            if target is None:
                continue
            payee = (entry.payee or "").strip() or None
            narration = (entry.narration or "").strip() or None
            merchant_text = " ".join(filter(None, [payee, narration])).strip()
            if not merchant_text:
                continue
            th = compute_txn_hash(entry)
            amount = _first_target_amount(entry, target)
            meta = getattr(entry, "meta", None) or {}
            file = str(meta.get("filename") or "") or None
            lineno = meta.get("lineno")
            lineno = int(lineno) if lineno is not None else None
            if isinstance(file, str) and file.startswith("<"):
                # Plugin-synthesized entries aren't real source lines; skip.
                continue
            embed_text = _compose_ledger_text(
                payee=payee, narration=narration, target_account=target,
            )
            to_embed.append(
                (
                    th, embed_text, merchant_text, target,
                    entry.date.isoformat(), str(amount),
                )
            )
            file_line_refs.append((file, lineno))

        if to_embed:
            # Up front: tell the run row how many rows are
            # expected, so /status can render a meaningful
            # "N of M embedded" progress line.
            if run_id is not None:
                try:
                    self.conn.execute(
                        "UPDATE vector_index_runs SET total = ? WHERE id = ?",
                        (len(to_embed), run_id),
                    )
                except Exception:  # noqa: BLE001
                    pass
            # Batch embed + upsert. Previously this embedded all
            # N rows in a single call, which meant no DB writes
            # until the entire batch completed — a 20k-txn ledger
            # showed "0 embedded" for 20 minutes before a sudden
            # jump at the end. Batching flushes embeddings every
            # EMBED_BATCH rows so /status can show live progress
            # AND memory stays bounded for very large ledgers.
            EMBED_BATCH = 128
            embed_fn = self._embed()
            processed = 0
            for batch_start in range(0, len(to_embed), EMBED_BATCH):
                batch = to_embed[batch_start:batch_start + EMBED_BATCH]
                batch_refs = file_line_refs[batch_start:batch_start + EMBED_BATCH]
                batch_texts = [t[1] for t in batch]
                batch_vectors = embed_fn(batch_texts)
                for (
                    (identity, _text, merchant_text, target, date_str, amt_str),
                    vec,
                    (file, lineno),
                ) in zip(batch, batch_vectors, batch_refs):
                    self._upsert(
                        source="ledger", identity=identity,
                        file=file, lineno=lineno,
                        merchant_text=merchant_text, target_account=target,
                        posting_date=date_str, amount=amt_str,
                        weight=1.0, vec=vec,
                    )
                    stats["ledger_added"] += 1
                    processed += 1
                if run_id is not None:
                    try:
                        self.conn.execute(
                            "UPDATE vector_index_runs SET processed = ? "
                            "WHERE id = ?",
                            (processed, run_id),
                        )
                    except Exception:  # noqa: BLE001
                        pass
                log.info(
                    "vector index progress: %d / %d embedded",
                    processed, len(to_embed),
                )

        # ---- corrections side ---------------------------------------
        if ai_decisions is not None:
            rows = ai_decisions.conn.execute(
                "SELECT id, input_ref, result, user_correction "
                "FROM ai_decisions "
                "WHERE decision_type = 'classify_txn' AND user_corrected = 1"
            ).fetchall()
            correction_inputs: list[tuple[str, str, str, str, str]] = []
            # (identity, text_to_embed, merchant_text, corrected_account, input_ref)
            for r in rows:
                correction = r["user_correction"]
                if not correction:
                    continue
                corrected_account = _extract_corrected_account(correction)
                if not corrected_account:
                    continue
                # Look up the ledger entry the correction applied to, so we
                # can capture merchant_text + date + amount.
                anchor = _find_txn_by_hash(entries, r["input_ref"])
                if anchor is None:
                    continue
                merchant_text = " ".join(filter(None, [
                    (anchor.payee or "").strip() or None,
                    (anchor.narration or "").strip() or None,
                ])).strip()
                if not merchant_text:
                    continue
                embed_text = _compose_correction_text(
                    merchant_text=merchant_text,
                    corrected_account=corrected_account,
                )
                amount = _first_target_amount(anchor, corrected_account) or Decimal("0")
                correction_inputs.append(
                    (
                        str(r["id"]), embed_text, merchant_text,
                        corrected_account, anchor.date.isoformat(),
                    )
                )
            if correction_inputs:
                texts = [t[1] for t in correction_inputs]
                vectors = self._embed()(texts)
                for (identity, _text, merchant_text, account, date_str), vec in zip(
                    correction_inputs, vectors,
                ):
                    self._upsert(
                        source="correction", identity=identity,
                        file=None, lineno=None,
                        merchant_text=merchant_text, target_account=account,
                        posting_date=date_str, amount="0",
                        weight=self.correction_weight, vec=vec,
                    )
                    stats["corrections_added"] += 1

        # Record the build signature so subsequent calls can skip.
        self.conn.execute(
            """
            INSERT INTO txn_embeddings_build
                (source, model_name, ledger_signature, row_count)
            VALUES ('ledger', ?, ?, ?)
            ON CONFLICT(source, model_name) DO UPDATE SET
                ledger_signature = excluded.ledger_signature,
                row_count = excluded.row_count,
                built_at = datetime('now')
            """,
            (
                self.model_name,
                ledger_signature,
                stats["ledger_added"] + stats["corrections_added"],
            ),
        )
        self.conn.commit()
        log.info(
            "vector index built: %d ledger + %d corrections (model=%s)",
            stats["ledger_added"], stats["corrections_added"], self.model_name,
        )
        if run_id is not None:
            try:
                total = stats["ledger_added"] + stats["corrections_added"]
                self.conn.execute(
                    "UPDATE vector_index_runs "
                    "SET state = 'complete', finished_at = datetime('now'), "
                    "    total = ?, processed = ? "
                    "WHERE id = ?",
                    (total, total, run_id),
                )
                self.conn.commit()
            except Exception:  # noqa: BLE001
                pass
        return stats

    def _upsert(
        self, *, source, identity, file, lineno, merchant_text,
        target_account, posting_date, amount, weight, vec,
    ):
        self.conn.execute(
            """
            INSERT INTO txn_embeddings
                (source, identity, file, lineno, merchant_text,
                 target_account, posting_date, amount, weight,
                 embedding, dims, model_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, identity) DO UPDATE SET
                file = excluded.file,
                lineno = excluded.lineno,
                merchant_text = excluded.merchant_text,
                target_account = excluded.target_account,
                posting_date = excluded.posting_date,
                amount = excluded.amount,
                weight = excluded.weight,
                embedding = excluded.embedding,
                dims = excluded.dims,
                model_name = excluded.model_name,
                created_at = datetime('now')
            """,
            (
                source, identity, file, lineno, merchant_text,
                target_account, posting_date, amount, weight,
                _pack(vec), len(vec), self.model_name,
            ),
        )

    # -- query ---------------------------------------------------------

    def query(
        self,
        *,
        needle: str,
        reference_date: date,
        limit: int = 5,
        min_similarity: float = 0.25,
        half_life_days: int = DEFAULT_RECENCY_HALF_LIFE_DAYS,
        target_roots: Sequence[str] | None = None,
    ) -> list[VectorMatch]:
        """Return top-K matches ranked by ``similarity * recency_factor * weight``.

        ``min_similarity`` filters out obviously-unrelated rows —
        cosine similarity below this is treated as no match. With
        sentence-transformers + short merchant text, 0.25 is a
        reasonable floor.

        ``target_roots`` scopes the candidate pool by the account
        root of each embedded row's target_account (e.g.,
        ``("Expenses",)`` only returns rows whose target is an
        Expenses:* account). This keeps expense-classification
        queries from seeing "PAYMENT THANK YOU CHASE" or other
        non-expense patterns as neighbors once the index is widened
        (Phase 1 of AI-AGENT.md). ``None`` means no filter.
        """
        if not needle.strip():
            return []
        roots = tuple(r for r in (target_roots or ()) if r)
        if roots:
            placeholders = " OR ".join(["target_account GLOB ?"] * len(roots))
            params: list[object] = [self.model_name]
            params.extend(f"{r}:*" for r in roots)
            rows = self.conn.execute(
                "SELECT source, merchant_text, target_account, posting_date, "
                "amount, weight, embedding FROM txn_embeddings "
                f"WHERE model_name = ? AND ({placeholders})",
                params,
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT source, merchant_text, target_account, posting_date, "
                "amount, weight, embedding FROM txn_embeddings "
                "WHERE model_name = ?",
                (self.model_name,),
            ).fetchall()
        if not rows:
            return []
        query_vec = self.embed_one(needle)
        matches: list[VectorMatch] = []
        for r in rows:
            vec = _unpack(r["embedding"])
            sim = _cosine(query_vec, vec)
            if sim < min_similarity:
                continue
            try:
                entry_date = date.fromisoformat(r["posting_date"])
            except (ValueError, TypeError):
                entry_date = reference_date
            rec = _recency_factor(
                entry_date, reference_date, half_life_days=half_life_days,
            )
            weight = float(r["weight"] or 1.0)
            try:
                amount = Decimal(r["amount"] or "0")
            except Exception:  # noqa: BLE001
                amount = Decimal("0")
            matches.append(
                VectorMatch(
                    source=r["source"],
                    merchant_text=r["merchant_text"],
                    target_account=r["target_account"] or "",
                    posting_date=entry_date,
                    amount=amount,
                    similarity=sim,
                    recency_factor=rec,
                    weight=weight,
                    score=sim * rec * weight,
                )
            )
        matches.sort(key=lambda m: m.score, reverse=True)
        return matches[:limit]


# ------------------------------------------------------------------
# Helpers used during build.
# ------------------------------------------------------------------


# Priority order for picking the embedding target. Expenses is
# preferred because it's what the original index was scoped to and
# we keep that behavior byte-identical where possible. Income,
# Liabilities, Equity, then Assets follow — each captures a
# distinct classification scenario the agent needs to reason
# about (income attribution, CC-payment / loan-servicing,
# owner-equity moves, plain asset transfers). See AI-AGENT.md
# for the full rationale and the per-case use cases.
_TARGET_ROOT_PRIORITY: tuple[str, ...] = (
    "Expenses", "Income", "Liabilities", "Equity", "Assets",
)

# Leaf names that mean "the user hasn't decided yet." Skip across
# all roots — FIXME in Income:FIXME or Liabilities:FIXME is just
# as unhelpful as Expenses:FIXME. Classify time is where these
# get resolved, and we don't want to mine them as priors.
_UNDECIDED_LEAVES: frozenset[str] = frozenset({
    "FIXME", "UNKNOWN", "UNCATEGORIZED", "UNCLASSIFIED",
})


def _leaf_is_undecided(account: str) -> bool:
    leaf = account.rsplit(":", 1)[-1].upper() if account else ""
    return leaf in _UNDECIDED_LEAVES


# Roots where ANY posting is a valid classification target regardless
# of sign. These are the "money mover" legs — Expenses is always the
# charge category, Income the attribution, Equity an owner move.
_MOVER_ROOTS: frozenset[str] = frozenset({"Expenses", "Income", "Equity"})

# Roots where only POSITIVE-amount postings count as targets. These
# are "account" legs — a negative amount means the txn was sourced
# FROM this account (card charged, cash withdrawn), which is never
# what classification targets. A positive amount means the account
# received money (CC paid down, savings transfer destination), which
# IS a classification target.
_ACCOUNT_ROOTS: frozenset[str] = frozenset({"Liabilities", "Assets"})


def _first_resolved_target(txn: Transaction) -> str | None:
    """Pick the posting that best identifies this txn for
    classification. Priority is Expenses > Income > Liabilities
    > Equity > Assets; within a root, FIXME / Unknown / etc.
    leaves are skipped.

    For mover roots (Expenses / Income / Equity), accept any
    non-FIXME posting — these are inherently classification
    targets regardless of amount sign.

    For account roots (Liabilities / Assets), accept only
    positive-amount postings — negative means "money left here"
    (source side), which is never a classification target. A CC
    payment's Liabilities posting is positive (debt reduced); a
    CC charge's Liabilities posting is negative (debt added)
    and should fall through rather than be picked as target
    when the Expenses leg is FIXME'd.
    """
    by_root: dict[str, list[tuple[str, Decimal]]] = {
        r: [] for r in _TARGET_ROOT_PRIORITY
    }
    for posting in txn.postings or []:
        acct = posting.account or ""
        if not acct:
            continue
        root = acct.split(":", 1)[0]
        if root not in by_root:
            continue
        if _leaf_is_undecided(acct):
            continue
        amount = Decimal("0")
        if posting.units is not None and posting.units.number is not None:
            amount = Decimal(posting.units.number)
        # Account-root postings must be destinations (positive).
        # Mover-root postings accept any sign.
        if root in _ACCOUNT_ROOTS and amount <= 0:
            continue
        by_root[root].append((acct, amount))

    for root in _TARGET_ROOT_PRIORITY:
        candidates = by_root[root]
        if not candidates:
            continue
        if len(candidates) == 1:
            return candidates[0][0]
        # Multiple postings in the same root — deterministic pick.
        # For account roots we've already filtered to positives,
        # so any tie-break is fine; use lex order.
        candidates.sort(key=lambda c: c[0])
        return candidates[0][0]
    return None


def _first_target_amount(txn: Transaction, account: str) -> Decimal:
    for posting in txn.postings or []:
        if posting.account == account and posting.units and posting.units.number is not None:
            return abs(Decimal(posting.units.number))
    return Decimal("0")


def _find_txn_by_hash(entries: Iterable, target_hash: str):
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        try:
            if compute_txn_hash(entry) == target_hash:
                return entry
        except Exception:  # noqa: BLE001
            continue
    return None


def _extract_corrected_account(user_correction: str) -> str | None:
    """ai_decisions.user_correction is a free-form string describing
    what the user set the txn to. The production review flow stores
    it as ``"auto_accepted→Expenses:Entity:Account"`` or just an
    account path; extract the account where we can."""
    s = (user_correction or "").strip()
    if not s:
        return None
    # If it has an arrow, take what's after the last arrow.
    for sep in ("→", "->", "=>"):
        if sep in s:
            s = s.rsplit(sep, 1)[1].strip()
    # Must look like an account path.
    if ":" not in s or " " in s:
        return None
    return s


# ------------------------------------------------------------------
# Seam used by classify context.
# ------------------------------------------------------------------


def similar_transactions_via_vector(
    conn,
    entries: Iterable,
    *,
    needle: str,
    reference_date: date,
    limit: int = 5,
    model_name: str = DEFAULT_MODEL,
    embed_fn: EmbedFn | None = None,
    ledger_signature: str = "",
    ai_decisions=None,
    target_roots: Sequence[str] | None = ("Expenses",),
) -> list[SimilarTxn]:
    """Vector-backed analogue of ``context.similar_transactions``.

    Builds or refreshes the index if the ``ledger_signature`` is
    stale, runs a vector query, and converts results into the
    ``SimilarTxn`` shape the classify prompt expects.

    ``target_roots`` defaults to ``("Expenses",)`` because every
    classify call today is expense-scoped. Phase 2 adds per-type
    branches that override this (``("Income",)`` for income
    attribution, etc.). Set to ``None`` to query across every
    embedded root (not usually what you want — see AI-AGENT.md).

    Raises ``VectorUnavailable`` when the sentence-transformers
    soft-dep isn't installed — callers catch and fall back to the
    substring path.
    """
    idx = VectorIndex(conn, model_name=model_name, embed_fn=embed_fn)
    idx.build(
        entries=entries,
        ai_decisions=ai_decisions,
        ledger_signature=ledger_signature,
    )
    matches = idx.query(
        needle=needle, reference_date=reference_date, limit=limit,
        target_roots=target_roots,
    )
    return [
        SimilarTxn(
            date=m.posting_date,
            amount=m.amount,
            narration=m.merchant_text,
            target_account=m.target_account,
        )
        for m in matches
    ]
