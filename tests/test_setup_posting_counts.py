# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Direct unit tests for the Phase 1.3 shared predicates in
``lamella.features.setup.posting_counts``.

These tests synthesize beancount entries in-memory (no ledger file,
no FastAPI) and assert each predicate's output. The handler-level
filter-parity tests in ``test_setup_filter_parity.py`` exercise the
same predicates through the live routes.
"""
from __future__ import annotations

from datetime import date

import pytest
from beancount.core.amount import Amount
from beancount.core.data import Close, Open, Posting, Transaction
from beancount.core.number import D

from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.features.setup.posting_counts import (
    already_migrated_hashes,
    count_unmigrated_postings,
    is_override_txn,
    is_vehicle_orphan,
    iter_unmigrated_txns_on,
    open_paths,
    unmigrated_postings_by_account,
)


# --- Fixtures --------------------------------------------------------------


def _txn(
    d: date, narration: str, *posts: tuple[str, str],
    tags: set[str] | None = None,
    meta: dict | None = None,
) -> Transaction:
    """Build a Transaction with the given postings. Each posting is a
    (account, "amount CCY") tuple."""
    postings: list[Posting] = []
    for acct, amt in posts:
        num, ccy = amt.split()
        postings.append(
            Posting(
                account=acct,
                units=Amount(D(num), ccy),
                cost=None, price=None, flag=None, meta=None,
            )
        )
    return Transaction(
        meta=meta or {},
        date=d,
        flag="*",
        payee=None,
        narration=narration,
        tags=tags or frozenset(),
        links=frozenset(),
        postings=postings,
    )


def _open(d: date, account: str) -> Open:
    return Open(meta={}, date=d, account=account, currencies=None, booking=None)


def _close(d: date, account: str) -> Close:
    return Close(meta={}, date=d, account=account)


# --- is_override_txn -------------------------------------------------------


class TestIsOverrideTxn:
    def test_untagged_txn_is_not_override(self):
        t = _txn(date(2024, 1, 1), "coffee", ("Expenses:Food", "5.00 USD"), ("Assets:Bank", "-5.00 USD"))
        assert is_override_txn(t) is False

    def test_bcg_override_tag_matches(self):
        t = _txn(
            date(2024, 1, 1), "migration",
            ("Expenses:NewAcct", "5.00 USD"), ("Expenses:OldAcct", "-5.00 USD"),
            tags={"lamella-override"},
        )
        assert is_override_txn(t) is True

    def test_other_tag_does_not_match(self):
        t = _txn(
            date(2024, 1, 1), "x", ("Expenses:A", "1 USD"), ("Expenses:B", "-1 USD"),
            tags={"lamella-intercompany"},
        )
        assert is_override_txn(t) is False

    def test_open_directive_has_no_tags_attr(self):
        o = _open(date(2024, 1, 1), "Assets:Bank")
        # Must not blow up on non-Transaction entries.
        assert is_override_txn(o) is False


# --- already_migrated_hashes ----------------------------------------------


class TestAlreadyMigratedHashes:
    def test_empty_entries(self):
        assert already_migrated_hashes([]) == set()

    def test_override_block_with_hash(self):
        override = _txn(
            date(2024, 2, 1), "migration",
            ("Expenses:New", "5.00 USD"), ("Expenses:Old", "-5.00 USD"),
            tags={"lamella-override"},
            meta={"lamella-override-of": "abcdef1234"},
        )
        assert already_migrated_hashes([override]) == {"abcdef1234"}

    def test_override_hash_with_quotes_is_stripped(self):
        override = _txn(
            date(2024, 2, 1), "migration",
            ("Expenses:New", "5.00 USD"), ("Expenses:Old", "-5.00 USD"),
            tags={"lamella-override"},
            meta={"lamella-override-of": '"abcdef1234"'},
        )
        assert already_migrated_hashes([override]) == {"abcdef1234"}

    def test_original_txn_not_included(self):
        original = _txn(
            date(2024, 1, 1), "orig",
            ("Expenses:Old", "5 USD"), ("Assets:Bank", "-5 USD"),
        )
        assert already_migrated_hashes([original]) == set()

    def test_override_without_of_meta_skipped(self):
        tagged = _txn(
            date(2024, 2, 1), "?",
            ("Expenses:A", "1 USD"), ("Expenses:B", "-1 USD"),
            tags={"lamella-override"},
            meta={},
        )
        assert already_migrated_hashes([tagged]) == set()


# --- open_paths ------------------------------------------------------------


class TestOpenPaths:
    def test_opens_minus_closes(self):
        entries = [
            _open(date(2020, 1, 1), "Assets:A"),
            _open(date(2020, 1, 1), "Assets:B"),
            _close(date(2021, 1, 1), "Assets:A"),
        ]
        assert open_paths(entries) == {"Assets:B"}

    def test_close_without_open_is_harmless(self):
        entries = [_close(date(2021, 1, 1), "Assets:Orphan")]
        assert open_paths(entries) == set()

    def test_open_without_close_stays_open(self):
        entries = [_open(date(2020, 1, 1), "Assets:A")]
        assert open_paths(entries) == {"Assets:A"}


# --- count_unmigrated_postings --------------------------------------------


class TestCountUnmigratedPostings:
    def test_plain_originals_are_counted(self):
        entries = [
            _txn(date(2024, 1, 1), "a", ("Expenses:Orphan", "10 USD"), ("Assets:Bank", "-10 USD")),
            _txn(date(2024, 1, 2), "b", ("Expenses:Orphan", "20 USD"), ("Assets:Bank", "-20 USD")),
        ]
        assert count_unmigrated_postings(entries, "Expenses:Orphan") == 2

    def test_override_txns_are_skipped(self):
        override = _txn(
            date(2024, 2, 1), "migration",
            ("Expenses:Orphan", "10 USD"), ("Expenses:Canonical", "-10 USD"),
            tags={"lamella-override"},
            meta={"lamella-override-of": "hash-a"},
        )
        orig = _txn(
            date(2024, 1, 1), "a",
            ("Expenses:Other", "5 USD"), ("Assets:Bank", "-5 USD"),
        )
        # Override touches Expenses:Orphan but is excluded; Expenses:Other
        # is untouched → zero counts.
        assert count_unmigrated_postings([override, orig], "Expenses:Orphan") == 0

    def test_already_migrated_original_not_counted(self):
        orig = _txn(
            date(2024, 1, 1), "a",
            ("Expenses:Orphan", "10 USD"), ("Assets:Bank", "-10 USD"),
        )
        orig_hash = txn_hash(orig)
        override = _txn(
            date(2024, 2, 1), "migration",
            ("Expenses:Canonical", "10 USD"), ("Expenses:Orphan", "-10 USD"),
            tags={"lamella-override"},
            meta={"lamella-override-of": orig_hash},
        )
        assert count_unmigrated_postings([orig, override], "Expenses:Orphan") == 0

    def test_mixed_migrated_and_unmigrated(self):
        orig1 = _txn(date(2024, 1, 1), "a", ("Expenses:Orphan", "10 USD"), ("Assets:Bank", "-10 USD"))
        orig2 = _txn(date(2024, 1, 2), "b", ("Expenses:Orphan", "20 USD"), ("Assets:Bank", "-20 USD"))
        override1 = _txn(
            date(2024, 2, 1), "m1",
            ("Expenses:Canonical", "10 USD"), ("Expenses:Orphan", "-10 USD"),
            tags={"lamella-override"},
            meta={"lamella-override-of": txn_hash(orig1)},
        )
        # orig2 still un-migrated.
        assert count_unmigrated_postings(
            [orig1, orig2, override1], "Expenses:Orphan",
        ) == 1


class TestUnmigratedPostingsByAccount:
    def test_batched_sums_per_account(self):
        entries = [
            _txn(date(2024, 1, 1), "a", ("Expenses:A", "1 USD"), ("Assets:Bank", "-1 USD")),
            _txn(date(2024, 1, 2), "b", ("Expenses:A", "2 USD"), ("Assets:Bank", "-2 USD")),
            _txn(date(2024, 1, 3), "c", ("Expenses:B", "3 USD"), ("Assets:Bank", "-3 USD")),
        ]
        counts = unmigrated_postings_by_account(
            entries, {"Expenses:A", "Expenses:B", "Expenses:Ghost"},
        )
        assert counts == {"Expenses:A": 2, "Expenses:B": 1}


# --- iter_unmigrated_txns_on ----------------------------------------------


class TestIterUnmigratedTxnsOn:
    def test_yields_only_unmigrated(self):
        orig1 = _txn(
            date(2024, 1, 1), "a",
            ("Expenses:Orphan", "10 USD"), ("Assets:Bank", "-10 USD"),
        )
        orig2 = _txn(
            date(2024, 1, 2), "b",
            ("Expenses:Orphan", "20 USD"), ("Assets:Bank", "-20 USD"),
        )
        override1 = _txn(
            date(2024, 2, 1), "m1",
            ("Expenses:Canonical", "10 USD"), ("Expenses:Orphan", "-10 USD"),
            tags={"lamella-override"},
            meta={"lamella-override-of": txn_hash(orig1)},
        )
        yielded = list(iter_unmigrated_txns_on(
            [orig1, orig2, override1], "Expenses:Orphan",
        ))
        assert len(yielded) == 1
        assert yielded[0][1] == txn_hash(orig2)


# --- is_vehicle_orphan -----------------------------------------------------


class TestIsVehicleOrphan:
    def test_canonical_expense_is_not_orphan(self):
        assert is_vehicle_orphan(
            "Expenses:Personal:Vehicle:FabrikamSuv:Fuel",
        ) is False

    def test_canonical_asset_is_not_orphan(self):
        assert is_vehicle_orphan("Assets:Personal:Vehicle:FabrikamSuv") is False

    def test_legacy_plural_vehicles_is_orphan(self):
        assert is_vehicle_orphan(
            "Expenses:Personal:Vehicles:FabrikamSuv:Fuel",
        ) is True

    def test_custom_bucket_is_orphan(self):
        # Uses a token still in ``VEHICLE_KEYWORDS_RE`` after the
        # post-incident tightening. The fictional ``FabrikamSuv``
        # placeholder used elsewhere in the suite is intentionally NOT
        # in the keyword set (Fabrikam is the canonical business name,
        # not a vehicle make/model).
        assert is_vehicle_orphan(
            "Expenses:Personal:Custom:TrailerFuel",
        ) is True

    def test_non_vehicle_keyword_path_is_not_orphan(self):
        assert is_vehicle_orphan("Expenses:Personal:Food:Groceries") is False

    def test_non_expense_non_asset_root_is_not_orphan(self):
        assert is_vehicle_orphan(
            "Liabilities:Personal:Loan:VehicleLoan",
        ) is False

    def test_auto_alone_is_not_orphan(self):
        # Schedule C line 9 / Schedule F line 9 entity-level Auto category.
        # The bulk close-unused-orphans handler wrongly closed these
        # on a populated production ledger when the regex matched on
        # "Auto" alone. The keyword set excludes generic chart words.
        assert is_vehicle_orphan("Expenses:AJQuick:Auto") is False
        assert is_vehicle_orphan("Expenses:DeltaFarmRanch:Auto") is False

    def test_fuel_alone_is_not_orphan(self):
        # Schedule F line 17 (Gasoline, fuel, oil) — legitimate farm
        # category. Schedule A Transportation:Fuel — legitimate
        # personal subcategory. Same false-positive shape as the Auto
        # case above.
        assert is_vehicle_orphan("Expenses:DeltaFarmRanch:Fuel") is False
        assert is_vehicle_orphan("Expenses:DeltaFarmRanch:Tractor:Fuel") is False
        assert is_vehicle_orphan("Expenses:Personal:Transportation:Fuel") is False

    def test_gas_alone_is_not_orphan(self):
        # If a Schedule F farm or utility has a Gas / Gasoline line.
        assert is_vehicle_orphan("Expenses:Personal:Utilities:Gas") is False
        assert is_vehicle_orphan("Expenses:DeltaFarmRanch:Gasoline") is False

    def test_legacy_plural_with_make_model_still_orphan(self):
        # Coverage that survives the keyword tightening: any path
        # using the literal "Vehicles" segment OR a make/model name
        # remains classified as orphan when not canonical. Uses a
        # token still in the keyword set; ``FabrikamSuv`` is a fictional
        # placeholder and is intentionally not a keyword.
        assert is_vehicle_orphan(
            "Expenses:Vehicles:V2008FabrikamSuv:Fuel",
        ) is True
        assert is_vehicle_orphan(
            "Expenses:Personal:Custom:TrailerFuel",
        ) is True
        assert is_vehicle_orphan("Assets:Vehicles:V2008FabrikamSuv") is True


# --- Delete-refusal predicates --------------------------------------------


class TestDeleteRefusalEntity:
    """Phase 1.4 follow-up: refuse entity delete when user-typed
    information OR live transactions exist. Empty scaffolding only."""

    def test_blocks_entity_with_transactions(self, db):
        from lamella.features.setup.posting_counts import (
            DeleteRefusal, assert_safe_to_delete_entity,
        )
        db.execute(
            "INSERT INTO entities (slug, is_active) VALUES (?, 1)",
            ("HasTxns",),
        )
        db.execute(
            "INSERT INTO accounts_meta "
            "(account_path, display_name, entity_slug) VALUES (?, ?, ?)",
            ("Expenses:HasTxns:Supplies", "HasTxns-Supplies", "HasTxns"),
        )
        db.commit()
        entries = [
            _open(date(2020, 1, 1), "Expenses:HasTxns:Supplies"),
            _txn(
                date(2024, 1, 1), "supplies",
                ("Expenses:HasTxns:Supplies", "10 USD"),
                ("Assets:Personal:Bank", "-10 USD"),
            ),
        ]

        def _refs(conn, slug, only_open=True):
            return list(conn.execute(
                "SELECT account_path FROM accounts_meta "
                "WHERE entity_slug = ? OR account_path LIKE ?",
                (slug, f"%:{slug}:%"),
            ).fetchall())

        with pytest.raises(DeleteRefusal) as exc:
            assert_safe_to_delete_entity(
                db, entries, "HasTxns",
                accounts_referencing_slug=_refs,
            )
        assert "1 transaction" in str(exc.value)
        assert "migrate or remove" in str(exc.value)

    def test_blocks_entity_with_user_info(self, db):
        from lamella.features.setup.posting_counts import (
            DeleteRefusal, assert_safe_to_delete_entity,
        )
        db.execute(
            "INSERT INTO entities (slug, display_name, entity_type, "
            "tax_schedule, is_active) VALUES (?, ?, ?, ?, 1)",
            ("Real", "Real Co", "llc", "C"),
        )
        db.commit()
        with pytest.raises(DeleteRefusal) as exc:
            assert_safe_to_delete_entity(
                db, [], "Real",
                accounts_referencing_slug=lambda *a, **k: [],
            )
        msg = str(exc.value)
        assert "user-set fields" in msg
        assert "display_name" in msg
        assert "entity_type" in msg

    def test_allows_empty_scaffolding(self, db):
        from lamella.features.setup.posting_counts import (
            assert_safe_to_delete_entity,
        )
        db.execute(
            "INSERT INTO entities (slug, is_active) VALUES (?, 1)",
            ("Empty",),
        )
        db.commit()
        # Must not raise.
        assert_safe_to_delete_entity(
            db, [], "Empty",
            accounts_referencing_slug=lambda *a, **k: [],
        )


class TestDeleteRefusalAccountMeta:
    def test_blocks_account_with_postings(self, db):
        from lamella.features.setup.posting_counts import (
            DeleteRefusal, assert_safe_to_delete_account_meta,
        )
        db.execute(
            "INSERT INTO accounts_meta "
            "(account_path, display_name) VALUES (?, ?)",
            ("Assets:Personal:Bank:Checking", "Chk"),
        )
        db.commit()
        entries = [
            _open(date(2020, 1, 1), "Assets:Personal:Bank:Checking"),
            _txn(
                date(2024, 1, 1), "deposit",
                ("Assets:Personal:Bank:Checking", "100 USD"),
                ("Income:Personal:Salary", "-100 USD"),
            ),
        ]
        with pytest.raises(DeleteRefusal) as exc:
            assert_safe_to_delete_account_meta(
                db, entries, "Assets:Personal:Bank:Checking",
            )
        assert "1 posting" in str(exc.value)

    def test_blocks_account_with_user_fields(self, db):
        from lamella.features.setup.posting_counts import (
            DeleteRefusal, assert_safe_to_delete_account_meta,
        )
        db.execute(
            "INSERT INTO accounts_meta "
            "(account_path, display_name, kind, institution) "
            "VALUES (?, ?, ?, ?)",
            ("Assets:Personal:Bank:Checking", "Chk", "checking", "BankOne"),
        )
        db.commit()
        with pytest.raises(DeleteRefusal) as exc:
            assert_safe_to_delete_account_meta(
                db, [], "Assets:Personal:Bank:Checking",
            )
        msg = str(exc.value)
        assert "user-set fields" in msg
        assert "kind" in msg

    def test_allows_pure_auto_scaffolded(self, db):
        from lamella.features.setup.posting_counts import (
            assert_safe_to_delete_account_meta,
        )
        db.execute(
            "INSERT INTO accounts_meta "
            "(account_path, display_name, seeded_from_ledger) "
            "VALUES (?, ?, 1)",
            ("Assets:GhostlyEntity:Bank:OldCheck", "OldCheck"),
        )
        db.commit()
        # Must not raise.
        assert_safe_to_delete_account_meta(
            db, [], "Assets:GhostlyEntity:Bank:OldCheck",
        )
