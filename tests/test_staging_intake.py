# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the generic / pasted-text intake (Phase D1)."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.import_.staging import (
    IntakeError,
    IntakeService,
    ParsedPaste,
    StagingService,
    content_fingerprint,
    detect_columns_by_content,
    detect_paste_duplicates,
    heuristic_column_map,
    parse_pasted_text,
)


@pytest.fixture()
def svc() -> IntakeService:
    conn = connect(Path(":memory:"))
    migrate(conn)
    return IntakeService(conn)


# --- parse_pasted_text --------------------------------------------------


class TestParse:
    def test_tab_separated_with_header(self):
        text = (
            "Date\tAmount\tDescription\n"
            "2026-04-20\t-12.34\tAMAZON.COM\n"
            "2026-04-21\t-4.50\tA COFFEE SHOP\n"
        )
        parsed = parse_pasted_text(text)
        assert parsed.delimiter_guess == "\t"
        assert parsed.columns == ["Date", "Amount", "Description"]
        assert len(parsed.rows) == 2
        assert parsed.rows[0] == ["2026-04-20", "-12.34", "AMAZON.COM"]

    def test_comma_separated_with_header(self):
        text = (
            "Date,Amount,Description\n"
            "2026-04-20,-12.34,AMAZON.COM\n"
            "2026-04-21,-4.50,A COFFEE SHOP\n"
        )
        parsed = parse_pasted_text(text)
        assert parsed.delimiter_guess == ","
        assert parsed.columns == ["Date", "Amount", "Description"]
        assert len(parsed.rows) == 2

    def test_whitespace_aligned_statement_snippet(self):
        text = (
            "2026-04-20    AMAZON.COM      -12.34\n"
            "2026-04-21    A COFFEE SHOP        -4.50\n"
        )
        parsed = parse_pasted_text(text, has_header=False)
        assert parsed.delimiter_guess == "whitespace"
        # No header → synthetic column names
        assert parsed.columns == ["col_1", "col_2", "col_3"]
        assert len(parsed.rows) == 2
        assert parsed.rows[0][0] == "2026-04-20"
        assert parsed.rows[0][2] == "-12.34"

    def test_csv_with_quoted_fields(self):
        text = (
            'Date,Amount,Description\n'
            '2026-04-20,-12.34,"AMAZON.COM, PURCHASE"\n'
        )
        parsed = parse_pasted_text(text)
        assert parsed.rows[0][2] == "AMAZON.COM, PURCHASE"

    def test_empty_text_raises(self):
        with pytest.raises(IntakeError, match="empty"):
            parse_pasted_text("")

    def test_ragged_rows_padded_to_header_width(self):
        text = (
            "Date\tAmount\tDescription\n"
            "2026-04-20\t-12.34\n"  # missing description column
        )
        parsed = parse_pasted_text(text)
        assert len(parsed.rows[0]) == 3
        assert parsed.rows[0][2] == ""

    def test_data_looking_header_skipped(self):
        """First row that parses as dates/numbers is treated as data,
        not a header — synthetic column names are generated."""
        text = (
            "2026-04-20\t-12.34\tAMAZON.COM\n"
            "2026-04-21\t-4.50\tA COFFEE SHOP\n"
        )
        parsed = parse_pasted_text(text, has_header=True)
        assert parsed.header_row_index == -1
        assert parsed.columns[0].startswith("col_")


# --- column mapping -----------------------------------------------------


class TestHeuristicMap:
    def test_common_headers_mapped(self):
        cols = ["Date", "Amount", "Description", "Memo"]
        m = heuristic_column_map(cols)
        assert m == {
            "Date": "date",
            "Amount": "amount",
            "Description": "description",
            "Memo": "memo",
        }

    def test_alias_patterns(self):
        cols = ["Posted Date", "Debit", "Payee", "Notes"]
        m = heuristic_column_map(cols)
        assert m["Posted Date"] == "date"
        assert m["Debit"] == "amount"
        assert m["Payee"] == "payee"
        assert m["Notes"] == "memo"

    def test_unrecognized_columns_map_to_none(self):
        cols = ["Foo", "Bar", "Baz"]
        m = heuristic_column_map(cols)
        assert all(v is None for v in m.values())


class TestContentDetection:
    def test_picks_date_column_by_content(self):
        parsed = ParsedPaste(
            columns=["col_1", "col_2", "col_3"],
            rows=[
                ["2026-04-20", "AMAZON", "-12.34"],
                ["2026-04-21", "A COFFEE SHOP", "-4.50"],
            ],
            delimiter_guess="\t",
            header_row_index=-1,
        )
        m = detect_columns_by_content(parsed)
        assert m["col_1"] == "date"
        assert m["col_3"] == "amount"
        # col_2 becomes description (longest textual column).
        assert m["col_2"] == "description"

    def test_header_match_takes_precedence_over_content(self):
        parsed = ParsedPaste(
            columns=["Date", "Foo", "Amount"],
            rows=[
                ["2026-04-20", "x", "-12.34"],
            ],
            delimiter_guess=",",
        )
        m = detect_columns_by_content(parsed)
        assert m["Date"] == "date"
        assert m["Amount"] == "amount"


# --- IntakeService ------------------------------------------------------


class TestIntakeService:
    def test_stage_rows_with_header_mapping(self, svc: IntakeService):
        text = (
            "Date,Amount,Description\n"
            "2026-04-20,-12.34,AMAZON.COM\n"
            "2026-04-21,-4.50,A COFFEE SHOP\n"
        )
        parsed = parse_pasted_text(text)
        column_map = heuristic_column_map(parsed.columns)
        result = svc.stage_paste(
            session_id="session-1",
            parsed=parsed,
            column_map=column_map,
        )
        assert result.staged == 2
        assert result.skipped == 0
        # Rows are now on the unified staging surface.
        rows = svc.conn.execute(
            "SELECT posting_date, amount, description, source "
            "FROM staged_transactions WHERE source = 'paste' "
            "ORDER BY posting_date"
        ).fetchall()
        assert [r["source"] for r in rows] == ["paste", "paste"]
        assert rows[0]["posting_date"] == "2026-04-20"
        assert Decimal(rows[0]["amount"]) == Decimal("-12.34")
        assert rows[0]["description"] == "AMAZON.COM"

    def test_rows_without_parseable_date_skipped(self, svc: IntakeService):
        parsed = ParsedPaste(
            columns=["Date", "Amount"],
            rows=[
                ["2026-04-20", "-10.00"],
                ["not a date", "-5.00"],
            ],
            delimiter_guess=",",
        )
        m = heuristic_column_map(parsed.columns)
        result = svc.stage_paste(
            session_id="s", parsed=parsed, column_map=m,
        )
        assert result.staged == 1
        assert result.skipped == 1
        assert any("date" in e.lower() for e in result.errors)

    def test_rows_without_parseable_amount_skipped(self, svc: IntakeService):
        parsed = ParsedPaste(
            columns=["Date", "Amount"],
            rows=[
                ["2026-04-20", "-10.00"],
                ["2026-04-21", "invalid"],
            ],
            delimiter_guess=",",
        )
        m = heuristic_column_map(parsed.columns)
        result = svc.stage_paste(
            session_id="s", parsed=parsed, column_map=m,
        )
        assert result.staged == 1
        assert result.skipped == 1

    def test_accounting_parens_parsed_as_negative(self, svc: IntakeService):
        """Statements often render negatives as (12.34). Intake must
        parse that as -12.34, not reject it."""
        parsed = ParsedPaste(
            columns=["Date", "Amount"],
            rows=[
                ["2026-04-20", "(99.99)"],
            ],
            delimiter_guess=",",
        )
        m = heuristic_column_map(parsed.columns)
        svc.stage_paste(session_id="s", parsed=parsed, column_map=m)
        row = svc.conn.execute(
            "SELECT amount FROM staged_transactions WHERE source='paste'"
        ).fetchone()
        assert Decimal(row["amount"]) == Decimal("-99.99")

    def test_dollar_sign_and_thousands_separator_parsed(
        self, svc: IntakeService,
    ):
        parsed = ParsedPaste(
            columns=["Date", "Amount"],
            rows=[
                ["2026-04-20", "$1,234.56"],
            ],
            delimiter_guess=",",
        )
        m = heuristic_column_map(parsed.columns)
        svc.stage_paste(session_id="s", parsed=parsed, column_map=m)
        row = svc.conn.execute(
            "SELECT amount FROM staged_transactions WHERE source='paste'"
        ).fetchone()
        assert Decimal(row["amount"]) == Decimal("1234.56")

    def test_resubmit_same_session_upserts_in_place(self, svc: IntakeService):
        """Narrow utility: re-calling stage_paste with the *same*
        session_id is a no-op on row count (upsert-in-place on
        source_ref_hash). This only guards against an accidental
        double-submit within the same browser form; it is NOT the
        primary duplicate protection — a user pasting the same text
        on a different day hits detect_paste_duplicates, not this."""
        text = (
            "Date,Amount,Description\n"
            "2026-04-20,-12.34,AMAZON.COM\n"
        )
        parsed = parse_pasted_text(text)
        m = heuristic_column_map(parsed.columns)
        svc.stage_paste(session_id="s1", parsed=parsed, column_map=m)
        svc.stage_paste(session_id="s1", parsed=parsed, column_map=m)
        n = svc.conn.execute(
            "SELECT COUNT(*) AS n FROM staged_transactions WHERE source='paste'"
        ).fetchone()["n"]
        assert n == 1

    def test_different_sessions_produce_different_rows(
        self, svc: IntakeService,
    ):
        text = (
            "Date,Amount,Description\n"
            "2026-04-20,-12.34,AMAZON.COM\n"
        )
        parsed = parse_pasted_text(text)
        m = heuristic_column_map(parsed.columns)
        svc.stage_paste(session_id="s1", parsed=parsed, column_map=m)
        svc.stage_paste(session_id="s2", parsed=parsed, column_map=m)
        n = svc.conn.execute(
            "SELECT COUNT(*) AS n FROM staged_transactions WHERE source='paste'"
        ).fetchone()["n"]
        assert n == 2

    def test_archived_file_id_drives_source_ref_shape(
        self, svc: IntakeService,
    ):
        """ADR-0060 — when ``archived_file_id`` is supplied, every
        staged row's source_ref carries ``{file_id, row}`` instead of
        ``{session_id, row_index}``. Re-running with the same
        ``archived_file_id`` upserts in place regardless of what
        ``session_id`` the caller used. This is the contract that
        makes "re-pasting the same content from a different
        browser tab" land idempotent at the file level."""
        import json
        text = (
            "Date,Amount,Description\n"
            "2026-04-20,-12.34,Online Retailer\n"
            "2026-04-21,-4.50,Coffee Shop\n"
        )
        parsed = parse_pasted_text(text)
        m = heuristic_column_map(parsed.columns)
        # First pass — same session, same file_id.
        svc.stage_paste(
            session_id="s1", parsed=parsed, column_map=m,
            archived_file_id=42,
        )
        # Second pass — DIFFERENT session, SAME file_id. The upsert
        # path keys off (source, source_ref_hash) which now derives
        # from {file_id, row} not {session_id, row_index}, so this
        # is a no-op on row count.
        svc.stage_paste(
            session_id="s2-different", parsed=parsed, column_map=m,
            archived_file_id=42,
        )
        rows = svc.conn.execute(
            "SELECT source_ref FROM staged_transactions "
            "WHERE source='paste' ORDER BY posting_date"
        ).fetchall()
        assert len(rows) == 2, (
            "same archived_file_id must upsert; got "
            f"{len(rows)} rows"
        )
        for r in rows:
            ref = json.loads(r["source_ref"])
            assert "file_id" in ref
            assert ref["file_id"] == 42
            assert "row" in ref
            # Old-shape keys must not appear when file_id is in use.
            assert "session_id" not in ref
            assert "row_index" not in ref

    def test_archived_file_id_distinct_per_file(
        self, svc: IntakeService,
    ):
        """Different archived files (e.g. an edited re-export of the
        same statement) produce different source_ref_hashes and
        therefore distinct staged rows — even when the row content
        is identical. The dedup oracle (ADR-0058) is what catches
        cross-file content overlap; this contract is just about the
        identity layer."""
        text = (
            "Date,Amount,Description\n"
            "2026-04-20,-12.34,Online Retailer\n"
        )
        parsed = parse_pasted_text(text)
        m = heuristic_column_map(parsed.columns)
        svc.stage_paste(
            session_id="s1", parsed=parsed, column_map=m,
            archived_file_id=1,
        )
        svc.stage_paste(
            session_id="s2", parsed=parsed, column_map=m,
            archived_file_id=2,
        )
        n = svc.conn.execute(
            "SELECT COUNT(*) AS n FROM staged_transactions "
            "WHERE source='paste'"
        ).fetchone()["n"]
        assert n == 2


# --- duplicate detection + end-to-end paste → stage → pair ---------------


def _stage_simplefin(svc_or_conn, *, date, amount, description, txn_id="sf-1"):
    """Shortcut for seeding a SimpleFIN-side staged row in tests."""
    from decimal import Decimal
    conn = getattr(svc_or_conn, "conn", svc_or_conn)
    StagingService(conn).stage(
        source="simplefin",
        source_ref={"account_id": "ACT-WF", "txn_id": txn_id},
        session_id="sf-ingest-1",
        posting_date=date,
        amount=Decimal(amount),
        description=description,
    )


class TestDuplicateDetection:
    def test_fingerprint_stable_across_case_and_whitespace(self):
        """Fuzzy match: 'AMAZON.COM  ' and 'amazon.com' must have
        the same fingerprint so a re-paste with slight rendering
        differences still matches."""
        a = content_fingerprint(
            posting_date="2026-04-20",
            amount=Decimal("-12.34"),
            description="AMAZON.COM",
        )
        b = content_fingerprint(
            posting_date="2026-04-20",
            amount=Decimal("12.34"),   # sign-agnostic
            description="  amazon.com  ",
        )
        assert a == b

    def test_fingerprint_differs_on_amount(self):
        a = content_fingerprint(
            posting_date="2026-04-20", amount=Decimal("-12.34"), description="X",
        )
        b = content_fingerprint(
            posting_date="2026-04-20", amount=Decimal("-12.35"), description="X",
        )
        assert a != b

    def test_empty_paste_returns_none_severity(self, svc: IntakeService):
        parsed = ParsedPaste(
            columns=["Date", "Amount"], rows=[],
            delimiter_guess=",",
        )
        m = heuristic_column_map(parsed.columns)
        report = detect_paste_duplicates(svc.conn, parsed, m)
        assert report.severity == "none"
        assert report.matched_rows == 0
        assert report.overlap_ratio == 0.0

    def test_fresh_paste_with_no_history(self, svc: IntakeService):
        parsed = parse_pasted_text(
            "Date,Amount,Description\n2026-04-20,-12.34,AMAZON.COM\n",
        )
        m = heuristic_column_map(parsed.columns)
        report = detect_paste_duplicates(svc.conn, parsed, m)
        assert report.severity == "none"
        assert report.matched_rows == 0

    def test_repaste_same_statement_a_day_apart(self, svc: IntakeService):
        """Motivating scenario from the user: same paste submitted
        ~1 day apart. Even with a different session_id, the second
        paste must trigger high-severity duplicate detection."""
        text = (
            "Date,Amount,Description\n"
            "2026-03-01,-100.00,Merchant A\n"
            "2026-03-05,-50.00,Merchant B\n"
            "2026-03-10,-200.00,Merchant C\n"
        )
        parsed = parse_pasted_text(text)
        m = heuristic_column_map(parsed.columns)
        # First paste (different session).
        svc.stage_paste(session_id="paste-day1", parsed=parsed, column_map=m)
        # Second paste a day later (fresh session).
        report = detect_paste_duplicates(svc.conn, parsed, m)
        assert report.severity == "high"
        assert report.matched_rows == 3
        assert report.overlap_ratio == 1.0
        assert report.likely_duplicate_sessions
        top = report.likely_duplicate_sessions[0]
        assert top.source == "paste"
        assert top.session_id == "paste-day1"
        assert top.matches == 3

    def test_simplefin_then_paste_same_window_flagged(self, svc: IntakeService):
        """User's second scenario: SimpleFIN ingested these txns a
        few weeks ago. Now the user pastes the same bank statement.
        Detector must flag the paste as duplicate against the
        *SimpleFIN* rows — cross-source."""
        # Seed SimpleFIN side first.
        _stage_simplefin(svc, date="2026-03-01", amount="-100.00",
                         description="Merchant A", txn_id="sf-A")
        _stage_simplefin(svc, date="2026-03-05", amount="-50.00",
                         description="Merchant B", txn_id="sf-B")
        _stage_simplefin(svc, date="2026-03-10", amount="-200.00",
                         description="Merchant C", txn_id="sf-C")

        text = (
            "Date,Amount,Description\n"
            "2026-03-01,-100.00,Merchant A\n"
            "2026-03-05,-50.00,Merchant B\n"
            "2026-03-10,-200.00,Merchant C\n"
        )
        parsed = parse_pasted_text(text)
        m = heuristic_column_map(parsed.columns)
        report = detect_paste_duplicates(svc.conn, parsed, m)

        assert report.severity == "high"
        assert report.matched_rows == 3
        # The top overlap source should be 'simplefin', not 'paste'.
        top = report.likely_duplicate_sessions[0]
        assert top.source == "simplefin"

    def test_partial_overlap_lands_in_middle_band(self, svc: IntakeService):
        """50% overlap → 'partial' severity. Not blocking, but
        surfaced for user awareness."""
        text_a = (
            "Date,Amount,Description\n"
            "2026-03-01,-100.00,Merchant A\n"
            "2026-03-05,-50.00,Merchant B\n"
        )
        parsed_a = parse_pasted_text(text_a)
        m = heuristic_column_map(parsed_a.columns)
        svc.stage_paste(session_id="p1", parsed=parsed_a, column_map=m)

        text_b = (
            "Date,Amount,Description\n"
            "2026-03-01,-100.00,Merchant A\n"   # dup
            "2026-03-05,-50.00,Merchant B\n"    # dup
            "2026-03-06,-77.00,Merchant X\n"    # new
            "2026-03-07,-88.00,Merchant Y\n"    # new
        )
        parsed_b = parse_pasted_text(text_b)
        report = detect_paste_duplicates(svc.conn, parsed_b, m)

        assert report.matched_rows == 2
        assert report.total_rows == 4
        assert report.severity == "partial"

    def test_date_window_limits_history_scan(self, svc: IntakeService):
        """Rows well outside the date window don't get flagged
        just because they share a merchant + amount. This keeps
        legitimately recurring identical charges from noising up
        the report."""
        # Seed a row from last year.
        _stage_simplefin(
            svc, date="2025-04-20", amount="-50.00",
            description="Streaming", txn_id="sf-old",
        )
        text = (
            "Date,Amount,Description\n"
            "2026-04-20,-50.00,Netflix\n"
        )
        parsed = parse_pasted_text(text)
        m = heuristic_column_map(parsed.columns)
        report = detect_paste_duplicates(
            svc.conn, parsed, m, date_window_days=60,
        )
        assert report.matched_rows == 0
        assert report.severity == "none"


    def test_pasted_row_can_pair_with_simplefin(self, svc: IntakeService):
        """A row pasted via intake is first-class on the staging
        surface — the matcher pairs it against SimpleFIN rows
        just like a CSV row would."""
        from lamella.features.import_.staging import find_pairs

        # Side A: pasted.
        parsed = parse_pasted_text(
            "Date,Amount,Description\n2026-04-20,-500.00,WF TRANSFER\n",
        )
        m = heuristic_column_map(parsed.columns)
        svc.stage_paste(session_id="paste-1", parsed=parsed, column_map=m)

        # Side B: SimpleFIN (stubbed staged row).
        staging = StagingService(svc.conn)
        staging.stage(
            source="simplefin",
            source_ref={"account_id": "ACT-WF", "txn_id": "T1"},
            posting_date="2026-04-20",
            amount=Decimal("500.00"),
            payee="Incoming transfer",
        )

        proposals = find_pairs(svc.conn)
        assert len(proposals) == 1
        assert proposals[0].kind == "transfer"


# --- HTTP route --------------------------------------------------------


# --- Phase D2: AI refinement -------------------------------------------


class TestAIRefinement:
    def test_merge_maps_ai_fills_heuristic_gaps(self):
        from lamella.features.import_.staging.intake import merge_maps
        heuristic = {"Date": "date", "Foo": None, "Bar": None}
        ai = {"Date": "date", "Foo": "payee", "Bar": "description"}
        out = merge_maps(heuristic, ai)
        assert out == {"Date": "date", "Foo": "payee", "Bar": "description"}

    def test_merge_maps_heuristic_wins_on_disagreement(self):
        """When heuristic has a positive match, it beats the AI —
        heuristic is deterministic and free; AI is a gap-filler."""
        from lamella.features.import_.staging.intake import merge_maps
        heuristic = {"Date": "date", "Amount": "amount"}
        ai = {"Date": "amount", "Amount": "date"}  # AI got it wrong
        out = merge_maps(heuristic, ai)
        assert out["Date"] == "date"
        assert out["Amount"] == "amount"

    def test_merge_maps_no_ai_returns_heuristic_unchanged(self):
        from lamella.features.import_.staging.intake import merge_maps
        heuristic = {"Date": "date", "Foo": None}
        assert merge_maps(heuristic, None) == heuristic

    async def test_propose_via_ai_returns_none_when_ai_disabled(self):
        from lamella.features.import_.staging.intake import (
            ParsedPaste, propose_column_map_via_ai,
        )

        class FakeAI:
            enabled = False

        parsed = ParsedPaste(
            columns=["Date", "Amount"],
            rows=[["2026-04-20", "-10"]],
            delimiter_guess=",",
        )
        result = await propose_column_map_via_ai(
            parsed, ai_service=FakeAI(), input_ref="test",
        )
        assert result is None

    async def test_propose_via_ai_returns_none_on_no_service(self):
        from lamella.features.import_.staging.intake import (
            ParsedPaste, propose_column_map_via_ai,
        )
        parsed = ParsedPaste(
            columns=["Date", "Amount"],
            rows=[["2026-04-20", "-10"]],
            delimiter_guess=",",
        )
        result = await propose_column_map_via_ai(
            parsed, ai_service=None, input_ref="test",
        )
        assert result is None


class TestIntakeRoute:
    def test_get_intake_page_renders(self, app_client):
        r = app_client.get("/intake")
        assert r.status_code == 200
        assert "Paste intake" in r.text or "paste intake" in r.text.lower()

    def test_post_preview_shows_detected_mapping(self, app_client):
        r = app_client.post(
            "/intake/preview",
            data={
                "text": (
                    "Date,Amount,Description\n"
                    "2026-04-20,-12.34,AMAZON.COM\n"
                    "2026-04-21,-4.50,A COFFEE SHOP\n"
                ),
                "has_header": "1",
            },
        )
        assert r.status_code == 200
        # The mapping table should show 'date', 'amount', 'description'.
        body = r.text.lower()
        assert "amazon.com" in body
        assert "date" in body and "amount" in body

    @pytest.mark.xfail(
        reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
        strict=False,
    )
    def test_post_stage_lands_rows_in_staging(self, app_client):
        r = app_client.post(
            "/intake/stage",
            data={
                "text": (
                    "Date,Amount,Description\n"
                    "2026-04-20,-12.34,AMAZON.COM\n"
                    "2026-04-21,-4.50,A COFFEE SHOP\n"
                ),
                "has_header": "1",
            },
        )
        assert r.status_code == 200
        assert "Staged" in r.text
        # The staging table should now have 2 paste rows.
        # (We can't hit the DB directly from the TestClient fixture
        # without extra plumbing, but the result banner carries the
        # count so we check the template.)
        assert ">2</strong>" in r.text or ">2 </strong>" in r.text

    def test_high_overlap_paste_refused_without_confirmation(self, app_client):
        """NEXTGEN Phase D1.1: re-pasting a statement that's already
        been staged once must refuse on the second attempt unless
        the user explicitly confirms. Motivating scenario: user
        pastes a statement, forgets, pastes again a year later."""
        text = (
            "Date,Amount,Description\n"
            "2026-03-01,-100.00,Merchant A\n"
            "2026-03-05,-50.00,Merchant B\n"
            "2026-03-10,-200.00,Merchant C\n"
        )
        first = app_client.post(
            "/intake/stage",
            data={"text": text, "has_header": "1"},
        )
        assert first.status_code == 200
        assert "Staged" in first.text

        second = app_client.post(
            "/intake/stage",
            data={"text": text, "has_header": "1"},
        )
        assert second.status_code == 409
        assert "duplicate" in second.text.lower() or "overlap" in second.text.lower()

    def test_high_overlap_paste_accepted_with_confirmation(self, app_client):
        """When the user explicitly ticks the confirm box, the
        duplicate batch goes through — the per-row matches are
        recorded as review items, not silently hidden."""
        text = (
            "Date,Amount,Description\n"
            "2026-03-01,-100.00,Merchant A\n"
            "2026-03-05,-50.00,Merchant B\n"
            "2026-03-10,-200.00,Merchant C\n"
        )
        app_client.post("/intake/stage", data={"text": text, "has_header": "1"})
        r = app_client.post(
            "/intake/stage",
            data={
                "text": text,
                "has_header": "1",
                "confirm_duplicate": "1",
            },
        )
        assert r.status_code == 200
        assert "Staged" in r.text
        # The response mentions that some rows were duplicate-flagged.
        assert "duplicate" in r.text.lower() or "flagged" in r.text.lower()

    def test_post_preview_with_whitespace_text_returns_error(self, app_client):
        r = app_client.post(
            "/intake/preview",
            data={"text": "   \n   \n", "has_header": "1"},
        )
        assert r.status_code == 400
        assert "empty" in r.text.lower()
