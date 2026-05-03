from __future__ import annotations


def test_simplefin_ingest_started_uses_app_timezone(app_client, db):
    # Stored ingest timestamps are UTC; /simplefin should render in APP_TZ.
    db.execute(
        """
        INSERT INTO simplefin_ingests (
            started_at, finished_at, trigger, new_txns, duplicate_txns,
            classified_by_rule, classified_by_ai, fixme_txns, bean_check_ok, error
        ) VALUES (?, ?, 'manual', 0, 0, 0, 0, 0, 1, NULL)
        """,
        ("2026-04-30T22:41:00+00:00", "2026-04-30T22:42:00+00:00"),
    )
    db.commit()

    app_client.app.state.settings.apply_kv_overrides({"app_tz": "America/Denver"})
    r = app_client.get("/simplefin")

    assert r.status_code == 200
    assert "2026-04-30 16:41" in r.text
    assert "2026-04-30 22:41" not in r.text


def test_simplefin_ingest_started_date_only_stays_same_day(app_client, db):
    db.execute(
        """
        INSERT INTO simplefin_ingests (
            started_at, finished_at, trigger, new_txns, duplicate_txns,
            classified_by_rule, classified_by_ai, fixme_txns, bean_check_ok, error
        ) VALUES (?, ?, 'manual', 0, 0, 0, 0, 0, 1, NULL)
        """,
        ("2026-04-28", "2026-04-28"),
    )
    db.commit()

    app_client.app.state.settings.apply_kv_overrides({"app_tz": "America/Denver"})
    r = app_client.get("/simplefin")

    assert r.status_code == 200
    assert "2026-04-28" in r.text
