"""Repeatable idempotency tests against a real PostgreSQL (separate TEST_POSTGRES_DB).

Demonstrates that duplicate / replayed readings and re-running billing do not change totals.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

from smartgrid import billing_job, db
from smartgrid.billing import TariffValidationError
from smartgrid.simulation import build_households, generate_reading
from smartgrid.tariffs import generate_tariff_rows, write_tariff_file

DAY = date(2026, 1, 1)
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def rows_for(settings, households, ticks):
    out = []
    for t in ticks:
        for h in households:
            r = generate_reading(settings, h, t, NOW)
            out.append(tuple(r[c] for c in db.READING_COLUMNS[:9]) + (0, t))
    return out


def totals(conn):
    return db.fetch_dicts(conn, "SELECT count(*) AS n, round(sum(consumption_kwh)::numeric, 6) AS kwh "
                                "FROM meter_readings")[0]


def bills_snapshot(conn):
    return db.fetch_dicts(conn, "SELECT household_id, consumption_kwh, rate_lkr_per_kwh, bill_lkr, readings_count "
                                "FROM household_bills ORDER BY bill_date, household_id")


def seed_day(conn, settings, include_next_day=True):
    hh = build_households(settings)
    db.register_households(conn, hh)
    ticks = range(0, settings.ticks_per_day + (1 if include_next_day else 0))
    db.insert_readings(conn, rows_for(settings, hh, ticks))
    conn.commit()
    return hh


def test_duplicate_and_replayed_readings_do_not_change_totals(pg_conn, settings):
    hh = build_households(settings)
    batch = rows_for(settings, hh, range(0, 10))
    assert db.insert_readings(pg_conn, batch) == 120
    pg_conn.commit()
    before = totals(pg_conn)

    # Same micro-batch replayed (e.g. Spark restart before checkpoint commit) + in-batch dupes.
    assert db.insert_readings(pg_conn, batch + batch[:7]) == 0
    pg_conn.commit()
    assert totals(pg_conn) == before
    assert before["n"] == 120


def test_rerunning_billing_is_idempotent(pg_conn, settings):
    seed_day(pg_conn, settings)
    path = write_tariff_file(settings.incoming_dir, DAY,
                             generate_tariff_rows(settings, DAY, build_households(settings)))
    tf = billing_job.find_pending_tariff_file(pg_conn, settings.incoming_dir)
    assert tf is not None and tf.file_name == path.name

    first = billing_job.run_billing(pg_conn, tf, settings.reports_dir, "run-1")
    snap1 = bills_snapshot(pg_conn)
    assert first.households_billed == 12 and len(snap1) == 12
    assert all(b["readings_count"] == 60 for b in snap1)
    assert billing_job.find_pending_tariff_file(pg_conn, settings.incoming_dir) is None

    # Replay readings, then re-run billing twice: bills must be byte-for-byte identical.
    db.insert_readings(pg_conn, rows_for(settings, build_households(settings), range(0, 60)))
    pg_conn.commit()
    second = billing_job.run_billing(pg_conn, tf, settings.reports_dir, "run-2")
    billing_job.run_billing(pg_conn, tf, settings.reports_dir, "run-3")
    assert bills_snapshot(pg_conn) == snap1
    assert second.total_bill_lkr == first.total_bill_lkr
    assert db.fetch_dicts(pg_conn, "SELECT count(*) AS n FROM household_bills")[0]["n"] == 12
    assert (settings.reports_dir / "billing_report_2026-01-01.csv").read_text().count("\n") == 13


def test_bill_equals_sum_of_readings_times_rate(pg_conn, settings):
    seed_day(pg_conn, settings)
    write_tariff_file(settings.incoming_dir, DAY, generate_tariff_rows(settings, DAY, build_households(settings)))
    tf = billing_job.find_pending_tariff_file(pg_conn, settings.incoming_dir)
    billing_job.run_billing(pg_conn, tf, settings.reports_dir, "run-1")
    row = db.fetch_dicts(pg_conn, """
        SELECT b.bill_lkr, round(round(sum(r.consumption_kwh)::numeric, 4) * b.rate_lkr_per_kwh, 2) AS expected
        FROM household_bills b JOIN meter_readings r
          ON r.household_id = b.household_id AND r.event_time >= '2026-01-01' AND r.event_time < '2026-01-02'
        WHERE b.household_id = 'H001' GROUP BY b.bill_lkr, b.rate_lkr_per_kwh""")[0]
    assert row["bill_lkr"] == row["expected"]


def test_early_tariff_file_waits_for_readings(pg_conn, settings):
    seed_day(pg_conn, settings, include_next_day=False)
    done, waiting = billing_job.readings_complete(pg_conn, DAY)
    assert not done and len(waiting) == 12
    hh = build_households(settings)
    db.insert_readings(pg_conn, rows_for(settings, hh[:5], [60]))  # next-day reading for 5 households
    pg_conn.commit()
    done, waiting = billing_job.readings_complete(pg_conn, DAY)
    assert not done and waiting == [h.household_id for h in hh[5:]]
    db.insert_readings(pg_conn, rows_for(settings, hh[5:], [60]))
    pg_conn.commit()
    assert billing_job.readings_complete(pg_conn, DAY) == (True, [])


def test_invalid_tariff_file_writes_no_bills(pg_conn, settings):
    seed_day(pg_conn, settings)
    rows = [r for r in generate_tariff_rows(settings, DAY, build_households(settings)) if r["household_id"] != "H003"]
    write_tariff_file(settings.incoming_dir, DAY, rows)
    tf = billing_job.find_pending_tariff_file(pg_conn, settings.incoming_dir)
    with pytest.raises(TariffValidationError, match="H003"):
        billing_job.run_billing(pg_conn, tf, settings.reports_dir, "run-bad")
    billing_job.record_failure(pg_conn, tf.to_dict(), "run-bad", "rejected", stage="validate_tariff")
    assert bills_snapshot(pg_conn) == []
    assert db.fetch_dicts(pg_conn, "SELECT status FROM billing_runs")[0]["status"] == "failed"
    # The rejected content is skipped (it must not block later days)...
    assert billing_job.find_pending_tariff_file(pg_conn, settings.incoming_dir) is None
    # ...but a corrected file (new sha256) is picked up automatically.
    write_tariff_file(settings.incoming_dir, DAY, generate_tariff_rows(settings, DAY, build_households(settings)))
    fixed = billing_job.find_pending_tariff_file(pg_conn, settings.incoming_dir)
    assert fixed is not None and fixed.sha256 != tf.sha256
    billing_job.run_billing(pg_conn, fixed, settings.reports_dir, "run-fixed")
    assert len(bills_snapshot(pg_conn)) == 12


def test_readiness_timeout_is_retried_not_skipped(pg_conn, settings):
    seed_day(pg_conn, settings, include_next_day=False)
    write_tariff_file(settings.incoming_dir, DAY, generate_tariff_rows(settings, DAY, build_households(settings)))
    tf = billing_job.find_pending_tariff_file(pg_conn, settings.incoming_dir)
    billing_job.record_failure(pg_conn, tf.to_dict(), "run-wait", "timeout", stage="wait_for_readings")
    assert billing_job.find_pending_tariff_file(pg_conn, settings.incoming_dir) == tf


def test_day_bounds_are_utc():
    start, end = billing_job.day_bounds(DAY)
    assert start.tzinfo == timezone.utc and end - start == timedelta(days=1)
