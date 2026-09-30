"""Read-side queries used by the Streamlit dashboard and the FastAPI service."""

from __future__ import annotations

from datetime import date

from smartgrid.alerts import DAYLIGHT_HOURS
from smartgrid.db import fetch_dicts


def pipeline_snapshot(conn) -> dict:
    snap = fetch_dicts(
        conn,
        """
        SELECT
          (SELECT max(ingested_at) FROM meter_readings)                                AS latest_ingested_at,
          (SELECT max(event_time) FROM meter_readings)                                 AS latest_event_time,
          (SELECT count(*) FROM meter_readings)                                        AS readings_total,
          (SELECT count(*) FROM invalid_readings)                                      AS invalid_total,
          (SELECT count(*) FROM meter_readings WHERE ingested_at > now() - interval '5 minutes')   AS recent_valid,
          (SELECT count(*) FROM invalid_readings WHERE ingested_at > now() - interval '5 minutes') AS recent_invalid,
          (SELECT coalesce(sum(duplicate_rows), 0) FROM stream_batches WHERE query_name = 'readings_sink')
                                                                                       AS duplicates_total,
          (SELECT max(processed_at) FROM stream_batches)                               AS latest_batch_at,
          (SELECT count(*) FROM households)                                            AS households
        """,
    )[0]
    last = fetch_dicts(
        conn,
        """SELECT tariff_file, bill_date, status, households_billed, total_bill_lkr,
                  speed_layer_max_diff_kwh, message, run_id, finished_at
           FROM billing_runs ORDER BY id DESC LIMIT 1""",
    )
    snap["last_billing_run"] = last[0] if last else None
    latest = fetch_dicts(conn, "SELECT * FROM meter_readings ORDER BY ingested_at DESC, event_time DESC LIMIT 1")
    snap["latest_reading"] = latest[0] if latest else None
    return snap


def zone_windows(conn, day: date) -> list[dict]:
    return fetch_dicts(
        conn,
        """SELECT zone_id, window_start, window_end, consumption_kwh, solar_kwh, estimated_net_kwh,
                  readings_count, households
           FROM zone_window_totals
           WHERE window_start >= %s::date AND window_start < %s::date + 1
           ORDER BY window_start, zone_id""",
        (day, day),
    )


def zone_day_totals(conn, day: date) -> list[dict]:
    return fetch_dicts(
        conn,
        """SELECT zone_id, SUM(consumption_kwh) AS consumption_kwh, SUM(solar_kwh) AS solar_kwh,
                  SUM(estimated_net_kwh) AS estimated_net_kwh,
                  CASE WHEN SUM(consumption_kwh) > 0 THEN SUM(solar_kwh) / SUM(consumption_kwh) END AS solar_share
           FROM household_daily_totals WHERE sim_day = %s
           GROUP BY zone_id ORDER BY zone_id""",
        (day,),
    )


def latest_zone_windows(conn) -> list[dict]:
    """Most recent (possibly still open) window per zone - the 'current grid load'."""
    return fetch_dicts(
        conn,
        """SELECT DISTINCT ON (zone_id) zone_id, window_start, window_end, consumption_kwh, solar_kwh,
                  estimated_net_kwh, readings_count
           FROM zone_window_totals ORDER BY zone_id, window_start DESC""",
    )


def latest_daylight_zone_windows(conn) -> list[dict]:
    """Latest daylight (10:00-14:00 simulated) window per zone, used by the low-renewable alert."""
    lo, hi = DAYLIGHT_HOURS
    return fetch_dicts(
        conn,
        """SELECT DISTINCT ON (zone_id) zone_id, window_start, consumption_kwh, solar_kwh
           FROM zone_window_totals
           WHERE extract(hour FROM window_start AT TIME ZONE 'UTC') >= %s
             AND extract(hour FROM window_start AT TIME ZONE 'UTC') < %s
           ORDER BY zone_id, window_start DESC""",
        (lo, hi),
    )


def household_daily(conn, day: date) -> list[dict]:
    return fetch_dicts(
        conn,
        """SELECT household_id, zone_id, consumption_kwh, solar_kwh, estimated_net_kwh, readings_count
           FROM household_daily_totals WHERE sim_day = %s ORDER BY household_id""",
        (day,),
    )


def bill_dates(conn) -> list[date]:
    return [r["bill_date"] for r in fetch_dicts(
        conn, "SELECT DISTINCT bill_date FROM household_bills ORDER BY bill_date DESC")]


def bills(conn, day: date) -> list[dict]:
    return fetch_dicts(
        conn,
        """SELECT bill_date, household_id, zone_id, consumption_kwh, solar_kwh,
                  CASE WHEN consumption_kwh > 0 THEN round(solar_kwh / consumption_kwh * 100, 2) ELSE 0 END
                      AS solar_share_pct,
                  consumption_kwh - solar_kwh AS estimated_net_kwh,
                  rate_lkr_per_kwh, bill_lkr, readings_count, billing_tier, subsidy_flag, tariff_file, computed_at
           FROM household_bills WHERE bill_date = %s ORDER BY household_id""",
        (day,),
    )


def recent_billing_runs(conn, limit: int = 10) -> list[dict]:
    return fetch_dicts(
        conn,
        """SELECT finished_at, tariff_file, bill_date, status, households_billed, total_bill_lkr,
                  speed_layer_max_diff_kwh, message
           FROM billing_runs ORDER BY id DESC LIMIT %s""",
        (limit,),
    )
