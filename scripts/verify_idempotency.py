"""Live end-to-end idempotency check against the running stack.

1. Snapshot totals for fully-settled simulated days (raw master data, speed-layer windows, bills).
2. Re-publish already-processed readings to Kafka (old ones + recent ones, several times each).
3. Wait for Spark to process them, and re-run billing for the latest billed tariff file.
4. Re-snapshot: every total must be unchanged, and Spark must report the replay as duplicates.

Run:  docker compose run --rm simulator python scripts/verify_idempotency.py
Exit code 0 = PASS, 1 = FAIL, 2 = not enough data yet (wait for the first bill).
"""

from __future__ import annotations

import json
import sys
import time
from datetime import timedelta

from confluent_kafka import Producer

from smartgrid import billing_job, db
from smartgrid.config import Settings
from smartgrid.logging_utils import get_logger

log = get_logger("verify")
READING_FIELDS = ["event_id", "meter_id", "household_id", "zone_id", "event_time", "generated_at",
                  "interval_minutes", "consumption_kwh", "solar_kwh"]


def snapshot(conn, days):
    raw = db.fetch_dicts(conn, """
        SELECT (event_time AT TIME ZONE 'UTC')::date AS day, count(*) AS n,
               round(sum(consumption_kwh)::numeric, 6) AS kwh, round(sum(solar_kwh)::numeric, 6) AS solar
        FROM meter_readings WHERE (event_time AT TIME ZONE 'UTC')::date = ANY(%s)
        GROUP BY 1 ORDER BY 1""", (days,))
    speed = db.fetch_dicts(conn, """
        SELECT sim_day AS day, sum(readings_count) AS n, round(sum(consumption_kwh)::numeric, 6) AS kwh
        FROM household_daily_totals WHERE sim_day = ANY(%s) GROUP BY 1 ORDER BY 1""", (days,))
    bills = db.fetch_dicts(conn, """
        SELECT bill_date, household_id, consumption_kwh, bill_lkr FROM household_bills
        WHERE bill_date = ANY(%s) ORDER BY 1, 2""", (days,))
    return {"raw": raw, "speed": speed, "bills": bills}


def to_message(row) -> bytes:
    msg = {k: row[k] for k in READING_FIELDS}
    msg["event_time"] = row["event_time"].isoformat()
    msg["generated_at"] = row["generated_at"].isoformat(timespec="milliseconds")
    return json.dumps(msg).encode()


def main() -> int:
    s = Settings.from_env()
    conn = db.connect(s)
    conn.autocommit = True
    billed = [r["bill_date"] for r in db.fetch_dicts(
        conn, "SELECT DISTINCT bill_date FROM household_bills ORDER BY 1")]
    latest = db.fetch_dicts(conn, "SELECT max(event_time) AS t FROM meter_readings")[0]["t"]
    if not billed or latest is None:
        print("NOT READY: no bills yet - wait until simulated day 1 has been billed (~6 real minutes).")
        return 2
    # Only compare days whose speed-layer windows are closed (older than the watermark).
    settled_before = (latest - timedelta(minutes=s.watermark_minutes + s.window_minutes)).date()
    days = [d for d in billed if d < settled_before] or billed[:1]
    before = snapshot(conn, days)
    dup_before = db.fetch_dicts(conn, "SELECT coalesce(sum(duplicate_rows),0) AS d FROM stream_batches "
                                      "WHERE query_name='readings_sink'")[0]["d"]
    print(f"Snapshot of settled days {[str(d) for d in days]}:")
    for r in before["raw"]:
        print(f"  raw   {r['day']}: {r['n']} readings, {r['kwh']} kWh")
    for r in before["speed"]:
        print(f"  speed {r['day']}: {r['n']} readings, {r['kwh']} kWh")
    print(f"  bills: {len(before['bills'])} rows, total LKR {sum(b['bill_lkr'] for b in before['bills'])}")

    old = db.fetch_dicts(conn, "SELECT * FROM meter_readings ORDER BY event_time LIMIT 50")
    recent = db.fetch_dicts(conn, "SELECT * FROM meter_readings ORDER BY event_time DESC LIMIT 24")
    producer = Producer({"bootstrap.servers": s.kafka_bootstrap, "acks": "all"})
    sent = 0
    for _ in range(2):
        for row in old + recent:
            producer.produce(s.kafka_topic, key=row["household_id"].encode(), value=to_message(row))
            sent += 1
    producer.flush(10)
    print(f"Replayed {sent} already-processed readings ({len(old)} oldest + {len(recent)} newest, x2) to Kafka.")

    tf_name = db.fetch_dicts(conn, "SELECT file_name FROM processed_tariff_files WHERE bill_date = %s",
                             (days[-1],))[0]["file_name"]
    tf = billing_job.describe_tariff_file(s.incoming_dir / tf_name)
    conn.autocommit = False
    billing_job.run_billing(conn, tf, s.reports_dir, "verify-idempotency-rerun")
    conn.autocommit = True
    print(f"Re-ran billing for {tf_name}.")

    print("Waiting 20 s for Spark to process the replay...")
    time.sleep(20)
    after = snapshot(conn, days)
    dup_after = db.fetch_dicts(conn, "SELECT coalesce(sum(duplicate_rows),0) AS d FROM stream_batches "
                                     "WHERE query_name='readings_sink'")[0]["d"]
    conn.close()

    checks = {
        "raw master totals unchanged": before["raw"] == after["raw"],
        "speed-layer daily totals unchanged": before["speed"] == after["speed"],
        "bills unchanged after re-run": before["bills"] == after["bills"],
        "Spark saw the replayed duplicates": dup_after - dup_before >= sent * 0.9,
    }
    print(f"Duplicates skipped by raw sink during check: {dup_after - dup_before} (sent {sent})")
    for name, ok in checks.items():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    ok = all(checks.values())
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
