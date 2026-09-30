"""Batch-layer billing job. The Airflow DAG is a thin wrapper around these functions.

Idempotency
-----------
* Bills are upserted on ``(bill_date, household_id)``; re-running replaces rows with identical
  values (consumption is recomputed from the de-duplicated master table ``meter_readings``).
* The bills, the ``processed_tariff_files`` marker and the ``billing_runs`` audit row are written
  in **one transaction**, so a crash never leaves a half-billed day.

Early tariff files
------------------
A tariff file is written the moment the simulated day ends, possibly before Spark has stored
the day's last readings. ``readings_complete`` therefore requires every household to have at
least one stored reading with ``event_time >= day_end``. Readings are keyed by household_id,
so each household's readings sit in one Kafka partition in time order; seeing a next-day reading
implies all earlier readings for that household were already committed. Until then the DAG's
sensor waits; if it times out the run fails, the file stays pending and the next run retries.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2.extras

from smartgrid.billing import (
    Bill,
    TariffRate,
    TariffValidationError,
    Usage,
    calculate_bills,
    parse_tariff_csv,
    validate_coverage,
)
from smartgrid.db import fetch_dicts
from smartgrid.logging_utils import get_logger
from smartgrid.tariffs import TARIFF_FILE_RE, atomic_write_text

log = get_logger("billing")

REPORT_COLUMNS = [
    "bill_date", "household_id", "zone_id", "consumption_kwh", "solar_kwh", "solar_share_pct",
    "estimated_net_kwh", "rate_lkr_per_kwh", "bill_lkr", "readings_count", "billing_tier", "subsidy_flag",
]


@dataclass(frozen=True)
class TariffFile:
    path: str
    file_name: str
    bill_date: str  # ISO date
    sha256: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class BillingResult:
    tariff_file: str
    bill_date: str
    households_billed: int
    total_bill_lkr: str
    total_consumption_kwh: str
    speed_layer_max_diff_kwh: float | None
    report_path: str


def day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def describe_tariff_file(path: Path) -> TariffFile:
    m = TARIFF_FILE_RE.match(path.name)
    if not m:
        raise TariffValidationError(f"{path.name} does not match tariffs_YYYY-MM-DD.csv")
    return TariffFile(str(path), path.name, m.group(1), sha256_file(path))


def list_tariff_files(incoming_dir: Path) -> list[Path]:
    """Completed tariff files only: temp files are hidden ``.*.tmp`` and never match."""
    if not incoming_dir.exists():
        return []
    return sorted(p for p in incoming_dir.iterdir() if p.is_file() and TARIFF_FILE_RE.match(p.name))


def find_pending_tariff_file(conn, incoming_dir: Path) -> TariffFile | None:
    """Oldest file that is new, or whose content changed since it was last billed.

    A file whose exact content (sha256) already failed *validation* is skipped so one bad file
    cannot block later days; replacing it with a corrected file changes the hash and re-queues it.
    Readiness timeouts are not skipped - those are retried on the next run.
    """
    done = {r["file_name"]: r["file_sha256"] for r in fetch_dicts(
        conn, "SELECT file_name, file_sha256 FROM processed_tariff_files")}
    rejected = {(r["tariff_file"], r["file_sha256"]) for r in fetch_dicts(
        conn, "SELECT tariff_file, file_sha256 FROM billing_runs "
              "WHERE status = 'failed' AND failed_stage = 'validate_tariff'")}
    for path in list_tariff_files(incoming_dir):
        tf = describe_tariff_file(path)
        if done.get(tf.file_name) != tf.sha256 and (tf.file_name, tf.sha256) not in rejected:
            return tf
    return None


def registered_households(conn) -> dict[str, str]:
    return {r["household_id"]: r["zone_id"] for r in fetch_dicts(
        conn, "SELECT household_id, zone_id FROM households")}


def load_and_validate(conn, tf: TariffFile) -> list[TariffRate]:
    text = Path(tf.path).read_text(encoding="utf-8")
    rates = parse_tariff_csv(text, date.fromisoformat(tf.bill_date))
    households = registered_households(conn)
    if not households:
        raise TariffValidationError("no households registered yet; is the simulator running?")
    validate_coverage(rates, set(households))
    return rates


def readings_complete(conn, bill_date: date) -> tuple[bool, list[str]]:
    """True when every registered household has a stored reading at/after the day's end."""
    _, day_end = day_bounds(bill_date)
    rows = fetch_dicts(
        conn,
        """
        SELECT h.household_id,
               EXISTS (SELECT 1 FROM meter_readings r
                        WHERE r.household_id = h.household_id AND r.event_time >= %s) AS done
        FROM households h
        """,
        (day_end,),
    )
    waiting = sorted(r["household_id"] for r in rows if not r["done"])
    return (bool(rows) and not waiting), waiting


def daily_usage(conn, bill_date: date) -> dict[str, Usage]:
    """Authoritative daily totals recomputed from the de-duplicated master dataset."""
    start, end = day_bounds(bill_date)
    rows = fetch_dicts(
        conn,
        """
        SELECT household_id, SUM(consumption_kwh) AS c, SUM(solar_kwh) AS s, COUNT(*) AS n
        FROM meter_readings WHERE event_time >= %s AND event_time < %s
        GROUP BY household_id
        """,
        (start, end),
    )
    return {r["household_id"]: Usage(r["household_id"], float(r["c"]), float(r["s"]), int(r["n"])) for r in rows}


def speed_layer_max_diff(conn, bill_date: date, usage: dict[str, Usage]) -> float | None:
    """Reconciliation metric: max |batch - speed| daily consumption across households."""
    rows = fetch_dicts(
        conn, "SELECT household_id, consumption_kwh FROM household_daily_totals WHERE sim_day = %s", (bill_date,))
    if not rows:
        return None
    speed = {r["household_id"]: float(r["consumption_kwh"]) for r in rows}
    keys = set(speed) | set(usage)
    return max(abs(speed.get(k, 0.0) - (usage[k].consumption_kwh if k in usage else 0.0)) for k in keys)


def upsert_bills(conn, bills: list[Bill], tariff_file: str, run_id: str) -> None:
    rows = [
        (b.bill_date, b.household_id, b.zone_id, b.consumption_kwh, b.solar_kwh, b.rate_lkr_per_kwh,
         b.bill_lkr, b.readings_count, b.billing_tier, b.subsidy_flag, tariff_file, run_id)
        for b in bills
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO household_bills (bill_date, household_id, zone_id, consumption_kwh, solar_kwh,
                rate_lkr_per_kwh, bill_lkr, readings_count, billing_tier, subsidy_flag, tariff_file, run_id)
            VALUES %s
            ON CONFLICT (bill_date, household_id) DO UPDATE SET
                zone_id = EXCLUDED.zone_id, consumption_kwh = EXCLUDED.consumption_kwh,
                solar_kwh = EXCLUDED.solar_kwh, rate_lkr_per_kwh = EXCLUDED.rate_lkr_per_kwh,
                bill_lkr = EXCLUDED.bill_lkr, readings_count = EXCLUDED.readings_count,
                billing_tier = EXCLUDED.billing_tier, subsidy_flag = EXCLUDED.subsidy_flag,
                tariff_file = EXCLUDED.tariff_file, run_id = EXCLUDED.run_id, computed_at = now()
            """,
            rows,
        )


def record_run(conn, *, tariff_file: str, bill_date: str | None, status: str, run_id: str,
               households_billed: int | None = None, total_bill_lkr: Decimal | None = None,
               speed_layer_max_diff_kwh: float | None = None, message: str | None = None,
               file_sha256: str | None = None, failed_stage: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO billing_runs (tariff_file, bill_date, status, households_billed, total_bill_lkr,
                                      speed_layer_max_diff_kwh, message, run_id, file_sha256, failed_stage)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (tariff_file, bill_date, status, households_billed, total_bill_lkr,
             speed_layer_max_diff_kwh, (message or "")[:2000], run_id, file_sha256, failed_stage),
        )


def render_report(bills: list[Bill]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=REPORT_COLUMNS, lineterminator="\n", extrasaction="ignore")
    writer.writeheader()
    for b in bills:
        writer.writerow(b.as_dict())
    return buf.getvalue()


def run_billing(conn, tf: TariffFile, reports_dir: Path, run_id: str) -> BillingResult:
    """Validate, compute and upsert bills for one tariff file in a single transaction.

    The caller owns ``conn``; this function commits on success and rolls back on error.
    """
    bill_date = date.fromisoformat(tf.bill_date)
    try:
        rates = load_and_validate(conn, tf)
        usage = daily_usage(conn, bill_date)
        zones = registered_households(conn)
        bills = calculate_bills(bill_date, rates, usage, zones)
        total = sum((b.bill_lkr for b in bills), Decimal("0"))
        diff = speed_layer_max_diff(conn, bill_date, usage)

        upsert_bills(conn, bills, tf.file_name, run_id)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_tariff_files (file_name, file_sha256, bill_date) VALUES (%s, %s, %s)
                ON CONFLICT (file_name) DO UPDATE
                   SET file_sha256 = EXCLUDED.file_sha256, bill_date = EXCLUDED.bill_date, processed_at = now()
                """,
                (tf.file_name, tf.sha256, bill_date),
            )
        missing_usage = sorted(b.household_id for b in bills if b.readings_count == 0)
        message = f"billed {len(bills)} households"
        if missing_usage:
            message += f"; no readings for {missing_usage} (billed 0 kWh)"
        record_run(conn, tariff_file=tf.file_name, bill_date=tf.bill_date, status="success", run_id=run_id,
                   households_billed=len(bills), total_bill_lkr=total, speed_layer_max_diff_kwh=diff,
                   message=message, file_sha256=tf.sha256)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    report = reports_dir / f"billing_report_{tf.bill_date}.csv"
    atomic_write_text(report, render_report(bills))
    result = BillingResult(
        tariff_file=tf.file_name,
        bill_date=tf.bill_date,
        households_billed=len(bills),
        total_bill_lkr=str(total),
        total_consumption_kwh=str(sum((b.consumption_kwh for b in bills), Decimal("0"))),
        speed_layer_max_diff_kwh=diff,
        report_path=str(report),
    )
    log.info("billing_completed", run_id=run_id, **asdict(result))
    if missing_usage:
        log.warning("households_without_readings", bill_date=tf.bill_date, households=missing_usage)
    if diff is not None and diff > 0.01:
        log.warning("speed_batch_mismatch", bill_date=tf.bill_date, max_diff_kwh=round(diff, 4),
                    hint="speed layer drops readings later than the watermark; batch layer is authoritative")
    return result


def record_failure(conn, tf: dict | None, run_id: str, message: str, stage: str | None = None) -> None:
    try:
        conn.rollback()
        record_run(conn, tariff_file=(tf or {}).get("file_name", "unknown"), bill_date=(tf or {}).get("bill_date"),
                   status="failed", run_id=run_id, message=message, file_sha256=(tf or {}).get("sha256"),
                   failed_stage=stage)
        conn.commit()
    except Exception as exc:  # never mask the original failure
        log.error("record_failure_failed", error=str(exc))
    log.error("billing_failed", run_id=run_id, tariff_file=(tf or {}).get("file_name"), message=message)
