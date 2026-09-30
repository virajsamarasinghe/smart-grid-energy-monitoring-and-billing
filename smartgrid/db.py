"""PostgreSQL helpers shared by the simulator, Spark sink, Airflow and serving layers."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterable, Iterator, Sequence

import psycopg2
import psycopg2.extras

from smartgrid.config import Settings
from smartgrid.logging_utils import get_logger
from smartgrid.simulation import Household

log = get_logger("db")


def connect(settings: Settings, dbname: str | None = None, retries: int = 30, delay: float = 2.0):
    """Open a connection, retrying while Postgres is starting up."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(settings.pg_dsn(dbname), connect_timeout=5)
        except psycopg2.OperationalError as exc:
            last_exc = exc
            log.warning("postgres_unavailable", attempt=attempt, retries=retries,
                        host=settings.pg_host, db=dbname or settings.pg_db, error=str(exc).strip())
            time.sleep(delay)
    raise RuntimeError(
        f"Could not connect to Postgres at {settings.pg_host}:{settings.pg_port} "
        f"after {retries} attempts: {last_exc}"
    )


@contextmanager
def transaction(settings: Settings, dbname: str | None = None, retries: int = 30) -> Iterator:
    """Yield a connection inside one transaction; commit on success, roll back on error."""
    conn = connect(settings, dbname, retries=retries)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def fetch_dicts(conn, sql: str, params: Sequence | dict | None = None) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def register_households(conn, households: Iterable[Household]) -> None:
    rows = [(h.household_id, h.meter_id, h.zone_id, h.has_solar, h.solar_capacity_kw) for h in households]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO households (household_id, meter_id, zone_id, has_solar, solar_capacity_kw)
            VALUES %s
            ON CONFLICT (household_id) DO UPDATE
               SET meter_id = EXCLUDED.meter_id, zone_id = EXCLUDED.zone_id,
                   has_solar = EXCLUDED.has_solar, solar_capacity_kw = EXCLUDED.solar_capacity_kw
            """,
            rows,
        )


READING_COLUMNS = (
    "event_id", "meter_id", "household_id", "zone_id", "event_time", "generated_at",
    "interval_minutes", "consumption_kwh", "solar_kwh", "kafka_partition", "kafka_offset",
)


def insert_readings(conn, rows: Sequence[Sequence]) -> int:
    """Insert readings (tuples in READING_COLUMNS order). Returns how many were new.

    Existing event_ids are silently skipped, which is what makes replays idempotent.
    """
    if not rows:
        return 0
    with conn.cursor() as cur:
        inserted = psycopg2.extras.execute_values(
            cur,
            f"""
            INSERT INTO meter_readings ({", ".join(READING_COLUMNS)}) VALUES %s
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            rows,
            page_size=1000,
            fetch=True,
        )
    return len(inserted)


def insert_invalid(conn, rows: Sequence[Sequence]) -> None:
    """rows: (topic, partition, offset, raw_value, error_reason, kafka_timestamp)."""
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO invalid_readings
                (kafka_topic, kafka_partition, kafka_offset, raw_value, error_reason, kafka_timestamp)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
        )


def upsert_window_usage(conn, rows: Sequence[Sequence]) -> None:
    """rows: (household_id, zone_id, window_start, window_end, consumption, solar, count)."""
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO household_window_usage
                (household_id, zone_id, window_start, window_end, consumption_kwh, solar_kwh, readings_count)
            VALUES %s
            ON CONFLICT (household_id, window_start) DO UPDATE
               SET consumption_kwh = EXCLUDED.consumption_kwh,
                   solar_kwh       = EXCLUDED.solar_kwh,
                   readings_count  = EXCLUDED.readings_count,
                   window_end      = EXCLUDED.window_end,
                   updated_at      = now()
            """,
            rows,
        )


def record_stream_batch(conn, query_name: str, batch_id: int, *, input_rows: int, valid_rows: int,
                        invalid_rows: int, inserted_rows: int, duplicate_rows: int,
                        max_event_time: str | None, duration_ms: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stream_batches (query_name, batch_id, input_rows, valid_rows, invalid_rows,
                                        inserted_rows, duplicate_rows, max_event_time, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (query_name, batch_id) DO UPDATE
               SET input_rows = EXCLUDED.input_rows, valid_rows = EXCLUDED.valid_rows,
                   invalid_rows = EXCLUDED.invalid_rows, inserted_rows = EXCLUDED.inserted_rows,
                   duplicate_rows = EXCLUDED.duplicate_rows, max_event_time = EXCLUDED.max_event_time,
                   duration_ms = EXCLUDED.duration_ms, processed_at = now()
            """,
            (query_name, batch_id, input_rows, valid_rows, invalid_rows, inserted_rows,
             duplicate_rows, max_event_time, duration_ms),
        )
