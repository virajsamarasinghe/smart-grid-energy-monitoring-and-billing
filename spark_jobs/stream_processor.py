"""Spark Structured Streaming job (speed layer + master-dataset writer).

Two queries read the same Kafka topic, each with its own checkpoint directory:

1. ``readings_sink`` - parse, validate, split valid/invalid, flag duplicates within the micro-batch,
   then in ONE Postgres transaction insert valid readings (ON CONFLICT (event_id) DO NOTHING),
   dead-letter invalid ones, and record per-batch metrics.
2. ``window_usage_sink`` - watermark + event_id dedup + tumbling event-time window aggregate per
   household, written in update mode as absolute values (upsert), so replays are harmless.

Checkpoints store Kafka offsets and aggregation state; together with the idempotent sinks this
gives effectively-once results across restarts.
"""

from __future__ import annotations

import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from smartgrid import db
from smartgrid.config import Settings
from smartgrid.logging_utils import get_logger
from spark_jobs.transforms import (
    VALID_COLUMNS,
    household_window_usage,
    iso_utc,
    mark_batch_duplicates,
    parse_readings,
    valid_readings,
)

log = get_logger("spark-stream")
SETTINGS = Settings.from_env()


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("smartgrid-stream-processor")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "3"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def write_readings_batch(batch_df: DataFrame, batch_id: int) -> None:
    """One Spark job per micro-batch: flag in-batch duplicates, collect (batches are small),
    then commit readings + dead letters + batch metrics in one Postgres transaction."""
    started = time.monotonic()
    cols = [iso_utc(c).alias(c) if c in ("event_time", "generated_at", "kafka_timestamp") else F.col(c)
            for c in VALID_COLUMNS + ["kafka_topic", "raw_value", "error_reason", "kafka_timestamp"]]
    rows = mark_batch_duplicates(batch_df).select(*cols, "is_batch_duplicate").collect()

    valid_rows, invalid_rows, reasons = [], [], {}
    valid_count = 0
    for r in rows:
        if r["error_reason"] is None:
            valid_count += 1
            if not r["is_batch_duplicate"]:
                valid_rows.append(tuple(r[c] for c in VALID_COLUMNS))
        else:
            invalid_rows.append((r["kafka_topic"], r["kafka_partition"], r["kafka_offset"], r["raw_value"],
                                 r["error_reason"], r["kafka_timestamp"]))
            reasons[r["error_reason"]] = reasons.get(r["error_reason"], 0) + 1
    max_event_time = max((r[4] for r in valid_rows), default=None)

    with db.transaction(SETTINGS) as conn:
        inserted = db.insert_readings(conn, valid_rows)
        db.insert_invalid(conn, invalid_rows)
        db.record_stream_batch(
            conn, "readings_sink", batch_id, input_rows=len(rows), valid_rows=valid_count,
            invalid_rows=len(invalid_rows), inserted_rows=inserted,
            duplicate_rows=valid_count - inserted, max_event_time=max_event_time,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
    log.info("readings_batch_committed", batch_id=batch_id, input_rows=len(rows), valid_rows=valid_count,
             inserted_rows=inserted, duplicate_rows=valid_count - inserted, invalid_rows=len(invalid_rows),
             invalid_reasons=reasons, max_event_time=max_event_time,
             duration_ms=int((time.monotonic() - started) * 1000))


def write_window_batch(batch_df: DataFrame, batch_id: int) -> None:
    started = time.monotonic()
    rows = [tuple(r) for r in batch_df.select(
        "household_id", "zone_id", iso_utc("window_start"), iso_utc("window_end"),
        "consumption_kwh", "solar_kwh", "readings_count").collect()]
    with db.transaction(SETTINGS) as conn:
        db.upsert_window_usage(conn, rows)
        db.record_stream_batch(
            conn, "window_usage_sink", batch_id, input_rows=len(rows), valid_rows=len(rows), invalid_rows=0,
            inserted_rows=len(rows), duplicate_rows=0, max_event_time=max((r[2] for r in rows), default=None),
            duration_ms=int((time.monotonic() - started) * 1000),
        )
    if rows:
        log.info("window_batch_committed", batch_id=batch_id, updated_windows=len(rows),
                 duration_ms=int((time.monotonic() - started) * 1000))


def main() -> None:
    checkpoint_root = os.getenv("CHECKPOINT_DIR", "/checkpoints")
    spark = build_spark()
    log.info("stream_processor_starting", kafka=SETTINGS.kafka_bootstrap, topic=SETTINGS.kafka_topic,
             checkpoint_root=checkpoint_root, window_minutes=SETTINGS.window_minutes,
             watermark_minutes=SETTINGS.watermark_minutes)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", SETTINGS.kafka_bootstrap)
        .option("subscribe", SETTINGS.kafka_topic)
        .option("startingOffsets", "earliest")  # only used when there is no checkpoint
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = parse_readings(kafka_df, SETTINGS.max_kwh_per_reading)

    readings_q = (
        parsed.writeStream.queryName("readings_sink")
        .foreachBatch(write_readings_batch)
        .option("checkpointLocation", f"{checkpoint_root}/readings_sink")
        .trigger(processingTime=os.getenv("READINGS_TRIGGER", "5 seconds"))
        .start()
    )
    window_q = (
        household_window_usage(valid_readings(parsed), SETTINGS.window_minutes, SETTINGS.watermark_minutes)
        .writeStream.queryName("window_usage_sink")
        .outputMode("update")
        .foreachBatch(write_window_batch)
        .option("checkpointLocation", f"{checkpoint_root}/window_usage_sink")
        .trigger(processingTime=os.getenv("WINDOW_TRIGGER", "10 seconds"))
        .start()
    )
    log.info("stream_queries_started", queries=[readings_q.name, window_q.name])

    try:
        spark.streams.awaitAnyTermination()
    except Exception as exc:
        # Container restart policy brings the job back; it resumes from the checkpoints.
        log.error("stream_query_failed", error=str(exc)[:2000])
        raise


if __name__ == "__main__":
    main()
