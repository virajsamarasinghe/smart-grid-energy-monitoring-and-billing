"""Spark transformations for the speed layer. Pure DataFrame -> DataFrame functions so they can
be unit-tested with a local SparkSession (batch or streaming) without Kafka or Postgres."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

READING_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("meter_id", StringType()),
    StructField("household_id", StringType()),
    StructField("zone_id", StringType()),
    StructField("event_time", StringType()),
    StructField("generated_at", StringType()),
    StructField("interval_minutes", DoubleType()),
    StructField("consumption_kwh", DoubleType()),
    StructField("solar_kwh", DoubleType()),
])
REQUIRED_FIELDS = [f.name for f in READING_SCHEMA.fields]
TZ_SUFFIX = r"(Z|[+-]\d{2}:?\d{2})$"

VALID_COLUMNS = [
    "event_id", "meter_id", "household_id", "zone_id", "event_time", "generated_at",
    "interval_minutes", "consumption_kwh", "solar_kwh", "kafka_partition", "kafka_offset",
]


def _validation_error(j: Column, event_time: Column, generated_at: Column, max_kwh: float) -> Column:
    """First failing rule as a short reason code, or NULL if the reading is valid."""
    all_null = reduce(lambda a, b: a & b, [j[f].isNull() for f in REQUIRED_FIELDS])
    rules: list[tuple[Column, str]] = [(j.isNull() | all_null, "malformed_json")]
    for f in REQUIRED_FIELDS:
        cond = j[f].isNull()
        if isinstance(READING_SCHEMA[f].dataType, StringType):
            cond = cond | (F.trim(j[f]) == "")
        rules.append((cond, f"missing_or_invalid:{f}"))
    rules += [
        (~j["household_id"].rlike(r"^H\d{3}$"), "invalid_household_id"),
        (~j["zone_id"].rlike(r"^Z\d+$"), "invalid_zone_id"),
        (~j["event_time"].rlike(TZ_SUFFIX) | ~j["generated_at"].rlike(TZ_SUFFIX), "timestamp_without_timezone"),
        (event_time.isNull(), "unparseable_event_time"),
        (generated_at.isNull(), "unparseable_generated_at"),
        (j["interval_minutes"] <= 0, "non_positive_interval"),
        (j["consumption_kwh"] < 0, "negative_consumption_kwh"),
        (j["solar_kwh"] < 0, "negative_solar_kwh"),
        (j["consumption_kwh"] > max_kwh, "consumption_kwh_out_of_range"),
        (j["solar_kwh"] > max_kwh, "solar_kwh_out_of_range"),
    ]
    expr = F.when(rules[0][0], F.lit(rules[0][1]))
    for cond, reason in rules[1:]:
        expr = expr.when(cond, F.lit(reason))
    return expr  # otherwise NULL


def parse_readings(kafka_df: DataFrame, max_kwh: float = 50.0) -> DataFrame:
    """Kafka rows (value, topic, partition, offset, timestamp) -> typed columns + ``error_reason``.

    Timestamps are cast with Spark's ISO-8601 parser, which honours the explicit offset; the
    session time zone is UTC so the resulting instants are unambiguous.
    """
    df = kafka_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn("j", F.from_json("raw_value", READING_SCHEMA, {"mode": "PERMISSIVE"}))

    event_time = F.col("j.event_time").cast("timestamp")
    generated_at = F.col("j.generated_at").cast("timestamp")
    return df.select(
        "raw_value", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        F.col("j.event_id").alias("event_id"),
        F.col("j.meter_id").alias("meter_id"),
        F.col("j.household_id").alias("household_id"),
        F.col("j.zone_id").alias("zone_id"),
        event_time.alias("event_time"),
        generated_at.alias("generated_at"),
        F.col("j.interval_minutes").alias("interval_minutes"),
        F.col("j.consumption_kwh").alias("consumption_kwh"),
        F.col("j.solar_kwh").alias("solar_kwh"),
        _validation_error(F.col("j"), event_time, generated_at, max_kwh).alias("error_reason"),
    )


def valid_readings(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("error_reason").isNull()).select(*VALID_COLUMNS)


def invalid_readings(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("error_reason").isNotNull()).select(
        "kafka_topic", "kafka_partition", "kafka_offset", "raw_value", "error_reason", "kafka_timestamp")


def mark_batch_duplicates(parsed: DataFrame) -> DataFrame:
    """Add ``is_batch_duplicate``: true for a valid reading whose event_id already appeared
    earlier (lower partition/offset) in the same micro-batch. Stateless within one batch;
    cross-batch duplicates are rejected by the ``meter_readings`` primary key."""
    first_seen = Window.partitionBy("event_id").orderBy("kafka_partition", "kafka_offset")
    return parsed.withColumn(
        "is_batch_duplicate",
        F.when(F.col("error_reason").isNull(), F.row_number().over(first_seen) > 1).otherwise(F.lit(False)),
    )


def dedupe_batch(parsed: DataFrame) -> DataFrame:
    """Valid readings with in-batch duplicates removed."""
    return valid_readings(mark_batch_duplicates(parsed).filter(~F.col("is_batch_duplicate")))


def household_window_usage(valid: DataFrame, window_minutes: int, watermark_minutes: int) -> DataFrame:
    """Streaming event-time aggregation per household per tumbling window.

    ``dropDuplicates(["event_id", "event_time"])`` behind a watermark is Spark's documented
    bounded-state dedup: event_id is derived from (household, event_time), so a duplicate
    always has the same event_time and its state can be evicted once the watermark passes.
    """
    w = F.window("event_time", f"{window_minutes} minutes")
    return (
        valid.withWatermark("event_time", f"{watermark_minutes} minutes")
        .dropDuplicates(["event_id", "event_time"])
        .groupBy(w.alias("w"), "household_id", "zone_id")
        .agg(
            F.sum("consumption_kwh").alias("consumption_kwh"),
            F.sum("solar_kwh").alias("solar_kwh"),
            F.count(F.lit(1)).alias("readings_count"),
        )
        .select(
            "household_id", "zone_id",
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "consumption_kwh", "solar_kwh", "readings_count",
        )
    )


def iso_utc(col: str) -> Column:
    """Render a timestamp as an ISO-8601 string with offset for a timezone-safe hand-off to Postgres."""
    return F.date_format(F.col(col), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
