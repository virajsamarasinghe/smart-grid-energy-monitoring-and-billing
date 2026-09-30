"""Spark validation / dedup / windowing tests with a local SparkSession (skipped without pyspark)."""

import json
from datetime import datetime, timezone

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from smartgrid.simulation import build_households, encode, generate_reading  # noqa: E402
from spark_jobs.transforms import (  # noqa: E402
    dedupe_batch,
    household_window_usage,
    invalid_readings,
    parse_readings,
    valid_readings,
)

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def kafka_like(spark, payloads):
    rows = [(p if isinstance(p, str) else p.decode(), i) for i, p in enumerate(payloads)]
    return (spark.createDataFrame(rows, "value string, offset long")
            .select(F.col("value").cast("binary").alias("value"), F.lit("meter-readings").alias("topic"),
                    F.lit(0).alias("partition"), "offset", F.current_timestamp().alias("timestamp")))


def reading(settings, i=0, tick=10, **override):
    r = generate_reading(settings, build_households(settings)[i], tick, NOW)
    r.update(override)
    return r


def test_valid_reading_is_parsed_with_utc_instants(spark, settings):
    r = reading(settings)
    out = parse_readings(kafka_like(spark, [encode(r)])).collect()[0]
    assert out.error_reason is None
    assert out.event_id == r["event_id"]
    assert out.event_time == datetime(2026, 1, 1, 4, 0)  # naive, in session/driver TZ = UTC
    assert out.consumption_kwh == r["consumption_kwh"]


def test_invalid_readings_are_separated_with_reasons(spark, settings):
    r = reading(settings)
    missing = dict(r)
    missing.pop("household_id")
    payloads = [
        "{not json",
        json.dumps(missing),
        json.dumps(dict(r, consumption_kwh=-1.0)),
        json.dumps(dict(r, solar_kwh=-0.2)),
        json.dumps(dict(r, event_time="2026-01-01T04:00:00")),  # no timezone
        json.dumps(dict(r, household_id="house-1")),
        json.dumps(dict(r, consumption_kwh=999.0)),
        json.dumps(dict(r, event_time="yesterday+00:00")),
        encode(r),
    ]
    parsed = parse_readings(kafka_like(spark, payloads))
    reasons = [row.error_reason for row in parsed.orderBy("kafka_offset").collect()]
    assert reasons == [
        "malformed_json",
        "missing_or_invalid:household_id",
        "negative_consumption_kwh",
        "negative_solar_kwh",
        "timestamp_without_timezone",
        "invalid_household_id",
        "consumption_kwh_out_of_range",
        "unparseable_event_time",
        None,
    ]
    assert valid_readings(parsed).count() == 1
    assert invalid_readings(parsed).count() == 8


def test_batch_dedup_by_event_id(spark, settings):
    a, b = reading(settings, 0), reading(settings, 1)
    parsed = parse_readings(kafka_like(spark, [encode(a), encode(a), encode(b), encode(a)]))
    deduped = dedupe_batch(parsed)
    assert sorted(r.event_id for r in deduped.collect()) == sorted([a["event_id"], b["event_id"]])


def test_streaming_window_aggregate_ignores_duplicates_across_restarts(spark, settings, tmp_path):
    """Two streaming runs over the same checkpoint; the second delivers replayed duplicates.

    Output rows are upserted into a dict exactly like the Postgres sink, so this checks the
    end-to-end speed-layer contract: totals equal the sum over *unique* events.
    """
    src, ckpt = tmp_path / "src", tmp_path / "ckpt"
    src.mkdir()
    hh = build_households(settings)[:2]
    first = [generate_reading(settings, h, t, NOW) for t in range(0, 10) for h in hh]  # 00:00-04:00
    second_new = [generate_reading(settings, h, t, NOW) for t in range(10, 15) for h in hh]
    (src / "part1.json").write_text("\n".join(json.dumps(r) for r in first + first[:4]) + "\n")

    sink: dict = {}

    def upsert(batch_df, _batch_id):
        for row in batch_df.collect():
            sink[(row.household_id, row.window_start)] = (row.consumption_kwh, row.readings_count)

    def run_once():
        stream = (spark.readStream.text(str(src))
                  .select(F.col("value").cast("binary").alias("value"), F.lit("t").alias("topic"),
                          F.lit(0).alias("partition"), F.lit(0).cast("long").alias("offset"),
                          F.current_timestamp().alias("timestamp")))
        agg = household_window_usage(valid_readings(parse_readings(stream)), 120, 60)
        q = (agg.writeStream.outputMode("update").foreachBatch(upsert)
             .option("checkpointLocation", str(ckpt)).trigger(availableNow=True).start())
        q.awaitTermination(120)
        assert q.exception() is None

    run_once()
    # "Restart": replay recent duplicates (still inside the watermark) plus new readings.
    (src / "part2.json").write_text("\n".join(json.dumps(r) for r in first[-6:] + second_new + second_new[:2]) + "\n")
    run_once()

    unique = {r["event_id"]: r for r in first + second_new}
    expected: dict = {}
    for r in unique.values():
        start = datetime.fromisoformat(r["event_time"]).replace(tzinfo=None)
        key = (r["household_id"], start.replace(hour=start.hour - start.hour % 2, minute=0))
        c, n = expected.get(key, (0.0, 0))
        expected[key] = (c + r["consumption_kwh"], n + 1)
    assert set(sink) == set(expected)
    for k, (c, n) in expected.items():
        assert sink[k][1] == n, k
        assert sink[k][0] == pytest.approx(c), k
