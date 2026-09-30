-- Smart-Grid Energy Monitoring and Billing: serving schema.
-- Idempotent: safe to apply on every start (applied by `python -m smartgrid.db_init`).
-- All timestamps are TIMESTAMPTZ and sessions run with timezone=UTC.

-- Meter registry (written by the simulator at start-up; used by billing validation).
CREATE TABLE IF NOT EXISTS households (
    household_id      TEXT PRIMARY KEY,
    meter_id          TEXT NOT NULL,
    zone_id           TEXT NOT NULL,
    has_solar         BOOLEAN NOT NULL,
    solar_capacity_kw DOUBLE PRECISION NOT NULL DEFAULT 0,
    registered_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Master dataset: every valid, de-duplicated reading (Spark raw sink). event_id is the
-- idempotency key: replays and duplicates hit ON CONFLICT DO NOTHING.
CREATE TABLE IF NOT EXISTS meter_readings (
    event_id         TEXT PRIMARY KEY,
    meter_id         TEXT NOT NULL,
    household_id     TEXT NOT NULL,
    zone_id          TEXT NOT NULL,
    event_time       TIMESTAMPTZ NOT NULL,   -- simulated clock, start of the interval
    generated_at     TIMESTAMPTZ NOT NULL,   -- real clock at the meter
    interval_minutes DOUBLE PRECISION NOT NULL,
    consumption_kwh  DOUBLE PRECISION NOT NULL CHECK (consumption_kwh >= 0),
    solar_kwh        DOUBLE PRECISION NOT NULL CHECK (solar_kwh >= 0),
    kafka_partition  INT,
    kafka_offset     BIGINT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_meter_readings_event_time ON meter_readings (event_time);
CREATE INDEX IF NOT EXISTS ix_meter_readings_household_time ON meter_readings (household_id, event_time);
CREATE INDEX IF NOT EXISTS ix_meter_readings_ingested_at ON meter_readings (ingested_at);

-- Dead-letter table for readings that failed validation. Keyed by Kafka coordinates so a
-- replayed micro-batch does not store the same bad message twice.
CREATE TABLE IF NOT EXISTS invalid_readings (
    kafka_topic     TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    raw_value       TEXT,
    error_reason    TEXT NOT NULL,
    kafka_timestamp TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS ix_invalid_readings_ingested_at ON invalid_readings (ingested_at);

-- Speed layer: Spark event-time window aggregate per household. Rows are upserted with
-- absolute values from Spark's checkpointed state, so re-running a micro-batch is harmless.
CREATE TABLE IF NOT EXISTS household_window_usage (
    household_id    TEXT NOT NULL,
    zone_id         TEXT NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    consumption_kwh DOUBLE PRECISION NOT NULL,
    solar_kwh       DOUBLE PRECISION NOT NULL,
    readings_count  INT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (household_id, window_start)
);
CREATE INDEX IF NOT EXISTS ix_hwu_window_start ON household_window_usage (window_start);

-- Zone-level time-window totals for the dashboard.
CREATE OR REPLACE VIEW zone_window_totals AS
SELECT zone_id,
       window_start,
       window_end,
       SUM(consumption_kwh)                  AS consumption_kwh,
       SUM(solar_kwh)                        AS solar_kwh,
       SUM(consumption_kwh) - SUM(solar_kwh) AS estimated_net_kwh,
       SUM(readings_count)                   AS readings_count,
       COUNT(DISTINCT household_id)          AS households
FROM household_window_usage
GROUP BY zone_id, window_start, window_end;

-- Household daily totals from the speed layer (live, "so far today").
CREATE OR REPLACE VIEW household_daily_totals AS
SELECT household_id,
       zone_id,
       (window_start AT TIME ZONE 'UTC')::date AS sim_day,
       SUM(consumption_kwh)                  AS consumption_kwh,
       SUM(solar_kwh)                        AS solar_kwh,
       SUM(consumption_kwh) - SUM(solar_kwh) AS estimated_net_kwh,
       SUM(readings_count)                   AS readings_count
FROM household_window_usage
GROUP BY household_id, zone_id, (window_start AT TIME ZONE 'UTC')::date;

-- One row per Spark micro-batch, for observability (throughput, invalid rate, duplicates).
CREATE TABLE IF NOT EXISTS stream_batches (
    query_name     TEXT NOT NULL,
    batch_id       BIGINT NOT NULL,
    input_rows     INT NOT NULL,
    valid_rows     INT NOT NULL,
    invalid_rows   INT NOT NULL,
    inserted_rows  INT NOT NULL,
    duplicate_rows INT NOT NULL,
    max_event_time TIMESTAMPTZ,
    duration_ms    INT,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);
CREATE INDEX IF NOT EXISTS ix_stream_batches_processed_at ON stream_batches (processed_at);

-- Batch layer: one authoritative bill per household per simulated day.
CREATE TABLE IF NOT EXISTS household_bills (
    bill_date        DATE NOT NULL,
    household_id     TEXT NOT NULL,
    zone_id          TEXT NOT NULL,
    consumption_kwh  NUMERIC(12, 4) NOT NULL,
    solar_kwh        NUMERIC(12, 4) NOT NULL,
    rate_lkr_per_kwh NUMERIC(10, 4) NOT NULL,
    bill_lkr         NUMERIC(14, 2) NOT NULL,
    readings_count   INT NOT NULL,
    billing_tier     TEXT,
    subsidy_flag     BOOLEAN,
    tariff_file      TEXT NOT NULL,
    run_id           TEXT,
    computed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bill_date, household_id)
);

-- Which tariff files (by content hash) have been billed successfully.
CREATE TABLE IF NOT EXISTS processed_tariff_files (
    file_name    TEXT PRIMARY KEY,
    file_sha256  TEXT NOT NULL,
    bill_date    DATE NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Audit log of billing job attempts (success and failure) for the health panel.
CREATE TABLE IF NOT EXISTS billing_runs (
    id                     BIGSERIAL PRIMARY KEY,
    tariff_file            TEXT NOT NULL,
    bill_date              DATE,
    status                 TEXT NOT NULL CHECK (status IN ('success', 'failed')),
    households_billed      INT,
    total_bill_lkr         NUMERIC(14, 2),
    speed_layer_max_diff_kwh DOUBLE PRECISION,
    message                TEXT,
    run_id                 TEXT,
    file_sha256            TEXT,
    failed_stage           TEXT,
    finished_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE billing_runs ADD COLUMN IF NOT EXISTS file_sha256 TEXT;
ALTER TABLE billing_runs ADD COLUMN IF NOT EXISTS failed_stage TEXT;
