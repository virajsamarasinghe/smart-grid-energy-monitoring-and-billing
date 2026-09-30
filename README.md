# Smart-Grid Energy Monitoring and Billing

EC8203 Applied Big Data Engineering mini-project, **Use Case 3**. It is a locally runnable
**Lambda architecture**:

* **Speed layer.** Simulated smart meters send readings to **Kafka**. **Spark Structured
  Streaming** validates and de-duplicates them, then writes event-time windows to **PostgreSQL**.
  This gives live zone load and solar contribution.
* **Batch layer.** A daily tariff CSV lands in a shared folder. An **Airflow** DAG recomputes
  that day's consumption from the raw readings and upserts one bill per household.
* **Serving.** A **Streamlit** dashboard and a **FastAPI** health/metrics API read from PostgreSQL.

Architecture diagram, Lambda-vs-Kappa justification, technology rationale and observability
design: **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

```
meter_simulator ──► Kafka (meter-readings, 3 partitions) ──► Spark ──► meter_readings (master)
       │                                                        └──► household_window_usage (2 h windows)
       └─(end of simulated day)─► data/incoming/tariffs_YYYY-MM-DD.csv ──► Airflow ──► household_bills
                                                                                    └─► data/reports/*.csv
Streamlit :8501  /  FastAPI :8000  ◄── PostgreSQL
```

## Simulated clock and data model

| Setting | Default | Meaning |
|---|---|---|
| `SIM_DAY_SECONDS` | 300 | one simulated day = 5 real minutes (288× speed-up) |
| `READING_INTERVAL_SECONDS` | 5 | one reading per household every 5 real seconds = **24 simulated minutes** |
| `SIM_START` | `2026-01-01T00:00:00+00:00` | simulated day 1 |
| `RANDOM_SEED` | 42 | all generated values are reproducible from (seed, household, tick) |
| households / zones | 12 / 3 | `H001`–`H012`, round-robin into `Z1`–`Z3` (4 per zone) |

* **Timezone:** every timestamp is **UTC** and carries an explicit `+00:00` offset. Postgres
  columns are `TIMESTAMPTZ`, Spark's session timezone is UTC, and the simulated clock treats the
  UTC hour as local solar time (solar noon = 12:00 UTC).
* **`event_time`** is simulated time: the *start* of the 24-minute interval the reading covers.
  **`generated_at`** is the real wall-clock time the meter produced it. Windows, daily totals and
  billing days all use `event_time`.
* **`consumption_kwh` / `solar_kwh`** are **energy over the interval**, not instantaneous power.
  Demand has morning and evening peaks. Solar is zero outside 06:00–18:00, with a per-zone daily
  cloud factor, and about 20% of zone-days are overcast (this triggers the low-renewable alert).
* **Estimated net energy** = `consumption_kwh − solar_kwh`. It is not instantaneous grid power.
* **Reading JSON:** `event_id, meter_id, household_id, zone_id, event_time, generated_at,
  interval_minutes, consumption_kwh, solar_kwh`. `event_id` is `uuid5(household_id|event_time)`,
  so a re-sent reading always has the same id. The brief's field names map as follows:
  `power_consumption_kwh → consumption_kwh`, `solar_generation_kwh → solar_kwh`,
  `grid_zone → zone_id`, `timestamp → event_time`.
* **Fault injection:** 2% of readings are re-sent (duplicates). 1% get an *extra* malformed
  message: bad JSON, a negative kWh value, a missing field, or a timestamp without a timezone.
  Set `DUPLICATE_RATE=0` and `INVALID_RATE=0` for a clean run.
* **Tariff CSV** (`data/incoming/tariffs_YYYY-MM-DD.csv`, one per simulated day): columns are
  `day, household_id, rate_lkr_per_kwh, billing_tier, subsidy_flag`. The rates are **sample
  values, not real tariffs**. `billing_tier` and `subsidy_flag` are informational only. The file
  is written as `.tariffs_….csv.tmp`, fsync'd, then atomically renamed.
* **Bill (MVP):** `bill_lkr = daily_consumption_kwh × rate_lkr_per_kwh`, rounded half-up to
  2 dp. There are no taxes, tiers, fixed charges or solar credits.

## Prerequisites

* Docker Desktop (or Docker Engine) with Compose v2. Tested with Docker 28.4 and Compose 2.39
  on macOS arm64.
* About **4 GB RAM** and 4+ CPUs for Docker. If Docker Desktop's built-in Kubernetes is enabled
  but not needed, disable it: it competes for the same VM.
* Free ports 8501 (dashboard), 8000 (API), 8080 (Airflow), 5433 (Postgres) and 29092 (Kafka).
  Change them in `.env` if needed.

## Setup and run

```bash
cp .env.example .env            # optional: every value has a default
docker compose up -d --build    # first build downloads images and Spark jars (~5-10 min)
docker compose ps -a            # db-init, kafka-init, airflow-init should be "Exited (0)"
```

Start-up order is enforced with health checks and `service_completed_successfully`:
postgres → db-init (databases, schema, `./data` dirs) → kafka → kafka-init (topic) →
simulator + spark-streaming, and airflow-init → airflow-scheduler + airflow-webserver. Services
also retry their own connections to Postgres and Kafka.

| UI | URL |
|---|---|
| Dashboard | http://localhost:8501 |
| API health / metrics | http://localhost:8000/health · http://localhost:8000/metrics · http://localhost:8000/docs |
| Airflow | http://localhost:8080 (user `admin` / password `admin`, local demo only) |

Timeline with the defaults: the first readings appear in Postgres within about 1 minute (Spark
start-up). Simulated day 1 ends about 5 minutes after the simulator starts. Its bill appears
roughly 1–2 minutes later.

## Verify

```bash
# 1. Unit + Spark + Postgres integration tests (uses a separate smartgrid_test database)
docker compose --profile test run --rm tests

# 2. Data is flowing (age of the newest reading, invalid and duplicate counts)
curl -s localhost:8000/health | python3 -m json.tool
docker compose exec postgres psql -U smartgrid -d smartgrid -c \
  "select query_name, batch_id, input_rows, inserted_rows, duplicate_rows, invalid_rows, duration_ms
     from stream_batches order by processed_at desc limit 6"

# 3. Bills (after simulated day 1 has ended)
docker compose exec postgres psql -U smartgrid -d smartgrid -c \
  "select bill_date, count(*) households, sum(bill_lkr) total_lkr, min(readings_count), max(readings_count)
     from household_bills group by 1 order by 1"
ls data/incoming data/reports

# 4. Live idempotency check: replays already-processed readings into Kafka, re-runs billing,
#    and asserts that raw totals, speed-layer totals and bills are unchanged. Prints PASS/FAIL.
docker compose exec simulator python scripts/verify_idempotency.py
```

`make test`, `make verify`, `make logs`, `make psql` and similar are shortcuts for these (see the `Makefile`).

## Demo scenarios

```bash
# Re-run billing through Airflow for a day that is already billed. Bills stay identical.
docker compose exec airflow-scheduler airflow dags trigger smartgrid_billing \
  -c '{"tariff_file": "tariffs_2026-01-01.csv"}'

# Drop a broken tariff file (household H003 missing). The run fails validation, no bills are
# written, and the dashboard shows the "billing_failed" alert.
docker compose exec simulator python -m simulator.tariff_simulator --day 2030-01-01 --drop-household H003
# ...then remove it so the pipeline goes green again:
rm data/incoming/tariffs_2030-01-01.csv

# Stale-data alert: stop the meters. After 30 s /health returns 503 and the dashboard turns red.
docker compose stop simulator
docker compose start simulator   # resumes the simulated clock from data/state/simulator_state.json

# Restart Spark mid-stream. It resumes from its checkpoint and totals do not change.
docker compose restart spark-streaming

# Structured logs from every stage
docker compose logs -f --tail=20 simulator spark-streaming airflow-scheduler
```

## Reset

```bash
docker compose --profile test down -v --remove-orphans   # removes Postgres, Kafka and Spark-checkpoint volumes
find data -mindepth 1 ! -name .gitkeep -exec rm -rf {} + # tariff files, reports, simulator clock
```

Always reset all of these together. For example, if you delete only the Kafka volume, Spark's
checkpointed offsets no longer match (it tolerates this via `failOnDataLoss=false`). If you
delete only the simulator state, it replays from day 1, and de-duplication absorbs that.

## Idempotency and failure behaviour

| Situation | What happens |
|---|---|
| Duplicate message (same `event_id`) in one micro-batch | Flagged by Spark (`row_number` over `event_id`) and not inserted. Counted in `stream_batches.duplicate_rows`. |
| Duplicate in a later batch, or a Kafka replay | `INSERT … ON CONFLICT (event_id) DO NOTHING` on `meter_readings`. The window query drops it via `withWatermark + dropDuplicates(event_id, event_time)`, or as late data. |
| Spark crashes or restarts | Resumes from the checkpoint (Kafka offsets + window state). A re-executed micro-batch rewrites the same absolute window values and hits the same primary keys. |
| Invalid reading | Stored in `invalid_readings` with an `error_reason` (dead letter). Never counted in totals. |
| Billing re-run (manual trigger, verify script, retry) | Consumption is recomputed from `meter_readings`, then `UPSERT` on `(bill_date, household_id)`. The result is the same rows and values. |
| **Tariff file arrives before all of that day's readings are processed** | This is the normal case, since the file is written the instant the day ends. The `wait_for_readings` sensor holds the run until **every household has a stored reading with `event_time` ≥ day end**. Readings are keyed by household, so they are ordered within a partition, and a next-day reading means that household's day is complete. If this doesn't happen within `READINESS_TIMEOUT_SECONDS` (180 s), the run fails and is logged in `billing_runs`, the file stays pending, and the next scheduled run retries. `{"force": true}` in the trigger conf skips the check. |
| Reading arrives after its day was billed | The bill is stale. The raw row is still stored, so re-triggering billing for that file corrects it. The speed layer drops readings later than the 60-minute watermark. `billing_runs.speed_layer_max_diff_kwh` shows the difference. |
| Invalid tariff file (missing, duplicate or unknown household, bad rate, wrong day) | The run fails fast and nothing is written. A `failed` row goes into `billing_runs` and the dashboard alerts. A corrected file (new content hash) is picked up automatically. |

## Observability

* **Structured logs:** JSON lines (`ts, level, service, event, …`) from the simulator, Spark
  (one line per committed micro-batch with counts and duration), Airflow tasks, db-init and the API.
* **Metrics tables:** `stream_batches` (per micro-batch), `invalid_readings` (dead letters),
  `billing_runs` (every billing attempt, including the speed-vs-batch reconciliation diff).
* **Alert rules** (`smartgrid/alerts.py`), shown on the dashboard, `/health` (HTTP 503 on
  critical) and `/metrics` (`smartgrid_alert_active{alert=…}`):
  * `stale_data`: no reading for more than 30 s.
  * `high_invalid_rate`: more than 5% of messages invalid in the last 5 minutes.
  * `low_renewable`: zone solar share below 10% in the latest 10:00–14:00 window.
  * `billing_failed`: the most recent billing run failed.

## Repository layout

```
smartgrid/          shared pure-Python logic: config, simulation model, tariffs, billing, alerts, DB helpers
simulator/          meter_simulator (Kafka producer) and tariff_simulator (daily CSV drop)
spark_jobs/         transforms.py (testable DataFrame logic) and stream_processor.py (the streaming job)
airflow/dags/       smartgrid_billing_dag.py (detect → validate → wait_for_readings → compute_bills)
dashboard/app.py    Streamlit dashboard
api/main.py         FastAPI /health, /metrics, /api/zones/current, /api/bills
sql/schema.sql      tables and views (applied idempotently by db-init)
scripts/            verify_idempotency.py (live end-to-end check)
tests/              simulation, validation, dedup, windowing, billing, alerts, DB idempotency
docker/             Dockerfiles; requirements/ holds the pinned dependencies
```

## Limitations and trade-offs

* Single-broker Kafka, Spark `local[2]` and one Postgres. Horizontal scale is not demonstrated.
* The Spark sink collects each micro-batch to the driver and writes with `psycopg2`. That is
  fine at about 3 msgs/s. At scale you would use a JDBC or COPY staging table plus `MERGE`.
* The speed layer is approximate: readings later than the watermark are dropped from windows.
  The batch layer is authoritative.
* Billing is intentionally simplistic: sample rates, and no tiers, taxes or solar credit.
* The Airflow DAG polls every 30 s, not event-driven. Readiness assumes per-household ordering
  in Kafka, and that holds because messages are keyed by `household_id`.
* Demo credentials live in `.env.example`. No authentication on the dashboard or API.
