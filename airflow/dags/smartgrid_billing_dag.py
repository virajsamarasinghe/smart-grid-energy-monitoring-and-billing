"""Batch layer: daily household billing, triggered by tariff files.

Runs every BILLING_POLL_SECONDS (default 30 s real time = 2.4 simulated hours). Each run bills
at most one pending tariff file (oldest first):

    detect_tariff_file -> validate_tariff -> wait_for_readings -> compute_bills

* detect      - finds a completed ``tariffs_YYYY-MM-DD.csv`` whose name+sha256 is not yet billed;
                skips the whole run if there is none.
* validate    - one positive rate per registered household, correct day, no unknown households.
                Fails fast (no retry) and records a failed billing_run; that exact file content is
                then skipped by detect so it cannot block later days (a corrected file is re-queued).
* wait        - sensor: waits until every household has a stored reading from the next day,
                i.e. the day's readings are fully processed by Spark (see billing_job docstring).
* compute     - recompute daily kWh from meter_readings, join rates, upsert bills, write report.

Manual re-run (idempotent - produces identical bills)::

    airflow dags trigger smartgrid_billing -c '{"tariff_file": "tariffs_2026-01-01.csv"}'
    airflow dags trigger smartgrid_billing -c '{"tariff_file": "tariffs_2026-01-01.csv", "force": true}'

``force`` skips the readiness sensor (e.g. the simulator was stopped right after a day ended).
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sensors.base import PokeReturnValue

from smartgrid import billing_job, db
from smartgrid.billing import TariffValidationError
from smartgrid.config import Settings
from smartgrid.logging_utils import get_logger

log = get_logger("airflow-billing")
POLL_SECONDS = int(os.getenv("BILLING_POLL_SECONDS", "30"))


def _settings() -> Settings:
    return Settings.from_env()


def _on_failure(context) -> None:
    """Record any task failure (validation, sensor timeout, DB error) for the health panel."""
    ti = context["task_instance"]
    tf = ti.xcom_pull(task_ids="detect_tariff_file")
    message = f"{ti.task_id}: {context.get('exception')}"
    conn = db.connect(_settings(), retries=3)
    try:
        billing_job.record_failure(conn, tf, context["run_id"], message, stage=ti.task_id)
    finally:
        conn.close()


@dag(
    dag_id="smartgrid_billing",
    description="Bill each household per simulated day when that day's tariff file arrives",
    schedule=timedelta(seconds=POLL_SECONDS),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10), "on_failure_callback": _on_failure},
    tags=["ec8203", "batch-layer", "billing"],
)
def smartgrid_billing():
    @task
    def detect_tariff_file(**context) -> dict:
        settings = _settings()
        requested = (context["dag_run"].conf or {}).get("tariff_file")
        if requested:
            path = settings.incoming_dir / Path(requested).name
            if not path.exists():
                raise AirflowFailException(f"requested tariff file {path} does not exist")
            tf = billing_job.describe_tariff_file(path)
        else:
            conn = db.connect(settings, retries=3)
            try:
                tf = billing_job.find_pending_tariff_file(conn, settings.incoming_dir)
            finally:
                conn.close()
        if tf is None:
            raise AirflowSkipException("no pending tariff file")
        log.info("tariff_file_detected", run_id=context["run_id"], **tf.to_dict())
        return tf.to_dict()

    @task(retries=0)
    def validate_tariff(tf: dict) -> dict:
        conn = db.connect(_settings(), retries=3)
        try:
            rates = billing_job.load_and_validate(conn, billing_job.TariffFile(**tf))
        except TariffValidationError as exc:
            raise AirflowFailException(f"tariff file {tf['file_name']} rejected: {exc}") from exc
        finally:
            conn.close()
        log.info("tariff_file_valid", file=tf["file_name"], rates=len(rates))
        return tf

    @task.sensor(poke_interval=10, timeout=int(os.getenv("READINESS_TIMEOUT_SECONDS", "180")),
                 mode="poke", retries=0)
    def wait_for_readings(tf: dict, **context) -> PokeReturnValue:
        if (context["dag_run"].conf or {}).get("force"):
            log.warning("readiness_check_forced", file=tf["file_name"])
            return PokeReturnValue(is_done=True)
        conn = db.connect(_settings(), retries=3)
        try:
            done, waiting = billing_job.readings_complete(conn, pendulum.parse(tf["bill_date"]).date())
        finally:
            conn.close()
        if not done:
            log.info("waiting_for_readings", bill_date=tf["bill_date"], households_waiting=waiting)
        return PokeReturnValue(is_done=done)

    @task
    def compute_bills(tf: dict, **context) -> dict:
        settings = _settings()
        conn = db.connect(settings, retries=3)
        try:
            result = billing_job.run_billing(conn, billing_job.TariffFile(**tf), settings.reports_dir,
                                             context["run_id"])
        finally:
            conn.close()
        return result.__dict__

    tf = detect_tariff_file()
    validated = validate_tariff(tf)
    wait_for_readings(validated) >> compute_bills(validated)


smartgrid_billing()
