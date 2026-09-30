"""One-shot initialiser: create databases, apply the schema and prepare shared directories.

Run by the ``db-init`` Compose service before anything else starts. Safe to re-run.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2

from smartgrid import db
from smartgrid.config import Settings
from smartgrid.logging_utils import get_logger

log = get_logger("db-init")
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def ensure_database(settings: Settings, name: str) -> None:
    conn = db.connect(settings, dbname="postgres")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
            if cur.fetchone():
                log.info("database_exists", database=name)
                return
            cur.execute(f'CREATE DATABASE "{name}"')
            log.info("database_created", database=name)
    except psycopg2.errors.DuplicateDatabase:
        log.info("database_exists", database=name)
    finally:
        conn.close()


def apply_schema(settings: Settings, dbname: str | None = None) -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with db.transaction(settings, dbname) as conn, conn.cursor() as cur:
        cur.execute(sql)
    log.info("schema_applied", database=dbname or settings.pg_db, schema=str(SCHEMA_PATH))


def prepare_dirs(settings: Settings) -> None:
    for d in (settings.incoming_dir, settings.reports_dir, settings.state_dir):
        d.mkdir(parents=True, exist_ok=True)
        # Airflow runs as uid 50000; the simulator runs as root. Keep the shared dirs writable.
        os.chmod(d, 0o777)
    log.info("data_dirs_ready", data_dir=str(settings.data_dir))


def main() -> None:
    settings = Settings.from_env()
    ensure_database(settings, settings.pg_db)
    airflow_db = os.getenv("AIRFLOW_DB_NAME")
    if airflow_db:
        ensure_database(settings, airflow_db)
    apply_schema(settings)
    if os.getenv("PREPARE_DATA_DIRS", "1") == "1":
        prepare_dirs(settings)


if __name__ == "__main__":
    main()
