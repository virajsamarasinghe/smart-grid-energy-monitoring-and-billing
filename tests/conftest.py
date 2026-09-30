"""Shared fixtures. Postgres-backed tests use a separate database (TEST_POSTGRES_DB) that is
dropped and recreated per session, so running tests never touches the demo data."""

from __future__ import annotations

import os
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("TZ", "UTC")

from smartgrid.config import Settings  # noqa: E402


@pytest.fixture
def settings(tmp_path) -> Settings:
    return replace(
        Settings(),
        sim_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        random_seed=42,
        data_dir=tmp_path,
    )


@pytest.fixture(scope="session")
def pg_settings():
    """Settings pointing at a fresh test database, or skip if Postgres is unreachable."""
    psycopg2 = pytest.importorskip("psycopg2")
    base = Settings.from_env()
    test_db = os.getenv("TEST_POSTGRES_DB", "smartgrid_test")
    try:
        admin = psycopg2.connect(base.pg_dsn("postgres"), connect_timeout=3)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable at {base.pg_host}:{base.pg_port}: {exc}")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{test_db}" WITH (FORCE)')
        cur.execute(f'CREATE DATABASE "{test_db}"')
    admin.close()

    from smartgrid.db_init import apply_schema

    s = replace(base, pg_db=test_db)
    apply_schema(s)
    return s


@pytest.fixture
def pg_conn(pg_settings):
    from smartgrid import db

    conn = db.connect(pg_settings, retries=1)
    with conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE households, meter_readings, invalid_readings, household_window_usage, stream_batches, "
            "household_bills, processed_tariff_files, billing_runs RESTART IDENTITY"
        )
    yield conn
    conn.close()
