"""Environment-driven configuration shared by every service.

All timestamps in the system are UTC (ISO-8601 with an explicit ``+00:00`` offset).
The simulated clock treats the UTC hour as the local "solar" hour, so 12:00 UTC on the
simulated clock is solar noon. See README "Simulated clock".
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


def _parse_start(value: str) -> datetime:
    start = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if start.tzinfo is None:
        raise ValueError(f"SIM_START must include a timezone offset, got {value!r}")
    start = start.astimezone(timezone.utc)
    if (start.hour, start.minute, start.second, start.microsecond) != (0, 0, 0, 0):
        raise ValueError(f"SIM_START must be midnight UTC so simulated days align, got {value!r}")
    return start


@dataclass(frozen=True)
class Settings:
    # Simulation
    num_households: int = 12
    num_zones: int = 3
    sim_day_seconds: float = 300.0  # real seconds per simulated day
    reading_interval_seconds: float = 5.0  # real seconds between readings per household
    sim_start: datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)
    random_seed: int = 42
    duplicate_rate: float = 0.02  # probability a reading is re-sent (at-least-once delivery)
    invalid_rate: float = 0.01  # probability an extra malformed message is injected

    # Kafka
    kafka_bootstrap: str = "kafka:9092"
    kafka_topic: str = "meter-readings"

    # Postgres
    pg_host: str = "postgres"
    pg_port: int = 5432
    pg_db: str = "smartgrid"
    pg_user: str = "smartgrid"
    pg_password: str = "smartgrid"

    # Shared file system
    data_dir: Path = Path("/data")

    # Speed layer (simulated minutes)
    window_minutes: int = 120
    watermark_minutes: int = 60
    max_kwh_per_reading: float = 50.0

    # Batch layer
    readiness_timeout_seconds: int = 180

    # Observability thresholds
    stale_after_seconds: int = 30
    invalid_rate_threshold: float = 0.05
    low_renewable_threshold: float = 0.10
    dashboard_refresh_seconds: int = 5

    @classmethod
    def from_env(cls) -> "Settings":
        s = cls(
            num_households=int(_env("NUM_HOUSEHOLDS", "12")),
            num_zones=int(_env("NUM_ZONES", "3")),
            sim_day_seconds=float(_env("SIM_DAY_SECONDS", "300")),
            reading_interval_seconds=float(_env("READING_INTERVAL_SECONDS", "5")),
            sim_start=_parse_start(_env("SIM_START", "2026-01-01T00:00:00+00:00")),
            random_seed=int(_env("RANDOM_SEED", "42")),
            duplicate_rate=float(_env("DUPLICATE_RATE", "0.02")),
            invalid_rate=float(_env("INVALID_RATE", "0.01")),
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_topic=_env("KAFKA_TOPIC", "meter-readings"),
            pg_host=_env("POSTGRES_HOST", "postgres"),
            pg_port=int(_env("POSTGRES_PORT", "5432")),
            pg_db=_env("POSTGRES_DB", "smartgrid"),
            pg_user=_env("POSTGRES_USER", "smartgrid"),
            pg_password=_env("POSTGRES_PASSWORD", "smartgrid"),
            data_dir=Path(_env("DATA_DIR", "/data")),
            window_minutes=int(_env("WINDOW_MINUTES", "120")),
            watermark_minutes=int(_env("WATERMARK_MINUTES", "60")),
            max_kwh_per_reading=float(_env("MAX_KWH_PER_READING", "50")),
            readiness_timeout_seconds=int(_env("READINESS_TIMEOUT_SECONDS", "180")),
            stale_after_seconds=int(_env("STALE_AFTER_SECONDS", "30")),
            invalid_rate_threshold=float(_env("INVALID_RATE_THRESHOLD", "0.05")),
            low_renewable_threshold=float(_env("LOW_RENEWABLE_THRESHOLD", "0.10")),
            dashboard_refresh_seconds=int(_env("DASHBOARD_REFRESH_SECONDS", "5")),
        )
        s.validate()
        return s

    # ---- derived values -------------------------------------------------
    @property
    def ticks_per_day(self) -> int:
        return round(self.sim_day_seconds / self.reading_interval_seconds)

    @property
    def interval_minutes(self) -> float:
        """Simulated minutes covered by one reading (24 with the defaults)."""
        return 1440.0 / self.ticks_per_day

    @property
    def sim_seconds_per_real_second(self) -> float:
        return 86400.0 / self.sim_day_seconds

    @property
    def incoming_dir(self) -> Path:
        return self.data_dir / "incoming"

    @property
    def reports_dir(self) -> Path:
        return self.data_dir / "reports"

    @property
    def state_dir(self) -> Path:
        return self.data_dir / "state"

    @property
    def zone_ids(self) -> list[str]:
        return [f"Z{i + 1}" for i in range(self.num_zones)]

    def validate(self) -> None:
        errors = []
        if self.num_households < 1 or self.num_households > 999:
            errors.append("NUM_HOUSEHOLDS must be between 1 and 999")
        if self.num_zones < 1 or self.num_zones > self.num_households:
            errors.append("NUM_ZONES must be between 1 and NUM_HOUSEHOLDS")
        if self.sim_day_seconds <= 0 or self.reading_interval_seconds <= 0:
            errors.append("SIM_DAY_SECONDS and READING_INTERVAL_SECONDS must be positive")
        else:
            ratio = self.sim_day_seconds / self.reading_interval_seconds
            if abs(ratio - round(ratio)) > 1e-9 or round(ratio) < 1:
                errors.append(
                    "SIM_DAY_SECONDS must be an integer multiple of READING_INTERVAL_SECONDS "
                    f"(got {self.sim_day_seconds}/{self.reading_interval_seconds})"
                )
        for name in ("duplicate_rate", "invalid_rate"):
            if not 0 <= getattr(self, name) <= 1:
                errors.append(f"{name.upper()} must be between 0 and 1")
        if self.window_minutes <= 0 or self.watermark_minutes < 0:
            errors.append("WINDOW_MINUTES must be positive and WATERMARK_MINUTES non-negative")
        if errors:
            raise ValueError("Invalid configuration: " + "; ".join(errors))

    def pg_dsn(self, dbname: str | None = None) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={dbname or self.pg_db} "
            f"user={self.pg_user} password={self.pg_password} options='-c timezone=UTC'"
        )
