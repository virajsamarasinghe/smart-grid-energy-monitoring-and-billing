"""Daily tariff file generation (the batch source).

Rates are illustrative sample values in LKR/kWh, **not** real CEB/LECO tariffs.
``billing_tier`` and ``subsidy_flag`` are informational columns required by the assignment's
data description; the MVP bill formula deliberately does not apply them.
"""

from __future__ import annotations

import csv
import io
import os
import random
import re
from datetime import date
from pathlib import Path

from smartgrid.config import Settings
from smartgrid.simulation import Household

TARIFF_COLUMNS = ["day", "household_id", "rate_lkr_per_kwh", "billing_tier", "subsidy_flag"]
TARIFF_FILE_RE = re.compile(r"^tariffs_(\d{4}-\d{2}-\d{2})\.csv$")

_TIER_BASE_RATE = {"domestic_standard": 42.0, "domestic_high_use": 55.0}


def tariff_file_name(day: date) -> str:
    return f"tariffs_{day.isoformat()}.csv"


def generate_tariff_rows(settings: Settings, day: date, households: list[Household]) -> list[dict]:
    day_factor = random.Random(f"{settings.random_seed}:tariff-day:{day.isoformat()}").uniform(0.95, 1.05)
    rows = []
    for hh in households:
        rng = random.Random(f"{settings.random_seed}:tariff:{hh.household_id}")
        tier = "domestic_high_use" if hh.base_load_kw > 0.45 else "domestic_standard"
        subsidy = rng.random() < 0.25
        rate = _TIER_BASE_RATE[tier] * day_factor * (0.8 if subsidy else 1.0)
        rows.append(
            {
                "day": day.isoformat(),
                "household_id": hh.household_id,
                "rate_lkr_per_kwh": f"{rate:.2f}",
                "billing_tier": tier,
                "subsidy_flag": "true" if subsidy else "false",
            }
        )
    return rows


def render_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=TARIFF_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def atomic_write_text(target: Path, content: str) -> Path:
    """Write to a hidden temp file in the same directory, fsync, then atomically rename.

    Readers that only look for the final name (``*.csv``) never observe a partial file.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as fh:
        fh.write(content)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, target)
    return target


def write_tariff_file(directory: Path, day: date, rows: list[dict]) -> Path:
    return atomic_write_text(directory / tariff_file_name(day), render_csv(rows))
