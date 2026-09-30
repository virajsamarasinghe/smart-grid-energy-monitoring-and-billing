"""Batch source: drops one tariff CSV per simulated day into the shared incoming directory.

Called automatically by the meter simulator at the end of each simulated day, and usable
standalone for demos, e.g. to drop a deliberately broken file::

    python -m simulator.tariff_simulator --day 2026-01-05 --drop-household H003
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from smartgrid.config import Settings
from smartgrid.logging_utils import get_logger
from smartgrid.simulation import build_households
from smartgrid.tariffs import generate_tariff_rows, write_tariff_file

log = get_logger("tariff-simulator")


def drop_tariff_file(settings: Settings, day: date, drop_household: str | None = None,
                     directory: Path | None = None) -> Path:
    rows = generate_tariff_rows(settings, day, build_households(settings))
    if drop_household:
        rows = [r for r in rows if r["household_id"] != drop_household]
    path = write_tariff_file(directory or settings.incoming_dir, day, rows)
    log.info("tariff_file_written", day=day.isoformat(), path=str(path), rows=len(rows),
             dropped_household=drop_household)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--day", required=True, type=date.fromisoformat, help="simulated day, YYYY-MM-DD")
    parser.add_argument("--drop-household", help="omit this household to demonstrate validation failure")
    args = parser.parse_args()
    drop_tariff_file(Settings.from_env(), args.day, args.drop_household)


if __name__ == "__main__":
    main()
