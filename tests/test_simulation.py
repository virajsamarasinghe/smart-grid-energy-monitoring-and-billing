import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from smartgrid.config import Settings
from smartgrid.simulation import (
    build_households,
    demand_profile,
    fault_messages,
    generate_reading,
    is_last_tick_of_day,
    sim_day_of_tick,
    solar_profile,
    tick_start,
)
from smartgrid.tariffs import TARIFF_COLUMNS, generate_tariff_rows, write_tariff_file

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def test_default_clock_matches_spec(settings):
    assert settings.ticks_per_day == 60  # 300 s day / 5 s readings
    assert settings.interval_minutes == 24
    assert settings.sim_seconds_per_real_second == 288


def test_households_spread_across_zones(settings):
    hh = build_households(settings)
    assert len(hh) == 12
    assert {h.zone_id for h in hh} == {"Z1", "Z2", "Z3"}
    assert all(sum(h.zone_id == z for h in hh) == 4 for z in ("Z1", "Z2", "Z3"))
    assert len({h.household_id for h in hh}) == 12


def test_reading_has_required_fields_and_tz_aware_timestamps(settings):
    r = generate_reading(settings, build_households(settings)[0], 5, NOW)
    for f in ("event_id", "household_id", "zone_id", "event_time", "generated_at",
              "interval_minutes", "consumption_kwh", "solar_kwh", "meter_id"):
        assert f in r
    assert datetime.fromisoformat(r["event_time"]).tzinfo is not None
    assert datetime.fromisoformat(r["generated_at"]).tzinfo is not None
    assert r["event_time"] == "2026-01-01T02:00:00+00:00"  # tick 5 * 24 min
    assert r["interval_minutes"] == 24


def test_same_seed_is_reproducible_and_event_id_deterministic(settings):
    h = build_households(settings)[3]
    a = generate_reading(settings, h, 42, NOW)
    b = generate_reading(settings, h, 42, datetime(2030, 1, 1, tzinfo=timezone.utc))
    assert {k: v for k, v in a.items() if k != "generated_at"} == {k: v for k, v in b.items() if k != "generated_at"}
    other = generate_reading(replace(settings, random_seed=7), build_households(replace(settings, random_seed=7))[3], 42, NOW)
    assert other["consumption_kwh"] != a["consumption_kwh"]
    assert other["event_id"] == a["event_id"]  # id depends only on household + event_time


def test_solar_only_in_daylight(settings):
    solar_hh = [h for h in build_households(settings) if h.has_solar]
    assert solar_hh, "seed 42 should produce solar households"
    per_day = settings.ticks_per_day
    for tick in range(per_day):
        hour = tick_start(settings, tick).hour
        for h in solar_hh:
            r = generate_reading(settings, h, tick, NOW)
            if hour < 6 or hour >= 18:
                assert r["solar_kwh"] == 0
    noon_tick = per_day // 2 - 1  # 11:36-12:00
    assert all(generate_reading(settings, h, noon_tick, NOW)["solar_kwh"] > 0 for h in solar_hh)
    assert solar_profile(3) == 0 and solar_profile(21) == 0 and solar_profile(12) == pytest.approx(1)


def test_demand_varies_by_time_of_day():
    assert demand_profile(19.5) > demand_profile(7) > demand_profile(3)
    assert demand_profile(19.5) > 2 * demand_profile(3)


def test_values_are_energy_over_interval(settings):
    """Halving the interval (same household/hour) roughly halves the kWh per reading."""
    h = build_households(settings)[0]
    fine = replace(settings, reading_interval_seconds=2.5)  # 12-minute intervals
    coarse_kwh = generate_reading(settings, h, 20, NOW)["consumption_kwh"]  # 08:00-08:24
    fine_kwh = generate_reading(fine, h, 40, NOW)["consumption_kwh"]  # 08:00-08:12
    assert 0.3 < fine_kwh / coarse_kwh < 0.8
    assert all(generate_reading(settings, h, t, NOW)["consumption_kwh"] >= 0 for t in range(60))


def test_day_boundaries(settings):
    assert sim_day_of_tick(settings, 59).isoformat() == "2026-01-01"
    assert sim_day_of_tick(settings, 60).isoformat() == "2026-01-02"
    assert is_last_tick_of_day(settings, 59) and not is_last_tick_of_day(settings, 60)


def test_fault_injection_duplicates_are_identical_and_invalids_are_extra(settings):
    s = replace(settings, duplicate_rate=1.0, invalid_rate=1.0)
    h = build_households(s)[0]
    r = generate_reading(s, h, 10, NOW)
    faults = dict(fault_messages(s, r, 10))
    assert json.loads(faults.pop("duplicate")) == r
    assert len(faults) == 1  # one invalid variant
    none = fault_messages(replace(settings, duplicate_rate=0, invalid_rate=0), r, 10)
    assert none == []


def test_config_rejects_non_integer_ticks_per_day():
    with pytest.raises(ValueError, match="integer multiple"):
        replace(Settings(), sim_day_seconds=300, reading_interval_seconds=7).validate()


def test_tariff_file_one_row_per_household_written_atomically(settings):
    hh = build_households(settings)
    day = sim_day_of_tick(settings, 0)
    rows = generate_tariff_rows(settings, day, hh)
    assert [r["household_id"] for r in rows] == [h.household_id for h in hh]
    assert all(float(r["rate_lkr_per_kwh"]) > 0 for r in rows)
    path = write_tariff_file(settings.incoming_dir, day, rows)
    assert path.name == "tariffs_2026-01-01.csv"
    assert list(settings.incoming_dir.iterdir()) == [path]  # no temp file left behind
    lines = path.read_text().splitlines()
    assert lines[0].split(",") == TARIFF_COLUMNS
    assert len(lines) == 13
