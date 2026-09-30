"""Pure smart-meter simulation model (no I/O).

Time model
----------
* Tick ``k`` covers the simulated interval ``[sim_start + k*interval, sim_start + (k+1)*interval)``.
* ``event_time`` is the **start** of that interval (simulated clock, UTC), so every reading
  belongs unambiguously to one simulated day.
* ``consumption_kwh`` / ``solar_kwh`` are *energy over the interval*, not instantaneous power.

Reproducibility
---------------
Every random draw is seeded from ``(RANDOM_SEED, household, tick)`` so a reading's values do
not depend on process restarts or on the order households are generated in. ``event_id`` is a
UUIDv5 of ``household_id|event_time`` which makes it a natural idempotency key: a re-sent or
replayed reading always carries the same id.
"""

from __future__ import annotations

import json
import math
import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from smartgrid.config import Settings

EVENT_NAMESPACE = uuid.UUID("6f1c2a52-2d2b-4c38-9f3e-6b1d7f0a9c11")


@dataclass(frozen=True)
class Household:
    household_id: str
    meter_id: str
    zone_id: str
    base_load_kw: float  # average demand in kW before the time-of-day profile
    solar_capacity_kw: float  # 0.0 means no rooftop solar

    @property
    def has_solar(self) -> bool:
        return self.solar_capacity_kw > 0


def build_households(settings: Settings) -> list[Household]:
    """Deterministically create households spread round-robin across zones."""
    households = []
    for i in range(settings.num_households):
        rng = random.Random(f"{settings.random_seed}:household:{i}")
        hid = f"H{i + 1:03d}"
        has_solar = rng.random() < 0.6
        households.append(
            Household(
                household_id=hid,
                meter_id=f"M-{hid}",
                zone_id=settings.zone_ids[i % settings.num_zones],
                base_load_kw=round(rng.uniform(0.25, 0.55), 3),
                solar_capacity_kw=round(rng.uniform(0.8, 2.0), 2) if has_solar else 0.0,
            )
        )
    return households


def demand_profile(hour: float) -> float:
    """Relative demand multiplier by hour of day: low at night, morning and evening peaks."""
    night = 0.45
    morning = 0.9 * math.exp(-((hour - 7.0) ** 2) / (2 * 1.2**2))
    midday = 0.35 * math.exp(-((hour - 13.0) ** 2) / (2 * 2.5**2))
    evening = 1.6 * math.exp(-((hour - 19.5) ** 2) / (2 * 1.6**2))
    return night + morning + midday + evening


def solar_profile(hour: float) -> float:
    """Clear-sky solar output fraction: zero outside 06:00-18:00, peak at 12:00."""
    if hour <= 6.0 or hour >= 18.0:
        return 0.0
    return math.sin(math.pi * (hour - 6.0) / 12.0)


def cloud_factor(settings: Settings, zone_id: str, day_index: int) -> float:
    """Per-zone, per-day sky condition. ~20% of zone-days are heavily overcast."""
    rng = random.Random(f"{settings.random_seed}:cloud:{zone_id}:{day_index}")
    if rng.random() < 0.2:
        return rng.uniform(0.05, 0.25)
    return rng.uniform(0.6, 1.0)


def tick_start(settings: Settings, tick: int) -> datetime:
    return settings.sim_start + timedelta(minutes=settings.interval_minutes * tick)


def sim_day_of_tick(settings: Settings, tick: int) -> date:
    return (settings.sim_start + timedelta(days=tick // settings.ticks_per_day)).date()


def is_last_tick_of_day(settings: Settings, tick: int) -> bool:
    return tick % settings.ticks_per_day == settings.ticks_per_day - 1


def make_event_id(household_id: str, event_time: datetime) -> str:
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{household_id}|{event_time.isoformat()}"))


def iso(ts: datetime, timespec: str = "seconds") -> str:
    if ts.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return ts.astimezone(timezone.utc).isoformat(timespec=timespec)


def generate_reading(settings: Settings, household: Household, tick: int, generated_at: datetime) -> dict:
    """Build one meter reading for ``household`` at simulation ``tick``."""
    start = tick_start(settings, tick)
    interval_h = settings.interval_minutes / 60.0
    mid = start + timedelta(minutes=settings.interval_minutes / 2)
    hour = mid.hour + mid.minute / 60.0 + mid.second / 3600.0
    day_index = tick // settings.ticks_per_day

    rng = random.Random(f"{settings.random_seed}:reading:{household.household_id}:{tick}")
    consumption = household.base_load_kw * demand_profile(hour) * interval_h * rng.uniform(0.85, 1.15)
    solar = 0.0
    if household.has_solar:
        solar = (
            household.solar_capacity_kw
            * solar_profile(hour)
            * cloud_factor(settings, household.zone_id, day_index)
            * interval_h
            * rng.uniform(0.9, 1.1)
        )

    return {
        "event_id": make_event_id(household.household_id, start),
        "meter_id": household.meter_id,
        "household_id": household.household_id,
        "zone_id": household.zone_id,
        "event_time": iso(start),
        "generated_at": iso(generated_at, "milliseconds"),
        "interval_minutes": settings.interval_minutes,
        "consumption_kwh": round(consumption, 4),
        "solar_kwh": round(solar, 4),
    }


def fault_messages(settings: Settings, reading: dict, tick: int) -> list[tuple[str, bytes]]:
    """Extra messages to send alongside a valid reading, to exercise dedup and validation.

    Returns ``(kind, payload)`` pairs. Invalid messages are *additional* to the valid reading,
    so fault injection never changes true household consumption.
    """
    rng = random.Random(f"{settings.random_seed}:faults:{reading['household_id']}:{tick}")
    out: list[tuple[str, bytes]] = []
    if rng.random() < settings.duplicate_rate:
        out.append(("duplicate", encode(reading)))
    if rng.random() < settings.invalid_rate:
        variant = rng.choice(["negative_consumption", "missing_household", "not_json", "naive_timestamp"])
        bad = dict(reading, event_id=str(uuid.UUID(int=rng.getrandbits(128))))
        if variant == "negative_consumption":
            bad["consumption_kwh"] = -abs(reading["consumption_kwh"]) - 0.1
            out.append((variant, encode(bad)))
        elif variant == "missing_household":
            bad.pop("household_id")
            out.append((variant, encode(bad)))
        elif variant == "naive_timestamp":
            bad["event_time"] = reading["event_time"].replace("+00:00", "")
            out.append((variant, encode(bad)))
        else:
            out.append((variant, b"{meter reading garbled in transit"))
    return out


def encode(reading: dict) -> bytes:
    return json.dumps(reading, separators=(",", ":")).encode("utf-8")
