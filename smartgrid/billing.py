"""Pure billing logic: tariff parsing/validation and bill calculation (no I/O besides reading text).

MVP formula (per household, per simulated day)::

    bill_lkr = daily_consumption_kwh * rate_lkr_per_kwh      (rounded half-up to 2 dp)

No taxes, tiers, fixed charges or solar credits are applied. Solar is reported alongside
the bill for the "solar contribution" part of the daily report only.
"""

from __future__ import annotations

import csv
import io
from dataclasses import asdict, dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

from smartgrid.tariffs import TARIFF_COLUMNS

KWH_Q = Decimal("0.0001")
LKR_Q = Decimal("0.01")


class TariffValidationError(ValueError):
    """The tariff file is unusable; the billing run must fail without writing bills."""


@dataclass(frozen=True)
class TariffRate:
    household_id: str
    rate_lkr_per_kwh: Decimal
    billing_tier: str
    subsidy_flag: bool


@dataclass(frozen=True)
class Usage:
    household_id: str
    consumption_kwh: float
    solar_kwh: float
    readings_count: int


@dataclass(frozen=True)
class Bill:
    bill_date: date
    household_id: str
    zone_id: str
    consumption_kwh: Decimal
    solar_kwh: Decimal
    rate_lkr_per_kwh: Decimal
    bill_lkr: Decimal
    readings_count: int
    billing_tier: str
    subsidy_flag: bool

    @property
    def solar_share_pct(self) -> Decimal:
        if self.consumption_kwh == 0:
            return Decimal("0.00")
        return (self.solar_kwh / self.consumption_kwh * 100).quantize(LKR_Q, ROUND_HALF_UP)

    @property
    def estimated_net_kwh(self) -> Decimal:
        return self.consumption_kwh - self.solar_kwh

    def as_dict(self) -> dict:
        d = asdict(self)
        d["solar_share_pct"] = self.solar_share_pct
        d["estimated_net_kwh"] = self.estimated_net_kwh
        return d


def parse_tariff_csv(text: str, expected_day: date) -> list[TariffRate]:
    """Parse and structurally validate a tariff CSV. Raises TariffValidationError."""
    reader = csv.DictReader(io.StringIO(text))
    header = reader.fieldnames or []
    missing = [c for c in ("day", "household_id", "rate_lkr_per_kwh") if c not in header]
    if missing:
        raise TariffValidationError(f"missing required columns {missing}; expected {TARIFF_COLUMNS}")

    rates: dict[str, TariffRate] = {}
    errors: list[str] = []
    for line_no, row in enumerate(reader, start=2):
        hid = (row.get("household_id") or "").strip()
        if not hid:
            errors.append(f"line {line_no}: empty household_id")
            continue
        if (row.get("day") or "").strip() != expected_day.isoformat():
            errors.append(f"line {line_no}: day {row.get('day')!r} does not match file day {expected_day}")
        try:
            rate = Decimal((row.get("rate_lkr_per_kwh") or "").strip())
            if not rate.is_finite() or rate <= 0:
                raise InvalidOperation
        except InvalidOperation:
            errors.append(f"line {line_no}: invalid rate_lkr_per_kwh {row.get('rate_lkr_per_kwh')!r}")
            continue
        if hid in rates:
            errors.append(f"line {line_no}: duplicate rate for household {hid}")
            continue
        rates[hid] = TariffRate(
            household_id=hid,
            rate_lkr_per_kwh=rate,
            billing_tier=(row.get("billing_tier") or "").strip() or "unspecified",
            subsidy_flag=(row.get("subsidy_flag") or "").strip().lower() in ("true", "1", "yes"),
        )
    if errors:
        raise TariffValidationError("; ".join(errors))
    if not rates:
        raise TariffValidationError("tariff file contains no rates")
    return list(rates.values())


def validate_coverage(rates: list[TariffRate], household_ids: set[str]) -> None:
    """Every registered household must have exactly one rate, and no unknown households."""
    have = {r.household_id for r in rates}
    missing = sorted(household_ids - have)
    unknown = sorted(have - household_ids)
    problems = []
    if missing:
        problems.append(f"no rate for households {missing}")
    if unknown:
        problems.append(f"rates for unknown households {unknown}")
    if problems:
        raise TariffValidationError("; ".join(problems))


def calculate_bills(
    bill_date: date,
    rates: list[TariffRate],
    usage: dict[str, Usage],
    zones: dict[str, str],
) -> list[Bill]:
    """Join rates to that day's usage. Households with no readings get a zero bill."""
    bills = []
    for rate in sorted(rates, key=lambda r: r.household_id):
        u = usage.get(rate.household_id) or Usage(rate.household_id, 0.0, 0.0, 0)
        kwh = Decimal(str(u.consumption_kwh)).quantize(KWH_Q, ROUND_HALF_UP)
        solar = Decimal(str(u.solar_kwh)).quantize(KWH_Q, ROUND_HALF_UP)
        bills.append(
            Bill(
                bill_date=bill_date,
                household_id=rate.household_id,
                zone_id=zones.get(rate.household_id, "unknown"),
                consumption_kwh=kwh,
                solar_kwh=solar,
                rate_lkr_per_kwh=rate.rate_lkr_per_kwh,
                bill_lkr=(kwh * rate.rate_lkr_per_kwh).quantize(LKR_Q, ROUND_HALF_UP),
                readings_count=u.readings_count,
                billing_tier=rate.billing_tier,
                subsidy_flag=rate.subsidy_flag,
            )
        )
    return bills
