from datetime import date
from decimal import Decimal

import pytest

from smartgrid.billing import (
    TariffValidationError,
    Usage,
    calculate_bills,
    parse_tariff_csv,
    validate_coverage,
)
from smartgrid.simulation import build_households
from smartgrid.tariffs import generate_tariff_rows, render_csv

DAY = date(2026, 1, 1)
HEADER = "day,household_id,rate_lkr_per_kwh,billing_tier,subsidy_flag\n"


def test_parse_generated_tariff_roundtrip(settings):
    hh = build_households(settings)
    rates = parse_tariff_csv(render_csv(generate_tariff_rows(settings, DAY, hh)), DAY)
    assert len(rates) == 12
    validate_coverage(rates, {h.household_id for h in hh})


@pytest.mark.parametrize("body,match", [
    ("2026-01-01,H001,40,a,false\n2026-01-01,H001,41,a,false\n", "duplicate rate"),
    ("2026-01-01,H001,-5,a,false\n", "invalid rate"),
    ("2026-01-01,H001,abc,a,false\n", "invalid rate"),
    ("2026-01-02,H001,40,a,false\n", "does not match file day"),
    ("", "no rates"),
])
def test_parse_rejects_bad_rows(body, match):
    with pytest.raises(TariffValidationError, match=match):
        parse_tariff_csv(HEADER + body, DAY)


def test_parse_rejects_missing_columns():
    with pytest.raises(TariffValidationError, match="missing required columns"):
        parse_tariff_csv("day,household_id\n2026-01-01,H001\n", DAY)


def test_coverage_requires_every_household_exactly_once():
    rates = parse_tariff_csv(HEADER + "2026-01-01,H001,40,a,false\n2026-01-01,H999,40,a,false\n", DAY)
    with pytest.raises(TariffValidationError) as e:
        validate_coverage(rates, {"H001", "H002"})
    assert "no rate for households ['H002']" in str(e.value)
    assert "unknown households ['H999']" in str(e.value)


def test_bill_is_consumption_times_rate_rounded_half_up():
    rates = parse_tariff_csv(HEADER + "2026-01-01,H001,42.50,a,true\n2026-01-01,H002,40,a,false\n", DAY)
    usage = {"H001": Usage("H001", 10.123456, 3.5, 60)}
    bills = {b.household_id: b for b in calculate_bills(DAY, rates, usage, {"H001": "Z1", "H002": "Z2"})}
    assert bills["H001"].consumption_kwh == Decimal("10.1235")
    assert bills["H001"].bill_lkr == Decimal("430.25")  # 10.1235 * 42.50 = 430.24875 -> 430.25
    assert bills["H001"].solar_share_pct == Decimal("34.57")
    assert bills["H001"].subsidy_flag is True
    # Household with no readings is still billed (0 kWh) so every household gets one bill.
    assert bills["H002"].bill_lkr == Decimal("0.00") and bills["H002"].readings_count == 0


def test_solar_is_not_credited_in_mvp():
    rates = parse_tariff_csv(HEADER + "2026-01-01,H001,50,a,false\n", DAY)
    with_solar = calculate_bills(DAY, rates, {"H001": Usage("H001", 8.0, 6.0, 60)}, {})[0]
    without = calculate_bills(DAY, rates, {"H001": Usage("H001", 8.0, 0.0, 60)}, {})[0]
    assert with_solar.bill_lkr == without.bill_lkr == Decimal("400.00")


def test_calculation_is_deterministic_on_rerun(settings):
    hh = build_households(settings)
    rates = parse_tariff_csv(render_csv(generate_tariff_rows(settings, DAY, hh)), DAY)
    usage = {h.household_id: Usage(h.household_id, 7.77, 1.1, 60) for h in hh}
    zones = {h.household_id: h.zone_id for h in hh}
    assert calculate_bills(DAY, rates, usage, zones) == calculate_bills(DAY, rates, usage, zones)
