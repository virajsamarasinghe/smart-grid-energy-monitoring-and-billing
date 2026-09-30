"""Health-check / alert rules, evaluated by both the dashboard and the ``/health`` API.

Rules (thresholds come from Settings / environment):

* ``stale_data`` (critical)       - no reading ingested for more than STALE_AFTER_SECONDS.
* ``no_data`` (critical)          - nothing has been ingested at all yet.
* ``high_invalid_rate`` (warning) - invalid / all messages in the last 5 real minutes above
  INVALID_RATE_THRESHOLD (requires at least 20 messages to avoid noise).
* ``low_renewable`` (warning)     - a zone's solar share in the latest daylight window
  (10:00-14:00 simulated) is below LOW_RENEWABLE_THRESHOLD.
* ``billing_failed`` (warning)    - the most recent billing run failed.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime

from smartgrid.config import Settings

DAYLIGHT_HOURS = (10, 14)  # window_start hours considered for the renewable-share rule
MIN_MESSAGES_FOR_RATE = 20


@dataclass(frozen=True)
class Alert:
    name: str
    severity: str  # "critical" | "warning"
    message: str

    def as_dict(self) -> dict:
        return asdict(self)


def evaluate_alerts(snapshot: dict, zone_daylight: list[dict], settings: Settings, now: datetime) -> list[Alert]:
    """``snapshot`` comes from ``queries.pipeline_snapshot``; ``zone_daylight`` from
    ``queries.latest_daylight_zone_windows`` (zone_id, window_start, consumption_kwh, solar_kwh)."""
    alerts: list[Alert] = []

    last = snapshot.get("latest_ingested_at")
    if last is None:
        alerts.append(Alert("no_data", "critical", "No meter readings have been stored yet."))
    else:
        age = (now - last).total_seconds()
        if age > settings.stale_after_seconds:
            alerts.append(Alert("stale_data", "critical",
                                f"Latest reading is {age:.0f}s old (threshold {settings.stale_after_seconds}s)."))

    valid, invalid = snapshot.get("recent_valid", 0) or 0, snapshot.get("recent_invalid", 0) or 0
    total = valid + invalid
    if total >= MIN_MESSAGES_FOR_RATE:
        rate = invalid / total
        if rate > settings.invalid_rate_threshold:
            alerts.append(Alert("high_invalid_rate", "warning",
                                f"{rate:.1%} of messages in the last 5 min were invalid "
                                f"(threshold {settings.invalid_rate_threshold:.0%})."))

    for z in zone_daylight:
        consumption = float(z["consumption_kwh"] or 0)
        if consumption <= 0:
            continue
        share = float(z["solar_kwh"] or 0) / consumption
        if share < settings.low_renewable_threshold:
            alerts.append(Alert("low_renewable", "warning",
                                f"Zone {z['zone_id']} solar share {share:.0%} in window starting "
                                f"{z['window_start']:%Y-%m-%d %H:%M} UTC "
                                f"(threshold {settings.low_renewable_threshold:.0%})."))

    run = snapshot.get("last_billing_run")
    if run and run.get("status") == "failed":
        alerts.append(Alert("billing_failed", "warning",
                            f"Last billing run for {run.get('tariff_file')} failed: {run.get('message')}"))
    return alerts


def overall_status(alerts: list[Alert]) -> str:
    if any(a.severity == "critical" for a in alerts):
        return "critical"
    return "warning" if alerts else "ok"
