from datetime import datetime, timedelta, timezone

from smartgrid.alerts import evaluate_alerts, overall_status

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def snap(**kw):
    base = {"latest_ingested_at": NOW - timedelta(seconds=5), "recent_valid": 100, "recent_invalid": 1,
            "last_billing_run": {"status": "success", "tariff_file": "t.csv", "message": "ok"}}
    base.update(kw)
    return base


def names(alerts):
    return {a.name for a in alerts}


def test_healthy_pipeline_has_no_alerts(settings):
    alerts = evaluate_alerts(snap(), [], settings, NOW)
    assert alerts == [] and overall_status(alerts) == "ok"


def test_stale_and_missing_data_are_critical(settings):
    stale = evaluate_alerts(snap(latest_ingested_at=NOW - timedelta(seconds=90)), [], settings, NOW)
    assert names(stale) == {"stale_data"} and overall_status(stale) == "critical"
    assert names(evaluate_alerts(snap(latest_ingested_at=None), [], settings, NOW)) == {"no_data"}


def test_invalid_rate_threshold_needs_enough_messages(settings):
    assert "high_invalid_rate" in names(evaluate_alerts(snap(recent_valid=80, recent_invalid=20), [], settings, NOW))
    assert "high_invalid_rate" not in names(evaluate_alerts(snap(recent_valid=5, recent_invalid=5), [], settings, NOW))


def test_low_renewable_and_billing_failure(settings):
    zones = [
        {"zone_id": "Z1", "window_start": NOW, "consumption_kwh": 10, "solar_kwh": 0.5},  # 5% -> alert
        {"zone_id": "Z2", "window_start": NOW, "consumption_kwh": 10, "solar_kwh": 4.0},
    ]
    alerts = evaluate_alerts(snap(last_billing_run={"status": "failed", "tariff_file": "x", "message": "bad"}),
                             zones, settings, NOW)
    assert names(alerts) == {"low_renewable", "billing_failed"}
    assert "Z1" in next(a.message for a in alerts if a.name == "low_renewable")
    assert overall_status(alerts) == "warning"
