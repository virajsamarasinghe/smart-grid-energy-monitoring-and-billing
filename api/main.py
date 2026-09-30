"""Serving/observability API.

* ``GET /health``             - alert rules; HTTP 503 when any critical alert is active.
* ``GET /api/zones/current``  - current grid load and renewable mix by zone (latest window + day so far).
* ``GET /api/bills?day=``     - daily billing and solar-contribution report per household.
* ``GET /metrics``            - Prometheus text exposition for scraping.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse

from smartgrid import db, queries
from smartgrid.alerts import evaluate_alerts, overall_status
from smartgrid.config import Settings
from smartgrid.logging_utils import get_logger

SETTINGS = Settings.from_env()
log = get_logger("api")
app = FastAPI(title="Smart-Grid Energy API", version="1.0.0")


def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_jsonable(x) for x in v]
    return v


def _conn():
    try:
        return db.connect(SETTINGS, retries=1)
    except RuntimeError as exc:
        log.error("db_unavailable", error=str(exc))
        raise HTTPException(status_code=503, detail="database unavailable") from exc


def _health(conn, now):
    snap = queries.pipeline_snapshot(conn)
    alerts = evaluate_alerts(snap, queries.latest_daylight_zone_windows(conn), SETTINGS, now)
    return snap, alerts


@app.get("/health")
def health():
    now = datetime.now(timezone.utc)
    conn = _conn()
    try:
        snap, alerts = _health(conn, now)
    finally:
        conn.close()
    status = overall_status(alerts)
    age = (now - snap["latest_ingested_at"]).total_seconds() if snap["latest_ingested_at"] else None
    body = {
        "status": status,
        "checked_at": now,
        "latest_reading_age_seconds": age,
        "latest_simulated_time": snap["latest_event_time"],
        "last_billing_run": snap["last_billing_run"],
        "alerts": [a.as_dict() for a in alerts],
    }
    return JSONResponse(_jsonable(body), status_code=503 if status == "critical" else 200)


@app.get("/api/zones/current")
def zones_current():
    conn = _conn()
    try:
        latest = queries.latest_zone_windows(conn)
        day = max((z["window_start"] for z in latest), default=None)
        totals = queries.zone_day_totals(conn, day.date()) if day else []
    finally:
        conn.close()
    by_zone = {t["zone_id"]: t for t in totals}
    out = []
    for z in latest:
        t = by_zone.get(z["zone_id"], {})
        cons = float(z["consumption_kwh"] or 0)
        out.append({
            "zone_id": z["zone_id"],
            "window_start": z["window_start"],
            "window_end": z["window_end"],
            "window_consumption_kwh": cons,
            "window_solar_kwh": float(z["solar_kwh"] or 0),
            "window_estimated_net_kwh": float(z["estimated_net_kwh"] or 0),
            "window_renewable_share": (float(z["solar_kwh"]) / cons) if cons else None,
            "day_consumption_kwh": t.get("consumption_kwh"),
            "day_solar_kwh": t.get("solar_kwh"),
            "day_renewable_share": t.get("solar_share"),
        })
    return _jsonable({"sim_day": day.date() if day else None, "zones": out})


@app.get("/api/bills")
def bills(day: date | None = Query(None, description="Simulated day (YYYY-MM-DD); default latest billed")):
    conn = _conn()
    try:
        if day is None:
            dates = queries.bill_dates(conn)
            if not dates:
                return {"bill_date": None, "bills": []}
            day = dates[0]
        rows = queries.bills(conn, day)
    finally:
        conn.close()
    return _jsonable({"bill_date": day, "total_bill_lkr": sum(float(r["bill_lkr"]) for r in rows), "bills": rows})


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    now = datetime.now(timezone.utc)
    conn = _conn()
    try:
        snap, alerts = _health(conn, now)
        zones = queries.latest_zone_windows(conn)
    finally:
        conn.close()
    lines = []

    def metric(name, help_, value, labels=None, mtype="gauge"):
        if not any(line.startswith(f"# HELP {name} ") for line in lines):
            lines.append(f"# HELP {name} {help_}")
            lines.append(f"# TYPE {name} {mtype}")
        lbl = "{" + ",".join(f'{k}="{v}"' for k, v in (labels or {}).items()) + "}" if labels else ""
        lines.append(f"{name}{lbl} {value}")

    age = (now - snap["latest_ingested_at"]).total_seconds() if snap["latest_ingested_at"] else -1
    metric("smartgrid_latest_reading_age_seconds", "Seconds since the newest stored reading (-1 = none)", round(age, 1))
    metric("smartgrid_readings_stored_total", "Valid de-duplicated readings stored", snap["readings_total"], mtype="counter")
    metric("smartgrid_invalid_readings_total", "Readings rejected by validation", snap["invalid_total"], mtype="counter")
    metric("smartgrid_duplicate_readings_total", "Duplicate readings skipped by the raw sink",
           snap["duplicates_total"], mtype="counter")
    # Prometheus requires each metric family's samples to be contiguous.
    for z in zones:
        metric("smartgrid_zone_window_consumption_kwh", "Consumption in the latest window per zone",
               float(z["consumption_kwh"]), {"zone": z["zone_id"]})
    for z in zones:
        metric("smartgrid_zone_window_solar_kwh", "Solar generation in the latest window per zone",
               float(z["solar_kwh"]), {"zone": z["zone_id"]})
    run = snap["last_billing_run"]
    metric("smartgrid_last_billing_success", "1 if the most recent billing run succeeded",
           1 if run and run["status"] == "success" else 0)
    for name in ("no_data", "stale_data", "high_invalid_rate", "low_renewable", "billing_failed"):
        metric("smartgrid_alert_active", "Active alerts by rule", sum(1 for a in alerts if a.name == name),
               {"alert": name})
    return "\n".join(lines) + "\n"
