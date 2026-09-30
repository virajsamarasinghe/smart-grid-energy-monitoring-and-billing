"""Streamlit dashboard: live zone energy (speed layer), household bills (batch layer), pipeline health.

Run: ``streamlit run dashboard/app.py`` (the Compose service does this on port 8501).
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from smartgrid import db, queries  # noqa: E402
from smartgrid.alerts import evaluate_alerts, overall_status  # noqa: E402
from smartgrid.config import Settings  # noqa: E402

SETTINGS = Settings.from_env()
# Validated categorical slots 1-3 (blue, orange, aqua); colour follows the zone, never its rank.
ZONE_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]

st.set_page_config(page_title="Smart-Grid Monitor", page_icon="⚡", layout="wide")


def zone_scale() -> alt.Scale:
    zones = SETTINGS.zone_ids
    return alt.Scale(domain=zones, range=ZONE_COLORS[: len(zones)])


def fmt_age(seconds: float | None) -> str:
    if seconds is None:
        return "–"
    return f"{seconds:.0f}s" if seconds < 120 else f"{seconds / 60:.1f} min"


def sim_day_number(day) -> int:
    return (day - SETTINGS.sim_start.date()).days + 1


def zone_line(df: pd.DataFrame, field: str, title: str) -> alt.Chart:
    return (
        alt.Chart(df, title=title)
        .mark_line(point=alt.OverlayMarkDef(size=64, filled=True), strokeWidth=2)
        .encode(
            x=alt.X("window_start:T", title="Simulated time (UTC)", axis=alt.Axis(format="%H:%M", grid=False)),
            y=alt.Y(f"{field}:Q", title="kWh per 2 h window"),
            color=alt.Color("zone_id:N", title="Zone", scale=zone_scale()),
            tooltip=[
                alt.Tooltip("zone_id:N", title="Zone"),
                alt.Tooltip("window_start:T", title="Window start", format="%H:%M"),
                alt.Tooltip("window_end:T", title="Window end", format="%H:%M"),
                alt.Tooltip(f"{field}:Q", title=title, format=".2f"),
                alt.Tooltip("readings_count:Q", title="Readings"),
            ],
        )
        .properties(height=260)
    )


def render_health(snap: dict, alerts, now: datetime) -> None:
    status = overall_status(alerts)
    badge = {"ok": "🟢 Healthy", "warning": "🟠 Warning", "critical": "🔴 Critical"}[status]
    st.subheader(f"Pipeline health: {badge}")

    latest_ingested = snap.get("latest_ingested_at")
    age = (now - latest_ingested).total_seconds() if latest_ingested else None
    run = snap.get("last_billing_run")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest reading age", fmt_age(age), help=f"Alert if older than {SETTINGS.stale_after_seconds}s")
    c2.metric("Readings stored", f"{snap['readings_total']:,}",
              help="Valid, de-duplicated readings in meter_readings")
    c3.metric("Invalid / duplicates dropped", f"{snap['invalid_total']:,} / {snap['duplicates_total']:,}")
    if run:
        icon = "✅" if run["status"] == "success" else "❌"
        c4.metric("Last billing job", f"{icon} {run['status']}",
                  help=f"{run['tariff_file']} at {run['finished_at']:%H:%M:%S} UTC: {run['message']}")
    else:
        c4.metric("Last billing job", "not run yet")

    if alerts:
        for a in alerts:
            (st.error if a.severity == "critical" else st.warning)(f"**{a.name}**: {a.message}", icon="⚠️")
    else:
        st.success("All health checks passing: data fresh, invalid rate normal, billing OK.", icon="✅")


def render_zones(conn, current_day) -> None:
    st.subheader(f"Zone energy - simulated day {sim_day_number(current_day)} ({current_day}) so far")
    totals = queries.zone_day_totals(conn, current_day)
    if totals:
        cols = st.columns(len(totals))
        for col, z in zip(cols, totals):
            share = z["solar_share"]
            col.metric(f"Zone {z['zone_id']} - estimated net energy",
                       f"{z['estimated_net_kwh']:.2f} kWh",
                       help="Estimated net energy = consumption_kwh - solar_kwh (energy, not instantaneous power)")
            col.caption(f"Consumed **{z['consumption_kwh']:.2f} kWh** · Solar **{z['solar_kwh']:.2f} kWh** · "
                        f"Solar share **{(share or 0):.0%}**")
    windows = pd.DataFrame(queries.zone_windows(conn, current_day))
    if windows.empty:
        st.info("Waiting for the first Spark window aggregates…")
        return
    for c in ("consumption_kwh", "solar_kwh", "estimated_net_kwh"):
        windows[c] = windows[c].astype(float)
    left, right = st.columns(2)
    left.altair_chart(zone_line(windows, "consumption_kwh", "Energy consumed"), use_container_width=True)
    right.altair_chart(zone_line(windows, "solar_kwh", "Solar generated"), use_container_width=True)
    st.altair_chart(zone_line(windows, "estimated_net_kwh", "Estimated net energy (consumption − solar)"),
                    use_container_width=True)
    with st.expander("Table view: zone windows"):
        st.dataframe(windows, hide_index=True, use_container_width=True)


def render_households(conn, current_day) -> None:
    st.subheader("Household totals today (speed layer, live)")
    rows = pd.DataFrame(queries.household_daily(conn, current_day))
    if rows.empty:
        st.info("No household totals yet.")
    else:
        st.dataframe(rows, hide_index=True, use_container_width=True, column_config={
            c: st.column_config.NumberColumn(format="%.3f")
            for c in ("consumption_kwh", "solar_kwh", "estimated_net_kwh")
        })


def render_bills(conn) -> None:
    st.subheader("Household daily bills (batch layer, Airflow)")
    dates = queries.bill_dates(conn)
    if not dates:
        st.info("No bills yet. The first tariff file is produced when simulated day 1 ends "
                f"(~{SETTINGS.sim_day_seconds / 60:.0f} real minutes after start).")
        return
    day = st.selectbox("Bill date", dates, format_func=lambda d: f"Day {sim_day_number(d)} - {d}",
                       key="bill_date")
    df = pd.DataFrame(queries.bills(conn, day))
    for c in ("consumption_kwh", "solar_kwh", "estimated_net_kwh", "rate_lkr_per_kwh", "bill_lkr",
              "solar_share_pct"):
        df[c] = df[c].astype(float)
    c1, c2, c3 = st.columns(3)
    c1.metric("Total billed", f"LKR {df['bill_lkr'].sum():,.2f}")
    c2.metric("Total consumption", f"{df['consumption_kwh'].sum():.2f} kWh")
    c3.metric("Households billed", len(df))
    st.caption("bill_lkr = daily consumption_kwh × rate_lkr_per_kwh (sample rates; no taxes, tiers or solar credits).")
    st.dataframe(df.drop(columns=["bill_date"]), hide_index=True, use_container_width=True, column_config={
        "bill_lkr": st.column_config.NumberColumn("bill_lkr", format="LKR %.2f"),
        "consumption_kwh": st.column_config.NumberColumn(format="%.4f"),
        "solar_kwh": st.column_config.NumberColumn(format="%.4f"),
        "estimated_net_kwh": st.column_config.NumberColumn(format="%.4f"),
        "solar_share_pct": st.column_config.NumberColumn(format="%.1f%%"),
    })
    with st.expander("Recent billing runs"):
        st.dataframe(pd.DataFrame(queries.recent_billing_runs(conn)), hide_index=True, use_container_width=True)


@st.fragment(run_every=SETTINGS.dashboard_refresh_seconds or None)  # 0 disables auto-refresh
def live_view() -> None:
    now = datetime.now(timezone.utc)
    try:
        conn = db.connect(SETTINGS, retries=1)
    except RuntimeError as exc:
        st.error(f"Cannot reach PostgreSQL: {exc}")
        return
    try:
        snap = queries.pipeline_snapshot(conn)
        alerts = evaluate_alerts(snap, queries.latest_daylight_zone_windows(conn), SETTINGS, now)
        latest = snap.get("latest_reading")

        if latest:
            sim_time = latest["event_time"]
            h1, h2 = st.columns(2)
            h1.metric("Simulated clock (latest reading interval start)",
                      f"Day {sim_day_number(sim_time.date())} · {sim_time:%Y-%m-%d %H:%M} UTC")
            h2.metric("Last received reading",
                      f"{latest['household_id']} ({latest['zone_id']}) at {latest['ingested_at']:%H:%M:%S} UTC",
                      help=f"event_id {latest['event_id']}, generated at {latest['generated_at']:%H:%M:%S} UTC, "
                           f"{latest['consumption_kwh']:.3f} kWh consumed, {latest['solar_kwh']:.3f} kWh solar")
        render_health(snap, alerts, now)
        st.divider()
        if latest:
            render_zones(conn, latest["event_time"].date())
            st.divider()
            render_households(conn, latest["event_time"].date())
            st.divider()
        render_bills(conn)
        every = f"every {SETTINGS.dashboard_refresh_seconds}s" if SETTINGS.dashboard_refresh_seconds else "auto-refresh off"
        st.caption(f"Refreshed {now:%H:%M:%S} UTC · {every}")
    finally:
        conn.close()


st.title("⚡ Smart-Grid Energy Monitoring & Billing")
st.caption(
    f"Simulation: 1 simulated day = {SETTINGS.sim_day_seconds:.0f} real seconds · "
    f"{SETTINGS.num_households} households in {SETTINGS.num_zones} zones · one reading per household every "
    f"{SETTINGS.reading_interval_seconds:.0f}s ({SETTINGS.interval_minutes:.0f} simulated minutes). "
    "All times UTC."
)
live_view()
