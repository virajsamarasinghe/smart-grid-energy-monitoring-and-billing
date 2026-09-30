"""Streaming source: simulated smart meters publishing readings to Kafka.

* One reading per household every READING_INTERVAL_SECONDS real seconds (default 5).
* One simulated day lasts SIM_DAY_SECONDS real seconds (default 300 = 5 minutes).
* Messages are keyed by household_id so each household's readings stay ordered in one partition.
* At the end of every simulated day the tariff simulator drops that day's tariff CSV.
* The simulated clock (next tick) is persisted in DATA_DIR/state so a restart resumes rather
  than rewinding. Deleting the state file replays from SIM_START; dedup makes that harmless.
"""

from __future__ import annotations

import json
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient

from simulator.tariff_simulator import drop_tariff_file
from smartgrid import db
from smartgrid.config import Settings
from smartgrid.logging_utils import get_logger
from smartgrid.simulation import (
    build_households,
    encode,
    fault_messages,
    generate_reading,
    is_last_tick_of_day,
    sim_day_of_tick,
    tick_start,
)
from smartgrid.tariffs import atomic_write_text

log = get_logger("meter-simulator")
_running = True


def _stop(signum, _frame):
    global _running
    _running = False
    log.info("shutdown_requested", signal=signum)


def wait_for_topic(settings: Settings, timeout_s: int = 120) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            md = admin.list_topics(timeout=5)
            topic = md.topics.get(settings.kafka_topic)
            if topic is not None and topic.error is None:
                log.info("kafka_topic_ready", topic=settings.kafka_topic, partitions=len(topic.partitions))
                return
            log.warning("kafka_topic_missing", topic=settings.kafka_topic)
        except KafkaException as exc:
            log.warning("kafka_unavailable", bootstrap=settings.kafka_bootstrap, error=str(exc))
        time.sleep(3)
    raise RuntimeError(f"Kafka topic {settings.kafka_topic!r} not available at {settings.kafka_bootstrap} "
                       f"after {timeout_s}s - did the kafka-init service run?")


def load_next_tick(state_file: Path, settings: Settings) -> int:
    if not state_file.exists():
        return 0
    state = json.loads(state_file.read_text())
    if state.get("sim_start") != settings.sim_start.isoformat() or state.get("ticks_per_day") != settings.ticks_per_day:
        log.warning("state_file_ignored", reason="simulation config changed", state=state)
        return 0
    return int(state["next_tick"])


def save_next_tick(state_file: Path, settings: Settings, next_tick: int) -> None:
    atomic_write_text(state_file, json.dumps({
        "next_tick": next_tick,
        "sim_start": settings.sim_start.isoformat(),
        "ticks_per_day": settings.ticks_per_day,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }))


def main() -> None:
    settings = Settings.from_env()
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    households = build_households(settings)
    with db.transaction(settings) as conn:
        db.register_households(conn, households)
    log.info("households_registered", count=len(households),
             zones={z: [h.household_id for h in households if h.zone_id == z] for z in settings.zone_ids})

    wait_for_topic(settings)
    delivery_errors = {"count": 0}

    def on_delivery(err, msg):
        if err is not None:
            delivery_errors["count"] += 1
            log.error("kafka_delivery_failed", error=str(err), key=(msg.key() or b"").decode())

    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "enable.idempotence": True,  # broker-side dedup of producer retries
        "acks": "all",
        "linger.ms": 50,
        "client.id": "meter-simulator",
    })

    state_file = settings.state_dir / "simulator_state.json"
    tick = load_next_tick(state_file, settings)
    log.info("simulation_started", start_tick=tick, sim_time=tick_start(settings, tick).isoformat(),
             sim_day_seconds=settings.sim_day_seconds, reading_interval_seconds=settings.reading_interval_seconds,
             interval_minutes=settings.interval_minutes, seed=settings.random_seed)

    next_wall = time.monotonic()
    while _running:
        now = datetime.now(timezone.utc)
        sent, faults = 0, {}
        for hh in households:
            reading = generate_reading(settings, hh, tick, now)
            key = hh.household_id.encode()
            producer.produce(settings.kafka_topic, key=key, value=encode(reading), on_delivery=on_delivery)
            sent += 1
            for kind, payload in fault_messages(settings, reading, tick):
                producer.produce(settings.kafka_topic, key=key, value=payload, on_delivery=on_delivery)
                faults[kind] = faults.get(kind, 0) + 1
        remaining = producer.flush(10)
        if remaining:
            log.error("kafka_flush_incomplete", undelivered=remaining)

        log.info("tick_published", tick=tick, sim_time=tick_start(settings, tick).isoformat(),
                 readings=sent, injected=faults, delivery_errors=delivery_errors["count"])

        if is_last_tick_of_day(settings, tick):
            drop_tariff_file(settings, sim_day_of_tick(settings, tick))

        tick += 1
        save_next_tick(state_file, settings, tick)

        next_wall += settings.reading_interval_seconds
        sleep_for = next_wall - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_wall = time.monotonic()  # fell behind; don't try to catch up in a burst

    producer.flush(10)
    log.info("simulation_stopped", next_tick=tick)


if __name__ == "__main__":
    main()
