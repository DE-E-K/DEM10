"""
detector.py – Anomaly detection consumer service.

Architecture
------------
This service runs as a **separate consumer group** (``cg.anomaly.v1``) on the
same ``events.raw.v1`` topic as the DB writer.  Running two independent consumer
groups means both services process every message independently — neither blocks
or affects the other's offset position.

Processing pipeline per message
--------------------------------
1. Deserialise JSON → ``HeartbeatEvent`` (skip malformed messages silently).
2. Call ``AnomalyRules.evaluate(event, recent_history)`` — a pure function with
   no I/O side effects.
3. If an anomaly is detected:
   a. Insert the anomaly record into PostgreSQL ``anomalies`` table.
   b. Publish the anomaly to ``events.anomaly.v1`` (for downstream alerting).
4. Update the customer's rolling rate history (last 6 readings, in-memory).
5. Commit the Kafka offset.

State management
----------------
Per-customer rate history is held in an in-memory ``defaultdict(deque)``.
This is intentionally simple:
 • The detector is stateless across restarts (history resets on restart).
 • The anomaly rules are designed to work with partial history (empty deque
   simply means no SPIKE check is performed for the first event).
 • For production deployments where cross-restart history matters, the last N
   readings per customer could be fetched from PostgreSQL on startup.

Observability
-------------
* Prometheus counter ``heartbeat_anomalies_total`` labelled by ``type`` and
  ``severity`` for per-type dashboard breakdown.
* Structured logs for every anomaly detected and every DB write.
* Graceful shutdown on SIGINT / SIGTERM.
"""

import json
import logging
import signal
import sys
from collections import defaultdict, deque

from confluent_kafka import KafkaError
from prometheus_client import Counter, start_http_server
from pydantic import ValidationError

from services.common.config import settings
from services.common.db import get_pool, insert_anomaly
from services.common.kafka_utils import build_consumer, build_producer
from services.common.models import HeartbeatEvent
from services.anomaly_detector.anomaly_rules import AnomalyRules

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

# ── Prometheus metrics ─────────────────────────────────────────────────────────
# Labelled counter: each (anomaly_type, severity) pair gets its own time series,
# enabling per-type breakdown in Grafana bar charts.
ANOMALIES_DETECTED = Counter(
    "heartbeat_anomalies_total",
    "Total anomalies detected, labelled by type and severity.",
    ["type", "severity"],
)

# ── Shutdown flag ──────────────────────────────────────────────────────────────
_shutdown_requested: bool = False


def _handle_signal(signum: int, _frame) -> None:
    """Request graceful shutdown on SIGINT / SIGTERM."""
    global _shutdown_requested
    logger.info("Shutdown signal %d received — stopping anomaly detector…", signum)
    _shutdown_requested = True


def main() -> None:
    """
    Run the anomaly detection consumer loop.

    Flow
    ----
    1. Register signal handlers.
    2. Start Prometheus /metrics HTTP server.
    3. Initialise in-memory rule engine, Kafka consumer, Kafka producer, DB pool.
    4. Poll messages, apply rules, persist anomalies, publish to anomaly topic.
    5. On shutdown: close consumer and pool gracefully.
    """
    # ── Signal handlers ────────────────────────────────────────────────────────
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # ── Prometheus /metrics ────────────────────────────────────────────────────
    # Offset port by 2 to avoid collision with producer (+0) and consumer (+1)
    metrics_port = settings.prometheus_port + 2
    start_http_server(metrics_port)
    logger.info("Prometheus /metrics available on port %d", metrics_port)

    # ── Kafka clients ──────────────────────────────────────────────────────────
    consumer = build_consumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_consumer_group_anomaly,
    )
    anomaly_producer = build_producer(settings.kafka_bootstrap_servers)
    consumer.subscribe([settings.kafka_topic_raw])

    # ── Database pool ──────────────────────────────────────────────────────────
    pool = get_pool()

    # ── Anomaly rule engine ────────────────────────────────────────────────────
    rules = AnomalyRules()

    # Per-customer rolling window of the last 6 heart-rate readings.
    # Used by the SPIKE rule to compute instantaneous delta.
    # maxlen=6 is a deque which auto-evicts oldest entries, keeping memory O(N_customers).
    last_rates: dict[str, deque[int]] = defaultdict(lambda: deque(maxlen=6))

    logger.info(
        "Anomaly detector started",
        extra={
            "topic": settings.kafka_topic_raw,
            "group": settings.kafka_consumer_group_anomaly,
            "low_threshold": settings.anomaly_low_threshold,
            "high_threshold": settings.anomaly_high_threshold,
            "spike_delta": settings.anomaly_spike_delta,
        },
    )

    try:
        while not _shutdown_requested:
            msg = consumer.poll(1.0)

            if msg is None:
                continue  # No message available — loop again

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Detector consumer error: %s", msg.error())
                continue

            # ── Parse message ──────────────────────────────────────────────────
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                event = HeartbeatEvent.model_validate(payload)
            except (json.JSONDecodeError, ValidationError) as exc:
                # Malformed messages are silently skipped here — the DB-writer
                # consumer has already routed them to events.invalid.v1.
                logger.debug("Skipping malformed message: %s", exc)
                consumer.commit(message=msg)
                continue

            # ── Apply anomaly rules ────────────────────────────────────────────
            # Pass the customer's recent history so the SPIKE rule can compute delta.
            history = list(last_rates[event.customer_id])
            anomaly = rules.evaluate(event, history)

            # Always update the rolling history, regardless of anomaly outcome
            last_rates[event.customer_id].append(event.heart_rate)

            # ── Persist and publish anomaly (if one was detected) ──────────────
            if anomaly is not None:
                logger.info(
                    "Anomaly detected",
                    extra={
                        "customer_id": anomaly.customer_id,
                        "type": anomaly.anomaly_type,
                        "severity": anomaly.severity,
                        "heart_rate": anomaly.heart_rate,
                        "details": anomaly.details,
                    },
                )

                try:
                    # Write to PostgreSQL (connection borrowed from pool)
                    with pool.connection() as conn:
                        insert_anomaly(conn, anomaly)
                        # conn.commit() called automatically by context manager

                    # Update Prometheus counter with labels for type/severity breakdown
                    ANOMALIES_DETECTED.labels(
                        type=anomaly.anomaly_type,
                        severity=anomaly.severity,
                    ).inc()

                    # Publish anomaly event to the anomaly topic for downstream consumers
                    anomaly_producer.produce(
                        settings.kafka_topic_anomaly,
                        key=anomaly.customer_id.encode("utf-8"),
                        value=json.dumps(anomaly.model_dump(mode="json")).encode("utf-8"),
                    )
                    anomaly_producer.flush(1) 

                except Exception as exc:
                    # Log but don't commit offset — we'll retry on next poll
                    logger.exception("Failed to persist/publish anomaly: %s", exc)
                    continue  # Skip the commit below — message will be re-delivered

            # Commit offset after successful processing (or after skipping anomaly)
            consumer.commit(message=msg)

    finally:
        # ── Graceful shutdown ──────────────────────────────────────────────────
        logger.info("Closing anomaly detector consumer and database pool…")
        consumer.close()
        pool.close()
        logger.info("Anomaly detector shut down cleanly.")
        sys.exit(0)


if __name__ == "__main__":
    main()
