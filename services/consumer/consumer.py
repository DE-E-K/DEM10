"""
consumer.py – Kafka consumer and PostgreSQL writer service.

Responsibilities
----------------
1. Subscribe to ``events.raw.v1`` as consumer group ``cg.db-writer.v1``.
2. For each message:
   a. Deserialise JSON → ``HeartbeatEvent`` (Pydantic validation).
   b. Apply soft domain bounds (configurable heart-rate range in settings).
   c. Write valid events to PostgreSQL ``heartbeat_events`` table.
   d. Upsert the consumer offset into ``ingest_checkpoint`` (application-level
      offset store for manual replay support).
   e. Commit the Kafka offset *only* after the DB write succeeds.
3. Route invalid messages (schema / domain failures) to ``events.invalid.v1``.
4. Route unexpected processing errors to ``events.dlq.v1`` for human inspection.

Exactly-once semantics (practical)
-----------------------------------
True exactly-once requires a distributed transaction between Kafka and Postgres,
which needs Kafka Transactions API and is complex.  We implement the next-best
alternative: **at-least-once delivery with idempotent writes**.

* Offsets are committed only after a successful DB transaction.
* ``INSERT … ON CONFLICT DO NOTHING`` means replaying the same message is safe.
* The ``ingest_checkpoint`` table gives us a recovery anchor after restarts.

Observability
-------------
* Prometheus counters emitted to ``http://0.0.0.0:{prometheus_port}/metrics``.
* Structured logging at configurable level (default INFO).
* SIGINT / SIGTERM triggers a graceful shutdown: close consumer → close pool.
"""

import json
import logging
import signal
import sys
import threading

from confluent_kafka import KafkaError
from prometheus_client import Counter, start_http_server
from pydantic import ValidationError

from services.common.config import settings
from services.common.db import get_pool, insert_heartbeat, upsert_checkpoint
from services.common.kafka_utils import build_consumer, build_producer
from services.common.models import HeartbeatEvent, InvalidEvent

# Logging
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

# Prometheus metrics─
MESSAGES_CONSUMED = Counter(
    "heartbeat_messages_consumed_total",
    "Total messages polled from the raw topic.",
)
DB_INSERTS = Counter(
    "heartbeat_db_inserts_total",
    "Total heartbeat events successfully written to PostgreSQL.",
)
INVALID_TOTAL = Counter(
    "heartbeat_invalid_total",
    "Total messages routed to the invalid topic (schema/domain failures).",
)
DLQ_TOTAL = Counter(
    "heartbeat_dlq_total",
    "Total messages routed to the DLQ topic (unexpected processing errors).",
)

# Shutdown flag
_shutdown_requested: bool = False


def _handle_signal(signum: int, _frame) -> None:
    """Request graceful shutdown on SIGINT / SIGTERM."""
    global _shutdown_requested
    logger.info("Shutdown signal %d received — stopping consumer loop…", signum)
    _shutdown_requested = True


def _publish_quarantine(producer, topic: str, raw: str, error: str,error_type: str,) -> None:
    """
    Publish a message to a quarantine topic (invalid or DLQ).

    Wraps the raw payload inside an ``InvalidEvent`` envelope so downstream
    consumers of these topics have a consistent, typed schema to parse.

    Parameters
    ----------
    producer:   confluent-kafka Producer used to publish the envelope.
    topic:      Destination topic (``events.invalid.v1`` or ``events.dlq.v1``).
    raw:        The original raw message string that failed processing.
    error:      Human-readable reason for failure.
    error_type: ``"VALIDATION"`` or ``"PROCESSING"``.
    """
    envelope = InvalidEvent(error=error, raw=raw, error_type=error_type)
    producer.produce(topic,
        value=json.dumps(envelope.model_dump()).encode("utf-8"),
    )
    producer.flush(1)


def main() -> None:
    """
    Run the consumer DB-writer loop.

    Flow
    ----
    1. Register SIGINT / SIGTERM handlers.
    2. Start Prometheus /metrics HTTP server (in a background thread).
    3. Initialise connection pool + Kafka consumer + quarantine producer.
    4. Poll messages indefinitely, processing each through the validation →
       write → commit pipeline.
    5. On shutdown signal: close Kafka consumer and pool gracefully.
    """
    # Signal handlers
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Prometheus /metrics (runs in its own daemon thread)
    # Offset the port by 1 so producer and consumer can coexist on the same host
    metrics_port = settings.prometheus_port + 1
    start_http_server(metrics_port)
    logger.info("Prometheus /metrics available on port %d", metrics_port)

    # Kafka clients
    consumer = build_consumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_consumer_group_db,
    )
    # Quarantine producer: routes invalid / DLQ messages to side topics
    quarantine_producer = build_producer(settings.kafka_bootstrap_servers)
    consumer.subscribe([settings.kafka_topic_raw])

    # Database connection pool
    pool = get_pool()

    logger.info(
        "Consumer started",
        extra={
            "topic": settings.kafka_topic_raw,
            "group": settings.kafka_consumer_group_db,
            "broker": settings.kafka_bootstrap_servers,
            "rate_bounds": f"[{settings.heart_rate_min}, {settings.heart_rate_max}]",
        },
    )

    try:
        while not _shutdown_requested:
            # poll(1.0) blocks for up to 1 second waiting for a new message.
            # Returns None on timeout — we simply loop again.
            msg = consumer.poll(1.0)

            if msg is None:
                continue  # Timeout — no message available right now

            if msg.error():
                # PARTITION_EOF is informational (reached the end of a partition);
                # real errors are logged and we continue rather than crashing.
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Consumer error: %s", msg.error())
                continue

            MESSAGES_CONSUMED.inc()
            raw_payload = msg.value().decode("utf-8")

            try:
                # Step 1: Deserialise and schema-validate
                payload = json.loads(raw_payload)
                event = HeartbeatEvent.model_validate(payload)

                # Step 2: Soft domain bounds (configurable via settings)
                # Hard bounds [0, 250] were already enforced in the Pydantic model.
                # Soft bounds [heart_rate_min, heart_rate_max] represent the
                # clinically acceptable range for this specific deployment.
                if not (settings.heart_rate_min <= event.heart_rate <= settings.heart_rate_max):
                    raise ValueError(
                        f"heart_rate {event.heart_rate} is outside the configured "
                        f"domain bounds [{settings.heart_rate_min}, {settings.heart_rate_max}]"
                    )

                # Step 3: Write to PostgreSQL (inside a pool connection)
                # The connection is borrowed from the pool and returned (via
                # context manager) when the ``with`` block exits.
                with pool.connection() as conn:
                    insert_heartbeat(
                        conn, event, msg.topic(), msg.partition(), msg.offset()
                    )
                    upsert_checkpoint(
                        conn,
                        settings.kafka_consumer_group_db,
                        msg.topic(),
                        msg.partition(), 
                        msg.offset(),
                    )
                    # conn.commit() is called automatically by the context manager

                DB_INSERTS.inc()

                # Step 4: Commit the Kafka offset
                # Only reached if the DB write succeeded, giving us at-least-once
                # guarantees: on failure, Kafka will re-deliver the same message.
                consumer.commit(message=msg)

            except (ValidationError, ValueError, json.JSONDecodeError) as exc:
                # Schema or domain validation failure — quarantine for audit
                logger.warning(
                    "Validation failure, routing to invalid topic | error=%s | payload=%s",
                    str(exc),
                    raw_payload[:200],
                    extra={"error": str(exc), "topic": settings.kafka_topic_invalid},
                )
                INVALID_TOTAL.inc()
                _publish_quarantine(
                    quarantine_producer,
                    settings.kafka_topic_invalid,
                    raw_payload,
                    str(exc),
                    "VALIDATION",
                )
                # Commit offset so we don't re-process this bad message
                consumer.commit(message=msg)

            except Exception as exc:
                # Unexpected error (DB down, bug in code, etc.) — route to DLQ
                # so the message is preserved for manual inspection / replay.
                logger.exception(
                    "Unexpected processing failure, routing to DLQ",
                    extra={"error": str(exc), "topic": settings.kafka_topic_dlq},
                )
                DLQ_TOTAL.inc()
                _publish_quarantine(
                    quarantine_producer,
                    settings.kafka_topic_dlq,
                    raw_payload,
                    str(exc),
                    "PROCESSING",
                )
                # We DO NOT commit the Kafka offset for unexpected failures —
                # the message stays available for the retry path / DLQ consumer.

    finally:
        # Graceful shutdown
        logger.info("Closing Kafka consumer and database pool…")
        consumer.close()
        pool.close()
        logger.info("Consumer shut down cleanly.")
        sys.exit(0)


if __name__ == "__main__":
    main()
