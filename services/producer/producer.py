"""
producer.py – Kafka producer service for synthetic heartbeat data.

Architecture
------------
This service is the **entry point** for all data in the pipeline.  It runs an
infinite loop that:

  1. Draws a batch of synthetic heartbeat events from the simulator.
  2. Serialises each event to JSON and publishes it to ``events.raw.v1``.
  3. Occasionally amplifies the batch size to simulate traffic bursts.
  4. Sleeps briefly between batches to throttle to the configured rate.

Observability
-------------
Prometheus metrics are exposed on ``http://0.0.0.0:{prometheus_port}/metrics``
so the scraper in ``monitoring/prometheus/prometheus.yml`` can collect them.

Structured logging is emitted at INFO level (configurable via LOG_LEVEL).
Summary stats are printed every 5 seconds so operators can see throughput at
a glance without being drowned in per-message noise.

Graceful shutdown
-----------------
SIGINT and SIGTERM are captured.  On receipt the producer flushes any
in-flight messages (up to 5 s) before exiting cleanly.  This prevents data
loss when the container is stopped by Docker Compose or Kubernetes.
"""

import json
import logging
import signal
import sys
import time

from confluent_kafka import KafkaException
from prometheus_client import Counter, start_http_server

from services.common.config import settings
from services.common.kafka_utils import build_producer, make_delivery_callback
from services.common.simulator import heartbeat_stream

# Logging setup
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

# Prometheus metrics
# ``messages_produced_total`` counts every message handed to the producer
# (delivery may still fail asynchronously — see ``produce_errors_total``).
MESSAGES_PRODUCED = Counter(
    "heartbeat_messages_produced_total",
    "Total number of heartbeat messages handed to the Kafka producer.",
)
# ``produce_errors_total`` is incremented by the delivery callback whenever
# the broker reports a permanent delivery failure.
PRODUCE_ERRORS = Counter(
    "heartbeat_produce_errors_total",
    "Total number of messages that failed delivery confirmation from the broker.",
)

# Shutdown flag
# Set to True by the signal handler; the main loop checks it after each batch.
_shutdown_requested: bool = False


def _handle_signal(signum: int, _frame) -> None:
    """
    Handle SIGINT / SIGTERM by requesting a graceful shutdown.

    The main loop will see ``_shutdown_requested`` on its next iteration,
    flush the producer, and exit cleanly rather than being killed mid-batch.
    """
    global _shutdown_requested
    logger.info("Shutdown signal %d received — draining producer…", signum)
    _shutdown_requested = True


def main() -> None:
    """
    Run the heartbeat producer loop.

    Steps
    -----
    1. Register signal handlers for graceful shutdown.
    2. Start the Prometheus /metrics HTTP server.
    3. Build the Kafka producer and heartbeat event stream.
    4. Loop: publish a batch, poll for delivery callbacks, sleep.
    5. On shutdown signal: flush remaining messages and exit.
    """
    # Signal handlers
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Prometheus /metrics endpoint
    start_http_server(settings.prometheus_port)
    logger.info("Prometheus /metrics available on port %d", settings.prometheus_port)

    # Build Kafka producer + delivery callback
    producer = build_producer(settings.kafka_bootstrap_servers)
    # Pass the counters into the callback factory so delivery reports update metrics
    on_delivery = make_delivery_callback(
        errors_counter=PRODUCE_ERRORS,
        produced_counter=MESSAGES_PRODUCED,
    )

    # Synthetic event stream 
    stream = heartbeat_stream(
        customer_count=settings.sim_customer_count,
        invalid_ratio=settings.sim_invalid_ratio,
    )

    logger.info(
        "Producer started",
        extra={
            "topic": settings.kafka_topic_raw,
            "broker": settings.kafka_bootstrap_servers,
            "customers": settings.sim_customer_count,
            "target_eps": settings.sim_events_per_second,
        },
    )

    # Throughput-tracking accumulators 
    batch_count: int = 0
    total_sent: int = 0
    stats_interval: int = 5          # Print throughput summary every N seconds
    last_stats_time: float = time.monotonic()

    # Main publish loop
    while not _shutdown_requested:
        # Every 10 s we activate a burst window to simulate traffic spikes.
        # ``int(time.time()) % 10 == 0`` fires approximately once per 10-second
        # window, though with sub-second resolution it may fire for multiple
        # iterations — the burst multiplier is modest so this is intentional.
        is_burst = (int(time.time()) % 10 == 0)
        batch_size = (
            settings.sim_events_per_second * settings.sim_burst_multiplier
            if is_burst
            else settings.sim_events_per_second
        )

        for _ in range(batch_size):
            event = next(stream)
            payload = event.model_dump(mode="json")
            # Use customer_id as the Kafka message key so all events for the
            # same customer land in the same partition → per-customer ordering.
            key = event.customer_id.encode("utf-8")

            try:
                producer.produce(
                    settings.kafka_topic_raw,
                    key=key,
                    value=json.dumps(payload).encode("utf-8"),
                    on_delivery=on_delivery,
                )
            except KafkaException as exc:
                # Local queue full or broker connection lost — log and continue.
                # The retry logic in the producer config handles transient errors.
                logger.error("produce() call raised KafkaException: %s", exc)
                PRODUCE_ERRORS.inc()

        # poll(0) drains the delivery callback queue without blocking.
        # flush(1) ensures the internal queue doesn't grow unboundedly.
        producer.poll(0)
        producer.flush(1)

        batch_count += 1
        total_sent += batch_size

        # Periodic stats log 
        now = time.monotonic()
        if now - last_stats_time >= stats_interval:
            elapsed = now - last_stats_time
            eps = total_sent / elapsed if elapsed > 0 else 0
            logger.info(
                "Producer stats",
                extra={
                    "batches": batch_count,
                    "events_sent": total_sent,
                    "approx_eps": round(eps, 1),
                    "burst": is_burst,
                },
            )
            # Reset counters for the next window
            total_sent = 0
            batch_count = 0
            last_stats_time = now

        time.sleep(settings.sim_sleep_seconds)

    # Graceful shutdown
    logger.info("Flushing remaining messages before exit…")
    remaining = producer.flush(timeout=5)
    if remaining > 0:
        logger.warning("%d messages were NOT flushed before timeout.", remaining)
    logger.info("Producer shut down cleanly.")
    sys.exit(0)


if __name__ == "__main__":
    main()
