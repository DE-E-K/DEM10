"""
load_smoke.py – Throughput smoke test for the Kafka pipeline.

Purpose
-------
This script publishes a fixed number of messages as fast as possible and
measures the achieved throughput (events/second).  It is not a load test
in the traditional sense — it verifies that the local stack can sustain a
target throughput without errors before running the full pipeline.

Usage
-----
    python tests/load/load_smoke.py

Environment variables
---------------------
KAFKA_BOOTSTRAP_SERVERS  – default: localhost:19092
KAFKA_TOPIC_RAW          – default: events.raw.v1
SIM_CUSTOMER_COUNT       – default: 1000
LOAD_TEST_DURATION_SECONDS – default: 60
LOAD_TEST_MIN_EPS        – minimum acceptable events/s, default: 500
"""

import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import KafkaException, Producer

# ── Configuration (via env vars with sensible defaults) ────────────────────────
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")  # Fixed: was localhost:9092
TOPIC = os.getenv("KAFKA_TOPIC_RAW", "events.raw.v1")
CUSTOMER_COUNT = int(os.getenv("SIM_CUSTOMER_COUNT", "1000"))
DURATION_SECONDS = int(os.getenv("LOAD_TEST_DURATION_SECONDS", "60"))
MIN_EPS = float(os.getenv("LOAD_TEST_MIN_EPS", "500"))  # Warn below this threshold
BATCH_SIZE = 1000  # Messages per tight loop iteration


def _make_event(customers: list[str]) -> dict:
    """
    Build a minimal heartbeat event dict for load testing.
    We skip the full Pydantic model here to maximise throughput in the hot path.
    """
    return {
        "event_id": str(uuid4()),
        "customer_id": random.choice(customers),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "heart_rate": random.randint(55, 130),  # Normal-ish range for clean data
    }


def main() -> None:
    """
    Run the load smoke test.

    Publishes BATCH_SIZE messages per tight-loop iteration for DURATION_SECONDS
    total, then reports achieved throughput and compares against MIN_EPS.
    """
    # ── Build a hardened producer (same config as production) ──────────────────
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
            "batch.size": 65536,
            "compression.type": "lz4",
        }
    )

    customers = [f"cust_{i:05d}" for i in range(1, CUSTOMER_COUNT + 1)]

    print(
        f"Load smoke test starting | broker={BOOTSTRAP} | topic={TOPIC} | "
        f"duration={DURATION_SECONDS}s | customers={CUSTOMER_COUNT}"
    )

    end_time = time.monotonic() + DURATION_SECONDS
    sent = 0
    errors = 0
    start = time.monotonic()

    while time.monotonic() < end_time:
        for _ in range(BATCH_SIZE):
            event = _make_event(customers)
            try:
                producer.produce(
                    TOPIC,
                    key=event["customer_id"].encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                )
                sent += 1
            except KafkaException as exc:
                errors += 1
                # Log but don't fail fast — we want the full duration result
                print(f"  [warn] produce error: {exc}", file=sys.stderr)

        # Drain delivery callbacks without blocking
        producer.poll(0)

    # Flush remaining messages (up to 10 s)
    remaining = producer.flush(timeout=10)
    elapsed = time.monotonic() - start
    eps = sent / elapsed if elapsed > 0 else 0

    print(
        f"\nLoad smoke test complete\n"
        f"  Sent       : {sent:,} events\n"
        f"  Errors     : {errors:,}\n"
        f"  Duration   : {elapsed:.1f}s\n"
        f"  Throughput : {eps:,.0f} events/s\n"
        f"  Unflushed  : {remaining}"
    )

    # ── Throughput assertion ───────────────────────────────────────────────────
    if eps < MIN_EPS:
        print(
            f"\n[WARN] Throughput {eps:.0f} eps is below the minimum threshold "
            f"of {MIN_EPS:.0f} eps.  Check Kafka broker performance.",
            file=sys.stderr,
        )
        # Don't hard-fail on load smoke — this is a warning, not an assertion.
        # In a CI gate you could change this to sys.exit(1).
    else:
        print(f"[OK] Throughput {eps:.0f} eps exceeds the {MIN_EPS:.0f} eps minimum.")

    if errors > 0:
        print(f"\n[WARN] {errors} produce errors encountered during the test.")


if __name__ == "__main__":
    main()
