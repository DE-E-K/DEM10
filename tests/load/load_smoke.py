import json
import os
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC_RAW", "events.raw.v1")
    customer_count = int(os.getenv("SIM_CUSTOMER_COUNT", "1000"))
    duration_seconds = int(os.getenv("LOAD_TEST_DURATION_SECONDS", "60"))

    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    customers = [f"cust_{i:05d}" for i in range(1, customer_count + 1)]
    end = time.time() + duration_seconds
    sent = 0

    while time.time() < end:
        for _ in range(1000):
            event = {
                "event_id": str(uuid4()),
                "customer_id": random.choice(customers),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "heart_rate": random.randint(50, 155),
            }
            producer.produce(topic, key=event["customer_id"], value=json.dumps(event))
            sent += 1
        producer.poll(0)
        producer.flush(1)

    print(f"Load smoke complete. Sent={sent} events in {duration_seconds}s")


if __name__ == "__main__":
    main()
