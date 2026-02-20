import json
from collections import defaultdict, deque

from confluent_kafka import KafkaError
from pydantic import ValidationError

from services.common.config import settings
from services.common.db import get_conn, insert_anomaly
from services.common.kafka_utils import build_consumer, build_producer
from services.common.models import AnomalyEvent, HeartbeatEvent


def main() -> None:
    consumer = build_consumer(settings.kafka_bootstrap_servers, settings.kafka_consumer_group_anomaly)
    producer = build_producer(settings.kafka_bootstrap_servers)
    consumer.subscribe([settings.kafka_topic_raw])
    conn = get_conn()

    last_rates = defaultdict(lambda: deque(maxlen=6))

    print(f"Anomaly detector on topic={settings.kafka_topic_raw}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"detector error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                event = HeartbeatEvent.model_validate(payload)
            except (json.JSONDecodeError, ValidationError):
                continue

            anomaly = None
            previous = last_rates[event.customer_id][-1] if last_rates[event.customer_id] else event.heart_rate
            delta = abs(event.heart_rate - previous)

            if event.heart_rate <= settings.anomaly_low_threshold:
                anomaly = AnomalyEvent(
                    event_id=event.event_id,
                    customer_id=event.customer_id,
                    timestamp=event.timestamp,
                    heart_rate=event.heart_rate,
                    anomaly_type="LOW_HEART_RATE",
                    severity="high",
                    details={"threshold": settings.anomaly_low_threshold},
                )
            elif event.heart_rate >= settings.anomaly_high_threshold:
                anomaly = AnomalyEvent(
                    event_id=event.event_id,
                    customer_id=event.customer_id,
                    timestamp=event.timestamp,
                    heart_rate=event.heart_rate,
                    anomaly_type="HIGH_HEART_RATE",
                    severity="high",
                    details={"threshold": settings.anomaly_high_threshold},
                )
            elif delta >= settings.anomaly_spike_delta:
                anomaly = AnomalyEvent(
                    event_id=event.event_id,
                    customer_id=event.customer_id,
                    timestamp=event.timestamp,
                    heart_rate=event.heart_rate,
                    anomaly_type="SPIKE",
                    severity="medium",
                    details={"delta": delta, "threshold": settings.anomaly_spike_delta},
                )

            last_rates[event.customer_id].append(event.heart_rate)

            if anomaly is not None:
                try:
                    insert_anomaly(conn, anomaly)
                    conn.commit()
                    producer.produce(
                        settings.kafka_topic_anomaly,
                        key=anomaly.customer_id.encode("utf-8"),
                        value=json.dumps(anomaly.model_dump(mode="json")).encode("utf-8"),
                    )
                    producer.flush(1)
                except Exception as exc:
                    conn.rollback()
                    print(f"anomaly write failed: {exc}")

            consumer.commit(message=msg)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
