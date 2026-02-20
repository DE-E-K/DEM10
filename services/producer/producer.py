import json
import time
from confluent_kafka import KafkaException

from services.common.config import settings
from services.common.kafka_utils import build_producer
from services.common.simulator import heartbeat_stream


def _on_delivery(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


def main() -> None:
    producer = build_producer(settings.kafka_bootstrap_servers)
    stream = heartbeat_stream(settings.sim_customer_count, settings.sim_invalid_ratio)

    print(f"Producing to topic={settings.kafka_topic_raw} on {settings.kafka_bootstrap_servers}")

    while True:
        batch_size = settings.sim_events_per_second
        if int(time.time()) % 10 == 0:
            batch_size *= settings.sim_burst_multiplier

        for _ in range(batch_size):
            event = next(stream)
            payload = event.model_dump(mode="json")
            key = payload["customer_id"]
            try:
                producer.produce(
                    settings.kafka_topic_raw,
                    key=key.encode("utf-8"),
                    value=json.dumps(payload).encode("utf-8"),
                    on_delivery=_on_delivery,
                )
            except KafkaException as exc:
                print(f"producer error: {exc}")

        producer.poll(0)
        producer.flush(1)
        time.sleep(settings.sim_sleep_seconds)


if __name__ == "__main__":
    main()
