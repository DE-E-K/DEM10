import json
from confluent_kafka import KafkaError
from pydantic import ValidationError

from services.common.config import settings
from services.common.db import get_conn, insert_heartbeat, upsert_checkpoint
from services.common.kafka_utils import build_consumer, build_producer
from services.common.models import HeartbeatEvent


def main() -> None:
    consumer = build_consumer(settings.kafka_bootstrap_servers, settings.kafka_consumer_group_db)
    invalid_producer = build_producer(settings.kafka_bootstrap_servers)
    consumer.subscribe([settings.kafka_topic_raw])

    conn = get_conn()
    print(f"Consuming topic={settings.kafka_topic_raw} as group={settings.kafka_consumer_group_db}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"consumer error: {msg.error()}")
                continue

            payload_raw = msg.value().decode("utf-8")
            try:
                payload = json.loads(payload_raw)
                event = HeartbeatEvent.model_validate(payload)
                insert_heartbeat(conn, event, msg.topic(), msg.partition(), msg.offset())
                upsert_checkpoint(conn, settings.kafka_consumer_group_db, msg.topic(), msg.partition(), msg.offset())
                conn.commit()
                consumer.commit(message=msg)
            except (ValidationError, ValueError, json.JSONDecodeError) as exc:
                conn.rollback()
                invalid_payload = {
                    "error": str(exc),
                    "raw": payload_raw,
                }
                invalid_producer.produce(
                    settings.kafka_topic_invalid,
                    value=json.dumps(invalid_payload).encode("utf-8"),
                )
                invalid_producer.flush(1)
            except Exception as exc:
                conn.rollback()
                print(f"processing failure: {exc}")
                dlq_payload = {
                    "error": str(exc),
                    "raw": payload_raw,
                }
                invalid_producer.produce(
                    settings.kafka_topic_dlq,
                    value=json.dumps(dlq_payload).encode("utf-8"),
                )
                invalid_producer.flush(1)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
