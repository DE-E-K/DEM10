import json
import psycopg

from services.common.config import settings
from services.common.models import AnomalyEvent, HeartbeatEvent


def get_conn() -> psycopg.Connection:
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        autocommit=False,
    )


def insert_heartbeat(conn: psycopg.Connection, event: HeartbeatEvent, topic: str, partition: int, offset: int) -> None:
    payload = {
        "event_id": str(event.event_id),
        "customer_id": event.customer_id,
        "timestamp": event.timestamp.isoformat(),
        "heart_rate": event.heart_rate,
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO heartbeat_events (
                event_id, customer_id, event_time, heart_rate,
                quality_flag, source_topic, source_partition, source_offset, payload
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (customer_id, event_id) DO NOTHING;
            """,
            (
                str(event.event_id),
                event.customer_id,
                event.timestamp,
                event.heart_rate,
                "valid",
                topic,
                partition,
                offset,
                json.dumps(payload),
            ),
        )


def insert_anomaly(conn: psycopg.Connection, anomaly: AnomalyEvent) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO anomalies (
                event_id, customer_id, event_time, heart_rate,
                anomaly_type, severity, details
            ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb);
            """,
            (
                str(anomaly.event_id),
                anomaly.customer_id,
                anomaly.timestamp,
                anomaly.heart_rate,
                anomaly.anomaly_type,
                anomaly.severity,
                json.dumps(anomaly.details),
            ),
        )


def upsert_checkpoint(conn: psycopg.Connection, consumer_group: str, topic: str, partition: int, offset: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_checkpoint (consumer_group, topic, partition, last_offset)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (consumer_group, topic, partition)
            DO UPDATE SET last_offset = EXCLUDED.last_offset, updated_at = NOW();
            """,
            (consumer_group, topic, partition, offset),
        )
