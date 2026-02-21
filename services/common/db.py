"""
db.py – Database access layer with connection pooling and retry logic.

Architecture
------------
Rather than holding a single connection per process (which is fragile — a
dropped connection kills the whole consumer), this module exposes a
``get_pool()`` singleton that manages a psycopg ``ConnectionPool``.

Consumers borrow a connection from the pool for each write operation and
return it automatically when the ``with pool.connection()`` block exits.

Retry logic
-----------
Transient ``OperationalError`` conditions (e.g. PostgreSQL momentarily
unreachable under container restart) are retried up to ``_MAX_RETRIES`` times
with exponential back-off before being re-raised.  This prevents a brief DB
hiccup from crashing the consumer service.

Usage
-----
>>> from services.common.db import get_pool, insert_heartbeat
>>> pool = get_pool()
>>> with pool.connection() as conn:
...     insert_heartbeat(conn, event, topic, partition, offset)
"""

import json
import logging
import time
from functools import wraps
from typing import Callable, TypeVar

import psycopg
from psycopg.errors import OperationalError
from psycopg_pool import ConnectionPool

from services.common.config import settings
from services.common.models import AnomalyEvent, HeartbeatEvent

logger = logging.getLogger(__name__)

# Retry configuration
_MAX_RETRIES: int = 5          # Maximum number of retry attempts on transient errors
_BASE_BACKOFF_S: float = 0.5   # Initial back-off duration (doubles on each retry)

# Connection pool singleton
# Lazily initialised on first call to get_pool() so unit tests that never touch
# the DB don't require a live PostgreSQL instance.
_pool: ConnectionPool | None = None


def _build_conninfo() -> str:
    """
    Construct a libpq connection-info string from settings.

    Returns
    -------
    str
        PostgreSQL DSN string, e.g.
        ``"host=localhost port=55432 dbname=heartbeat user=... password=..."``
    """
    return (
        f"host={settings.postgres_host} "
        f"port={settings.postgres_port} "
        f"dbname={settings.postgres_db} "
        f"user={settings.postgres_user} "
        f"password={settings.postgres_password}"
    )


def get_pool() -> ConnectionPool:
    """
    Return the module-level connection-pool singleton, creating it on first call.

    Pool sizing is driven by ``settings.db_pool_min`` / ``settings.db_pool_max``
    so it can be tuned via environment variables without code changes.

    Thread safety: psycopg's ``ConnectionPool`` is fully thread-safe.

    Returns
    -------
    ConnectionPool
        A ready-to-use psycopg v3 connection pool.
    """
    global _pool
    if _pool is None:
        conninfo = _build_conninfo()
        logger.info(
            "Initialising PostgreSQL connection pool",
            extra={
                "min_size": settings.db_pool_min,
                "max_size": settings.db_pool_max,
                "host": settings.postgres_host,
                "port": settings.postgres_port,
                "db": settings.postgres_db,
            },
        )
        _pool = ConnectionPool(
            conninfo=conninfo,
            min_size=settings.db_pool_min,
            max_size=settings.db_pool_max,
            # Block (up to 30 s) rather than raise if pool is exhausted
            timeout=30,
            # Validate connections borrowed from the pool
            reconnect_failed=lambda pool: logger.error("Pool reconnect failed!"),
        )
    return _pool


# Retry decorator
# =======================================================================

_F = TypeVar("_F", bound=Callable)


def _with_retry(fn: _F) -> _F:
    """
    Decorator: retry ``fn`` on ``psycopg.OperationalError`` with exponential back-off.

    This handles transient failures such as a brief network interruption between
    the consumer container and PostgreSQL.  After ``_MAX_RETRIES`` failures the
    exception is re-raised so the caller can route the message to the DLQ.

    Parameters
    ----------
    fn:
        Any callable that performs a psycopg DB operation.

    Returns
    -------
    callable
        The wrapped function with retry behaviour.
    """

    @wraps(fn)
    def _wrapper(*args, **kwargs):
        delay = _BASE_BACKOFF_S
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                return fn(*args, **kwargs)
            except OperationalError as exc:
                if attempt == _MAX_RETRIES:
                    logger.error(
                        "DB operation failed after %d retries: %s", _MAX_RETRIES, exc
                    )
                    raise
                logger.warning(
                    "Transient DB error (attempt %d/%d), retrying in %.1fs: %s",
                    attempt,
                    _MAX_RETRIES,
                    delay,
                    exc,
                )
                time.sleep(delay)
                delay *= 2  # Exponential back-off

    return _wrapper  # type: ignore[return-value]


# Write helpers
# =======================================================================

@_with_retry
def insert_heartbeat(
    conn: psycopg.Connection,
    event: HeartbeatEvent,
    topic: str,
    partition: int,
    offset: int,
) -> None:
    """
    Persist a validated heartbeat event to ``heartbeat_events``.

    Idempotency
    -----------
    ``ON CONFLICT (customer_id, event_id) DO NOTHING`` means re-processing the
    same Kafka message (e.g. after a consumer restart) is safe — the row already
    exists and the insert becomes a no-op.

    Provenance columns
    ------------------
    We store ``source_topic``, ``source_partition``, and ``source_offset`` so
    that any row in the DB can be traced back to the exact Kafka message that
    produced it.  Useful for auditing and debugging replays.

    Parameters
    ----------
    conn:       Active psycopg connection (borrowed from the pool by the caller).
    event:      Validated ``HeartbeatEvent`` from the Kafka message.
    topic:      Kafka topic name (e.g. ``events.raw.v1``).
    partition:  Kafka partition number.
    offset:     Kafka message offset within the partition.
    """
    # Build the JSON payload stored verbatim for full auditing
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
                "valid",          # soft-valid: passed domain checks in consumer
                topic,
                partition,
                offset,
                json.dumps(payload),
            ),
        )

    logger.debug(
        "Inserted heartbeat",
        extra={
            "event_id": str(event.event_id),
            "customer_id": event.customer_id,
            "heart_rate": event.heart_rate,
        },
    )


@_with_retry
def insert_anomaly(conn: psycopg.Connection, anomaly: AnomalyEvent) -> None:
    """
    Persist a detected anomaly record to the ``anomalies`` table.

    The ``event_id`` is NOT unique-constrained here — an event can theoretically
    be flagged by multiple rules — but the anomaly detector currently emits at
    most one anomaly per event.

    Parameters
    ----------
    conn:    Active psycopg connection.
    anomaly: ``AnomalyEvent`` produced by the anomaly detector service.
    """
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

    logger.info(
        "Inserted anomaly",
        extra={
            "event_id": str(anomaly.event_id),
            "customer_id": anomaly.customer_id,
            "type": anomaly.anomaly_type,
            "severity": anomaly.severity,
            "heart_rate": anomaly.heart_rate,
        },
    )


@_with_retry
def upsert_checkpoint(
    conn: psycopg.Connection,
    consumer_group: str,
    topic: str,
    partition: int,
    offset: int,
) -> None:
    """
    Record (or update) the last successfully processed Kafka offset.

    Purpose
    -------
    This table acts as an application-level offset store that complements
    Kafka's built-in consumer-group offset commit.  If the consumer restarts
    and Kafka's offset store is unavailable (or we want to do a manual replay),
    we can recover the last known good position from this table.

    Parameters
    ----------
    conn:           Active psycopg connection.
    consumer_group: Kafka consumer group identifier.
    topic:          Kafka topic name.
    partition:      Partition number.
    offset:         Offset of the last successfully processed message.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_checkpoint (consumer_group, topic, partition, last_offset)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (consumer_group, topic, partition)
            DO UPDATE SET
                last_offset = EXCLUDED.last_offset,
                updated_at  = NOW();
            """,
            (consumer_group, topic, partition, offset),
        )
