"""
kafka_utils.py – Factory helpers for production-hardened Kafka clients.

Producer settings explained
---------------------------
* ``acks=all``                  – Leader waits for all in-sync replicas to acknowledge before returning success.
* ``enable.idempotence=true``   – Exactly-once delivery within a single producer session; prevents duplicates on retry.
* ``retries=10``                – Retry transient failures up to 10 times before raising to the caller.
* ``linger.ms=5``               – Micro-batch messages for up to 5 ms before sending; dramatically improves throughput 
                                  with minimal latency cost.
* ``batch.size=65536``          – 64 KB batch buffer per partition.
* ``compression.type=lz4``      – Fast compression; reduces network + broker I/O.
* ``max.in.flight.requests.per.connection=5`` – Keeps pipeline full while retaining ordering (safe with idempotence enabled).

Consumer settings explained
---------------------------
* ``enable.auto.commit=false``     – We commit offsets *only* after the DB write succeeds, giving us at-least-once semantics.
* ``auto.offset.reset=earliest``   – Start from the earliest unread offset for consumer groups that haven't committed yet.
* ``max.poll.interval.ms=300000``  – Allow up to 5 min of processing time per batch before the group rebalances 
                                     (important for slow DB writes under back-pressure).
* ``session.timeout.ms=45000``     – Mark consumer dead if no heartbeat received in 45 s (3× the default heartbeat interval).
* ``heartbeat.interval.ms=15000``  – Send heartbeat every 15 s (must be < 1/3 of session.timeout.ms).
* ``fetch.min.bytes=1``            – Return immediately even if only 1 byte is available (low-latency mode for sensor data).
""" 

import logging

from confluent_kafka import Consumer, KafkaError, Message, Producer

logger = logging.getLogger(__name__)


# Delivery report callback
# =======================================================================

def make_delivery_callback(errors_counter=None, produced_counter=None):
    """
    Build a closure suitable for passing to ``producer.produce(on_delivery=...)``.

    The returned callback fires for every message once the broker acknowledges
    it (success) or once the producer gives up (failure).  Optionally updates
    Prometheus counters supplied by the caller.

    Parameters
    ----------
    errors_counter:
        A ``prometheus_client.Counter`` (or None) to increment on delivery failure.
    produced_counter:
        A ``prometheus_client.Counter`` (or None) to increment on delivery success.

    Returns
    -------
    callable
        A ``(err, msg) -> None`` callback.
    """

    def _on_delivery(err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            logger.error(
                "Delivery failed",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "error": str(err),
                },
            )
            if errors_counter is not None:
                errors_counter.inc()
        else:
            logger.debug(
                "Message delivered",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )
            if produced_counter is not None:
                produced_counter.inc()

    return _on_delivery


# Client factories
# =======================================================================

def build_producer(bootstrap_servers: str) -> Producer:
    """
    Create a production-hardened confluent-kafka ``Producer``.

    Key guarantees
    --------------
    * Idempotent delivery (no duplicates from retries).
    * Message batching + LZ4 compression for high throughput.
    * All-in-sync-replica acknowledgement for durability.

    Parameters
    ----------
    bootstrap_servers:
        Comma-separated ``host:port`` list of Kafka broker addresses.

    Returns
    -------
    Producer
        A configured confluent-kafka Producer instance.
    """
    config = {
        # Connectivity
        "bootstrap.servers": bootstrap_servers,
        # Durability: wait for all in-sync replicas
        "acks": "all",
        # Exactly-once: prevent duplicates across retries
        "enable.idempotence": True,
        # Retry transient errors automatically
        "retries": 10,
        # Throughput: accumulate messages for up to 5 ms before sending
        "linger.ms": 5,
        # Throughput: 64 KB batch buffer per partition
        "batch.size": 65536,
        # Throughput: fast in-flight pipeline depth (safe with idempotence)
        "max.in.flight.requests.per.connection": 5,
        # Storage/network efficiency: fast LZ4 compression
        "compression.type": "lz4",
    }
    logger.info("Building Kafka producer", extra={"bootstrap": bootstrap_servers})
    return Producer(config)


def build_consumer(bootstrap_servers: str, group_id: str) -> Consumer:
    """
    Create a production-hardened confluent-kafka ``Consumer``.

    Key guarantees
    --------------
    * Manual offset commit — offsets are only committed after DB writes succeed.
    * Heartbeat / session timeouts tuned for processing workloads under load.
    * ``earliest`` reset so new consumer groups don't miss historical messages.

    Parameters
    ----------
    bootstrap_servers:
        Comma-separated ``host:port`` list of Kafka broker addresses.
    group_id:
        Consumer group identifier.  Must be unique per logical consumer role
        (e.g. ``cg.db-writer.v1`` and ``cg.anomaly.v1`` are independent).

    Returns
    -------
    Consumer: A configured confluent-kafka Consumer instance.
    """
    config = {
        # Connectivity
        "bootstrap.servers": bootstrap_servers,
        # Group membership
        "group.id": group_id,
        # Safety: never auto-commit; we control when offsets advance
        "enable.auto.commit": False,
        # Start from the beginning for new/unknown consumer groups
        "auto.offset.reset": "earliest",
        # Rebalance protection: allow 5 min of processing per batch
        "max.poll.interval.ms": 300_000,
        # Heartbeat tuning: 45 s session timeout, heartbeat every 15 s
        "session.timeout.ms": 45_000,
        "heartbeat.interval.ms": 15_000,
        # Low-latency: return immediately if any data is available
        "fetch.min.bytes": 1,
    }
    logger.info(
        "Building Kafka consumer",
        extra={"bootstrap": bootstrap_servers, "group_id": group_id},
    )
    return Consumer(config)
