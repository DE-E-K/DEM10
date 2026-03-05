"""
anomaly_job.py – PyFlink job: detect anomalies in validated heartbeat events.

Replaces the original Python anomaly detector (services/anomaly_detector/detector.py)
with a distributed Flink streaming pipeline that:

1. Reads validated heartbeat JSON from ``events.validated.v1`` (KafkaSource).
2. Keys the stream by ``customer_id`` so each customer's history is co-located
   on a single task slot (partition-local state).
3. Applies the same three anomaly rules (LOW, HIGH, SPIKE) via a
   ``KeyedProcessFunction`` that maintains a rolling window of the last 6
   heart-rate readings in Flink managed state (RocksDB-backed ``ListState``).
4. Writes detected anomalies to:
   a. PostgreSQL ``anomalies`` table (JDBC sink with upsert).
   b. ``events.anomaly.v1`` topic (for downstream alerting / dashboards).

State management improvement
-----------------------------
Unlike the original in-memory ``defaultdict(deque)`` which resets on every
restart, Flink's checkpointed ``ListState`` survives restarts and rescaling.
The SPIKE rule always has full history available, even after failover.

Backpressure handling
---------------------
Flink's credit-based flow control automatically slows the source when the JDBC
sink or anomaly Kafka sink can't keep up, preventing OOM.
"""

import json
import logging
import os
import sys

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

# ---------------------------------------------------------------------------
# Ensure the project root is importable so we can use services.common.*
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from services.common.config import settings
from services.common.logging_config import setup_logging
from services.common.models import HeartbeatEvent, AnomalyEvent

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
setup_logging()
logger = logging.getLogger("flink.anomaly_job")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_WINDOW_SIZE = 6  # Keep last N readings per customer for SPIKE detection


class AnomalyDetectFunction(KeyedProcessFunction):
    """
    Keyed process function that applies anomaly detection rules per customer.

    State
    -----
    ``recent_rates`` : ListState[int]
        Rolling window of the last ``_WINDOW_SIZE`` heart-rate readings for the
        current customer_id key.  Backed by RocksDB; survives checkpoints and
        restarts.
    """

    def __init__(
        self,
        low_threshold: int,
        high_threshold: int,
        spike_delta: int,
    ):
        self._low_threshold = low_threshold
        self._high_threshold = high_threshold
        self._spike_delta = spike_delta
        self._recent_rates = None  # initialised in open()

    def open(self, runtime_context: RuntimeContext):
        """Register Flink managed state."""
        desc = ListStateDescriptor("recent_rates", Types.INT())
        self._recent_rates = runtime_context.get_list_state(desc)

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        """
        Evaluate anomaly rules for a single validated heartbeat event.

        Yields
        ------
        str : JSON-serialised ``AnomalyEvent`` for each anomaly detected.
        """
        try:
            payload = json.loads(value)
            event = HeartbeatEvent.model_validate(payload)
        except Exception as exc:
            logger.warning("Skipping malformed validated event: %s", exc)
            return

        rate = event.heart_rate

        # Retrieve state (list of ints from RocksDB)
        history = list(self._recent_rates.get())
        anomaly = self._evaluate_rules(event, rate, history)

        # Update rolling window (keep last _WINDOW_SIZE entries)
        history.append(rate)
        if len(history) > _WINDOW_SIZE:
            history = history[-_WINDOW_SIZE:]

        # Replace state with updated window
        self._recent_rates.clear()
        for r in history:
            self._recent_rates.add(r)

        # Emit anomaly if detected
        if anomaly is not None:
            yield json.dumps(anomaly.model_dump(mode="json"))

    def _evaluate_rules(
        self,
        event: HeartbeatEvent,
        rate: int,
        history: list[int],
    ) -> AnomalyEvent | None:
        """
        Apply LOW → HIGH → SPIKE rules (same logic as anomaly_rules.py).

        First matching rule wins.
        """
        # Rule 1: Absolute low threshold
        if rate <= self._low_threshold:
            return AnomalyEvent(
                event_id=event.event_id,
                customer_id=event.customer_id,
                timestamp=event.timestamp,
                heart_rate=rate,
                anomaly_type="LOW_HEART_RATE",
                severity="high",
                details={
                    "threshold": self._low_threshold,
                    "measured": rate,
                },
            )

        # Rule 2: Absolute high threshold
        if rate >= self._high_threshold:
            return AnomalyEvent(
                event_id=event.event_id,
                customer_id=event.customer_id,
                timestamp=event.timestamp,
                heart_rate=rate,
                anomaly_type="HIGH_HEART_RATE",
                severity="high",
                details={
                    "threshold": self._high_threshold,
                    "measured": rate,
                },
            )

        # Rule 3: Sudden spike relative to last reading
        if history:
            previous = history[-1]
            delta = abs(rate - previous)
            if delta >= self._spike_delta:
                return AnomalyEvent(
                    event_id=event.event_id,
                    customer_id=event.customer_id,
                    timestamp=event.timestamp,
                    heart_rate=rate,
                    anomaly_type="SPIKE",
                    severity="medium",
                    details={
                        "delta": delta,
                        "threshold": self._spike_delta,
                        "previous": previous,
                        "measured": rate,
                    },
                )

        return None


# ---------------------------------------------------------------------------
# Kafka source / sink builders
# ---------------------------------------------------------------------------


def _build_kafka_source() -> KafkaSource:
    """Read validated heartbeat JSON from events.validated.v1."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", settings.kafka_bootstrap_servers)
    validated_topic = settings.kafka_topic_validated
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(validated_topic)
        .set_group_id(settings.kafka_consumer_group_anomaly)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def _build_kafka_sink(topic: str) -> KafkaSink:
    """Write JSON strings to the given Kafka topic."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", settings.kafka_bootstrap_servers)
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def _build_jdbc_anomaly_sink_ddl() -> str:
    """
    Return a Flink SQL DDL for a JDBC sink table matching the PostgreSQL
    ``anomalies`` table schema.
    """
    pg_host = os.getenv("POSTGRES_HOST", settings.postgres_host)
    pg_port = os.getenv("POSTGRES_PORT", str(settings.postgres_port))
    pg_db = os.getenv("POSTGRES_DB", settings.postgres_db)
    pg_user = os.getenv("POSTGRES_USER", settings.postgres_user)
    pg_password = os.getenv("POSTGRES_PASSWORD", settings.postgres_password)
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    return f"""
        CREATE TABLE anomaly_jdbc_sink (
            event_id      STRING,
            customer_id   STRING,
            event_time    TIMESTAMP(3),
            heart_rate    SMALLINT,
            anomaly_type  STRING,
            severity      STRING,
            details       STRING,
            PRIMARY KEY (event_id, anomaly_type) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{jdbc_url}',
            'table-name' = 'anomalies',
            'username' = '{pg_user}',
            'password' = '{pg_password}',
            'sink.buffer-flush.max-rows' = '200',
            'sink.buffer-flush.interval' = '2s',
            'sink.max-retries' = '5'
        )
    """


# Main pipeline
# ---------------------------------------------------------------------------


def main():
    """Wire up the anomaly detection pipeline."""
    # Flink environment 
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30_000)

    # Source: events.validated.v1
    kafka_source = _build_kafka_source()
    validated_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "kafka-validated-source"
    )

    # Key by customer_id for per-customer state 
    from pyflink.datastream.functions import MapFunction, KeySelector

    class ExtractCustomerId(KeySelector):
        """Extract customer_id from JSON string for keying the stream."""

        def get_key(self, value: str) -> str:
            d = json.loads(value)
            return d.get("customer_id", "unknown")

    keyed_stream = validated_stream.key_by(ExtractCustomerId(), key_type=Types.STRING())

    # ── Anomaly detection (stateful per-customer processing) ───────────
    anomaly_stream = keyed_stream.process(
        AnomalyDetectFunction(
            low_threshold=settings.anomaly_low_threshold,
            high_threshold=settings.anomaly_high_threshold,
            spike_delta=settings.anomaly_spike_delta,
        ),
        output_type=Types.STRING(),
    )

    # ── Anomaly output → Kafka events.anomaly.v1 ──────────────────────
    anomaly_stream.sink_to(_build_kafka_sink(settings.kafka_topic_anomaly))

    # ── Anomaly output → PostgreSQL anomalies table (JDBC sink) ────────
    from pyflink.datastream import StreamTableEnvironment
    from pyflink.common import Row

    t_env = StreamTableEnvironment.create(env)
    t_env.execute_sql(_build_jdbc_anomaly_sink_ddl())

    class AnomalyJsonToRow(MapFunction):
        """Parse anomaly JSON into a Row matching anomaly_jdbc_sink schema."""

        def map(self, value: str) -> Row:
            d = json.loads(value)
            from datetime import datetime

            ts = datetime.fromisoformat(d["timestamp"].replace("Z", "+00:00"))
            return Row(
                str(d["event_id"]),
                d["customer_id"],
                ts,
                int(d["heart_rate"]),
                d["anomaly_type"],
                d["severity"],
                json.dumps(d.get("details", {})),
            )

    anomaly_rows = anomaly_stream.map(
        AnomalyJsonToRow(),
        output_type=Types.ROW_NAMED(
            [
                "event_id",
                "customer_id",
                "event_time",
                "heart_rate",
                "anomaly_type",
                "severity",
                "details",
            ],
            [
                Types.STRING(),
                Types.STRING(),
                Types.SQL_TIMESTAMP(),
                Types.SHORT(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
            ],
        ),
    )

    table = t_env.from_data_stream(anomaly_rows)
    table.execute_insert("anomaly_jdbc_sink")

    # Execute the Flink job
    logger.info("Starting Flink anomaly detection job…")
    env.execute("heartbeat-anomaly-detection")


if __name__ == "__main__":
    main()
