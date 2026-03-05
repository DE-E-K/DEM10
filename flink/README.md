# Flink Jobs

This directory contains PyFlink streaming jobs that replace the Python
consumer and anomaly detector services with a distributed, backpressure-aware
stream processor.

## Jobs

| Job | Replaces | Description |
|-----|----------|-------------|
| `validation_sink_job.py` | `services/consumer/consumer.py` | Deserialises, validates (hard + soft bounds), writes to PostgreSQL, quarantines invalid/DLQ messages |
| `anomaly_job.py` | `services/anomaly_detector/detector.py` | Keyed by `customer_id`, maintains rolling window in Flink state, evaluates anomaly rules, persists to DB + anomaly topic |

## Submitting jobs

```bash
# From the project root, with docker compose running:
docker exec heartbeat-flink-jobmanager \
  flink run -py /opt/flink/jobs/validation_sink_job.py

docker exec heartbeat-flink-jobmanager \
  flink run -py /opt/flink/jobs/anomaly_job.py
```

## Monitoring

- **Flink Web UI**: http://localhost:8081
- **Prometheus metrics**: http://localhost:9249/metrics (scraped automatically)

## Key improvements over Python consumers

1. **Backpressure**: Flink's credit-based flow control automatically slows the
   Kafka source when the JDBC sink is saturated no message loss, no OOM.
2. **Exactly-once**: Checkpoint-based offset commits ensure each event is
   processed exactly once (Kafka offsets committed atomically with state).
3. **Durable state**: Per-customer anomaly history survives restarts (stored in
   RocksDB, checkpointed to disk) SPIKE detection works immediately after
   restart, unlike the old in-memory deque.
4. **Horizontal scaling**: Increase `taskmanager.numberOfTaskSlots` or add
   more TaskManager instances to scale processing parallelism.
