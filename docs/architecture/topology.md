# System Topology

```mermaid
flowchart LR
  G[Generator] --> P[Kafka Producer]
  P --> RAW[(events.raw.v1)]
  RAW --> FV[Flink: Validation + Sink Job]
  FV --> DB_HB[(PostgreSQL heartbeat_events)]
  FV --> VAL[(events.validated.v1)]
  FV --> INV[(events.invalid.v1)]
  FV --> DLQ[(events.dlq.v1)]
  VAL --> FA[Flink: Anomaly Detection Job]
  FA --> DB_AN[(PostgreSQL anomalies)]
  FA --> ANOM[(events.anomaly.v1)]
  DB_HB --> GR[Grafana]
  DB_AN --> GR
  FV -.-> PROM[Prometheus]
  FA -.-> PROM
  PROM --> GR
  RAW --> UI[Kafka UI]
```

Export this diagram to PNG or PDF for submission.
