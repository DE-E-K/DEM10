# Ingestion Sequence

```mermaid
sequenceDiagram
  participant Sim as Simulator
  participant Prod as Producer
  participant Kafka as Kafka
  participant FlinkVal as Flink Validation Job
  participant DB as PostgreSQL
  participant FlinkAnom as Flink Anomaly Job

  Sim->>Prod: Generate heartbeat event
  Prod->>Kafka: Publish to events.raw.v1
  Kafka->>FlinkVal: KafkaSource reads message
  FlinkVal->>FlinkVal: Validate schema + domain bounds
  alt Valid
    FlinkVal->>DB: JDBC sink → heartbeat_events
    FlinkVal->>Kafka: Forward to events.validated.v1
    Kafka->>FlinkAnom: KafkaSource reads validated event
    FlinkAnom->>FlinkAnom: Apply anomaly rules (keyed state)
    opt Anomaly detected
      FlinkAnom->>DB: JDBC sink → anomalies
      FlinkAnom->>Kafka: Publish to events.anomaly.v1
    end
  else Invalid
    FlinkVal->>Kafka: Side output → events.invalid.v1
  else Processing error
    FlinkVal->>Kafka: Side output → events.dlq.v1
  end
  Note over FlinkVal,FlinkAnom: Checkpoint commits offsets + state atomically
```

Export this sequence diagram to PNG or PDF for submission.
