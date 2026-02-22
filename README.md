# Real-Time Customer Heartbeat Monitoring System

A data pipeline that simulates real-time heart-rate sensor
data, streams it through Apache Kafka, validates it, stores it in PostgreSQL,
detects physiological anomalies, and visualises everything in Grafana.

## Architecture

```mermaid
flowchart LR
  G[Generator] --> P[Kafka Producer]
  P --> K[(Kafka Topic events.raw.v1)]
  K --> C[Consumer + DB Writer]
  K --> A[Anomaly Detector]
  C --> D[(PostgreSQL heartbeat_events)]
  A --> AD[(PostgreSQL anomalies)]
  A --> KA[(Kafka Topic events.anomaly.v1)]
  D --> GR[Grafana]
  AD --> GR
  K --> UI[Kafka UI]
```

## Stack

| Layer | Technology |
|---|---|
| Simulation | Python (Gaussian-baseline simulator) |
| Streaming | Apache Kafka + Zookeeper (Confluent 7.6.1) |
| Processing | Python (confluent-kafka, pydantic) |
| Storage | PostgreSQL 16 |
| Config | pydantic-settings (env-var validated) |
| Metrics | prometheus-client (per-service /metrics endpoints) |
| Dashboards | Grafana 11.4.0 |
| Observability | Kafka UI |
| Testing | pytest |
| Infra | Docker Compose |

## Project Structure

```
DEM10/
├── docker-compose.yml              ← Root-level compose file (use this)
├── .env.example                    ← Copy to .env before running
├── requirements.txt
├── services/
│   ├── common/
│   │   ├── config.py               ← Validated settings (pydantic-settings)
│   │   ├── models.py               ← HeartbeatEvent, AnomalyEvent, InvalidEvent
│   │   ├── simulator.py            ← Physiologically realistic data generator
│   │   ├── kafka_utils.py          ← Hardened producer/consumer factories
│   │   └── db.py                   ← Connection pool + retry logic
│   ├── producer/producer.py        ← Kafka producer (Prometheus + graceful shutdown)
│   ├── consumer/consumer.py        ← DB-writer consumer (Prometheus + graceful shutdown)
│   └── anomaly_detector/
│       ├── anomaly_rules.py        ← Pure, testable rule engine
│       └── detector.py             ← Anomaly consumer (Prometheus + graceful shutdown)
├── db/schema/01_schema.sql         ← Fully-commented PostgreSQL schema
├── tests/
│   ├── unit/
│   │   ├── test_generator.py       ← Simulator unit tests
│   │   ├── test_validation.py      ← Model validation tests
│   │   └── test_anomaly_rules.py   ← Anomaly rule engine tests (new)
│   ├── integration/
│   │   └── test_db_connection.py   ← DB connectivity + schema tests
│   └── load/
│       └── load_smoke.py           ← Throughput smoke test
├── monitoring/
│   ├── grafana/dashboards/         ← 7-panel dashboard (auto-provisioned)
│   └── prometheus/prometheus.yml   ← Scrape config
├── scripts/create-topics.ps1       ← PowerShell topic creation helper
└── docs/architecture/              ← Architecture docs + Mermaid diagrams
```

## Quick Start

### Prerequisites
- Docker Desktop (running)
- Python 3.11+
- PowerShell (Windows)

### 1 – Environment setup

```powershell
# Copy environment template
Copy-Item .env.example .env

# Create and activate virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 2 – Start infrastructure

```powershell
# From the project root (uses the root docker-compose.yml)
docker compose up -d

# Verify all containers are healthy
docker compose ps
```

### 3 – Create Kafka topics

```powershell
./scripts/create-topics.ps1
```

Or manually:
```powershell
docker exec heartbeat-kafka kafka-topics --bootstrap-server localhost:19092 --create --if-not-exists --topic events.raw.v1     --partitions 24 --replication-factor 1
docker exec heartbeat-kafka kafka-topics --bootstrap-server localhost:19092 --create --if-not-exists --topic events.invalid.v1  --partitions 6  --replication-factor 1
docker exec heartbeat-kafka kafka-topics --bootstrap-server localhost:19092 --create --if-not-exists --topic events.anomaly.v1  --partitions 6  --replication-factor 1
docker exec heartbeat-kafka kafka-topics --bootstrap-server localhost:19092 --create --if-not-exists --topic events.dlq.v1      --partitions 6  --replication-factor 1
```

### 4 – Run the pipeline (3 terminals)

```powershell
# Terminal 1 – Producer (publishes synthetic heartbeat events)
python -m services.producer.producer

# Terminal 2 – Consumer DB Writer (validates + persists to PostgreSQL)
python -m services.consumer.consumer

# Terminal 3 – Anomaly Detector (detects and alerts on anomalies)
python -m services.anomaly_detector.detector
```

### 5 – Verify data flowing

```powershell
# Count ingested events
psql -h localhost -p 55432 -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT COUNT(*) FROM heartbeat_events;"

# View recent anomalies
psql -h localhost -p 55432 -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT customer_id, anomaly_type, severity, heart_rate FROM anomalies ORDER BY event_time DESC LIMIT 10;"
```

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Kafka broker endpoint(s) |
| `KAFKA_TOPIC_RAW` | `events.raw.v1` | Raw heartbeat topic |
| `KAFKA_TOPIC_INVALID` | `events.invalid.v1` | Validation-failure quarantine topic |
| `KAFKA_TOPIC_ANOMALY` | `events.anomaly.v1` | Anomaly events topic |
| `KAFKA_TOPIC_DLQ` | `events.dlq.v1` | Dead-letter queue topic |
| `KAFKA_CONSUMER_GROUP_DB` | `cg.db-writer.v1` | Consumer group for DB writer |
| `KAFKA_CONSUMER_GROUP_ANOMALY` | `cg.anomaly.v1` | Consumer group for anomaly detector |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `55432` | PostgreSQL port |
| `POSTGRES_DB` | `heartbeat` | PostgreSQL database name |
| `POSTGRES_USER` | `from .env` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `from .env` | PostgreSQL password |
| `SIM_CUSTOMER_COUNT` | `1000` | Total simulated customer identities |
| `SIM_EVENTS_PER_SECOND` | `200` | Base producer throughput per second |
| `SIM_BURST_MULTIPLIER` | `4` | Burst multiplier every 10-second window |
| `SIM_SLEEP_SECONDS` | `0.2` | Producer loop sleep interval (seconds) |
| `SIM_INVALID_RATIO` | `0.02` | Fraction of deliberately invalid events |
| `SIM_DYNAMIC_CUSTOMERS` | `false` | Enable dynamic active-customer subset |
| `SIM_ACTIVE_CUSTOMERS_MIN` | `200` | Minimum active customers in dynamic mode |
| `SIM_ACTIVE_CUSTOMERS_MAX` | `1000` | Maximum active customers in dynamic mode |
| `SIM_ACTIVE_CUSTOMERS_REFRESH_PROBABILITY` | `0.03` | Probability to refresh active subset per event |
| `HEART_RATE_MIN` | `45` | Domain minimum valid heart rate |
| `HEART_RATE_MAX` | `185` | Domain maximum valid heart rate |
| `ANOMALY_LOW_THRESHOLD` | `50` | Low-rate anomaly threshold |
| `ANOMALY_HIGH_THRESHOLD` | `140` | High-rate anomaly threshold |
| `ANOMALY_SPIKE_DELTA` | `30` | Spike anomaly delta threshold |

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin (default)|
| Prometheus | http://localhost:9090 | – |
| Kafka UI | http://localhost:8080 | – |
| Producer /metrics | http://localhost:8000/metrics | – |
| Consumer /metrics | http://localhost:8001/metrics | – |
| Detector /metrics | http://localhost:8002/metrics | – |

## Tests

```powershell
# Unit tests (no Docker required)
pytest tests/unit -q -v

# Integration tests (requires Docker Compose running)
pytest tests/integration -q -v

# Load smoke test (requires Docker Compose running)
python tests/load/load_smoke.py
```

## Key Prometheus Metrics

| Metric | Service | Description |
|---|---|---|
| `heartbeat_messages_produced_total` | Producer | Messages published to Kafka |
| `heartbeat_produce_errors_total` | Producer | Delivery failures |
| `heartbeat_messages_consumed_total` | Consumer | Messages polled from Kafka |
| `heartbeat_db_inserts_total` | Consumer | Rows written to PostgreSQL |
| `heartbeat_invalid_total` | Consumer | Messages routed to invalid topic |
| `heartbeat_dlq_total` | Consumer | Messages routed to DLQ |
| `heartbeat_anomalies_total{type,severity}` | Detector | Anomalies detected (labelled) |

## Grafana Dashboard Panels
Dashboard preview:

<img width="1918" height="1059" alt="Dashboard Preview" src="https://github.com/user-attachments/assets/26df5d5f-f4ca-4be9-862f-4cded6e95549" />

1. **Heartbeats Ingested per Minute** – ingestion timeseries
2. **Anomalies per Minute** – anomaly rate timeseries
3. **Anomaly Breakdown by Type** – bar chart (LOW / HIGH / SPIKE)
4. **Average Heart Rate (5 min)** – population mean stat
5. **p95 Heart Rate (5 min)** – 95th percentile stat
6. **Active Customers (5 min)** – distinct customer count stat
7. **Recent Anomalies** – live table of last 20 flagged events

## Anomaly Detection Rules

| Rule | Condition | Severity |
|---|---|---|
| `LOW_HEART_RATE` | rate ≤ 50 bpm | high |
| `HIGH_HEART_RATE` | rate ≥ 140 bpm | high |
| `SPIKE` | \|rate − prev\| ≥ 30 bpm | medium |

All thresholds are configurable via environment variables (`ANOMALY_LOW_THRESHOLD`, `ANOMALY_HIGH_THRESHOLD`, `ANOMALY_SPIKE_DELTA`).

Simulation customer behavior is also configurable via `.env`: `SIM_DYNAMIC_CUSTOMERS`, `SIM_ACTIVE_CUSTOMERS_MIN`, `SIM_ACTIVE_CUSTOMERS_MAX`, and `SIM_ACTIVE_CUSTOMERS_REFRESH_PROBABILITY`.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `dependency zookeeper failed to start` (or Zookeeper = `unhealthy`) | Healthcheck depends on `ruok` 4LW command, which may be unavailable/blocked in this image even when Zookeeper is running | Use TCP healthcheck `nc -z 127.0.0.1 2181` in Compose, then run `docker compose up -d --force-recreate zookeeper kafka` |
| `Connection refused localhost:19092` | Kafka not ready | Wait for `docker compose ps` to show Kafka as healthy |
| `Connection refused localhost:55432` | PostgreSQL not ready | Wait for `pg_isready` healthcheck to pass |
| `ModuleNotFoundError: services` | Wrong working directory | Run commands from the project root |
| `NoBrokersAvailable` | Wrong bootstrap server | Check `KAFKA_BOOTSTRAP_SERVERS` in your `.env` |
| Consumer not writing rows | Topics not created | Run `./scripts/create-topics.ps1` |
| Grafana panels show "No data" | DB not connected | Add PostgreSQL datasource in Grafana → Settings → Data Sources |

## Architecture Documentation

- Full architecture: [docs/architecture/architecture-overview.md](docs/architecture/architecture-overview.md)
- Data-flow topology: [docs/architecture/topology.md](docs/architecture/topology.md)
- Sequence diagram: [docs/architecture/sequence-ingest.md](docs/architecture/sequence-ingest.md)

## Runbooks
- Windows notes: [docs/runbooks/windows-notes.md](docs/runbooks/windows-notes.md)

## Deliverables Mapping

| Deliverable | Location |
|---|---|
| Python scripts | `services/` |
| SQL schema | `db/schema/01_schema.sql` |
| Docker Compose | `docker-compose.yml` (root) |
| Setup guide | This README |
| Data-flow diagrams | `docs/architecture/` |
| Sample outputs | `docs/screenshots/` (add after running) |
| Dashboard | Grafana at http://localhost:3000 |
