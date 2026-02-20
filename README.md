# Real-Time Customer Heartbeat Monitoring System

This project simulates customer heart rate streams, ingests them through Kafka, processes them in Python, stores them in PostgreSQL, and visualizes results in Grafana.

## Stack
- Python (`confluent-kafka`, `psycopg`, `pydantic`)
- Apache Kafka + Zookeeper
- PostgreSQL
- Docker Compose
- Grafana + Prometheus

## Project Structure
- `docker/compose`: local infrastructure
- `services/generator`: synthetic event sample generator
- `services/producer`: Kafka producer loop
- `services/consumer`: Kafka consumer + DB writer
- `services/anomaly_detector`: anomaly detection consumer
- `db/schema`: SQL schema initialization
- `tests/unit`, `tests/integration`, `tests/load`: test suites
- `monitoring`: Grafana/Prometheus provisioning
- `docs/architecture`: data-flow diagrams (Mermaid)

## Prerequisites
- Docker Desktop running
- Python 3.11+
- PowerShell (Windows)

## Setup
1. Copy env file:
   - `Copy-Item .env.example .env`
2. Install Python dependencies:
   - `python -m venv .venv`
   - `.\.venv\Scripts\Activate.ps1`
   - `pip install -r requirements.txt`
3. Start infrastructure:
   - `docker compose -f docker/compose/docker-compose.yml up -d`
4. Create Kafka topics:
   - `./scripts/create-topics.ps1`
   - If PowerShell policy blocks scripts, run directly:
     - `docker exec heartbeat-kafka kafka-topics --bootstrap-server localhost:19092 --create --if-not-exists --topic events.raw.v1 --partitions 24 --replication-factor 1`
     - `docker exec heartbeat-kafka kafka-topics --bootstrap-server localhost:19092 --create --if-not-exists --topic events.invalid.v1 --partitions 6 --replication-factor 1`
     - `docker exec heartbeat-kafka kafka-topics --bootstrap-server localhost:19092 --create --if-not-exists --topic events.anomaly.v1 --partitions 6 --replication-factor 1`
     - `docker exec heartbeat-kafka kafka-topics --bootstrap-server localhost:19092 --create --if-not-exists --topic events.dlq.v1 --partitions 6 --replication-factor 1`

## Local Ports
- Kafka bootstrap (host): `localhost:19092`
- PostgreSQL (host): `localhost:55432`
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Kafka UI: `http://localhost:8080`

## Run Pipeline
Open 3 terminals (with `.venv` activated):
1. Producer:
   - `python -m services.producer.producer`
2. Consumer (DB writer):
   - `python -m services.consumer.consumer`
3. Anomaly detector:
   - `python -m services.anomaly_detector.detector`

Optional sample generator output:
- `python -m services.generator.generate`

## Query Data
Use psql (or any SQL client):
- `psql -h localhost -p 55432 -U heartbeat_user -d heartbeat -c "SELECT * FROM heartbeat_events ORDER BY event_time DESC LIMIT 20;"`
- `psql -h localhost -p 55432 -U heartbeat_user -d heartbeat -c "SELECT * FROM anomalies ORDER BY event_time DESC LIMIT 20;"`

## Tests
- Unit: `pytest tests/unit -q`
- Integration: `pytest tests/integration -q`
- Load smoke: `python tests/load/load_smoke.py`

## Grafana
- URL: `http://localhost:3000`
- User/Pass: `admin/admin`
- Dashboard: `Heartbeat Monitoring Overview`

## Required Deliverables Mapping
- Python scripts: `services/*`
- SQL schema: `db/schema/01_schema.sql`
- Docker Compose: `docker/compose/docker-compose.yml`
- Setup guide: this `README.md`
- Data flow diagrams: `docs/architecture/topology.md`, `docs/architecture/sequence-ingest.md`
- Sample outputs/screenshots: add screenshots under `docs/screenshots` after running pipeline
- Optional dashboard: Grafana provisioning in `monitoring/grafana`

## Notes for Windows + OneDrive
See `docs/runbooks/windows-notes.md`.
