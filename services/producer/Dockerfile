# =============================================================================
# Dockerfile – producer service
# =============================================================================
# Context: build from the project ROOT so that the entire `services/` package
# is available (producer.py imports from services.common.*).
#
#   docker build -f services/producer/Dockerfile -t heartbeat-producer .
# =============================================================================

# Base image
# Slim Debian-based Python image keeps the layer small while including the C
# toolchain needed by confluent-kafka and psycopg[binary].
FROM python:3.11-slim

# Labels
LABEL org.opencontainers.image.title="heartbeat-producer" \
    org.opencontainers.image.description="Kafka producer for synthetic heartbeat events"

# System dependencies
# librdkafka headers are pulled in transitively by confluent-kafka wheel.
# gcc + libpq-dev are required to compile psycopg (binary extras handle this,
# but we keep them for pip's build-isolation fallback path).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Application code
# Copy the entire project so that `services/` is importable as a package.
COPY services/ ./services/

# Runtime
# Prometheus /metrics port (default 8000, configurable via PROMETHEUS_PORT env)
EXPOSE 8000

# Runs the producer as a module so Python resolves `services.*` imports
# relative to WORKDIR (/app).
CMD ["python", "-m", "services.producer.producer"]
