# =============================================================================
# Dockerfile – consumer service
# =============================================================================
# Context: build from the project ROOT so that the entire `services/` package
# is available (consumer.py imports from services.common.*).
#
#   docker build -f services/consumer/Dockerfile -t heartbeat-consumer .
# =============================================================================

# Base image
FROM python:3.11-slim

# Labels
LABEL org.opencontainers.image.title="heartbeat-consumer" \
    org.opencontainers.image.description="Kafka consumer / PostgreSQL DB-writer service"

# System dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

# Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY services/ ./services/

# Runtime
# Consumer Prometheus port is producer_port + 1 (default 8001).
EXPOSE 8001

CMD ["python", "-m", "services.consumer.consumer"]
