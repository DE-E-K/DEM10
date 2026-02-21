# =============================================================================
# Dockerfile – anomaly_detector service
# =============================================================================
# Context: build from the project ROOT so that the entire `services/` package
# is available (detector.py imports from services.common.* and
# services.anomaly_detector.anomaly_rules).
#
#   docker build -f services/anomaly_detector/Dockerfile -t heartbeat-anomaly-detector .
# =============================================================================

# Base image
FROM python:3.11-slim

# Labels
LABEL org.opencontainers.image.title="heartbeat-anomaly-detector" \
    org.opencontainers.image.description="Rule-based anomaly detection consumer service"

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
# Anomaly detector Prometheus port is producer_port + 2 (default 8002).
EXPOSE 8002

CMD ["python", "-m", "services.anomaly_detector.detector"]
