# Flink image with PyFlink, connector JARs, and Python dependencies
# =============================================================================
# PyFlink 1.19 requires Python 3.8–3.11 (does NOT support 3.12+).
# We use the official Flink image which ships Java 17 and add Python on top.
# =============================================================================

FROM flink:1.19-java17

# Install Python 3.11 + pip inside the Flink container
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Connector JARs for Kafka, JDBC, and PostgreSQL.
# These are placed in /opt/flink/lib/ so they are on the classpath for all jobs.
# NOTE: We use the *sql-connector* uber JARs which bundle transitive deps
#       (e.g. kafka-clients) so nothing is missing at runtime.
ARG FLINK_SQL_CONNECTOR_KAFKA_VERSION=3.2.0-1.19
ARG FLINK_CONNECTOR_JDBC_VERSION=3.2.0-1.19
ARG PG_JDBC_VERSION=42.7.3

RUN mkdir -p /opt/flink/lib && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_SQL_CONNECTOR_KAFKA_VERSION}/flink-sql-connector-kafka-${FLINK_SQL_CONNECTOR_KAFKA_VERSION}.jar" \
         -o /opt/flink/lib/flink-sql-connector-kafka.jar && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${FLINK_CONNECTOR_JDBC_VERSION}/flink-connector-jdbc-${FLINK_CONNECTOR_JDBC_VERSION}.jar" \
         -o /opt/flink/lib/flink-connector-jdbc.jar && \
    curl -fSL "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
         -o /opt/flink/lib/postgresql-jdbc.jar

# Prometheus metrics reporter for Flink. This allows us to scrape Flink's internal metrics (e.g. throughput, latency, backpressure) with Prometheus and visualize them in Grafana.
# Enables Flink to expose metrics at /metrics on port 9249 for Prometheus scraping.
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.19.1/flink-metrics-prometheus-1.19.1.jar" \
         -o /opt/flink/lib/flink-metrics-prometheus.jar

# Python dependencies for PyFlink UDFs
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir \
    apache-flink==1.19.1 \
    apache-flink-libraries==1.19.1 \
    -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Application code 
# Copy the shared Python modules so Flink jobs can import them
# NOTE: services/ goes to /opt/flink/app/ (NOT /opt/flink/jobs/) because
# docker-compose mounts ./flink/jobs over /opt/flink/jobs at runtime.
COPY services/ /opt/flink/app/services/
COPY flink/jobs/ /opt/flink/jobs/

# Ensure services module is importable by TaskManager Python workers
ENV PYTHONPATH="/opt/flink/app:${PYTHONPATH}"

# Ensure the checkpoints directory exists and is writable by flink user
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints

# Set working directory so relative imports resolve
WORKDIR /opt/flink/jobs

# Drop back to the flink user for security
USER flink
