"""
config.py – Centralised, validated application settings.

Uses pydantic-settings so every environment variable is:
  • Type-coerced (str, int, float, bool)
  • Range-checkable via Field/validator
  • Auto-documented via the field descriptions
  • Clearly reported on misconfiguration instead of crashing deep in the app

Usage
-----
>>> from services.common.config import settings
>>> print(settings.kafka_bootstrap_servers)
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    All runtime configuration is pulled from environment variables (or .env).

    Sections
    --------
    kafka_*         – Kafka broker and topic names / consumer groups
    postgres_*      – PostgreSQL connection parameters
    db_pool_*       – psycopg connection-pool sizing
    sim_*           – Synthetic-data simulation knobs
    heart_rate_*    – Hard physiological bounds for rate validation
    anomaly_*       – Anomaly detection thresholds
    log_level       – Root Python logging level (DEBUG / INFO / WARNING / ERROR)
    prometheus_port – Port on which each service exposes /metrics for Prometheus scraping
    """

    model_config = SettingsConfigDict(
        env_file=".env",           # Load from .env if present
        env_file_encoding="utf-8",
        case_sensitive=False,      # KAFKA_BOOTSTRAP_SERVERS == kafka_bootstrap_servers
        extra="ignore",            # Silently ignore unknown env vars
    )

    # Kafka 
    kafka_bootstrap_servers: str = Field(
        default="localhost:19092",
        description="Comma-separated list of Kafka broker host:port pairs.",
    )
    kafka_topic_raw: str = Field(
        default="events.raw.v1",
        description="Topic where the producer publishes validated heartbeat events.",
    )
    kafka_topic_invalid: str = Field(
        default="events.invalid.v1",
        description="Quarantine topic for messages that fail schema/domain validation.",
    )
    kafka_topic_anomaly: str = Field(
        default="events.anomaly.v1",
        description="Topic where the anomaly detector publishes flagged events.",
    )
    kafka_topic_dlq: str = Field(
        default="events.dlq.v1",
        description="Dead-letter queue for messages that caused unexpected processing errors.",
    )
    kafka_consumer_group_db: str = Field(
        default="cg.db-writer.v1",
        description="Consumer group used by the DB-writer consumer.",
    )
    kafka_consumer_group_anomaly: str = Field(
        default="cg.anomaly.v1",
        description="Consumer group used by the anomaly-detection consumer.",
    )

    # PostgreSQL
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=55432, ge=1, le=65535)
    postgres_db: str = Field(default="heartbeat")
    postgres_user: str = Field(default="heartbeat_user")
    postgres_password: str = Field(default="heartbeat_pass")

    # Connection pool
    db_pool_min: int = Field(
        default=2,
        ge=1,
        description="Minimum number of idle connections kept alive in the pool.",
    )
    db_pool_max: int = Field(
        default=10,
        ge=1,
        description="Maximum number of connections the pool will open simultaneously.",
    )

    # Simulation
    sim_customer_count: int = Field(
        default=1000,
        ge=1,
        description="Number of synthetic customer identities to simulate.",
    )
    sim_events_per_second: int = Field(
        default=200,
        ge=1,
        description="Target ingestion rate in events/s (normal mode).",
    )
    sim_burst_multiplier: int = Field(
        default=4,
        ge=1,
        description="Multiplier applied during burst windows (every 10 s).",
    )
    sim_sleep_seconds: float = Field(
        default=0.2,
        ge=0.0,
        description="Sleep between producer batch iterations (seconds).",
    )
    sim_invalid_ratio: float = Field(
        default=0.02,
        ge=0.0,
        le=1.0,
        description="Fraction of generated events that are deliberately out-of-range.",
    )
    sim_dynamic_customers: bool = Field(
        default=False,
        description="When true, events are emitted from a changing active subset of customers.",
    )
    sim_active_customers_min: int = Field(
        default=200,
        ge=1,
        description="Minimum active customers when dynamic-customer mode is enabled.",
    )
    sim_active_customers_max: int = Field(
        default=1000,
        ge=1,
        description="Maximum active customers when dynamic-customer mode is enabled.",
    )
    sim_active_customers_refresh_probability: float = Field(
        default=0.03,
        ge=0.0,
        le=1.0,
        description="Probability per event to resample the active customer subset in dynamic mode.",
    )

    # Heart-rate domain bounds
    heart_rate_min: int = Field(
        default=45,
        ge=0,
        description="Minimum acceptable heart rate (bpm). Events below this are invalid.",
    )
    heart_rate_max: int = Field(
        default=185,
        ge=1,
        description="Maximum acceptable heart rate (bpm). Events above this are invalid.",
    )

    # Anomaly detection thresholds
    anomaly_low_threshold: int = Field(
        default=50,
        ge=0,
        description="Heart rate at or below this value triggers a LOW_HEART_RATE anomaly.",
    )
    anomaly_high_threshold: int = Field(
        default=140,
        ge=1,
        description="Heart rate at or above this value triggers a HIGH_HEART_RATE anomaly.",
    )
    anomaly_spike_delta: int = Field(
        default=30,
        ge=1,
        description="Absolute bpm change from the customer's last reading that triggers a SPIKE anomaly.",
    )

    # Observability─
    log_level: str = Field(
        default="INFO",
        description="Root Python logging level. One of: DEBUG, INFO, WARNING, ERROR, CRITICAL.",
    )
    prometheus_port: int = Field(
        default=8000,
        ge=1024,
        le=65535,
        description="HTTP port on which each service exposes /metrics for Prometheus scraping.",
    )


# Module-level singleton – import this everywhere instead of instantiating Settings again.
settings = Settings()
