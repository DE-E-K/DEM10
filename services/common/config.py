import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic_raw: str = os.getenv("KAFKA_TOPIC_RAW", "events.raw.v1")
    kafka_topic_invalid: str = os.getenv("KAFKA_TOPIC_INVALID", "events.invalid.v1")
    kafka_topic_anomaly: str = os.getenv("KAFKA_TOPIC_ANOMALY", "events.anomaly.v1")
    kafka_topic_dlq: str = os.getenv("KAFKA_TOPIC_DLQ", "events.dlq.v1")
    kafka_consumer_group_db: str = os.getenv("KAFKA_CONSUMER_GROUP_DB", "cg.db-writer.v1")
    kafka_consumer_group_anomaly: str = os.getenv("KAFKA_CONSUMER_GROUP_ANOMALY", "cg.anomaly.v1")

    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "heartbeat")
    postgres_user: str = os.getenv("POSTGRES_USER", "heartbeat_user")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "heartbeat_pass")

    sim_customer_count: int = int(os.getenv("SIM_CUSTOMER_COUNT", "1000"))
    sim_events_per_second: int = int(os.getenv("SIM_EVENTS_PER_SECOND", "200"))
    sim_burst_multiplier: int = int(os.getenv("SIM_BURST_MULTIPLIER", "4"))
    sim_sleep_seconds: float = float(os.getenv("SIM_SLEEP_SECONDS", "0.2"))
    sim_invalid_ratio: float = float(os.getenv("SIM_INVALID_RATIO", "0.02"))

    heart_rate_min: int = int(os.getenv("HEART_RATE_MIN", "45"))
    heart_rate_max: int = int(os.getenv("HEART_RATE_MAX", "185"))

    anomaly_low_threshold: int = int(os.getenv("ANOMALY_LOW_THRESHOLD", "50"))
    anomaly_high_threshold: int = int(os.getenv("ANOMALY_HIGH_THRESHOLD", "140"))
    anomaly_spike_delta: int = int(os.getenv("ANOMALY_SPIKE_DELTA", "30"))


settings = Settings()
