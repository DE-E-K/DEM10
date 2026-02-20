CREATE TABLE IF NOT EXISTS heartbeat_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    heart_rate SMALLINT NOT NULL,
    quality_flag VARCHAR(16) NOT NULL DEFAULT 'valid',
    source_topic VARCHAR(128) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    payload JSONB NOT NULL,
    UNIQUE (customer_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_heartbeat_customer_time ON heartbeat_events (customer_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_heartbeat_event_time ON heartbeat_events (event_time DESC);

CREATE TABLE IF NOT EXISTS anomalies (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    heart_rate SMALLINT NOT NULL,
    anomaly_type VARCHAR(32) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    details JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_time ON anomalies (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_customer_time ON anomalies (customer_id, event_time DESC);

CREATE TABLE IF NOT EXISTS ingest_checkpoint (
    consumer_group VARCHAR(128) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    partition INT NOT NULL,
    last_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic, partition)
);
