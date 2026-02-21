/* =============================================================================
01_schema.sql  –  PostgreSQL schema for the Real-Time Heartbeat Pipeline
=============================================================================
This file is auto-executed by the PostgreSQL Docker container at first start
(mounted into /docker-entrypoint-initdb.d/).  It is idempotent: all
statements use  CREATE TABLE IF NOT EXISTS  and  CREATE INDEX IF NOT EXISTS
so running it a second time is safe.
Tables
------
  heartbeat_events  : Every valid heart-rate reading that passes validation.
  anomalies         : Anomalous readings flagged by the detection service.
  ingest_checkpoint : Application-level Kafka offset store per consumer group.
Indexing strategy
-----------------
All time-series dashboards filter on event_time in the last N minutes, so
descending time indexes are placed on every table.  Customer-scoped queries
(e.g. "recent events for cust_00042") benefit from the composite
(customer_id, event_time DESC) indexes.
=============================================================================


=============================================================================
heartbeat_events
Stores one row per validated heart-rate reading ingested from Kafka.
===========================================================================*/
CREATE TABLE IF NOT EXISTS heartbeat_events (
    -- Surrogate primary key (auto-incremented) for internal row addressing.
    id               BIGSERIAL        PRIMARY KEY,

    -- UUID of the originating HeartbeatEvent.  Combined with customer_id,
    -- forms the unique idempotency key that prevents duplicate inserts when
    -- the consumer replays messages after a restart.
    event_id         UUID             NOT NULL,

    -- Customer identifier in the form 'cust_NNNNN'.
    customer_id      VARCHAR(64)      NOT NULL,

    -- UTC wall-clock time the reading was captured by the sensor simulator.
    event_time       TIMESTAMPTZ      NOT NULL,

    -- UTC wall-clock time the row was inserted into PostgreSQL.
    -- Useful for measuring end-to-end pipeline latency.
    ingest_time      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),

    -- Heart rate in beats per minute (bpm).  Stored as SMALLINT (2 bytes)
    -- since valid rates are always in [0, 250].
    heart_rate       SMALLINT         NOT NULL,

    -- Data quality label.  'valid' = passed schema + domain bounds checks.
    -- Future values might include 'repaired' if we add imputation logic.
    quality_flag     VARCHAR(16)      NOT NULL DEFAULT 'valid',

    -- Kafka provenance: which topic / partition / offset produced this row.
    -- Allows any row to be traced back to its exact Kafka message for auditing
    -- and manual replay.
    source_topic     VARCHAR(128)     NOT NULL,
    source_partition INT              NOT NULL,
    source_offset    BIGINT           NOT NULL,

    -- Full JSON copy of the original event payload for schema-evolution safety.
    -- If we add new fields to HeartbeatEvent later, old rows still have the
    -- original values accessible without a schema migration.
    payload          JSONB            NOT NULL,

    -- Idempotency guard: the same (customer, event) pair can only exist once.
    -- ON CONFLICT DO NOTHING in insert_heartbeat() relies on this constraint.
    UNIQUE (customer_id, event_id)
);

-- Customer + time index: powers "last N readings for customer X" queries.
CREATE INDEX IF NOT EXISTS idx_heartbeat_customer_time
    ON heartbeat_events (customer_id, event_time DESC);

-- Global time index: powers "all events in the last hour" dashboard queries.
CREATE INDEX IF NOT EXISTS idx_heartbeat_event_time
    ON heartbeat_events (event_time DESC);

-- Partial index on non-valid events: used by audit / quality-monitoring queries
-- without scanning the (large) set of valid rows.
CREATE INDEX IF NOT EXISTS idx_heartbeat_quality_flag
    ON heartbeat_events (quality_flag, event_time DESC)
    WHERE quality_flag != 'valid';


-- =============================================================================
-- anomalies
-- Stores one row per anomaly detected by the anomaly_detector service.
-- A single heartbeat event can theoretically produce multiple anomaly rows
-- (e.g. if we add compound rules), though the current rule engine emits at most
-- one anomaly per event.
-- =============================================================================
CREATE TABLE IF NOT EXISTS anomalies (
    -- Surrogate primary key.
    id           BIGSERIAL    PRIMARY KEY,

    -- UUID of the originating HeartbeatEvent (NOT unique — see note above).
    event_id     UUID         NOT NULL,

    -- Customer the anomaly belongs to.
    customer_id  VARCHAR(64)  NOT NULL,

    -- UTC time of the original reading that triggered the anomaly.
    event_time   TIMESTAMPTZ  NOT NULL,

    -- Heart rate (bpm) at the time of the anomaly.
    heart_rate   SMALLINT     NOT NULL,

    -- Anomaly classification.  One of:
    --   LOW_HEART_RATE  : rate ≤ anomaly_low_threshold
    --   HIGH_HEART_RATE : rate ≥ anomaly_high_threshold
    --   SPIKE           : |rate - previous_rate| ≥ anomaly_spike_delta
    anomaly_type VARCHAR(32)  NOT NULL,

    -- Severity tier.  'high' for absolute threshold breaches,
    -- 'medium' for relative spikes.
    severity     VARCHAR(16)  NOT NULL,

    -- Free-form context dict.  For LOW/HIGH: {"threshold": N, "measured": M}.
    -- For SPIKE: {"delta": D, "threshold": T, "previous": P, "measured": M}.
    details      JSONB        NOT NULL,

    -- Row creation timestamp (different from event_time for observability).
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Global time index: powers "anomalies in the last hour" dashboard queries.
CREATE INDEX IF NOT EXISTS idx_anomalies_time
    ON anomalies (event_time DESC);

-- Customer + time index: powers "anomalies for customer X" queries.
CREATE INDEX IF NOT EXISTS idx_anomalies_customer_time
    ON anomalies (customer_id, event_time DESC);

-- Type + time index: powers "breakdown by anomaly type" bar-chart queries.
-- e.g. SELECT anomaly_type, COUNT(*) FROM anomalies WHERE event_time > now() - '1h' GROUP BY 1
CREATE INDEX IF NOT EXISTS idx_anomalies_type_time
    ON anomalies (anomaly_type, event_time DESC);


-- =============================================================================
-- ingest_checkpoint
-- Application-level Kafka offset store.
-- Complements Kafka's built-in consumer group offset commits.
-- Enables manual replay: if Kafka's offset store is reset or unavailable,
-- we can recover the last known good position from this table.
-- =============================================================================
CREATE TABLE IF NOT EXISTS ingest_checkpoint (
    -- Consumer group identifier (e.g. 'cg.db-writer.v1').
    consumer_group  VARCHAR(128)  NOT NULL,

    -- Kafka topic name (e.g. 'events.raw.v1').
    topic           VARCHAR(128)  NOT NULL,

    -- Kafka partition number (0-based).
    partition       INT           NOT NULL,

    -- Offset of the last successfully committed message for this
    -- (consumer_group, topic, partition) triple.
    last_offset     BIGINT        NOT NULL,

    -- Timestamp of the last update (set by the ON CONFLICT DO UPDATE clause).
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    -- Each (group, topic, partition) tuple is unique — upserts use this key.
    PRIMARY KEY (consumer_group, topic, partition)
);
