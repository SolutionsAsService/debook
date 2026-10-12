CREATE DATABASE IF NOT EXISTS debook;

CREATE TABLE IF NOT EXISTS debook.event_log
(
    event_id UUID, event_type LowCardinality(String), schema_version UInt16,
    aggregate_id UUID, occurred_at DateTime64(3, 'UTC'),
    correlation_id Nullable(String), causation_id Nullable(String),
    producer LowCardinality(String), payload String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree PARTITION BY toYYYYMM(occurred_at)
ORDER BY (event_type, occurred_at, event_id);

CREATE TABLE IF NOT EXISTS debook.booking_facts
(
    event_id UUID, booking_id UUID, slot_id UUID, tenant_id String,
    customer_ref String, wallet_address String, event_type LowCardinality(String),
    booking_status LowCardinality(String), region_id String, resource_id String,
    occurred_at DateTime64(3, 'UTC'), expires_at Nullable(DateTime64(3, 'UTC')),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree
ORDER BY (tenant_id, occurred_at, booking_id, event_id);
