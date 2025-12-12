-- Phase 2: Stream Processing & Storage Schema Updates
-- Database: base_data

USE base_data;

-- ============================================================================
-- Update rt_cdp_risk_metrics to match Phase 2 design
-- ============================================================================

-- Drop and recreate with proper schema
DROP TABLE IF EXISTS rt_cdp_risk_metrics_new;

CREATE TABLE rt_cdp_risk_metrics_new (
    -- Keys
    chain               LowCardinality(String),
    user_address        FixedString(42),
    protocol            LowCardinality(String),

    -- Risk Metrics
    health_factor       Float64,
    risk_level          Enum8('SAFE'=1, 'WARNING'=2, 'DANGER'=3, 'CRITICAL'=4, 'LIQUIDATABLE'=5),
    previous_risk_level Enum8('SAFE'=1, 'WARNING'=2, 'DANGER'=3, 'CRITICAL'=4, 'LIQUIDATABLE'=5),
    risk_changed_at     DateTime64(3),

    -- Position Data
    total_collateral_usd Decimal128(8),
    total_debt_usd       Decimal128(8),
    available_borrow_usd Decimal128(8),
    ltv                  Float64,
    liquidation_threshold Float64,

    -- Metadata
    block_number        Int64,
    updated_at          DateTime64(3),

    -- Version for ReplacingMergeTree
    version             UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(updated_at)
ORDER BY (chain, protocol, user_address)
SETTINGS index_granularity = 8192;

-- Secondary index for risk level queries
ALTER TABLE rt_cdp_risk_metrics_new ADD INDEX idx_risk_level risk_level TYPE set(5) GRANULARITY 4;

-- Rename tables (swap)
RENAME TABLE rt_cdp_risk_metrics TO rt_cdp_risk_metrics_old,
             rt_cdp_risk_metrics_new TO rt_cdp_risk_metrics;

-- ============================================================================
-- Update rt_token_flow_hourly to match Phase 2 design
-- Using simple UInt64 for unique counts (approximate, computed by Flink)
-- ============================================================================

DROP TABLE IF EXISTS rt_token_flow_hourly_new;

CREATE TABLE rt_token_flow_hourly_new (
    -- Keys
    chain               LowCardinality(String),
    token_address       FixedString(42),
    hour                DateTime,

    -- Aggregated Metrics
    transfer_count      UInt64,
    unique_senders_count UInt64,      -- Approximate count from Flink (hash-based)
    unique_receivers_count UInt64,    -- Approximate count from Flink (hash-based)
    total_volume        Decimal128(0),

    -- Pre-computed for query convenience
    avg_transfer_size   Float64,
    max_transfer_size   Decimal128(0),

    -- Metadata
    updated_at          DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(hour)
ORDER BY (chain, token_address, hour)
SETTINGS index_granularity = 8192;

RENAME TABLE rt_token_flow_hourly TO rt_token_flow_hourly_old,
             rt_token_flow_hourly_new TO rt_token_flow_hourly;

-- ============================================================================
-- Update rt_dq_metrics to match Phase 2 design
-- ============================================================================

DROP TABLE IF EXISTS rt_dq_metrics_new;

CREATE TABLE rt_dq_metrics_new (
    -- Keys
    chain               LowCardinality(String),
    topic               LowCardinality(String),
    check_time          DateTime64(3),

    -- Volume Metrics
    message_count       UInt64,
    message_rate_per_sec Float64,

    -- Quality Metrics
    null_count          UInt64,
    schema_error_count  UInt64,
    duplicate_count     UInt64,
    late_arrival_count  UInt64,

    -- Latency Metrics
    avg_latency_ms      Float64,
    p95_latency_ms      Float64,
    p99_latency_ms      Float64,
    max_latency_ms      Float64,

    -- Anomaly Flags
    is_rate_anomaly     UInt8,
    is_latency_anomaly  UInt8,

    -- Metadata
    window_start        DateTime64(3),
    window_end          DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(check_time)
ORDER BY (chain, topic, check_time)
TTL check_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

RENAME TABLE rt_dq_metrics TO rt_dq_metrics_old,
             rt_dq_metrics_new TO rt_dq_metrics;

-- ============================================================================
-- Add raw_positions table for Kafka Connect sink
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw_positions (
    -- Keys
    chain               LowCardinality(String),
    protocol            LowCardinality(String),
    user_address        FixedString(42),
    block_number        Int64,

    -- Position Data
    total_collateral_eth    Decimal128(18),
    total_debt_eth          Decimal128(18),
    available_borrow_eth    Decimal128(18),
    current_liquidation_threshold Decimal64(4),
    ltv                     Decimal64(4),
    health_factor           Float64,

    -- Collateral Breakdown (JSON for flexibility)
    collateral_assets   String,
    debt_assets         String,

    -- USD Values (computed at indexer time)
    total_collateral_usd    Decimal128(8),
    total_debt_usd          Decimal128(8),
    available_borrow_usd    Decimal128(8),
    net_worth_usd           Decimal128(8),

    -- Metadata
    timestamp           DateTime64(3),
    kafka_offset        Int64,
    kafka_partition     Int32,

    -- Deduplication
    _dedup_key          String DEFAULT concat(chain, '-', protocol, '-', user_address, '-', toString(block_number))
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (chain, protocol, user_address, block_number)
SETTINGS index_granularity = 8192;

-- Index for cold start bootstrap query
ALTER TABLE raw_positions ADD INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 4;

-- ============================================================================
-- Add alert events table for Phase 2
-- ============================================================================

DROP TABLE IF EXISTS rt_risk_alerts;

CREATE TABLE rt_risk_alerts (
    -- Keys
    alert_id            String,
    alert_type          Enum8('RISK_INCREASED'=1, 'RISK_DECREASED'=2, 'LIQUIDATION_RISK'=3, 'RAPID_HF_DECLINE'=4),
    severity            Enum8('INFO'=1, 'WARNING'=2, 'HIGH'=3, 'CRITICAL'=4),

    -- Target
    chain               LowCardinality(String),
    protocol            LowCardinality(String),
    user_address        FixedString(42),

    -- Details
    previous_risk_level LowCardinality(String),
    current_risk_level  LowCardinality(String),
    previous_health_factor Float64,
    current_health_factor Float64,
    health_factor_change_pct Float64,
    total_collateral_usd Decimal128(8),
    total_debt_usd      Decimal128(8),

    -- Metadata
    block_number        Int64,
    event_time          DateTime64(3),
    processing_time     DateTime64(3),
    flink_job_id        String,

    -- Inserted timestamp
    inserted_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (chain, user_address, event_time)
TTL event_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

ALTER TABLE rt_risk_alerts ADD INDEX idx_alert_type alert_type TYPE set(4) GRANULARITY 4;
ALTER TABLE rt_risk_alerts ADD INDEX idx_severity severity TYPE set(4) GRANULARITY 4;

-- ============================================================================
-- Cleanup old tables (optional - comment out if you want to keep backups)
-- ============================================================================

-- DROP TABLE IF EXISTS rt_cdp_risk_metrics_old;
-- DROP TABLE IF EXISTS rt_token_flow_hourly_old;
-- DROP TABLE IF EXISTS rt_dq_metrics_old;
