-- Base Data Hub - ClickHouse RT Layer Tables
-- Database: base_data

USE base_data;

-- Real-time CDP Risk Metrics
CREATE TABLE IF NOT EXISTS rt_cdp_risk_metrics (
    chain String,
    protocol String,
    address String,
    health_factor Float64,
    ltv Float64,
    total_collateral_usd Float64,
    total_debt_usd Float64,
    available_borrow_usd Float64,
    risk_level Enum8('safe' = 1, 'warning' = 2, 'critical' = 3, 'liquidatable' = 4),
    safety_margin Float64,
    time_in_risk_seconds UInt64,
    state_changed_at Nullable(DateTime64(3)),
    last_alert_time Nullable(DateTime64(3)),
    updated_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (chain, toYYYYMM(updated_at))
ORDER BY (chain, protocol, address)
SETTINGS index_granularity = 8192;

-- Real-time Token Flow Hourly Aggregation
CREATE TABLE IF NOT EXISTS rt_token_flow_hourly (
    chain String,
    hour DateTime,
    address String,
    inflow_count UInt32,
    inflow_usd Float64,
    outflow_count UInt32,
    outflow_usd Float64,
    net_flow_usd Float64,
    unique_counterparties UInt32,
    top_inflow_token String,
    top_outflow_token String,
    top_counterparty String,
    updated_at DateTime64(3)
)
ENGINE = SummingMergeTree()
PARTITION BY (chain, toYYYYMM(hour))
ORDER BY (chain, hour, address)
SETTINGS index_granularity = 8192;

-- Real-time Address Behavior Features
CREATE TABLE IF NOT EXISTS rt_address_behavior_features (
    chain String,
    address String,
    window_start DateTime,
    window_end DateTime,
    tx_count_1h UInt32,
    tx_count_24h UInt32,
    tx_count_7d UInt32,
    volume_usd_1h Float64,
    volume_usd_24h Float64,
    volume_usd_7d Float64,
    unique_protocols_7d UInt16,
    unique_tokens_7d UInt16,
    avg_tx_size_usd Float64,
    max_tx_size_usd Float64,
    activity_hours Array(UInt8),
    is_likely_bot UInt8,
    is_likely_whale UInt8,
    updated_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (chain, toYYYYMM(window_end))
ORDER BY (chain, address, window_end)
SETTINGS index_granularity = 8192;

-- Real-time Data Quality Metrics
CREATE TABLE IF NOT EXISTS rt_dq_metrics (
    metric_time DateTime,
    topic String,
    partition UInt16,
    message_count_1m UInt32,
    message_rate_per_sec Float64,
    avg_latency_ms Float64,
    p95_latency_ms Float64,
    max_latency_ms Float64,
    null_field_count UInt32,
    schema_error_count UInt32,
    rate_anomaly UInt8,
    latency_anomaly UInt8,
    updated_at DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(metric_time)
ORDER BY (topic, partition, metric_time)
TTL metric_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Real-time Alert Events
CREATE TABLE IF NOT EXISTS rt_alert_events (
    alert_id String,
    chain String,
    protocol String,
    address String,
    alert_type Enum8(
        'HEALTH_FACTOR_WARNING' = 1,
        'HEALTH_FACTOR_CRITICAL' = 2,
        'LIQUIDATION' = 3,
        'LARGE_TRANSFER' = 4,
        'WHALE_MOVEMENT' = 5,
        'SUSPICIOUS_ACTIVITY' = 6
    ),
    severity Enum8('INFO' = 1, 'WARNING' = 2, 'CRITICAL' = 3),
    current_value Float64,
    threshold_value Float64,
    message String,
    details Nullable(String),
    timestamp DateTime64(3),
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY (chain, toYYYYMM(timestamp))
ORDER BY (chain, address, timestamp)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
