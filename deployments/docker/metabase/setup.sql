-- Metabase Dashboard SQL Queries
-- These queries should be used to create Questions in Metabase

-- ============================================
-- RISK DASHBOARD QUERIES
-- ============================================

-- Query 1: Risk Distribution Summary
SELECT
    risk_level,
    count(*) as user_count,
    sum(total_collateral_usd) as total_collateral,
    sum(total_debt_usd) as total_debt,
    avg(health_factor) as avg_health_factor
FROM core_address_positions FINAL
GROUP BY risk_level
ORDER BY
    CASE risk_level
        WHEN 'SAFE' THEN 1
        WHEN 'WARNING' THEN 2
        WHEN 'DANGER' THEN 3
        WHEN 'CRITICAL' THEN 4
        WHEN 'LIQUIDATABLE' THEN 5
    END;

-- Query 2: High Risk Positions
SELECT
    user_address,
    health_factor,
    risk_level,
    total_collateral_usd,
    total_debt_usd,
    utilization_rate,
    position_timestamp
FROM core_address_positions FINAL
WHERE risk_level IN ('CRITICAL', 'LIQUIDATABLE', 'DANGER')
  AND total_collateral_usd >= 1000
ORDER BY health_factor ASC
LIMIT 100;

-- Query 3: Health Factor Trend (7 days)
SELECT
    metrics_date,
    protocol,
    avg_health_factor,
    min_health_factor,
    critical_positions,
    liquidatable_positions
FROM core_cdp_metrics_daily
WHERE metrics_date >= today() - INTERVAL 7 DAY
ORDER BY metrics_date ASC;

-- ============================================
-- FLOW DASHBOARD QUERIES
-- ============================================

-- Query 4: Hourly Volume (last 24 hours)
SELECT
    toStartOfHour(event_timestamp) as hour,
    sum(amount_usd) as volume_usd,
    count(*) as transfer_count,
    count(distinct from_address) + count(distinct to_address) as unique_addresses
FROM stg_base_transfers
WHERE event_timestamp >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour ASC;

-- Query 5: Token Distribution
SELECT
    token_symbol,
    sum(amount_usd) as total_volume,
    count(*) as transfer_count,
    count(distinct from_address) as unique_senders
FROM stg_base_transfers
WHERE event_timestamp >= now() - INTERVAL 24 HOUR
GROUP BY token_symbol
ORDER BY total_volume DESC
LIMIT 10;

-- Query 6: Whale Movements
SELECT
    event_timestamp,
    from_address,
    to_address,
    token_symbol,
    amount,
    amount_usd,
    whale_direction,
    tx_hash
FROM mart_whale_movements
WHERE event_timestamp >= now() - INTERVAL 24 HOUR
  AND amount_usd >= 100000
ORDER BY event_timestamp DESC
LIMIT 100;

-- ============================================
-- ECOSYSTEM DASHBOARD QUERIES
-- ============================================

-- Query 7: Ecosystem KPIs
SELECT
    total_positions,
    unique_users,
    total_tvl,
    total_debt,
    avg_health_factor,
    at_risk_positions,
    volume_24h,
    transfers_24h,
    active_users_24h
FROM mart_ecosystem_kpis
WHERE metric_type = 'ecosystem_summary'
ORDER BY snapshot_time DESC
LIMIT 1;

-- Query 8: Protocol Comparison
SELECT
    protocol,
    unique_users,
    total_tvl_usd,
    total_debt_usd,
    utilization_rate,
    avg_health_factor,
    at_risk_positions
FROM mart_protocol_summary
ORDER BY total_tvl_usd DESC;

-- Query 9: TVL Trend (30 days)
SELECT
    metrics_date,
    sum(total_tvl_usd) as total_tvl,
    sum(total_debt_usd) as total_debt,
    sum(total_users) as total_users
FROM core_cdp_metrics_daily
WHERE metrics_date >= today() - INTERVAL 30 DAY
GROUP BY metrics_date
ORDER BY metrics_date ASC;
