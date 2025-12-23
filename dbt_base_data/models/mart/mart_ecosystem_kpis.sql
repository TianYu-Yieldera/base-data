-- models/mart/mart_ecosystem_kpis.sql
{{
  config(
    materialized='table',
    tags=['mart', 'kpis']
  )
}}

with daily_metrics as (
    select
        event_date,

        -- Volume metrics
        count(distinct from_address) as unique_senders,
        count(distinct to_address) as unique_receivers,
        count(*) as transfer_count,
        sum(amount_usd) as volume_usd,

        -- Token breakdown
        countIf(token_symbol = 'USDC') as usdc_transfers,
        sumIf(amount_usd, token_symbol = 'USDC') as usdc_volume,
        countIf(token_symbol = 'WETH') as weth_transfers,
        sumIf(amount_usd, token_symbol = 'WETH') as weth_volume

    from {{ ref('stg_base_transfers') }}
    where event_date >= today() - 30
    group by event_date
),

position_metrics as (
    select
        count(*) as total_positions,
        count(distinct user_address) as unique_users,
        sum(total_collateral_usd) as total_tvl,
        sum(total_debt_usd) as total_debt,
        avg(health_factor) as avg_health_factor,
        countIf(risk_level in ('CRITICAL', 'LIQUIDATABLE')) as at_risk_positions
    from {{ ref('core_address_positions') }}
),

aave_metrics as (
    select
        event_date,
        event_type,
        count(*) as event_count,
        sum(amount_usd) as volume_usd
    from {{ ref('stg_base_aave_events') }}
    where event_date >= today() - 30
    group by event_date, event_type
)

select
    'ecosystem_summary' as metric_type,
    now() as snapshot_time,

    -- Position metrics
    (select total_positions from position_metrics) as total_positions,
    (select unique_users from position_metrics) as unique_users,
    (select total_tvl from position_metrics) as total_tvl,
    (select total_debt from position_metrics) as total_debt,
    (select avg_health_factor from position_metrics) as avg_health_factor,
    (select at_risk_positions from position_metrics) as at_risk_positions,

    -- 24h metrics
    (select sum(volume_usd) from daily_metrics where event_date = today()) as volume_24h,
    (select sum(transfer_count) from daily_metrics where event_date = today()) as transfers_24h,
    (select unique_senders from daily_metrics where event_date = today()) as active_users_24h
