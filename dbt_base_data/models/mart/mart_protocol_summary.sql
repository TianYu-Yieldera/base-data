-- models/mart/mart_protocol_summary.sql
{{
  config(
    materialized='table',
    tags=['mart', 'protocols']
  )
}}

with protocol_positions as (
    select
        protocol,
        count(distinct user_address) as unique_users,
        sum(total_collateral_usd) as total_tvl_usd,
        sum(total_debt_usd) as total_debt_usd,
        avg(health_factor) as avg_health_factor,
        min(health_factor) as min_health_factor,

        -- Risk distribution
        countIf(risk_level = 'SAFE') as safe_positions,
        countIf(risk_level = 'WARNING') as warning_positions,
        countIf(risk_level = 'DANGER') as danger_positions,
        countIf(risk_level = 'CRITICAL') as critical_positions,
        countIf(risk_level = 'LIQUIDATABLE') as liquidatable_positions,

        -- Size distribution
        countIf(position_size_tier = 'whale') as whale_positions,
        countIf(position_size_tier = 'large') as large_positions,
        countIf(position_size_tier = 'medium') as medium_positions,
        countIf(position_size_tier = 'small') as small_positions

    from {{ ref('core_address_positions') }}
    group by protocol
),

protocol_activity_7d as (
    select
        protocol,
        count(*) as events_7d,
        count(distinct user_address) as active_users_7d,
        sum(amount_usd) as volume_7d,

        sumIf(amount_usd, event_type = 'supply') as supply_volume_7d,
        sumIf(amount_usd, event_type = 'borrow') as borrow_volume_7d,
        countIf(event_type = 'liquidation') as liquidations_7d
    from {{ ref('stg_base_aave_events') }}
    where event_date >= today() - 7
    group by protocol
),

protocol_activity_30d as (
    select
        protocol,
        count(*) as events_30d,
        count(distinct user_address) as active_users_30d,
        sum(amount_usd) as volume_30d
    from {{ ref('stg_base_aave_events') }}
    where event_date >= today() - 30
    group by protocol
)

select
    p.protocol,
    '{{ var("chain_name") }}' as chain,

    -- Position metrics
    p.unique_users,
    p.total_tvl_usd,
    p.total_debt_usd,
    case
        when p.total_tvl_usd > 0
        then p.total_debt_usd / p.total_tvl_usd
        else 0
    end as utilization_rate,

    -- Health metrics
    p.avg_health_factor,
    p.min_health_factor,

    -- Risk distribution
    p.safe_positions,
    p.warning_positions,
    p.danger_positions,
    p.critical_positions,
    p.liquidatable_positions,
    p.critical_positions + p.liquidatable_positions as at_risk_positions,

    -- Size distribution
    p.whale_positions,
    p.large_positions,
    p.medium_positions,
    p.small_positions,

    -- 7-day activity
    coalesce(a7.events_7d, 0) as events_7d,
    coalesce(a7.active_users_7d, 0) as active_users_7d,
    coalesce(a7.volume_7d, 0) as volume_7d,
    coalesce(a7.supply_volume_7d, 0) as supply_volume_7d,
    coalesce(a7.borrow_volume_7d, 0) as borrow_volume_7d,
    coalesce(a7.liquidations_7d, 0) as liquidations_7d,

    -- 30-day activity
    coalesce(a30.events_30d, 0) as events_30d,
    coalesce(a30.active_users_30d, 0) as active_users_30d,
    coalesce(a30.volume_30d, 0) as volume_30d,

    now() as snapshot_time

from protocol_positions p
left join protocol_activity_7d a7 on p.protocol = a7.protocol
left join protocol_activity_30d a30 on p.protocol = a30.protocol
