-- models/mart/mart_risk_dashboard.sql
{{
  config(
    materialized='table',
    tags=['mart', 'dashboard']
  )
}}

with positions as (
    select * from {{ ref('core_address_positions') }}
),

risk_distribution as (
    select
        risk_level,
        count(*) as user_count,
        sum(total_collateral_usd) as total_collateral_usd,
        sum(total_debt_usd) as total_debt_usd,
        avg(health_factor) as avg_health_factor,
        min(health_factor) as min_health_factor
    from positions
    group by risk_level
),

size_distribution as (
    select
        position_size_tier,
        count(*) as user_count,
        sum(total_collateral_usd) as total_collateral_usd,
        avg(health_factor) as avg_health_factor
    from positions
    group by position_size_tier
),

summary as (
    select
        count(*) as total_users,
        sum(total_collateral_usd) as total_tvl_usd,
        sum(total_debt_usd) as total_debt_usd,
        avg(health_factor) as avg_health_factor,
        countIf(risk_level = 'SAFE') as safe_users,
        countIf(risk_level = 'WARNING') as warning_users,
        countIf(risk_level = 'DANGER') as danger_users,
        countIf(risk_level = 'CRITICAL') as critical_users,
        countIf(risk_level = 'LIQUIDATABLE') as liquidatable_users
    from positions
)

-- Output summary section
select
    'summary' as section,
    'total_users' as metric_name,
    toString(total_users) as metric_value,
    now() as snapshot_time
from summary

union all

select
    'summary' as section,
    'total_tvl_usd' as metric_name,
    toString(total_tvl_usd) as metric_value,
    now() as snapshot_time
from summary

union all

select
    'summary' as section,
    'total_debt_usd' as metric_name,
    toString(total_debt_usd) as metric_value,
    now() as snapshot_time
from summary

union all

select
    'summary' as section,
    'avg_health_factor' as metric_name,
    toString(avg_health_factor) as metric_value,
    now() as snapshot_time
from summary

union all

select
    'risk_distribution' as section,
    risk_level as metric_name,
    toString(user_count) as metric_value,
    now() as snapshot_time
from risk_distribution

union all

select
    'size_distribution' as section,
    position_size_tier as metric_name,
    toString(user_count) as metric_value,
    now() as snapshot_time
from size_distribution
