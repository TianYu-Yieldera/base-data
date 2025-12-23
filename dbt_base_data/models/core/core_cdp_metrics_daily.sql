-- models/core/core_cdp_metrics_daily.sql
{{
  config(
    materialized='incremental',
    unique_key='metrics_key',
    incremental_strategy='delete+insert',
    partition_by='metrics_date',
    order_by=['metrics_date', 'protocol'],
    tags=['core', 'metrics']
  )
}}

with daily_events as (
    select
        event_date as metrics_date,
        protocol,
        event_type,
        count(*) as event_count,
        count(distinct user_address) as unique_users,
        sum(amount_usd) as volume_usd,
        avg(amount_usd) as avg_amount_usd
    from {{ ref('stg_base_aave_events') }}
    {% if is_incremental() %}
    where event_date >= (select coalesce(max(metrics_date), '2024-01-01') from {{ this }})
    {% endif %}
    group by event_date, protocol, event_type
),

daily_positions as (
    select
        position_date as metrics_date,
        protocol,
        count(distinct user_address) as total_users,
        sum(total_collateral_usd) as total_tvl_usd,
        sum(total_debt_usd) as total_debt_usd,
        avg(health_factor) as avg_health_factor,
        min(health_factor) as min_health_factor,
        countIf(health_factor < {{ var('health_factor_critical') }}) as critical_positions,
        countIf(health_factor < 1.0) as liquidatable_positions
    from {{ ref('stg_base_positions') }}
    {% if is_incremental() %}
    where position_date >= (select coalesce(max(metrics_date), '2024-01-01') from {{ this }})
    {% endif %}
    group by position_date, protocol
),

-- Pivot event types
event_pivot as (
    select
        metrics_date,
        protocol,
        sumIf(event_count, event_type = 'supply') as supply_count,
        sumIf(volume_usd, event_type = 'supply') as supply_volume_usd,
        sumIf(event_count, event_type = 'withdraw') as withdraw_count,
        sumIf(volume_usd, event_type = 'withdraw') as withdraw_volume_usd,
        sumIf(event_count, event_type = 'borrow') as borrow_count,
        sumIf(volume_usd, event_type = 'borrow') as borrow_volume_usd,
        sumIf(event_count, event_type = 'repay') as repay_count,
        sumIf(volume_usd, event_type = 'repay') as repay_volume_usd,
        sumIf(event_count, event_type = 'liquidation') as liquidation_count,
        sumIf(volume_usd, event_type = 'liquidation') as liquidation_volume_usd,
        sum(unique_users) as total_active_users
    from daily_events
    group by metrics_date, protocol
)

select
    concat(toString(p.metrics_date), '-', p.protocol) as metrics_key,
    '{{ var("chain_name") }}' as chain,
    p.metrics_date,
    p.protocol,

    -- Position metrics
    p.total_users,
    p.total_tvl_usd,
    p.total_debt_usd,
    p.avg_health_factor,
    p.min_health_factor,
    p.critical_positions,
    p.liquidatable_positions,

    -- Event metrics
    coalesce(e.supply_count, 0) as supply_count,
    coalesce(e.supply_volume_usd, 0) as supply_volume_usd,
    coalesce(e.withdraw_count, 0) as withdraw_count,
    coalesce(e.withdraw_volume_usd, 0) as withdraw_volume_usd,
    coalesce(e.borrow_count, 0) as borrow_count,
    coalesce(e.borrow_volume_usd, 0) as borrow_volume_usd,
    coalesce(e.repay_count, 0) as repay_count,
    coalesce(e.repay_volume_usd, 0) as repay_volume_usd,
    coalesce(e.liquidation_count, 0) as liquidation_count,
    coalesce(e.liquidation_volume_usd, 0) as liquidation_volume_usd,

    -- Derived metrics
    coalesce(e.supply_volume_usd, 0) - coalesce(e.withdraw_volume_usd, 0) as net_supply_usd,
    coalesce(e.borrow_volume_usd, 0) - coalesce(e.repay_volume_usd, 0) as net_borrow_usd,

    -- Utilization rate
    case
        when p.total_tvl_usd > 0
        then p.total_debt_usd / p.total_tvl_usd
        else 0
    end as utilization_rate,

    now() as dbt_updated_at

from daily_positions p
left join event_pivot e
    on p.metrics_date = e.metrics_date
    and p.protocol = e.protocol
