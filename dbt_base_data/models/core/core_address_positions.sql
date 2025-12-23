-- models/core/core_address_positions.sql
{{
  config(
    materialized='incremental',
    unique_key='position_key',
    incremental_strategy='delete+insert',
    tags=['core', 'positions']
  )
}}

with latest_positions as (
    select
        chain,
        protocol,
        user_address,
        block_number,
        health_factor,
        total_collateral_usd,
        total_debt_usd,
        available_borrow_usd,
        ltv,
        liquidation_threshold,
        timestamp,
        row_number() over (
            partition by chain, protocol, user_address
            order by block_number desc
        ) as rn
    from {{ source('clickhouse_raw', 'raw_positions') }}
    {% if is_incremental() %}
    where block_number > (select coalesce(max(block_number), 0) from {{ this }})
    {% endif %}
),

latest_risk as (
    select
        chain,
        user_address,
        protocol,
        risk_level,
        previous_risk_level,
        risk_changed_at,
        updated_at
    from {{ source('clickhouse_rt', 'rt_cdp_risk_metrics') }} final
),

enriched as (
    select
        -- Key
        concat(p.chain, '-', p.protocol, '-', p.user_address) as position_key,

        -- Dimensions
        p.chain,
        p.protocol,
        p.user_address,

        -- Current Position
        p.health_factor,
        p.total_collateral_usd,
        p.total_debt_usd,
        p.available_borrow_usd,
        p.ltv,
        p.liquidation_threshold,

        -- Risk Level (from RT)
        coalesce(r.risk_level, {{ calculate_risk_level('p.health_factor') }}) as risk_level,
        r.previous_risk_level,
        r.risk_changed_at,

        -- Size Classification
        case
            when p.total_collateral_usd >= 1000000 then 'whale'
            when p.total_collateral_usd >= 100000 then 'large'
            when p.total_collateral_usd >= 10000 then 'medium'
            else 'small'
        end as position_size_tier,

        -- Utilization
        case
            when p.total_collateral_usd > 0
            then p.total_debt_usd / p.total_collateral_usd
            else 0
        end as utilization_rate,

        -- Metadata
        p.block_number,
        toDateTime64(p.timestamp / 1000, 3) as position_timestamp,
        now() as dbt_updated_at

    from latest_positions p
    left join latest_risk r
        on p.chain = r.chain
        and p.protocol = r.protocol
        and p.user_address = r.user_address
    where p.rn = 1
)

select * from enriched
