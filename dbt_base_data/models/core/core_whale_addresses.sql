-- models/core/core_whale_addresses.sql
{{
  config(
    materialized='table',
    tags=['core', 'whales']
  )
}}

with position_whales as (
    -- Whales by collateral
    select distinct
        user_address as address,
        'position_whale' as whale_type,
        total_collateral_usd as whale_value_usd
    from {{ ref('core_address_positions') }}
    where total_collateral_usd >= 1000000
),

transfer_whales as (
    -- Whales by transfer volume (last 30 days)
    select
        address,
        'transfer_whale' as whale_type,
        total_volume_usd as whale_value_usd
    from (
        select
            from_address as address,
            sum(amount_usd) as total_volume_usd
        from {{ ref('stg_base_transfers') }}
        where event_date >= today() - 30
        group by from_address
        having total_volume_usd >= 1000000
    )
),

combined as (
    select * from position_whales
    union all
    select * from transfer_whales
),

-- Aggregate whale types per address
aggregated as (
    select
        address,
        groupArray(whale_type) as whale_types,
        max(whale_value_usd) as max_whale_value_usd,
        count(*) as whale_type_count
    from combined
    group by address
)

select
    address,
    whale_types,
    max_whale_value_usd,
    whale_type_count,
    has(whale_types, 'position_whale') as is_position_whale,
    has(whale_types, 'transfer_whale') as is_transfer_whale,
    now() as dbt_updated_at
from aggregated
