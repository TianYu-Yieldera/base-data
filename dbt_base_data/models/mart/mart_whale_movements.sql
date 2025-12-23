-- models/mart/mart_whale_movements.sql
{{
  config(
    materialized='incremental',
    unique_key='movement_id',
    incremental_strategy='append',
    tags=['mart', 'whales']
  )
}}

with whale_addresses as (
    select address from {{ ref('core_whale_addresses') }}
),

-- Use JOIN instead of IN (SELECT...) for better ClickHouse performance
whale_transfers as (
    select
        t.transfer_id,
        t.event_timestamp,
        t.event_date,
        t.token_address,
        t.token_symbol,
        t.from_address,
        t.to_address,
        t.amount,
        t.amount_usd,
        t.tx_hash,
        t.block_number,
        case
            when w_from.address is not null then 'whale_out'
            when w_to.address is not null then 'whale_in'
        end as whale_direction
    from {{ ref('stg_base_transfers') }} t
    left join whale_addresses w_from on t.from_address = w_from.address
    left join whale_addresses w_to on t.to_address = w_to.address
    where (w_from.address is not null or w_to.address is not null)
      and t.amount_usd >= 10000  -- Minimum $10k for whale movement
    {% if is_incremental() %}
      and t.event_timestamp > (select coalesce(max(event_timestamp), toDateTime64('2024-01-01', 3)) from {{ this }})
    {% endif %}
)

select
    transfer_id as movement_id,
    event_timestamp,
    event_date,
    token_address,
    token_symbol,
    from_address,
    to_address,
    amount,
    amount_usd,
    whale_direction,
    tx_hash,
    block_number,
    now() as dbt_updated_at
from whale_transfers
order by event_timestamp desc
