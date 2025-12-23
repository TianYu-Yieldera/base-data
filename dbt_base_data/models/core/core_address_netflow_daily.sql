-- models/core/core_address_netflow_daily.sql
{{
  config(
    materialized='incremental',
    unique_key='netflow_key',
    incremental_strategy='delete+insert',
    partition_by='event_date',
    tags=['core', 'netflow']
  )
}}

with transfers as (
    select * from {{ ref('stg_base_transfers') }}
    {% if is_incremental() %}
    where event_date >= (select coalesce(max(event_date), '2024-01-01') from {{ this }})
    {% endif %}
),

-- Inflows per address
inflows as (
    select
        event_date,
        to_address as address,
        token_address,
        token_symbol,
        count(*) as inflow_count,
        sum(amount) as inflow_amount,
        sum(amount_usd) as inflow_usd
    from transfers
    where transfer_type != 'burn'
    group by event_date, to_address, token_address, token_symbol
),

-- Outflows per address
outflows as (
    select
        event_date,
        from_address as address,
        token_address,
        token_symbol,
        count(*) as outflow_count,
        sum(amount) as outflow_amount,
        sum(amount_usd) as outflow_usd
    from transfers
    where transfer_type != 'mint'
    group by event_date, from_address, token_address, token_symbol
),

-- Combine
netflow as (
    select
        coalesce(i.event_date, o.event_date) as event_date,
        coalesce(i.address, o.address) as address,
        coalesce(i.token_address, o.token_address) as token_address,
        coalesce(i.token_symbol, o.token_symbol) as token_symbol,

        coalesce(i.inflow_count, 0) as inflow_count,
        coalesce(i.inflow_amount, 0) as inflow_amount,
        coalesce(i.inflow_usd, 0) as inflow_usd,

        coalesce(o.outflow_count, 0) as outflow_count,
        coalesce(o.outflow_amount, 0) as outflow_amount,
        coalesce(o.outflow_usd, 0) as outflow_usd,

        coalesce(i.inflow_amount, 0) - coalesce(o.outflow_amount, 0) as net_amount,
        coalesce(i.inflow_usd, 0) - coalesce(o.outflow_usd, 0) as net_usd

    from inflows i
    full outer join outflows o
        on i.event_date = o.event_date
        and i.address = o.address
        and i.token_address = o.token_address
)

select
    concat(toString(event_date), '-', address, '-', token_address) as netflow_key,
    'base' as chain,
    *,
    now() as dbt_updated_at
from netflow
where address != '0x0000000000000000000000000000000000000000'
