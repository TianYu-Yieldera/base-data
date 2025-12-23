-- models/core/core_token_metrics_daily.sql
{{
  config(
    materialized='incremental',
    unique_key='metrics_key',
    incremental_strategy='delete+insert',
    partition_by='metrics_date',
    order_by=['metrics_date', 'token_address'],
    tags=['core', 'metrics', 'tokens']
  )
}}

with daily_transfers as (
    select
        event_date as metrics_date,
        token_address,
        token_symbol,

        -- Volume metrics
        count(*) as transfer_count,
        count(distinct from_address) as unique_senders,
        count(distinct to_address) as unique_receivers,
        sum(amount) as total_volume,
        sum(amount_usd) as total_volume_usd,
        avg(amount_usd) as avg_transfer_usd,
        max(amount_usd) as max_transfer_usd,

        -- Transfer type breakdown
        countIf(transfer_type = 'mint') as mint_count,
        sumIf(amount, transfer_type = 'mint') as mint_volume,
        countIf(transfer_type = 'burn') as burn_count,
        sumIf(amount, transfer_type = 'burn') as burn_volume,
        countIf(transfer_type = 'transfer') as regular_transfer_count

    from {{ ref('stg_base_transfers') }}
    {% if is_incremental() %}
    where event_date >= (select coalesce(max(metrics_date), '2024-01-01') from {{ this }})
    {% endif %}
    group by event_date, token_address, token_symbol
),

daily_prices as (
    select
        price_date as metrics_date,
        token_address,
        avg(price_usd) as avg_price_usd,
        min(price_usd) as min_price_usd,
        max(price_usd) as max_price_usd,
        argMax(price_usd, price_timestamp) as close_price_usd,
        argMin(price_usd, price_timestamp) as open_price_usd
    from {{ ref('stg_market_prices') }}
    {% if is_incremental() %}
    where price_date >= (select coalesce(max(metrics_date), '2024-01-01') from {{ this }})
    {% endif %}
    group by price_date, token_address
)

select
    concat(toString(t.metrics_date), '-', t.token_address) as metrics_key,
    '{{ var("chain_name") }}' as chain,
    t.metrics_date,
    t.token_address,
    t.token_symbol,

    -- Transfer metrics
    t.transfer_count,
    t.unique_senders,
    t.unique_receivers,
    t.unique_senders + t.unique_receivers as total_active_addresses,
    t.total_volume,
    t.total_volume_usd,
    t.avg_transfer_usd,
    t.max_transfer_usd,

    -- Mint/Burn metrics
    t.mint_count,
    t.mint_volume,
    t.burn_count,
    t.burn_volume,
    t.mint_volume - t.burn_volume as net_supply_change,
    t.regular_transfer_count,

    -- Price metrics
    coalesce(p.avg_price_usd, 0) as avg_price_usd,
    coalesce(p.min_price_usd, 0) as min_price_usd,
    coalesce(p.max_price_usd, 0) as max_price_usd,
    coalesce(p.open_price_usd, 0) as open_price_usd,
    coalesce(p.close_price_usd, 0) as close_price_usd,

    -- Price volatility (high-low range)
    case
        when p.min_price_usd > 0
        then (p.max_price_usd - p.min_price_usd) / p.min_price_usd
        else 0
    end as price_volatility,

    now() as dbt_updated_at

from daily_transfers t
left join daily_prices p
    on t.metrics_date = p.metrics_date
    and t.token_address = p.token_address
