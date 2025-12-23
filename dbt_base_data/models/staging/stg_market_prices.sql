-- models/staging/stg_market_prices.sql
{{
  config(
    materialized='view',
    tags=['staging', 'prices']
  )
}}

with source as (
    select * from {{ source('clickhouse_raw', 'raw_prices') }}
),

cleaned as (
    select
        -- Primary Key
        concat(chain, '-', token_address, '-', toString(timestamp)) as price_id,

        -- Dimensions
        lower(coalesce(chain, 'base')) as chain,
        lower(token_address) as token_address,
        upper(coalesce(token_symbol, 'UNKNOWN')) as token_symbol,

        -- Price data
        toFloat64(price_usd) as price_usd,
        toFloat64(coalesce(price_eth, 0)) as price_eth,

        -- Market data
        toDecimal128(coalesce(market_cap_usd, 0), 2) as market_cap_usd,
        toDecimal128(coalesce(volume_24h_usd, 0), 2) as volume_24h_usd,
        toFloat64(coalesce(price_change_24h_pct, 0)) as price_change_24h_pct,

        -- Source info
        coalesce(price_source, 'unknown') as price_source,

        -- Timestamps
        toDateTime64(timestamp / 1000, 3) as price_timestamp,
        toDate(timestamp / 1000) as price_date,
        toStartOfHour(toDateTime(timestamp / 1000)) as price_hour,

        -- Raw timestamp
        timestamp as timestamp_ms

    from source
    where price_usd > 0
      and token_address != ''
)

select * from cleaned
