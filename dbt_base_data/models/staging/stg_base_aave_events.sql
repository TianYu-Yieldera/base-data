-- models/staging/stg_base_aave_events.sql
{{
  config(
    materialized='view',
    tags=['staging', 'aave']
  )
}}

with source as (
    select * from {{ source('clickhouse_raw', 'raw_aave_events') }}
),

cleaned as (
    select
        -- Primary Key
        concat(chain, '-', toString(block_number), '-', toString(log_index)) as event_id,

        -- Dimensions
        lower(chain) as chain,
        lower(protocol) as protocol,
        lower(event_type) as event_type,
        lower(user_address) as user_address,
        lower(coalesce(asset_address, '')) as asset_address,
        upper(coalesce(asset_symbol, 'UNKNOWN')) as asset_symbol,

        -- Metrics
        toDecimal128(coalesce(amount, '0'), 18) as amount_raw,
        toFloat64(coalesce(amount_usd, 0)) as amount_usd,

        -- For liquidations
        lower(coalesce(liquidator_address, '')) as liquidator_address,
        lower(coalesce(collateral_asset, '')) as collateral_asset,
        lower(coalesce(debt_asset, '')) as debt_asset,

        -- Block info
        block_number,
        lower(tx_hash) as tx_hash,
        log_index,

        -- Timestamps
        toDateTime64(timestamp / 1000, 3) as event_timestamp,
        toDate(timestamp / 1000) as event_date,

        -- Metadata
        kafka_offset,
        kafka_partition

    from source
    where chain = 'base'
      and timestamp > 0
)

select * from cleaned
