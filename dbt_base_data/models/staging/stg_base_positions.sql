-- models/staging/stg_base_positions.sql
{{
  config(
    materialized='view',
    tags=['staging', 'positions']
  )
}}

with source as (
    select * from {{ source('clickhouse_raw', 'raw_positions') }}
),

cleaned as (
    select
        -- Primary Key
        concat(chain, '-', protocol, '-', user_address, '-', toString(block_number)) as position_id,

        -- Dimensions
        lower(chain) as chain,
        lower(protocol) as protocol,
        lower(user_address) as user_address,

        -- Position metrics
        toFloat64(coalesce(health_factor, 999)) as health_factor,
        toDecimal128(coalesce(total_collateral_usd, 0), 2) as total_collateral_usd,
        toDecimal128(coalesce(total_debt_usd, 0), 2) as total_debt_usd,
        toDecimal128(coalesce(available_borrow_usd, 0), 2) as available_borrow_usd,
        toFloat64(coalesce(ltv, 0)) as ltv,
        toFloat64(coalesce(liquidation_threshold, 0)) as liquidation_threshold,

        -- Asset breakdown (JSON arrays)
        coalesce(collateral_assets, '[]') as collateral_assets,
        coalesce(debt_assets, '[]') as debt_assets,

        -- Block info
        block_number,

        -- Timestamps
        toDateTime64(timestamp / 1000, 3) as position_timestamp,
        toDate(timestamp / 1000) as position_date,

        -- Raw timestamp for joins
        timestamp as timestamp_ms,

        -- Metadata
        kafka_offset,
        kafka_partition

    from source
    where chain = 'base'
      and user_address != ''
)

select * from cleaned
