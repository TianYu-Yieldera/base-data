-- models/staging/stg_base_blocks.sql
{{
  config(
    materialized='view',
    tags=['staging', 'blocks']
  )
}}

with source as (
    select * from {{ source('clickhouse_raw', 'raw_blocks') }}
),

cleaned as (
    select
        -- Primary Key
        block_number,

        -- Block identifiers
        lower(block_hash) as block_hash,
        lower(coalesce(parent_hash, '')) as parent_hash,

        -- Block metadata
        toDateTime64(timestamp / 1000, 3) as block_timestamp,
        toDate(timestamp / 1000) as block_date,
        toStartOfHour(toDateTime(timestamp / 1000)) as block_hour,

        -- Block metrics
        coalesce(gas_used, 0) as gas_used,
        coalesce(gas_limit, 0) as gas_limit,
        case
            when gas_limit > 0
            then toFloat64(gas_used) / toFloat64(gas_limit)
            else 0
        end as gas_utilization,

        coalesce(transaction_count, 0) as transaction_count,
        coalesce(base_fee_per_gas, 0) as base_fee_per_gas,

        -- Miner/Validator info
        lower(coalesce(miner, '')) as miner_address,

        -- Raw timestamp for joins
        timestamp as timestamp_ms,

        -- Metadata
        kafka_offset,
        kafka_partition

    from source
    where block_number > 0
)

select * from cleaned
