-- models/staging/stg_base_transfers.sql
{{
  config(
    materialized='view',
    tags=['staging', 'transfers']
  )
}}

with source as (
    select * from {{ source('clickhouse_raw', 'raw_transfers') }}
),

cleaned as (
    select
        -- Primary Key
        coalesce(transfer_id, concat(chain, '-', tx_hash, '-', toString(log_index))) as transfer_id,

        -- Dimensions
        lower(chain) as chain,
        lower(token_address) as token_address,
        upper(coalesce(token_symbol, 'UNKNOWN')) as token_symbol,
        coalesce(token_decimals, 18) as token_decimals,
        lower(from_address) as from_address,
        lower(to_address) as to_address,

        -- Amount handling
        amount_raw,
        coalesce(amount, toFloat64(amount_raw) / pow(10, coalesce(token_decimals, 18))) as amount,
        coalesce(amount_usd, 0) as amount_usd,

        -- Classification
        case
            when from_address = '0x0000000000000000000000000000000000000000' then 'mint'
            when to_address = '0x0000000000000000000000000000000000000000' then 'burn'
            else 'transfer'
        end as transfer_type,

        -- Block info
        block_number,
        lower(tx_hash) as tx_hash,
        log_index,

        -- Timestamps
        toDateTime64(timestamp / 1000, 3) as event_timestamp,
        toDate(timestamp / 1000) as event_date,
        toStartOfHour(toDateTime(timestamp / 1000)) as event_hour

    from source
    where chain = 'base'
)

select * from cleaned
