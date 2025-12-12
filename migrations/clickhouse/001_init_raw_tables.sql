-- Base Data Hub - ClickHouse Schema Initialization
-- Database: base_data

CREATE DATABASE IF NOT EXISTS base_data;

USE base_data;

-- Raw Layer Tables

CREATE TABLE IF NOT EXISTS raw_base_blocks (
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    parent_hash String,
    timestamp DateTime64(3),
    tx_count UInt32,
    gas_used UInt64,
    gas_limit UInt64,
    base_fee_per_gas Nullable(String),
    miner String,
    indexed_at DateTime64(3),
    kafka_partition UInt16,
    kafka_offset UInt64,
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS raw_base_aave_events (
    event_id String,
    event_type Enum8(
        'SUPPLY' = 1,
        'WITHDRAW' = 2,
        'BORROW' = 3,
        'REPAY' = 4,
        'LIQUIDATION' = 5,
        'FLASH_LOAN' = 6
    ),
    protocol String DEFAULT 'aave_v3',
    chain String DEFAULT 'base',
    user_address String,
    reserve_address String,
    amount String,
    amount_usd Nullable(Float64),
    on_behalf_of Nullable(String),
    liquidator Nullable(String),
    collateral_asset Nullable(String),
    debt_to_cover Nullable(String),
    block_number UInt64,
    tx_hash String,
    log_index UInt32,
    timestamp DateTime64(3),
    indexed_at DateTime64(3),
    kafka_partition UInt16,
    kafka_offset UInt64,
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY (chain, toYYYYMM(timestamp))
ORDER BY (chain, protocol, user_address, timestamp, log_index)
SETTINGS index_granularity = 8192;

ALTER TABLE raw_base_aave_events ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 4;
ALTER TABLE raw_base_aave_events ADD INDEX IF NOT EXISTS idx_user_address user_address TYPE bloom_filter GRANULARITY 4;

CREATE TABLE IF NOT EXISTS raw_base_erc20_transfers (
    transfer_id String,
    chain String DEFAULT 'base',
    token_address String,
    token_symbol Nullable(String),
    token_decimals Nullable(UInt8),
    from_address String,
    to_address String,
    amount_raw String,
    amount Nullable(Float64),
    amount_usd Nullable(Float64),
    block_number UInt64,
    tx_hash String,
    log_index UInt32,
    timestamp DateTime64(3),
    indexed_at DateTime64(3),
    kafka_partition UInt16,
    kafka_offset UInt64,
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY (chain, toYYYYMM(timestamp))
ORDER BY (chain, token_address, timestamp, log_index)
SETTINGS index_granularity = 8192;

ALTER TABLE raw_base_erc20_transfers ADD INDEX IF NOT EXISTS idx_from from_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE raw_base_erc20_transfers ADD INDEX IF NOT EXISTS idx_to to_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE raw_base_erc20_transfers ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 4;

CREATE TABLE IF NOT EXISTS raw_base_positions_snapshot (
    snapshot_id String,
    chain String DEFAULT 'base',
    protocol String,
    user_address String,
    total_collateral_eth String,
    total_collateral_usd Float64,
    total_debt_eth String,
    total_debt_usd Float64,
    available_borrow_eth String,
    health_factor Float64,
    ltv Float64,
    current_liquidation_threshold Float64,
    block_number UInt64,
    timestamp DateTime64(3),
    indexed_at DateTime64(3),
    kafka_partition UInt16,
    kafka_offset UInt64,
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY (chain, toYYYYMM(timestamp))
ORDER BY (chain, protocol, user_address, timestamp)
SETTINGS index_granularity = 8192;

ALTER TABLE raw_base_positions_snapshot ADD INDEX IF NOT EXISTS idx_user_address user_address TYPE bloom_filter GRANULARITY 4;

CREATE TABLE IF NOT EXISTS raw_market_prices (
    price_id String,
    chain String,
    asset_address String,
    asset_symbol String,
    price_usd Float64,
    price_eth Nullable(Float64),
    source Enum8('CHAINLINK' = 1, 'PYTH' = 2, 'DEX_TWAP' = 3, 'COINGECKO' = 4),
    confidence Nullable(Float64),
    block_number Nullable(UInt64),
    timestamp DateTime64(3),
    indexed_at DateTime64(3),
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY (chain, toYYYYMM(timestamp))
ORDER BY (chain, asset_address, timestamp)
SETTINGS index_granularity = 8192;
