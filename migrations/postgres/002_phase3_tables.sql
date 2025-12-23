-- Phase 3: PostgreSQL Schema for Labels, Config, and Alert History
-- Run this migration after 001_init_tables.sql

-- Address Labels
CREATE TABLE IF NOT EXISTS address_labels (
    id SERIAL PRIMARY KEY,
    address VARCHAR(42) NOT NULL,
    chain VARCHAR(20) NOT NULL DEFAULT 'base',
    label_type VARCHAR(50) NOT NULL,  -- 'exchange', 'whale', 'contract', 'scam', etc.
    label_name VARCHAR(100) NOT NULL,
    label_source VARCHAR(50),          -- 'manual', 'etherscan', 'arkham', etc.
    confidence DECIMAL(3,2) DEFAULT 1.0,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(address, chain, label_type, label_name)
);

CREATE INDEX IF NOT EXISTS idx_address_labels_address ON address_labels(address);
CREATE INDEX IF NOT EXISTS idx_address_labels_type ON address_labels(label_type);
CREATE INDEX IF NOT EXISTS idx_address_labels_chain ON address_labels(chain);

-- Alert Configurations
CREATE TABLE IF NOT EXISTS alert_configs (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,     -- Telegram chat ID or user identifier
    config_type VARCHAR(50) NOT NULL,  -- 'address_watch', 'risk_threshold', 'whale_alert'
    config_value JSONB NOT NULL,
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alert_configs_user ON alert_configs(user_id);
CREATE INDEX IF NOT EXISTS idx_alert_configs_type ON alert_configs(config_type);
CREATE INDEX IF NOT EXISTS idx_alert_configs_enabled ON alert_configs(enabled);

-- Example config_value for address_watch:
-- {"addresses": ["0x123...", "0x456..."], "min_severity": "WARNING"}

-- Example config_value for risk_threshold:
-- {"risk_levels": ["CRITICAL", "LIQUIDATABLE"], "min_collateral_usd": 10000}

-- Example config_value for whale_alert:
-- {"min_amount_usd": 100000, "tokens": ["USDC", "WETH"]}

-- Alert History (for audit and analytics)
CREATE TABLE IF NOT EXISTS alert_history (
    id SERIAL PRIMARY KEY,
    alert_id VARCHAR(50) UNIQUE NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    protocol VARCHAR(50),
    user_address VARCHAR(42),
    details JSONB,
    dispatched_to JSONB,  -- ["telegram", "email"]
    dispatched_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alert_history_address ON alert_history(user_address);
CREATE INDEX IF NOT EXISTS idx_alert_history_created ON alert_history(created_at);
CREATE INDEX IF NOT EXISTS idx_alert_history_type ON alert_history(alert_type);
CREATE INDEX IF NOT EXISTS idx_alert_history_severity ON alert_history(severity);

-- API Keys for authentication
CREATE TABLE IF NOT EXISTS api_keys (
    id SERIAL PRIMARY KEY,
    key_hash VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    user_id VARCHAR(100),
    permissions JSONB DEFAULT '["read"]',
    rate_limit_rps INT DEFAULT 100,
    is_active BOOLEAN DEFAULT true,
    last_used_at TIMESTAMP,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);

-- Protocol metadata
CREATE TABLE IF NOT EXISTS protocols (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    display_name VARCHAR(100) NOT NULL,
    chain VARCHAR(20) NOT NULL DEFAULT 'base',
    category VARCHAR(50),  -- 'lending', 'dex', 'bridge', etc.
    website_url VARCHAR(255),
    logo_url VARCHAR(255),
    description TEXT,
    contracts JSONB,  -- Array of contract addresses
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_protocols_chain ON protocols(chain);
CREATE INDEX IF NOT EXISTS idx_protocols_category ON protocols(category);

-- Insert initial protocol data
INSERT INTO protocols (name, display_name, chain, category, contracts) VALUES
('aave_v3', 'Aave V3', 'base', 'lending', '{"pool": "0xa238dd80c259a72e81d7e4664a9801593f98d1c5", "oracle": "0x2d5ee574e710219a521449679a4a7f2b43f046ad"}')
ON CONFLICT (name) DO NOTHING;

-- Insert sample address labels
INSERT INTO address_labels (address, chain, label_type, label_name, label_source, confidence) VALUES
('0x3304e22ddaa22bcdc5fca2269b418046ae7b566a', 'base', 'exchange', 'Coinbase', 'manual', 1.0),
('0x28c6c06298d514db089934071355e5743bf21d60', 'base', 'exchange', 'Binance', 'manual', 1.0),
('0xa238dd80c259a72e81d7e4664a9801593f98d1c5', 'base', 'protocol', 'Aave V3 Pool', 'manual', 1.0),
('0x0000000000000000000000000000000000000000', 'base', 'system', 'Null Address', 'manual', 1.0)
ON CONFLICT (address, chain, label_type, label_name) DO NOTHING;

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
DROP TRIGGER IF EXISTS update_address_labels_updated_at ON address_labels;
CREATE TRIGGER update_address_labels_updated_at
    BEFORE UPDATE ON address_labels
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_alert_configs_updated_at ON alert_configs;
CREATE TRIGGER update_alert_configs_updated_at
    BEFORE UPDATE ON alert_configs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_api_keys_updated_at ON api_keys;
CREATE TRIGGER update_api_keys_updated_at
    BEFORE UPDATE ON api_keys
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_protocols_updated_at ON protocols;
CREATE TRIGGER update_protocols_updated_at
    BEFORE UPDATE ON protocols
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
