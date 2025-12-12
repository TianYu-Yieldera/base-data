-- Base Data Hub - PostgreSQL Schema Initialization
-- Database: base_data

-- Address Labels Table
CREATE TABLE IF NOT EXISTS address_labels (
    id SERIAL PRIMARY KEY,
    address VARCHAR(42) NOT NULL,
    chain VARCHAR(20) NOT NULL DEFAULT 'base',
    label_type VARCHAR(50) NOT NULL,
    label_value VARCHAR(100) NOT NULL,
    label_subtype VARCHAR(50),
    source VARCHAR(50) NOT NULL,
    confidence DECIMAL(3,2) DEFAULT 1.0,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(address, chain, label_type, label_value)
);

CREATE INDEX IF NOT EXISTS idx_labels_address ON address_labels(address);
CREATE INDEX IF NOT EXISTS idx_labels_chain ON address_labels(chain);
CREATE INDEX IF NOT EXISTS idx_labels_type ON address_labels(label_type);
CREATE INDEX IF NOT EXISTS idx_labels_value ON address_labels(label_value);
CREATE INDEX IF NOT EXISTS idx_labels_source ON address_labels(source);

COMMENT ON TABLE address_labels IS 'Address labels and entity metadata';
COMMENT ON COLUMN address_labels.label_type IS 'entity, contract_type, behavior, risk';

-- Alert Policies Table
CREATE TABLE IF NOT EXISTS alert_policies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    policy_type VARCHAR(50) NOT NULL,
    chain VARCHAR(20) DEFAULT 'base',
    protocol VARCHAR(50),
    config JSONB NOT NULL,
    notification_channels JSONB,
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_policies_type ON alert_policies(policy_type);
CREATE INDEX IF NOT EXISTS idx_policies_chain ON alert_policies(chain);
CREATE INDEX IF NOT EXISTS idx_policies_enabled ON alert_policies(enabled);

COMMENT ON TABLE alert_policies IS 'Alert policy configurations';

-- Alert Events Table
CREATE TABLE IF NOT EXISTS alert_events (
    id SERIAL PRIMARY KEY,
    policy_id INT REFERENCES alert_policies(id),
    chain VARCHAR(20),
    protocol VARCHAR(50),
    address VARCHAR(42),
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    current_value DECIMAL(20,8),
    threshold_value DECIMAL(20,8),
    message TEXT,
    details JSONB,
    acknowledged BOOLEAN DEFAULT false,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_address ON alert_events(address);
CREATE INDEX IF NOT EXISTS idx_alerts_type ON alert_events(alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alert_events(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alert_events(created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_acknowledged ON alert_events(acknowledged);

COMMENT ON TABLE alert_events IS 'Historical alert events';

-- Checkpoint Table for Indexer State
CREATE TABLE IF NOT EXISTS indexer_checkpoints (
    id SERIAL PRIMARY KEY,
    indexer_name VARCHAR(50) NOT NULL,
    chain VARCHAR(20) NOT NULL DEFAULT 'base',
    last_block_number BIGINT NOT NULL,
    last_block_hash VARCHAR(66),
    last_indexed_at TIMESTAMP NOT NULL,
    metadata JSONB,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(indexer_name, chain)
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_indexer ON indexer_checkpoints(indexer_name);

COMMENT ON TABLE indexer_checkpoints IS 'Indexer checkpoint state for crash recovery';

-- Token Metadata Cache
CREATE TABLE IF NOT EXISTS token_metadata (
    id SERIAL PRIMARY KEY,
    chain VARCHAR(20) NOT NULL DEFAULT 'base',
    address VARCHAR(42) NOT NULL,
    symbol VARCHAR(20),
    name VARCHAR(100),
    decimals INT,
    total_supply VARCHAR(78),
    is_verified BOOLEAN DEFAULT false,
    logo_url TEXT,
    website TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(chain, address)
);

CREATE INDEX IF NOT EXISTS idx_token_address ON token_metadata(address);
CREATE INDEX IF NOT EXISTS idx_token_symbol ON token_metadata(symbol);

COMMENT ON TABLE token_metadata IS 'Token metadata cache';

-- Protocol Registry
CREATE TABLE IF NOT EXISTS protocol_registry (
    id SERIAL PRIMARY KEY,
    chain VARCHAR(20) NOT NULL DEFAULT 'base',
    protocol_name VARCHAR(50) NOT NULL,
    protocol_type VARCHAR(50) NOT NULL,
    contract_addresses JSONB NOT NULL,
    abi_hash VARCHAR(66),
    version VARCHAR(20),
    is_active BOOLEAN DEFAULT true,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(chain, protocol_name, version)
);

CREATE INDEX IF NOT EXISTS idx_protocol_name ON protocol_registry(protocol_name);
CREATE INDEX IF NOT EXISTS idx_protocol_type ON protocol_registry(protocol_type);

COMMENT ON TABLE protocol_registry IS 'Supported protocol configurations';

-- User Watchlist
CREATE TABLE IF NOT EXISTS user_watchlist (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    chain VARCHAR(20) NOT NULL DEFAULT 'base',
    address VARCHAR(42) NOT NULL,
    label VARCHAR(100),
    alert_on_risk BOOLEAN DEFAULT true,
    alert_on_activity BOOLEAN DEFAULT false,
    risk_threshold DECIMAL(5,2) DEFAULT 1.20,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(user_id, chain, address)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_user ON user_watchlist(user_id);
CREATE INDEX IF NOT EXISTS idx_watchlist_address ON user_watchlist(address);

COMMENT ON TABLE user_watchlist IS 'User address watchlist for alerts';

-- Insert default alert policies
INSERT INTO alert_policies (name, description, policy_type, config, notification_channels)
VALUES
    ('Health Factor Warning', 'Alert when health factor drops below 1.5', 'health_factor',
     '{"metric": "health_factor", "operator": "<", "threshold": 1.5, "cooldown_minutes": 30}',
     '{"telegram": true, "webhook": false}'),
    ('Health Factor Critical', 'Alert when health factor drops below 1.2', 'health_factor',
     '{"metric": "health_factor", "operator": "<", "threshold": 1.2, "cooldown_minutes": 10}',
     '{"telegram": true, "webhook": true}'),
    ('Large Transfer Alert', 'Alert on transfers over $100k', 'large_transfer',
     '{"metric": "amount_usd", "operator": ">", "threshold": 100000, "cooldown_minutes": 5}',
     '{"telegram": true, "webhook": false}')
ON CONFLICT DO NOTHING;

-- Insert default protocol registry
INSERT INTO protocol_registry (chain, protocol_name, protocol_type, contract_addresses, version)
VALUES
    ('base', 'aave_v3', 'lending',
     '{"pool": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5", "pool_data_provider": "0x2d8A3C5677189723C4cB8873CfC9C8976FDF38Ac"}',
     'v3'),
    ('base', 'uniswap_v3', 'dex',
     '{"factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD", "router": "0x2626664c2603336E57B271c5C0b26F421741e481"}',
     'v3')
ON CONFLICT DO NOTHING;

-- Functions

-- Update timestamp trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to tables
CREATE TRIGGER update_address_labels_updated_at
    BEFORE UPDATE ON address_labels
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_alert_policies_updated_at
    BEFORE UPDATE ON alert_policies
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_indexer_checkpoints_updated_at
    BEFORE UPDATE ON indexer_checkpoints
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_token_metadata_updated_at
    BEFORE UPDATE ON token_metadata
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_protocol_registry_updated_at
    BEFORE UPDATE ON protocol_registry
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_watchlist_updated_at
    BEFORE UPDATE ON user_watchlist
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
