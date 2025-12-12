package models

import (
	"time"
)

type Block struct {
	ChainID       int64     `json:"chain_id" avro:"chain_id"`
	BlockNumber   int64     `json:"block_number" avro:"block_number"`
	BlockHash     string    `json:"block_hash" avro:"block_hash"`
	ParentHash    string    `json:"parent_hash" avro:"parent_hash"`
	Timestamp     time.Time `json:"timestamp" avro:"timestamp"`
	TxCount       int32     `json:"tx_count" avro:"tx_count"`
	GasUsed       int64     `json:"gas_used" avro:"gas_used"`
	GasLimit      int64     `json:"gas_limit" avro:"gas_limit"`
	BaseFeePerGas *string   `json:"base_fee_per_gas" avro:"base_fee_per_gas"`
	Miner         string    `json:"miner" avro:"miner"`
	IndexedAt     time.Time `json:"indexed_at" avro:"indexed_at"`
}

type AaveEventType string

const (
	AaveEventSupply      AaveEventType = "SUPPLY"
	AaveEventWithdraw    AaveEventType = "WITHDRAW"
	AaveEventBorrow      AaveEventType = "BORROW"
	AaveEventRepay       AaveEventType = "REPAY"
	AaveEventLiquidation AaveEventType = "LIQUIDATION"
	AaveEventFlashLoan   AaveEventType = "FLASH_LOAN"
)

type AaveEvent struct {
	EventID         string        `json:"event_id" avro:"event_id"`
	EventType       AaveEventType `json:"event_type" avro:"event_type"`
	Protocol        string        `json:"protocol" avro:"protocol"`
	Chain           string        `json:"chain" avro:"chain"`
	UserAddress     string        `json:"user_address" avro:"user_address"`
	ReserveAddress  string        `json:"reserve_address" avro:"reserve_address"`
	Amount          string        `json:"amount" avro:"amount"`
	AmountUSD       *float64      `json:"amount_usd" avro:"amount_usd"`
	OnBehalfOf      *string       `json:"on_behalf_of" avro:"on_behalf_of"`
	Liquidator      *string       `json:"liquidator" avro:"liquidator"`
	CollateralAsset *string       `json:"collateral_asset" avro:"collateral_asset"`
	DebtToCover     *string       `json:"debt_to_cover" avro:"debt_to_cover"`
	BlockNumber     int64         `json:"block_number" avro:"block_number"`
	TxHash          string        `json:"tx_hash" avro:"tx_hash"`
	LogIndex        int32         `json:"log_index" avro:"log_index"`
	Timestamp       time.Time     `json:"timestamp" avro:"timestamp"`
	IndexedAt       time.Time     `json:"indexed_at" avro:"indexed_at"`
}

type ERC20Transfer struct {
	TransferID    string    `json:"transfer_id" avro:"transfer_id"`
	Chain         string    `json:"chain" avro:"chain"`
	TokenAddress  string    `json:"token_address" avro:"token_address"`
	TokenSymbol   *string   `json:"token_symbol" avro:"token_symbol"`
	TokenDecimals *int32    `json:"token_decimals" avro:"token_decimals"`
	FromAddress   string    `json:"from_address" avro:"from_address"`
	ToAddress     string    `json:"to_address" avro:"to_address"`
	AmountRaw     string    `json:"amount_raw" avro:"amount_raw"`
	Amount        *float64  `json:"amount" avro:"amount"`
	AmountUSD     *float64  `json:"amount_usd" avro:"amount_usd"`
	BlockNumber   int64     `json:"block_number" avro:"block_number"`
	TxHash        string    `json:"tx_hash" avro:"tx_hash"`
	LogIndex      int32     `json:"log_index" avro:"log_index"`
	Timestamp     time.Time `json:"timestamp" avro:"timestamp"`
	IndexedAt     time.Time `json:"indexed_at" avro:"indexed_at"`
}

type PositionSnapshot struct {
	SnapshotID                  string    `json:"snapshot_id" avro:"snapshot_id"`
	Chain                       string    `json:"chain" avro:"chain"`
	Protocol                    string    `json:"protocol" avro:"protocol"`
	UserAddress                 string    `json:"user_address" avro:"user_address"`
	TotalCollateralETH          string    `json:"total_collateral_eth" avro:"total_collateral_eth"`
	TotalCollateralUSD          float64   `json:"total_collateral_usd" avro:"total_collateral_usd"`
	TotalDebtETH                string    `json:"total_debt_eth" avro:"total_debt_eth"`
	TotalDebtUSD                float64   `json:"total_debt_usd" avro:"total_debt_usd"`
	AvailableBorrowETH          string    `json:"available_borrow_eth" avro:"available_borrow_eth"`
	HealthFactor                float64   `json:"health_factor" avro:"health_factor"`
	LTV                         float64   `json:"ltv" avro:"ltv"`
	CurrentLiquidationThreshold float64   `json:"current_liquidation_threshold" avro:"current_liquidation_threshold"`
	BlockNumber                 int64     `json:"block_number" avro:"block_number"`
	Timestamp                   time.Time `json:"timestamp" avro:"timestamp"`
	IndexedAt                   time.Time `json:"indexed_at" avro:"indexed_at"`
}

type PriceSource string

const (
	PriceSourceChainlink PriceSource = "CHAINLINK"
	PriceSourcePyth      PriceSource = "PYTH"
	PriceSourceDEXTWAP   PriceSource = "DEX_TWAP"
	PriceSourceCoinGecko PriceSource = "COINGECKO"
)

type AssetPrice struct {
	PriceID      string      `json:"price_id" avro:"price_id"`
	Chain        string      `json:"chain" avro:"chain"`
	AssetAddress string      `json:"asset_address" avro:"asset_address"`
	AssetSymbol  string      `json:"asset_symbol" avro:"asset_symbol"`
	PriceUSD     float64     `json:"price_usd" avro:"price_usd"`
	PriceETH     *float64    `json:"price_eth" avro:"price_eth"`
	Source       PriceSource `json:"source" avro:"source"`
	Confidence   *float64    `json:"confidence" avro:"confidence"`
	BlockNumber  *int64      `json:"block_number" avro:"block_number"`
	Timestamp    time.Time   `json:"timestamp" avro:"timestamp"`
	IndexedAt    time.Time   `json:"indexed_at" avro:"indexed_at"`
}

type AlertType string

const (
	AlertTypeHealthFactorWarning  AlertType = "HEALTH_FACTOR_WARNING"
	AlertTypeHealthFactorCritical AlertType = "HEALTH_FACTOR_CRITICAL"
	AlertTypeLiquidation          AlertType = "LIQUIDATION"
	AlertTypeLargeTransfer        AlertType = "LARGE_TRANSFER"
	AlertTypeWhaleMovement        AlertType = "WHALE_MOVEMENT"
	AlertTypeSuspiciousActivity   AlertType = "SUSPICIOUS_ACTIVITY"
)

type Severity string

const (
	SeverityInfo     Severity = "INFO"
	SeverityWarning  Severity = "WARNING"
	SeverityCritical Severity = "CRITICAL"
)

type RiskAlert struct {
	AlertID        string    `json:"alert_id" avro:"alert_id"`
	Chain          string    `json:"chain" avro:"chain"`
	Protocol       string    `json:"protocol" avro:"protocol"`
	Address        string    `json:"address" avro:"address"`
	AlertType      AlertType `json:"alert_type" avro:"alert_type"`
	Severity       Severity  `json:"severity" avro:"severity"`
	CurrentValue   float64   `json:"current_value" avro:"current_value"`
	ThresholdValue float64   `json:"threshold_value" avro:"threshold_value"`
	Message        string    `json:"message" avro:"message"`
	Details        *string   `json:"details" avro:"details"`
	Timestamp      time.Time `json:"timestamp" avro:"timestamp"`
}
