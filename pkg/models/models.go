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

// API Response Models

// AddressRisk represents risk information for an address
type AddressRisk struct {
	Address              string    `json:"address"`
	Chain                string    `json:"chain"`
	Protocol             string    `json:"protocol"`
	HealthFactor         float64   `json:"health_factor"`
	RiskLevel            string    `json:"risk_level"`
	TotalCollateralUSD   string    `json:"total_collateral_usd"`
	TotalDebtUSD         string    `json:"total_debt_usd"`
	AvailableBorrowUSD   string    `json:"available_borrow_usd"`
	LTV                  float64   `json:"ltv"`
	LiquidationThreshold float64   `json:"liquidation_threshold"`
	PositionSizeTier     string    `json:"position_size_tier"`
	UtilizationRate      float64   `json:"utilization_rate"`
	UpdatedAt            time.Time `json:"updated_at"`
}

// AddressFlow represents flow history for an address
type AddressFlow struct {
	Address       string      `json:"address"`
	PeriodDays    int         `json:"period_days"`
	TotalInflowUSD  string    `json:"total_inflow_usd"`
	TotalOutflowUSD string    `json:"total_outflow_usd"`
	NetFlowUSD    string      `json:"net_flow_usd"`
	DailyFlows    []DailyFlow `json:"daily_flows"`
}

// DailyFlow represents daily flow metrics
type DailyFlow struct {
	Date       string `json:"date"`
	InflowUSD  string `json:"inflow_usd"`
	OutflowUSD string `json:"outflow_usd"`
	NetUSD     string `json:"net_usd"`
}

// HighRiskList represents a list of high risk addresses
type HighRiskList struct {
	TotalCount int           `json:"total_count"`
	RiskLevel  string        `json:"risk_level"`
	Addresses  []AddressRisk `json:"addresses"`
}

// WhaleMovement represents a single whale movement
type WhaleMovement struct {
	Timestamp   time.Time `json:"timestamp"`
	FromAddress string    `json:"from_address"`
	ToAddress   string    `json:"to_address"`
	TokenSymbol string    `json:"token_symbol"`
	Amount      string    `json:"amount"`
	AmountUSD   string    `json:"amount_usd"`
	Direction   string    `json:"direction"`
	TxHash      string    `json:"tx_hash"`
}

// WhaleMovements represents recent whale movements
type WhaleMovements struct {
	PeriodHours    int             `json:"period_hours"`
	TotalMovements int             `json:"total_movements"`
	TotalVolumeUSD string          `json:"total_volume_usd"`
	Movements      []WhaleMovement `json:"movements"`
}

// WhaleAddress represents a known whale address
type WhaleAddress struct {
	Address         string   `json:"address"`
	WhaleTypes      []string `json:"whale_types"`
	MaxWhaleValueUSD string  `json:"max_whale_value_usd"`
	IsPositionWhale bool     `json:"is_position_whale"`
	IsTransferWhale bool     `json:"is_transfer_whale"`
}

// EcosystemStats represents ecosystem statistics
type EcosystemStats struct {
	SnapshotTime     time.Time        `json:"snapshot_time"`
	TotalTVLUSD      string           `json:"total_tvl_usd"`
	TotalDebtUSD     string           `json:"total_debt_usd"`
	TotalUsers       int              `json:"total_users"`
	ActiveUsers24h   int              `json:"active_users_24h"`
	Volume24hUSD     string           `json:"volume_24h_usd"`
	AvgHealthFactor  float64          `json:"avg_health_factor"`
	RiskDistribution RiskDistribution `json:"risk_distribution"`
}

// RiskDistribution represents the distribution of risk levels
type RiskDistribution struct {
	Safe         int `json:"safe"`
	Warning      int `json:"warning"`
	Danger       int `json:"danger"`
	Critical     int `json:"critical"`
	Liquidatable int `json:"liquidatable"`
}

// ErrorResponse represents an API error response
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// AddressLabel represents a label for an address
type AddressLabel struct {
	Address    string  `json:"address"`
	LabelType  string  `json:"label_type"`
	LabelName  string  `json:"label_name"`
	Source     string  `json:"source"`
	Confidence float64 `json:"confidence"`
}

// StreamingRiskAlert represents a risk alert from Flink streaming
type StreamingRiskAlert struct {
	AlertID     string                   `json:"alert_id"`
	Chain       string                   `json:"chain"`
	Protocol    string                   `json:"protocol"`
	UserAddress string                   `json:"user_address"`
	AlertType   string                   `json:"alert_type"`
	Severity    string                   `json:"severity"`
	Details     StreamingRiskAlertDetail `json:"details"`
	Timestamp   time.Time                `json:"timestamp"`
}

// StreamingRiskAlertDetail contains alert details from Flink
type StreamingRiskAlertDetail struct {
	CurrentHealthFactor   float64 `json:"current_health_factor"`
	PreviousHealthFactor  float64 `json:"previous_health_factor"`
	HealthFactorChangePct float64 `json:"health_factor_change_pct"`
	CurrentRiskLevel      string  `json:"current_risk_level"`
	PreviousRiskLevel     string  `json:"previous_risk_level"`
	TotalCollateralUSD    string  `json:"total_collateral_usd"`
	TotalDebtUSD          string  `json:"total_debt_usd"`
}
