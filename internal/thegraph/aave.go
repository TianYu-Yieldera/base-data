package thegraph

import (
	"context"
	"time"
)

// AaveClient is a client for Aave V3 subgraph
type AaveClient struct {
	client *Client
}

// NewAaveClient creates a new Aave subgraph client
func NewAaveClient(endpoint string) *AaveClient {
	return &AaveClient{
		client: NewClient(endpoint),
	}
}

// Reserve represents an Aave reserve
type Reserve struct {
	ID                  string `json:"id"`
	Symbol              string `json:"symbol"`
	Name                string `json:"name"`
	Decimals            int    `json:"decimals"`
	TotalLiquidity      string `json:"totalLiquidity"`
	TotalLiquidityUSD   string `json:"totalLiquidityUSD"`
	TotalBorrows        string `json:"totalBorrows"`
	TotalBorrowsUSD     string `json:"totalBorrowsUSD"`
	AvailableLiquidity  string `json:"availableLiquidity"`
	UtilizationRate     string `json:"utilizationRate"`
	LiquidityRate       string `json:"liquidityRate"`
	VariableBorrowRate  string `json:"variableBorrowRate"`
	StableBorrowRate    string `json:"stableBorrowRate"`
	LastUpdateTimestamp int64  `json:"lastUpdateTimestamp"`
}

// UserReserve represents a user's position in a reserve
type UserReserve struct {
	ID                       string  `json:"id"`
	User                     User    `json:"user"`
	Reserve                  Reserve `json:"reserve"`
	CurrentATokenBalance     string  `json:"currentATokenBalance"`
	CurrentVariableDebt      string  `json:"currentVariableDebt"`
	CurrentStableDebt        string  `json:"currentStableDebt"`
	UsageAsCollateralEnabled bool    `json:"usageAsCollateralEnabled"`
}

// User represents an Aave user
type User struct {
	ID string `json:"id"`
}

// Asset represents a token asset
type Asset struct {
	Symbol string `json:"symbol"`
}

// Liquidation represents a liquidation event
type Liquidation struct {
	ID               string `json:"id"`
	User             User   `json:"user"`
	Liquidator       User   `json:"liquidator"`
	CollateralAsset  Asset  `json:"collateralAsset"`
	PrincipalAsset   Asset  `json:"principalAsset"`
	CollateralAmount string `json:"collateralAmount"`
	PrincipalAmount  string `json:"principalAmount"`
	Timestamp        int64  `json:"timestamp"`
	TxHash           string `json:"txHash"`
}

// GetReserves fetches all reserves
func (c *AaveClient) GetReserves(ctx context.Context) ([]Reserve, error) {
	query := `
		query GetReserves {
			reserves(first: 100) {
				id
				symbol
				name
				decimals
				totalLiquidity
				totalLiquidityUSD
				totalBorrows
				totalBorrowsUSD
				availableLiquidity
				utilizationRate
				liquidityRate
				variableBorrowRate
				stableBorrowRate
				lastUpdateTimestamp
			}
		}
	`

	var result struct {
		Reserves []Reserve `json:"reserves"`
	}

	if err := c.client.Query(ctx, query, nil, &result); err != nil {
		return nil, err
	}

	return result.Reserves, nil
}

// GetUserReserves fetches user positions
func (c *AaveClient) GetUserReserves(ctx context.Context, userAddress string) ([]UserReserve, error) {
	query := `
		query GetUserReserves($user: String!) {
			userReserves(where: {user: $user}) {
				id
				user {
					id
				}
				reserve {
					id
					symbol
					decimals
				}
				currentATokenBalance
				currentVariableDebt
				currentStableDebt
				usageAsCollateralEnabled
			}
		}
	`

	variables := map[string]interface{}{
		"user": userAddress,
	}

	var result struct {
		UserReserves []UserReserve `json:"userReserves"`
	}

	if err := c.client.Query(ctx, query, variables, &result); err != nil {
		return nil, err
	}

	return result.UserReserves, nil
}

// GetLiquidations fetches recent liquidations
func (c *AaveClient) GetLiquidations(ctx context.Context, since time.Time, limit int) ([]Liquidation, error) {
	query := `
		query GetLiquidations($timestamp: Int!, $limit: Int!) {
			liquidationCalls(
				first: $limit,
				where: {timestamp_gte: $timestamp},
				orderBy: timestamp,
				orderDirection: desc
			) {
				id
				user {
					id
				}
				liquidator {
					id
				}
				collateralAsset {
					symbol
				}
				principalAsset {
					symbol
				}
				collateralAmount
				principalAmount
				timestamp
				txHash
			}
		}
	`

	variables := map[string]interface{}{
		"timestamp": since.Unix(),
		"limit":     limit,
	}

	var result struct {
		LiquidationCalls []Liquidation `json:"liquidationCalls"`
	}

	if err := c.client.Query(ctx, query, variables, &result); err != nil {
		return nil, err
	}

	return result.LiquidationCalls, nil
}

// GetReserveBySymbol fetches a specific reserve by symbol
func (c *AaveClient) GetReserveBySymbol(ctx context.Context, symbol string) (*Reserve, error) {
	query := `
		query GetReserveBySymbol($symbol: String!) {
			reserves(where: {symbol: $symbol}, first: 1) {
				id
				symbol
				name
				decimals
				totalLiquidity
				totalLiquidityUSD
				totalBorrows
				totalBorrowsUSD
				availableLiquidity
				utilizationRate
				liquidityRate
				variableBorrowRate
				stableBorrowRate
				lastUpdateTimestamp
			}
		}
	`

	variables := map[string]interface{}{
		"symbol": symbol,
	}

	var result struct {
		Reserves []Reserve `json:"reserves"`
	}

	if err := c.client.Query(ctx, query, variables, &result); err != nil {
		return nil, err
	}

	if len(result.Reserves) == 0 {
		return nil, nil
	}

	return &result.Reserves[0], nil
}
