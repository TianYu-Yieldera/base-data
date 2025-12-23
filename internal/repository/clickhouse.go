package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
)

var ErrNotFound = errors.New("record not found")

// ClickHouseRepo handles ClickHouse database operations
type ClickHouseRepo struct {
	db *sql.DB
}

// NewClickHouseRepo creates a new ClickHouse repository
func NewClickHouseRepo(dsn string) (*ClickHouseRepo, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse connection: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	return &ClickHouseRepo{db: db}, nil
}

// Close closes the database connection
func (r *ClickHouseRepo) Close() error {
	return r.db.Close()
}

// Ping checks database connectivity
func (r *ClickHouseRepo) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

// GetAddressRisk retrieves risk information for an address
func (r *ClickHouseRepo) GetAddressRisk(ctx context.Context, address string) (*models.AddressRisk, error) {
	query := `
		SELECT
			chain,
			protocol,
			user_address,
			health_factor,
			risk_level,
			toString(total_collateral_usd),
			toString(total_debt_usd),
			toString(available_borrow_usd),
			ltv,
			liquidation_threshold,
			updated_at
		FROM rt_cdp_risk_metrics FINAL
		WHERE user_address = ?
		ORDER BY updated_at DESC
		LIMIT 1
	`

	row := r.db.QueryRowContext(ctx, query, address)

	var risk models.AddressRisk
	err := row.Scan(
		&risk.Chain,
		&risk.Protocol,
		&risk.Address,
		&risk.HealthFactor,
		&risk.RiskLevel,
		&risk.TotalCollateralUSD,
		&risk.TotalDebtUSD,
		&risk.AvailableBorrowUSD,
		&risk.LTV,
		&risk.LiquidationThreshold,
		&risk.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan address risk: %w", err)
	}

	return &risk, nil
}

// GetHighRiskAddresses retrieves addresses with high risk levels
func (r *ClickHouseRepo) GetHighRiskAddresses(ctx context.Context, riskLevel string, limit int, minCollateral float64) ([]models.AddressRisk, error) {
	query := `
		SELECT
			chain,
			protocol,
			user_address,
			health_factor,
			risk_level,
			toString(total_collateral_usd),
			toString(total_debt_usd),
			toString(available_borrow_usd),
			ltv,
			liquidation_threshold,
			updated_at
		FROM rt_cdp_risk_metrics FINAL
		WHERE risk_level = ?
		  AND total_collateral_usd >= ?
		ORDER BY health_factor ASC
		LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, query, riskLevel, minCollateral, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query high risk addresses: %w", err)
	}
	defer rows.Close()

	var results []models.AddressRisk
	for rows.Next() {
		var risk models.AddressRisk
		err := rows.Scan(
			&risk.Chain,
			&risk.Protocol,
			&risk.Address,
			&risk.HealthFactor,
			&risk.RiskLevel,
			&risk.TotalCollateralUSD,
			&risk.TotalDebtUSD,
			&risk.AvailableBorrowUSD,
			&risk.LTV,
			&risk.LiquidationThreshold,
			&risk.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		results = append(results, risk)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// GetRiskDistribution retrieves the distribution of risk levels
func (r *ClickHouseRepo) GetRiskDistribution(ctx context.Context) (*models.RiskDistribution, error) {
	query := `
		SELECT
			countIf(risk_level = 'SAFE') as safe,
			countIf(risk_level = 'WARNING') as warning,
			countIf(risk_level = 'DANGER') as danger,
			countIf(risk_level = 'CRITICAL') as critical,
			countIf(risk_level = 'LIQUIDATABLE') as liquidatable
		FROM rt_cdp_risk_metrics FINAL
	`

	row := r.db.QueryRowContext(ctx, query)

	var dist models.RiskDistribution
	err := row.Scan(&dist.Safe, &dist.Warning, &dist.Danger, &dist.Critical, &dist.Liquidatable)
	if err != nil {
		return nil, fmt.Errorf("failed to get risk distribution: %w", err)
	}

	return &dist, nil
}

// GetAddressFlow retrieves flow history for an address
func (r *ClickHouseRepo) GetAddressFlow(ctx context.Context, address string, days int, token string) (*models.AddressFlow, error) {
	tokenFilter := ""
	args := []interface{}{address, days}
	if token != "" {
		tokenFilter = "AND token_address = ?"
		args = append(args, token)
	}

	query := fmt.Sprintf(`
		SELECT
			event_date,
			sum(inflow_usd) as inflow_usd,
			sum(outflow_usd) as outflow_usd,
			sum(net_usd) as net_usd
		FROM core_address_netflow_daily
		WHERE address = ?
		  AND event_date >= today() - ?
		  %s
		GROUP BY event_date
		ORDER BY event_date DESC
	`, tokenFilter)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query address flow: %w", err)
	}
	defer rows.Close()

	var dailyFlows []models.DailyFlow
	var totalInflow, totalOutflow float64

	for rows.Next() {
		var date time.Time
		var inflow, outflow, net float64
		if err := rows.Scan(&date, &inflow, &outflow, &net); err != nil {
			return nil, fmt.Errorf("failed to scan flow row: %w", err)
		}
		dailyFlows = append(dailyFlows, models.DailyFlow{
			Date:       date.Format("2006-01-02"),
			InflowUSD:  fmt.Sprintf("%.2f", inflow),
			OutflowUSD: fmt.Sprintf("%.2f", outflow),
			NetUSD:     fmt.Sprintf("%.2f", net),
		})
		totalInflow += inflow
		totalOutflow += outflow
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating flow rows: %w", err)
	}

	return &models.AddressFlow{
		Address:         address,
		PeriodDays:      days,
		TotalInflowUSD:  fmt.Sprintf("%.2f", totalInflow),
		TotalOutflowUSD: fmt.Sprintf("%.2f", totalOutflow),
		NetFlowUSD:      fmt.Sprintf("%.2f", totalInflow-totalOutflow),
		DailyFlows:      dailyFlows,
	}, nil
}

// GetWhaleMovements retrieves recent whale movements
func (r *ClickHouseRepo) GetWhaleMovements(ctx context.Context, hours int, minUSD float64, direction string) (*models.WhaleMovements, error) {
	directionFilter := ""
	if direction == "in" {
		directionFilter = "AND whale_direction = 'whale_in'"
	} else if direction == "out" {
		directionFilter = "AND whale_direction = 'whale_out'"
	}

	query := fmt.Sprintf(`
		SELECT
			event_timestamp,
			from_address,
			to_address,
			token_symbol,
			toString(amount),
			toString(amount_usd),
			whale_direction,
			tx_hash
		FROM mart_whale_movements
		WHERE event_timestamp >= now() - INTERVAL ? HOUR
		  AND amount_usd >= ?
		  %s
		ORDER BY event_timestamp DESC
		LIMIT 1000
	`, directionFilter)

	rows, err := r.db.QueryContext(ctx, query, hours, minUSD)
	if err != nil {
		return nil, fmt.Errorf("failed to query whale movements: %w", err)
	}
	defer rows.Close()

	var movements []models.WhaleMovement
	var totalVolume float64

	for rows.Next() {
		var m models.WhaleMovement
		var amountUSDFloat float64
		err := rows.Scan(
			&m.Timestamp,
			&m.FromAddress,
			&m.ToAddress,
			&m.TokenSymbol,
			&m.Amount,
			&m.AmountUSD,
			&m.Direction,
			&m.TxHash,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan movement row: %w", err)
		}
		fmt.Sscanf(m.AmountUSD, "%f", &amountUSDFloat)
		totalVolume += amountUSDFloat
		movements = append(movements, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating movement rows: %w", err)
	}

	return &models.WhaleMovements{
		PeriodHours:    hours,
		TotalMovements: len(movements),
		TotalVolumeUSD: fmt.Sprintf("%.2f", totalVolume),
		Movements:      movements,
	}, nil
}

// GetWhaleList retrieves known whale addresses
func (r *ClickHouseRepo) GetWhaleList(ctx context.Context, limit int) ([]models.WhaleAddress, error) {
	query := `
		SELECT
			address,
			whale_types,
			toString(max_whale_value_usd),
			is_position_whale,
			is_transfer_whale
		FROM core_whale_addresses
		ORDER BY max_whale_value_usd DESC
		LIMIT ?
	`

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query whale list: %w", err)
	}
	defer rows.Close()

	var whales []models.WhaleAddress
	for rows.Next() {
		var w models.WhaleAddress
		err := rows.Scan(
			&w.Address,
			&w.WhaleTypes,
			&w.MaxWhaleValueUSD,
			&w.IsPositionWhale,
			&w.IsTransferWhale,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan whale row: %w", err)
		}
		whales = append(whales, w)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating whale rows: %w", err)
	}

	return whales, nil
}

// GetEcosystemStats retrieves ecosystem statistics
func (r *ClickHouseRepo) GetEcosystemStats(ctx context.Context) (*models.EcosystemStats, error) {
	query := `
		SELECT
			snapshot_time,
			toString(total_tvl),
			toString(total_debt),
			unique_users,
			active_users_24h,
			toString(volume_24h),
			avg_health_factor,
			at_risk_positions
		FROM mart_ecosystem_kpis
		LIMIT 1
	`

	row := r.db.QueryRowContext(ctx, query)

	var stats models.EcosystemStats
	var atRiskPositions int
	err := row.Scan(
		&stats.SnapshotTime,
		&stats.TotalTVLUSD,
		&stats.TotalDebtUSD,
		&stats.TotalUsers,
		&stats.ActiveUsers24h,
		&stats.Volume24hUSD,
		&stats.AvgHealthFactor,
		&atRiskPositions,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get ecosystem stats: %w", err)
	}

	// Get risk distribution
	dist, err := r.GetRiskDistribution(ctx)
	if err != nil {
		return nil, err
	}
	stats.RiskDistribution = *dist

	return &stats, nil
}
