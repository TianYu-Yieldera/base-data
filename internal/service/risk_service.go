package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/TianYu-Yieldera/base-data/internal/repository"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
)

var ErrAddressNotFound = errors.New("address not found")

// RiskService handles risk-related business logic
type RiskService struct {
	repo *repository.ClickHouseRepo
}

// NewRiskService creates a new RiskService
func NewRiskService(repo *repository.ClickHouseRepo) *RiskService {
	return &RiskService{repo: repo}
}

// GetAddressRisk retrieves risk information for an address
func (s *RiskService) GetAddressRisk(ctx context.Context, address string) (*models.AddressRisk, error) {
	risk, err := s.repo.GetAddressRisk(ctx, address)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, ErrAddressNotFound
		}
		return nil, err
	}

	// Calculate derived fields
	risk.PositionSizeTier = calculatePositionSizeTier(risk.TotalCollateralUSD)
	risk.UtilizationRate = calculateUtilizationRate(risk.TotalDebtUSD, risk.TotalCollateralUSD)

	return risk, nil
}

// GetHighRiskAddresses retrieves high risk addresses
func (s *RiskService) GetHighRiskAddresses(ctx context.Context, riskLevel string, limit int, minCollateral float64) (*models.HighRiskList, error) {
	addresses, err := s.repo.GetHighRiskAddresses(ctx, riskLevel, limit, minCollateral)
	if err != nil {
		return nil, err
	}

	return &models.HighRiskList{
		TotalCount: len(addresses),
		RiskLevel:  riskLevel,
		Addresses:  addresses,
	}, nil
}

// GetRiskDistribution retrieves risk distribution stats
func (s *RiskService) GetRiskDistribution(ctx context.Context) (*models.RiskDistribution, error) {
	return s.repo.GetRiskDistribution(ctx)
}

// Helper functions
func calculatePositionSizeTier(collateralUSD string) string {
	var value float64
	_ = parseFloat(collateralUSD, &value)

	switch {
	case value >= 1000000:
		return "whale"
	case value >= 100000:
		return "large"
	case value >= 10000:
		return "medium"
	default:
		return "small"
	}
}

func calculateUtilizationRate(debtUSD, collateralUSD string) float64 {
	var debt, collateral float64
	_ = parseFloat(debtUSD, &debt)
	_ = parseFloat(collateralUSD, &collateral)

	if collateral == 0 {
		return 0
	}
	return debt / collateral
}

func parseFloat(s string, v *float64) error {
	_, err := fmt.Sscanf(s, "%f", v)
	return err
}
