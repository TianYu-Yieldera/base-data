package service

import (
	"context"

	"github.com/TianYu-Yieldera/base-data/internal/repository"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
)

// StatsService handles statistics-related business logic
type StatsService struct {
	repo *repository.ClickHouseRepo
}

// NewStatsService creates a new StatsService
func NewStatsService(repo *repository.ClickHouseRepo) *StatsService {
	return &StatsService{repo: repo}
}

// GetEcosystemStats retrieves ecosystem statistics
func (s *StatsService) GetEcosystemStats(ctx context.Context) (*models.EcosystemStats, error) {
	return s.repo.GetEcosystemStats(ctx)
}

// GetTVLHistory retrieves TVL history
func (s *StatsService) GetTVLHistory(ctx context.Context, days int) ([]TVLDataPoint, error) {
	// This would query from mart_protocol_summary or similar
	// For now, return empty slice
	return []TVLDataPoint{}, nil
}

// TVLDataPoint represents a single TVL data point
type TVLDataPoint struct {
	Date   string `json:"date"`
	TVLUSD string `json:"tvl_usd"`
}
