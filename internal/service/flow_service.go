package service

import (
	"context"
	"strconv"

	"github.com/TianYu-Yieldera/base-data/internal/repository"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
)

// FlowService handles flow-related business logic
type FlowService struct {
	repo *repository.ClickHouseRepo
}

// NewFlowService creates a new FlowService
func NewFlowService(repo *repository.ClickHouseRepo) *FlowService {
	return &FlowService{repo: repo}
}

// GetAddressFlow retrieves flow history for an address
func (s *FlowService) GetAddressFlow(ctx context.Context, address string, daysStr string, token string) (*models.AddressFlow, error) {
	days, err := strconv.Atoi(daysStr)
	if err != nil || days <= 0 {
		days = 30
	}
	if days > 90 {
		days = 90
	}

	return s.repo.GetAddressFlow(ctx, address, days, token)
}
