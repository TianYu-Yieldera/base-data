package handler

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/TianYu-Yieldera/base-data/internal/service"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
)

// RiskHandler handles risk-related API endpoints
type RiskHandler struct {
	riskService *service.RiskService
}

// NewRiskHandler creates a new RiskHandler
func NewRiskHandler(rs *service.RiskService) *RiskHandler {
	return &RiskHandler{riskService: rs}
}

// GetHighRiskAddresses handles GET /risk/high-risk
func (h *RiskHandler) GetHighRiskAddresses(c *gin.Context) {
	riskLevel := c.DefaultQuery("risk_level", "CRITICAL")
	limitStr := c.DefaultQuery("limit", "100")
	minCollateralStr := c.DefaultQuery("min_collateral_usd", "1000")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	minCollateral, err := strconv.ParseFloat(minCollateralStr, 64)
	if err != nil || minCollateral < 0 {
		minCollateral = 1000
	}

	// Validate risk level
	validLevels := map[string]bool{
		"CRITICAL":     true,
		"LIQUIDATABLE": true,
		"DANGER":       true,
	}
	if !validLevels[riskLevel] {
		riskLevel = "CRITICAL"
	}

	result, err := h.riskService.GetHighRiskAddresses(c.Request.Context(), riskLevel, limit, minCollateral)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Code:    "INTERNAL_ERROR",
			Message: "Failed to retrieve high risk addresses",
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// GetRiskDistribution handles GET /risk/distribution
func (h *RiskHandler) GetRiskDistribution(c *gin.Context) {
	dist, err := h.riskService.GetRiskDistribution(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Code:    "INTERNAL_ERROR",
			Message: "Failed to retrieve risk distribution",
		})
		return
	}

	c.JSON(http.StatusOK, dist)
}
