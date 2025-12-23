package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/TianYu-Yieldera/base-data/internal/service"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
)

// EcosystemHandler handles ecosystem-related API endpoints
type EcosystemHandler struct {
	statsService *service.StatsService
}

// NewEcosystemHandler creates a new EcosystemHandler
func NewEcosystemHandler(ss *service.StatsService) *EcosystemHandler {
	return &EcosystemHandler{statsService: ss}
}

// GetStats handles GET /ecosystem/stats
func (h *EcosystemHandler) GetStats(c *gin.Context) {
	stats, err := h.statsService.GetEcosystemStats(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Code:    "INTERNAL_ERROR",
			Message: "Failed to retrieve ecosystem statistics",
		})
		return
	}

	c.JSON(http.StatusOK, stats)
}

// GetTVLHistory handles GET /ecosystem/tvl
func (h *EcosystemHandler) GetTVLHistory(c *gin.Context) {
	// For now, return empty array
	// In production, this would query historical TVL data
	c.JSON(http.StatusOK, gin.H{
		"period_days": 30,
		"data_points": []service.TVLDataPoint{},
	})
}
