package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/TianYu-Yieldera/base-data/internal/repository"
)

// HealthHandler handles health check endpoints
type HealthHandler struct {
	repo *repository.ClickHouseRepo
}

// NewHealthHandler creates a new HealthHandler
func NewHealthHandler(repo *repository.ClickHouseRepo) *HealthHandler {
	return &HealthHandler{repo: repo}
}

// Health handles GET /health
func (h *HealthHandler) Health(c *gin.Context) {
	// Check database connectivity
	err := h.repo.Ping(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":    "unhealthy",
			"error":     "Database connection failed",
			"clickhouse": "down",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":     "healthy",
		"clickhouse": "up",
	})
}
