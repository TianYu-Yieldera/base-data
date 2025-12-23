package handler

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/TianYu-Yieldera/base-data/internal/repository"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
)

// WhaleHandler handles whale-related API endpoints
type WhaleHandler struct {
	repo *repository.ClickHouseRepo
}

// NewWhaleHandler creates a new WhaleHandler
func NewWhaleHandler(repo *repository.ClickHouseRepo) *WhaleHandler {
	return &WhaleHandler{repo: repo}
}

// GetWhaleMovements handles GET /whales/movements
func (h *WhaleHandler) GetWhaleMovements(c *gin.Context) {
	hoursStr := c.DefaultQuery("hours", "24")
	minUSDStr := c.DefaultQuery("min_usd", "100000")
	direction := c.DefaultQuery("direction", "all")

	hours, err := strconv.Atoi(hoursStr)
	if err != nil || hours <= 0 {
		hours = 24
	}
	if hours > 168 { // Max 7 days
		hours = 168
	}

	minUSD, err := strconv.ParseFloat(minUSDStr, 64)
	if err != nil || minUSD < 0 {
		minUSD = 100000
	}

	// Validate direction
	if direction != "in" && direction != "out" && direction != "all" {
		direction = "all"
	}

	movements, err := h.repo.GetWhaleMovements(c.Request.Context(), hours, minUSD, direction)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Code:    "INTERNAL_ERROR",
			Message: "Failed to retrieve whale movements",
		})
		return
	}

	c.JSON(http.StatusOK, movements)
}

// GetWhaleList handles GET /whales/list
func (h *WhaleHandler) GetWhaleList(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "100")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	whales, err := h.repo.GetWhaleList(c.Request.Context(), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Code:    "INTERNAL_ERROR",
			Message: "Failed to retrieve whale list",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"total_count": len(whales),
		"whales":      whales,
	})
}
