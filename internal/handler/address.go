package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/TianYu-Yieldera/base-data/internal/service"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
	"github.com/TianYu-Yieldera/base-data/pkg/validator"
)

// AddressHandler handles address-related API endpoints
type AddressHandler struct {
	riskService *service.RiskService
	flowService *service.FlowService
	logger      *zap.Logger
}

// NewAddressHandler creates a new AddressHandler
func NewAddressHandler(rs *service.RiskService, fs *service.FlowService, logger *zap.Logger) *AddressHandler {
	return &AddressHandler{
		riskService: rs,
		flowService: fs,
		logger:      logger,
	}
}

// GetAddressRisk handles GET /address/:address/risk
func (h *AddressHandler) GetAddressRisk(c *gin.Context) {
	address := c.Param("address")

	// Validate address format
	if !validator.IsValidAddress(address) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Code:    "INVALID_ADDRESS",
			Message: "Invalid Ethereum address format",
		})
		return
	}

	// Normalize address
	address = validator.NormalizeAddress(address)

	// Get risk info
	risk, err := h.riskService.GetAddressRisk(c.Request.Context(), address)
	if err != nil {
		if errors.Is(err, service.ErrAddressNotFound) {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Code:    "ADDRESS_NOT_FOUND",
				Message: "No position found for this address",
			})
			return
		}
		h.logger.Error("Failed to get address risk",
			zap.String("address", address),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Code:    "INTERNAL_ERROR",
			Message: "Failed to retrieve risk information",
		})
		return
	}

	c.JSON(http.StatusOK, risk)
}

// GetAddressFlow handles GET /address/:address/flow
func (h *AddressHandler) GetAddressFlow(c *gin.Context) {
	address := c.Param("address")
	days := c.DefaultQuery("days", "30")
	token := c.Query("token")

	if !validator.IsValidAddress(address) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Code:    "INVALID_ADDRESS",
			Message: "Invalid Ethereum address format",
		})
		return
	}

	// Validate token address if provided
	if token != "" && !validator.IsValidAddress(token) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Code:    "INVALID_TOKEN_ADDRESS",
			Message: "Invalid token address format",
		})
		return
	}

	address = validator.NormalizeAddress(address)
	if token != "" {
		token = validator.NormalizeAddress(token)
	}

	flow, err := h.flowService.GetAddressFlow(c.Request.Context(), address, days, token)
	if err != nil {
		h.logger.Error("Failed to get address flow",
			zap.String("address", address),
			zap.String("days", days),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Code:    "INTERNAL_ERROR",
			Message: "Failed to retrieve flow information",
		})
		return
	}

	c.JSON(http.StatusOK, flow)
}

// GetAddressPositions handles GET /address/:address/positions
func (h *AddressHandler) GetAddressPositions(c *gin.Context) {
	address := c.Param("address")

	if !validator.IsValidAddress(address) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Code:    "INVALID_ADDRESS",
			Message: "Invalid Ethereum address format",
		})
		return
	}

	address = validator.NormalizeAddress(address)

	// Get risk info which contains position data
	risk, err := h.riskService.GetAddressRisk(c.Request.Context(), address)
	if err != nil {
		if errors.Is(err, service.ErrAddressNotFound) {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Code:    "ADDRESS_NOT_FOUND",
				Message: "No position found for this address",
			})
			return
		}
		h.logger.Error("Failed to get address positions",
			zap.String("address", address),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Code:    "INTERNAL_ERROR",
			Message: "Failed to retrieve position information",
		})
		return
	}

	c.JSON(http.StatusOK, risk)
}

// GetAddressLabels handles GET /address/:address/labels
func (h *AddressHandler) GetAddressLabels(c *gin.Context) {
	address := c.Param("address")

	if !validator.IsValidAddress(address) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Code:    "INVALID_ADDRESS",
			Message: "Invalid Ethereum address format",
		})
		return
	}

	// For now, return empty labels array
	// In production, this would query PostgreSQL
	c.JSON(http.StatusOK, []models.AddressLabel{})
}
