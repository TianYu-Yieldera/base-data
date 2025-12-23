package processor

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"

	"github.com/TianYu-Yieldera/base-data/pkg/models"
)

// Dispatcher defines the interface for alert notification channels
type Dispatcher interface {
	Dispatch(ctx context.Context, alert *models.StreamingRiskAlert) error
	Name() string
}

// AlertProcessor processes alerts from Kafka
type AlertProcessor struct {
	dispatcher Dispatcher
	logger     *zap.Logger
}

// NewAlertProcessor creates a new processor
func NewAlertProcessor(disp Dispatcher, logger *zap.Logger) *AlertProcessor {
	return &AlertProcessor{
		dispatcher: disp,
		logger:     logger,
	}
}

// Process handles incoming alert messages
func (p *AlertProcessor) Process(ctx context.Context, msg []byte) error {
	var alert models.StreamingRiskAlert
	if err := json.Unmarshal(msg, &alert); err != nil {
		p.logger.Error("Failed to unmarshal alert", zap.Error(err))
		return err
	}

	// Log the alert (primary output for this phase)
	p.logger.Info("ALERT received",
		zap.String("severity", alert.Severity),
		zap.String("alert_type", alert.AlertType),
		zap.String("user_address", truncateAddress(alert.UserAddress)),
		zap.Float64("prev_hf", alert.Details.PreviousHealthFactor),
		zap.Float64("curr_hf", alert.Details.CurrentHealthFactor),
		zap.String("prev_risk", alert.Details.PreviousRiskLevel),
		zap.String("curr_risk", alert.Details.CurrentRiskLevel),
	)

	// Dispatch via interface (currently no-op, reserved for future)
	if p.dispatcher != nil {
		return p.dispatcher.Dispatch(ctx, &alert)
	}

	return nil
}

func truncateAddress(addr string) string {
	if len(addr) <= 10 {
		return addr
	}
	return addr[:6] + "..." + addr[len(addr)-4:]
}
