package dispatcher

import (
	"context"

	"go.uber.org/zap"

	"github.com/TianYu-Yieldera/base-data/internal/alert/processor"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
)

// Ensure dispatchers implement the processor.Dispatcher interface
var (
	_ processor.Dispatcher = (*LogDispatcher)(nil)
	_ processor.Dispatcher = (*TelegramDispatcher)(nil)
	_ processor.Dispatcher = (*SlackDispatcher)(nil)
)

// LogDispatcher is the default dispatcher that logs to stdout
type LogDispatcher struct {
	logger *zap.Logger
}

// NewLogDispatcher creates a new log dispatcher
func NewLogDispatcher(logger *zap.Logger) *LogDispatcher {
	return &LogDispatcher{logger: logger}
}

// Name returns the dispatcher name
func (d *LogDispatcher) Name() string {
	return "log"
}

// Dispatch logs the alert
func (d *LogDispatcher) Dispatch(ctx context.Context, alert *models.StreamingRiskAlert) error {
	d.logger.Info("Alert dispatched",
		zap.String("dispatcher", d.Name()),
		zap.String("alert_type", alert.AlertType),
		zap.String("user_address", alert.UserAddress),
		zap.String("severity", alert.Severity),
	)
	return nil
}

// TelegramDispatcher is reserved for future implementation
type TelegramDispatcher struct {
	logger   *zap.Logger
	botToken string
	chatID   int64
}

// NewTelegramDispatcher creates a new Telegram dispatcher
func NewTelegramDispatcher(logger *zap.Logger, botToken string, chatID int64) *TelegramDispatcher {
	return &TelegramDispatcher{
		logger:   logger,
		botToken: botToken,
		chatID:   chatID,
	}
}

// Name returns the dispatcher name
func (d *TelegramDispatcher) Name() string {
	return "telegram"
}

// Dispatch sends alert via Telegram (not implemented)
func (d *TelegramDispatcher) Dispatch(ctx context.Context, alert *models.StreamingRiskAlert) error {
	// TODO: Implement Telegram notification
	d.logger.Info("Telegram dispatch not implemented",
		zap.String("dispatcher", d.Name()),
	)
	return nil
}

// SlackDispatcher is reserved for future implementation
type SlackDispatcher struct {
	logger     *zap.Logger
	webhookURL string
}

// NewSlackDispatcher creates a new Slack dispatcher
func NewSlackDispatcher(logger *zap.Logger, webhookURL string) *SlackDispatcher {
	return &SlackDispatcher{
		logger:     logger,
		webhookURL: webhookURL,
	}
}

// Name returns the dispatcher name
func (d *SlackDispatcher) Name() string {
	return "slack"
}

// Dispatch sends alert via Slack (not implemented)
func (d *SlackDispatcher) Dispatch(ctx context.Context, alert *models.StreamingRiskAlert) error {
	// TODO: Implement Slack notification
	d.logger.Info("Slack dispatch not implemented",
		zap.String("dispatcher", d.Name()),
	)
	return nil
}
