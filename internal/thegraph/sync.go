package thegraph

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// SyncService syncs data from TheGraph to Kafka
type SyncService struct {
	aaveClient  *AaveClient
	kafkaWriter *kafka.Writer
	logger      *zap.Logger
}

// SyncConfig holds sync service configuration
type SyncConfig struct {
	AaveEndpoint string
	KafkaBrokers []string
	KafkaTopic   string
}

// NewSyncService creates a new sync service
func NewSyncService(config SyncConfig, logger *zap.Logger) *SyncService {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(config.KafkaBrokers...),
		Topic:    config.KafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}

	return &SyncService{
		aaveClient:  NewAaveClient(config.AaveEndpoint),
		kafkaWriter: writer,
		logger:      logger,
	}
}

// SyncReserves syncs reserve data to Kafka
func (s *SyncService) SyncReserves(ctx context.Context) error {
	reserves, err := s.aaveClient.GetReserves(ctx)
	if err != nil {
		s.logger.Error("Failed to fetch reserves", zap.Error(err))
		return err
	}

	messages := make([]kafka.Message, 0, len(reserves))
	for _, reserve := range reserves {
		data, err := json.Marshal(reserve)
		if err != nil {
			s.logger.Error("Failed to marshal reserve", zap.Error(err), zap.String("id", reserve.ID))
			continue
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(reserve.ID),
			Value: data,
			Headers: []kafka.Header{
				{Key: "type", Value: []byte("reserve")},
				{Key: "source", Value: []byte("thegraph")},
				{Key: "timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
			},
		})
	}

	if len(messages) > 0 {
		if err := s.kafkaWriter.WriteMessages(ctx, messages...); err != nil {
			s.logger.Error("Failed to write reserves to Kafka", zap.Error(err))
			return err
		}
	}

	s.logger.Info("Synced reserves", zap.Int("count", len(reserves)))
	return nil
}

// SyncLiquidations syncs liquidation events to Kafka
func (s *SyncService) SyncLiquidations(ctx context.Context, since time.Time) error {
	liquidations, err := s.aaveClient.GetLiquidations(ctx, since, 1000)
	if err != nil {
		s.logger.Error("Failed to fetch liquidations", zap.Error(err))
		return err
	}

	if len(liquidations) == 0 {
		s.logger.Debug("No new liquidations found", zap.Time("since", since))
		return nil
	}

	messages := make([]kafka.Message, 0, len(liquidations))
	for _, liq := range liquidations {
		data, err := json.Marshal(liq)
		if err != nil {
			s.logger.Error("Failed to marshal liquidation", zap.Error(err), zap.String("id", liq.ID))
			continue
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(liq.ID),
			Value: data,
			Headers: []kafka.Header{
				{Key: "type", Value: []byte("liquidation")},
				{Key: "source", Value: []byte("thegraph")},
				{Key: "timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
			},
		})
	}

	if len(messages) > 0 {
		if err := s.kafkaWriter.WriteMessages(ctx, messages...); err != nil {
			s.logger.Error("Failed to write liquidations to Kafka", zap.Error(err))
			return err
		}
	}

	s.logger.Info("Synced liquidations", zap.Int("count", len(liquidations)))
	return nil
}

// SyncUserReserves syncs user reserve data for a specific address
func (s *SyncService) SyncUserReserves(ctx context.Context, userAddress string) error {
	userReserves, err := s.aaveClient.GetUserReserves(ctx, userAddress)
	if err != nil {
		s.logger.Error("Failed to fetch user reserves",
			zap.Error(err),
			zap.String("user", userAddress))
		return err
	}

	if len(userReserves) == 0 {
		s.logger.Debug("No user reserves found", zap.String("user", userAddress))
		return nil
	}

	messages := make([]kafka.Message, 0, len(userReserves))
	for _, ur := range userReserves {
		data, err := json.Marshal(ur)
		if err != nil {
			s.logger.Error("Failed to marshal user reserve", zap.Error(err), zap.String("id", ur.ID))
			continue
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(ur.ID),
			Value: data,
			Headers: []kafka.Header{
				{Key: "type", Value: []byte("user_reserve")},
				{Key: "source", Value: []byte("thegraph")},
				{Key: "user", Value: []byte(userAddress)},
				{Key: "timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
			},
		})
	}

	if len(messages) > 0 {
		if err := s.kafkaWriter.WriteMessages(ctx, messages...); err != nil {
			s.logger.Error("Failed to write user reserves to Kafka", zap.Error(err))
			return err
		}
	}

	s.logger.Info("Synced user reserves",
		zap.String("user", userAddress),
		zap.Int("count", len(userReserves)))
	return nil
}

// Close closes the sync service
func (s *SyncService) Close() error {
	return s.kafkaWriter.Close()
}
