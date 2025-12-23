package consumer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/TianYu-Yieldera/base-data/internal/alert/processor"
)

// KafkaConsumer consumes messages from Kafka
type KafkaConsumer struct {
	reader    *kafka.Reader
	processor *processor.AlertProcessor
	logger    *zap.Logger
}

// NewKafkaConsumer creates a new Kafka consumer
func NewKafkaConsumer(brokers []string, topic, groupID string, proc *processor.AlertProcessor, logger *zap.Logger) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1e3,  // 1KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
	})

	return &KafkaConsumer{
		reader:    reader,
		processor: proc,
		logger:    logger,
	}
}

// Start starts consuming messages
func (c *KafkaConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting Kafka consumer...")

	for {
		// ReadMessage blocks until a message is available or context is cancelled
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			// Check if context was cancelled (graceful shutdown)
			if ctx.Err() != nil {
				c.logger.Info("Context cancelled, stopping consumer...")
				return c.reader.Close()
			}
			c.logger.Error("Error reading message", zap.Error(err))
			// Brief sleep to avoid tight loop on persistent errors
			time.Sleep(100 * time.Millisecond)
			continue
		}

		c.logger.Debug("Received message",
			zap.String("topic", msg.Topic),
			zap.Int("partition", msg.Partition),
			zap.Int64("offset", msg.Offset),
		)

		if err := c.processor.Process(ctx, msg.Value); err != nil {
			c.logger.Error("Error processing message",
				zap.Error(err),
				zap.Int64("offset", msg.Offset),
			)
		}
	}
}
