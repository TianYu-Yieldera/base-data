package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/TianYu-Yieldera/base-data/internal/alert/consumer"
	"github.com/TianYu-Yieldera/base-data/internal/alert/dispatcher"
	"github.com/TianYu-Yieldera/base-data/internal/alert/processor"
)

func main() {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	logger.Info("Starting Alert Service...")

	// Configuration (from environment)
	brokers := []string{getEnv("KAFKA_BROKERS", "localhost:9092")}
	topic := getEnv("KAFKA_TOPIC", "alerts.risk")
	groupID := getEnv("KAFKA_GROUP_ID", "alert-service")

	// Create dispatcher (log-only for now)
	disp := dispatcher.NewLogDispatcher(logger)
	logger.Info("Using dispatcher", zap.String("name", disp.Name()))

	// Create processor
	proc := processor.NewAlertProcessor(disp, logger)

	// Create consumer
	cons := consumer.NewKafkaConsumer(brokers, topic, groupID, proc, logger)

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("Shutdown signal received")
		cancel()
	}()

	// Start consuming
	if err := cons.Start(ctx); err != nil {
		logger.Fatal("Consumer error", zap.Error(err))
	}

	logger.Info("Alert Service stopped")
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
