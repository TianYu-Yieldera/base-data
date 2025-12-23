package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TianYu-Yieldera/base-data/internal/thegraph"
	"go.uber.org/zap"
)

// Config holds the service configuration
type Config struct {
	AaveEndpoint string
	KafkaBrokers []string
	KafkaTopic   string
	SyncInterval time.Duration
}

func main() {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	// Load configuration from environment
	config := loadConfig()

	logger.Info("Starting TheGraph sync service",
		zap.String("endpoint", config.AaveEndpoint),
		zap.Strings("brokers", config.KafkaBrokers),
		zap.String("topic", config.KafkaTopic),
		zap.Duration("interval", config.SyncInterval))

	// Create sync service
	syncConfig := thegraph.SyncConfig{
		AaveEndpoint: config.AaveEndpoint,
		KafkaBrokers: config.KafkaBrokers,
		KafkaTopic:   config.KafkaTopic,
	}
	syncService := thegraph.NewSyncService(syncConfig, logger)
	defer func() {
		if err := syncService.Close(); err != nil {
			logger.Error("Failed to close sync service", zap.Error(err))
		}
	}()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start sync loop
	ticker := time.NewTicker(config.SyncInterval)
	defer ticker.Stop()

	// Initial sync
	runSync(ctx, syncService, logger)

	for {
		select {
		case <-ticker.C:
			runSync(ctx, syncService, logger)
		case sig := <-sigChan:
			logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
			cancel()
			return
		case <-ctx.Done():
			logger.Info("Context cancelled, shutting down")
			return
		}
	}
}

func runSync(ctx context.Context, syncService *thegraph.SyncService, logger *zap.Logger) {
	logger.Info("Starting sync cycle")
	startTime := time.Now()

	// Sync reserves
	if err := syncService.SyncReserves(ctx); err != nil {
		logger.Error("Failed to sync reserves", zap.Error(err))
	}

	// Sync liquidations from the last hour
	since := time.Now().Add(-1 * time.Hour)
	if err := syncService.SyncLiquidations(ctx, since); err != nil {
		logger.Error("Failed to sync liquidations", zap.Error(err))
	}

	logger.Info("Sync cycle completed", zap.Duration("duration", time.Since(startTime)))
}

func loadConfig() Config {
	aaveEndpoint := os.Getenv("THEGRAPH_AAVE_ENDPOINT")
	if aaveEndpoint == "" {
		// Default to Base mainnet Aave V3 subgraph
		aaveEndpoint = "https://api.studio.thegraph.com/query/aave/aave-v3-base/version/latest"
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "thegraph.aave"
	}

	syncIntervalStr := os.Getenv("SYNC_INTERVAL")
	syncInterval := 5 * time.Minute
	if syncIntervalStr != "" {
		if parsed, err := time.ParseDuration(syncIntervalStr); err == nil {
			syncInterval = parsed
		}
	}

	return Config{
		AaveEndpoint: aaveEndpoint,
		KafkaBrokers: []string{kafkaBrokers},
		KafkaTopic:   kafkaTopic,
		SyncInterval: syncInterval,
	}
}
