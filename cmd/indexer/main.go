package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/TianYu-Yieldera/base-data/internal/checkpoint"
	"github.com/TianYu-Yieldera/base-data/internal/config"
	"github.com/TianYu-Yieldera/base-data/internal/ethereum"
	"github.com/TianYu-Yieldera/base-data/internal/indexer/block"
	"github.com/TianYu-Yieldera/base-data/internal/indexer/event"
	"github.com/TianYu-Yieldera/base-data/internal/indexer/position"
	"github.com/TianYu-Yieldera/base-data/internal/indexer/price"
	"github.com/TianYu-Yieldera/base-data/internal/kafka"

	"github.com/IBM/sarama"
)

var (
	configPath  = flag.String("config", "", "Path to configuration file")
	logLevel    = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	backfill    = flag.Bool("backfill", false, "Run in backfill mode")
	startBlock  = flag.Int64("start-block", 0, "Start block for backfill")
	endBlock    = flag.Int64("end-block", 0, "End block for backfill")
)

func main() {
	flag.Parse()

	logger := initLogger(*logLevel)
	defer func() { _ = logger.Sync() }()

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ethClient, err := ethereum.NewClient(ctx, cfg.Chain.RPCURL, cfg.Chain.WSURL, logger)
	if err != nil {
		logger.Fatal("failed to create ethereum client", zap.Error(err))
	}
	defer ethClient.Close()

	logger.Info("connected to ethereum",
		zap.Int64("chain_id", ethClient.ChainID().Int64()))

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Fatal("failed to connect to redis", zap.Error(err))
	}

	checkpointMgr := checkpoint.NewManager(redisClient, "base-data", logger)

	compressionCodec := sarama.CompressionSnappy
	switch cfg.Kafka.Producer.CompressionType {
	case "gzip":
		compressionCodec = sarama.CompressionGZIP
	case "lz4":
		compressionCodec = sarama.CompressionLZ4
	case "zstd":
		compressionCodec = sarama.CompressionZSTD
	}

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:         cfg.Kafka.Brokers,
		BatchSize:       cfg.Kafka.Producer.BatchSize,
		BatchTimeout:    cfg.Kafka.Producer.BatchTimeout,
		MaxRetries:      cfg.Kafka.Producer.MaxRetries,
		RequiredAcks:    sarama.RequiredAcks(cfg.Kafka.Producer.RequiredAcks),
		CompressionType: compressionCodec,
	}, logger)
	if err != nil {
		logger.Fatal("failed to create kafka producer", zap.Error(err))
	}
	defer producer.Close()

	if cfg.Metrics.Enabled {
		go func() {
			mux := http.NewServeMux()
			mux.Handle(cfg.Metrics.Path, promhttp.Handler())
			mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("OK"))
			})

			addr := fmt.Sprintf(":%d", cfg.Metrics.Port)
			logger.Info("starting metrics server", zap.String("addr", addr))
			if err := http.ListenAndServe(addr, mux); err != nil {
				logger.Error("metrics server error", zap.Error(err))
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("received shutdown signal")
		cancel()
	}()

	if *backfill {
		runBackfill(ctx, cfg, ethClient, producer, checkpointMgr, logger)
		return
	}

	runIndexers(ctx, cfg, ethClient, producer, checkpointMgr, logger)
}

func initLogger(level string) *zap.Logger {
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}

	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(zapLevel),
		Development: false,
		Encoding:    "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "timestamp",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "message",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, _ := config.Build()
	return logger
}

func runIndexers(
	ctx context.Context,
	cfg *config.Config,
	ethClient *ethereum.Client,
	producer *kafka.Producer,
	checkpointMgr *checkpoint.Manager,
	logger *zap.Logger,
) {
	errCh := make(chan error, 4)

	if cfg.Indexer.BlockIndexer.Enabled {
		blockIndexer := block.NewIndexer(cfg, ethClient, producer, checkpointMgr, logger)
		go func() {
			if err := blockIndexer.Start(ctx); err != nil {
				errCh <- fmt.Errorf("block indexer error: %w", err)
			}
		}()
		logger.Info("started block indexer")
	}

	if cfg.Indexer.EventIndexer.Enabled {
		eventIndexer, err := event.NewIndexer(cfg, ethClient, producer, checkpointMgr, logger)
		if err != nil {
			logger.Fatal("failed to create event indexer", zap.Error(err))
		}
		go func() {
			if err := eventIndexer.Start(ctx); err != nil {
				errCh <- fmt.Errorf("event indexer error: %w", err)
			}
		}()
		logger.Info("started event indexer")
	}

	if cfg.Indexer.PositionIndexer.Enabled {
		positionIndexer := position.NewIndexer(cfg, ethClient, producer, logger)
		go func() {
			if err := positionIndexer.Start(ctx); err != nil {
				errCh <- fmt.Errorf("position indexer error: %w", err)
			}
		}()
		logger.Info("started position indexer")
	}

	if cfg.Indexer.PriceIndexer.Enabled {
		priceIndexer := price.NewIndexer(cfg, ethClient, producer, logger)
		go func() {
			if err := priceIndexer.Start(ctx); err != nil {
				errCh <- fmt.Errorf("price indexer error: %w", err)
			}
		}()
		logger.Info("started price indexer")
	}

	select {
	case err := <-errCh:
		logger.Error("indexer error", zap.Error(err))
	case <-ctx.Done():
		logger.Info("shutting down indexers")
	}
}

func runBackfill(
	ctx context.Context,
	cfg *config.Config,
	ethClient *ethereum.Client,
	producer *kafka.Producer,
	checkpointMgr *checkpoint.Manager,
	logger *zap.Logger,
) {
	if *startBlock == 0 || *endBlock == 0 {
		logger.Fatal("start-block and end-block are required for backfill mode")
	}

	logger.Info("starting backfill",
		zap.Int64("start_block", *startBlock),
		zap.Int64("end_block", *endBlock))

	blockIndexer := block.NewIndexer(cfg, ethClient, producer, checkpointMgr, logger)
	if err := blockIndexer.Backfill(ctx, *startBlock, *endBlock); err != nil {
		logger.Fatal("backfill failed", zap.Error(err))
	}

	logger.Info("backfill completed")
}
