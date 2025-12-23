package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/TianYu-Yieldera/base-data/internal/handler"
	"github.com/TianYu-Yieldera/base-data/internal/middleware"
	"github.com/TianYu-Yieldera/base-data/internal/repository"
	"github.com/TianYu-Yieldera/base-data/internal/service"
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

	// Load configuration
	cfg := loadConfig()

	// Initialize repositories
	clickhouseRepo, err := repository.NewClickHouseRepo(cfg.ClickHouseDSN)
	if err != nil {
		logger.Fatal("Failed to connect to ClickHouse", zap.Error(err))
	}
	defer clickhouseRepo.Close()

	// Initialize services
	riskService := service.NewRiskService(clickhouseRepo)
	flowService := service.NewFlowService(clickhouseRepo)
	statsService := service.NewStatsService(clickhouseRepo)

	// Initialize handlers
	addressHandler := handler.NewAddressHandler(riskService, flowService, logger)
	riskHandler := handler.NewRiskHandler(riskService)
	whaleHandler := handler.NewWhaleHandler(clickhouseRepo)
	ecosystemHandler := handler.NewEcosystemHandler(statsService)
	healthHandler := handler.NewHealthHandler(clickhouseRepo)

	// Setup Gin router
	if cfg.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()

	// Middlewares
	router.Use(gin.Recovery())
	router.Use(middleware.Logger(logger))
	router.Use(middleware.CORS())
	router.Use(middleware.RateLimit(cfg.RateLimitRPS))

	// Health check endpoint
	router.GET("/health", healthHandler.Health)

	// API v1 routes
	v1 := router.Group("/api/v1")
	{
		// Address endpoints
		address := v1.Group("/address")
		{
			address.GET("/:address/risk", addressHandler.GetAddressRisk)
			address.GET("/:address/flow", addressHandler.GetAddressFlow)
			address.GET("/:address/positions", addressHandler.GetAddressPositions)
			address.GET("/:address/labels", addressHandler.GetAddressLabels)
		}

		// Risk endpoints
		risk := v1.Group("/risk")
		{
			risk.GET("/high-risk", middleware.OptionalAuth(), riskHandler.GetHighRiskAddresses)
			risk.GET("/distribution", riskHandler.GetRiskDistribution)
		}

		// Whale endpoints
		whales := v1.Group("/whales")
		{
			whales.GET("/movements", whaleHandler.GetWhaleMovements)
			whales.GET("/list", whaleHandler.GetWhaleList)
		}

		// Ecosystem endpoints
		ecosystem := v1.Group("/ecosystem")
		{
			ecosystem.GET("/stats", ecosystemHandler.GetStats)
			ecosystem.GET("/tvl", ecosystemHandler.GetTVLHistory)
		}
	}

	// Create HTTP server
	srv := &http.Server{
		Addr:         cfg.ServerAddr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		logger.Info("Starting API server", zap.String("addr", cfg.ServerAddr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Failed to start server", zap.Error(err))
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Fatal("Server forced to shutdown", zap.Error(err))
	}

	logger.Info("Server exited properly")
}

// Config holds application configuration
type Config struct {
	Environment   string
	ServerAddr    string
	ClickHouseDSN string
	PostgresDSN   string
	RedisAddr     string
	JWTSecret     string
	RateLimitRPS  int
}

func loadConfig() *Config {
	return &Config{
		Environment:   getEnv("ENVIRONMENT", "development"),
		ServerAddr:    getEnv("SERVER_ADDR", ":8080"),
		ClickHouseDSN: getEnv("CLICKHOUSE_DSN", "tcp://localhost:9000/base_data"),
		PostgresDSN:   getEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/base_data"),
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		JWTSecret:     getEnv("JWT_SECRET", "dev-secret-key"),
		RateLimitRPS:  100,
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
