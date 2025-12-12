package position

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"

	"github.com/TianYu-Yieldera/base-data/internal/config"
	"github.com/TianYu-Yieldera/base-data/internal/ethereum"
	"github.com/TianYu-Yieldera/base-data/internal/kafka"
	"github.com/TianYu-Yieldera/base-data/internal/metrics"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
	"go.uber.org/zap"
)

const (
	IndexerName = "position-indexer"

	HealthFactorScale = 1e18
	USDScale          = 1e8

	HighRiskThreshold = 1.5
	CriticalThreshold = 1.2
)

var getUserAccountDataSelector = ethcommon.HexToHash("0xbf92857c")

type Indexer struct {
	cfg       *config.Config
	ethClient *ethereum.Client
	producer  *kafka.Producer
	logger    *zap.Logger
	chainName string

	mu              sync.RWMutex
	activeAddresses map[string]time.Time
	highRiskAddresses map[string]float64
}

func NewIndexer(
	cfg *config.Config,
	ethClient *ethereum.Client,
	producer *kafka.Producer,
	logger *zap.Logger,
) *Indexer {
	return &Indexer{
		cfg:               cfg,
		ethClient:         ethClient,
		producer:          producer,
		logger:            logger.Named(IndexerName),
		chainName:         "base",
		activeAddresses:   make(map[string]time.Time),
		highRiskAddresses: make(map[string]float64),
	}
}

func (i *Indexer) Start(ctx context.Context) error {
	i.logger.Info("starting position indexer",
		zap.String("aave_pool", i.cfg.Contracts.AavePool))

	go i.pollHighRisk(ctx)

	go i.pollActive(ctx)

	go i.pollInactive(ctx)

	<-ctx.Done()
	i.logger.Info("position indexer stopped")
	return ctx.Err()
}

func (i *Indexer) pollHighRisk(ctx context.Context) {
	ticker := time.NewTicker(i.cfg.Indexer.PositionIndexer.HighRiskPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i.mu.RLock()
			addresses := make([]string, 0, len(i.highRiskAddresses))
			for addr := range i.highRiskAddresses {
				addresses = append(addresses, addr)
			}
			i.mu.RUnlock()

			i.pollAddresses(ctx, addresses)
		}
	}
}

func (i *Indexer) pollActive(ctx context.Context) {
	ticker := time.NewTicker(i.cfg.Indexer.PositionIndexer.ActivePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-24 * time.Hour)

			i.mu.RLock()
			addresses := make([]string, 0)
			for addr, lastActive := range i.activeAddresses {
				if lastActive.After(cutoff) {
					if _, isHighRisk := i.highRiskAddresses[addr]; !isHighRisk {
						addresses = append(addresses, addr)
					}
				}
			}
			i.mu.RUnlock()

			i.pollAddresses(ctx, addresses)
		}
	}
}

func (i *Indexer) pollInactive(ctx context.Context) {
	ticker := time.NewTicker(i.cfg.Indexer.PositionIndexer.InactivePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-24 * time.Hour)

			i.mu.RLock()
			addresses := make([]string, 0)
			for addr, lastActive := range i.activeAddresses {
				if lastActive.Before(cutoff) {
					if _, isHighRisk := i.highRiskAddresses[addr]; !isHighRisk {
						addresses = append(addresses, addr)
					}
				}
			}
			i.mu.RUnlock()

			i.pollAddresses(ctx, addresses)
		}
	}
}

func (i *Indexer) pollAddresses(ctx context.Context, addresses []string) {
	batchSize := i.cfg.Indexer.PositionIndexer.BatchSize
	for j := 0; j < len(addresses); j += batchSize {
		end := j + batchSize
		if end > len(addresses) {
			end = len(addresses)
		}

		batch := addresses[j:end]
		for _, addr := range batch {
			if err := i.pollPosition(ctx, addr); err != nil {
				i.logger.Debug("failed to poll position",
					zap.String("address", addr),
					zap.Error(err))
			}
		}
	}
}

func (i *Indexer) pollPosition(ctx context.Context, address string) error {
	start := time.Now()
	defer func() {
		metrics.ProcessingDuration.WithLabelValues(i.chainName, IndexerName, "poll_position").Observe(time.Since(start).Seconds())
	}()

	blockNumber, err := i.ethClient.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get block number: %w", err)
	}

	snapshot, err := i.getUserAccountData(ctx, address, int64(blockNumber))
	if err != nil {
		return fmt.Errorf("failed to get account data: %w", err)
	}

	if snapshot.TotalCollateralUSD == 0 && snapshot.TotalDebtUSD == 0 {
		return nil
	}

	if err := i.producer.Send(ctx, i.cfg.Kafka.Topics.Positions, snapshot.SnapshotID, snapshot); err != nil {
		return fmt.Errorf("failed to send position to kafka: %w", err)
	}

	metrics.PositionsPolled.WithLabelValues(i.chainName, "aave_v3").Inc()
	metrics.KafkaMessagesProduced.WithLabelValues(i.cfg.Kafka.Topics.Positions).Inc()

	i.updateRiskClassification(address, snapshot.HealthFactor)

	return nil
}

func (i *Indexer) getUserAccountData(ctx context.Context, address string, blockNumber int64) (*models.PositionSnapshot, error) {
	poolAddr := ethcommon.HexToAddress(i.cfg.Contracts.AavePool)
	userAddr := ethcommon.HexToAddress(address)

	data := make([]byte, 36)
	copy(data[0:4], getUserAccountDataSelector.Bytes()[:4])
	copy(data[16:36], userAddr.Bytes())

	callMsg := struct {
		To   *ethcommon.Address
		Data []byte
	}{
		To:   &poolAddr,
		Data: data,
	}

	result, err := i.ethClient.CallContract(ctx, callMsg, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("contract call failed: %w", err)
	}

	if len(result) < 192 {
		return nil, fmt.Errorf("invalid response length")
	}

	totalCollateralBase := new(big.Int).SetBytes(result[0:32])
	totalDebtBase := new(big.Int).SetBytes(result[32:64])
	availableBorrowsBase := new(big.Int).SetBytes(result[64:96])
	currentLiquidationThreshold := new(big.Int).SetBytes(result[96:128])
	ltv := new(big.Int).SetBytes(result[128:160])
	healthFactor := new(big.Int).SetBytes(result[160:192])

	totalCollateralUSD, _ := new(big.Float).Quo(
		new(big.Float).SetInt(totalCollateralBase),
		big.NewFloat(USDScale),
	).Float64()

	totalDebtUSD, _ := new(big.Float).Quo(
		new(big.Float).SetInt(totalDebtBase),
		big.NewFloat(USDScale),
	).Float64()

	availableBorrowUSD, _ := new(big.Float).Quo(
		new(big.Float).SetInt(availableBorrowsBase),
		big.NewFloat(USDScale),
	).Float64()

	hf, _ := new(big.Float).Quo(
		new(big.Float).SetInt(healthFactor),
		big.NewFloat(HealthFactorScale),
	).Float64()

	ltvFloat, _ := new(big.Float).Quo(
		new(big.Float).SetInt(ltv),
		big.NewFloat(10000),
	).Float64()

	liquidationThreshold, _ := new(big.Float).Quo(
		new(big.Float).SetInt(currentLiquidationThreshold),
		big.NewFloat(10000),
	).Float64()

	snapshotID := fmt.Sprintf("%s-%s-%d", i.chainName, strings.ToLower(address), time.Now().UnixNano())

	return &models.PositionSnapshot{
		SnapshotID:                  snapshotID,
		Chain:                       i.chainName,
		Protocol:                    "aave_v3",
		UserAddress:                 strings.ToLower(address),
		TotalCollateralETH:          totalCollateralBase.String(),
		TotalCollateralUSD:          totalCollateralUSD,
		TotalDebtETH:                totalDebtBase.String(),
		TotalDebtUSD:                totalDebtUSD,
		AvailableBorrowETH:          availableBorrowsBase.String(),
		HealthFactor:                hf,
		LTV:                         ltvFloat,
		CurrentLiquidationThreshold: liquidationThreshold,
		BlockNumber:                 blockNumber,
		Timestamp:                   time.Now().UTC(),
		IndexedAt:                   time.Now().UTC(),
	}, nil
}

func (i *Indexer) updateRiskClassification(address string, healthFactor float64) {
	i.mu.Lock()
	defer i.mu.Unlock()

	addr := strings.ToLower(address)

	if healthFactor < CriticalThreshold {
		i.highRiskAddresses[addr] = healthFactor
		metrics.HighRiskPositions.WithLabelValues(i.chainName, "aave_v3", "critical").Inc()
	} else if healthFactor < HighRiskThreshold {
		i.highRiskAddresses[addr] = healthFactor
		metrics.HighRiskPositions.WithLabelValues(i.chainName, "aave_v3", "warning").Inc()
	} else {
		delete(i.highRiskAddresses, addr)
	}
}

func (i *Indexer) AddActiveAddress(address string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.activeAddresses[strings.ToLower(address)] = time.Now()
}

func (i *Indexer) GetHighRiskAddresses() map[string]float64 {
	i.mu.RLock()
	defer i.mu.RUnlock()

	result := make(map[string]float64, len(i.highRiskAddresses))
	for k, v := range i.highRiskAddresses {
		result[k] = v
	}
	return result
}
