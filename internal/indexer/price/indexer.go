package price

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
	IndexerName = "price-indexer"

	PriceScale = 1e8
)

var latestRoundDataSelector = ethcommon.HexToHash("0xfeaf968c")

type PriceFeed struct {
	Address string
	Symbol  string
	Asset   string
}

type Indexer struct {
	cfg       *config.Config
	ethClient *ethereum.Client
	producer  *kafka.Producer
	logger    *zap.Logger
	chainName string

	mu         sync.RWMutex
	priceFeeds []PriceFeed
	lastPrices map[string]float64
}

func NewIndexer(
	cfg *config.Config,
	ethClient *ethereum.Client,
	producer *kafka.Producer,
	logger *zap.Logger,
) *Indexer {
	priceFeeds := []PriceFeed{
		{
			Address: cfg.Contracts.ChainlinkETHUSD,
			Symbol:  "ETH",
			Asset:   "0x4200000000000000000000000000000000000006",
		},
	}

	return &Indexer{
		cfg:        cfg,
		ethClient:  ethClient,
		producer:   producer,
		logger:     logger.Named(IndexerName),
		chainName:  "base",
		priceFeeds: priceFeeds,
		lastPrices: make(map[string]float64),
	}
}

func (i *Indexer) Start(ctx context.Context) error {
	i.logger.Info("starting price indexer",
		zap.Int("feeds", len(i.priceFeeds)))

	ticker := time.NewTicker(i.cfg.Indexer.PriceIndexer.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			i.logger.Info("price indexer stopped")
			return ctx.Err()
		case <-ticker.C:
			i.pollPrices(ctx)
		}
	}
}

func (i *Indexer) pollPrices(ctx context.Context) {
	blockNumber, err := i.ethClient.BlockNumber(ctx)
	if err != nil {
		i.logger.Error("failed to get block number", zap.Error(err))
		return
	}

	for _, feed := range i.priceFeeds {
		price, err := i.fetchPrice(ctx, feed, int64(blockNumber))
		if err != nil {
			i.logger.Warn("failed to fetch price",
				zap.String("symbol", feed.Symbol),
				zap.Error(err))
			continue
		}

		if err := i.producer.Send(ctx, i.cfg.Kafka.Topics.Prices, price.PriceID, price); err != nil {
			i.logger.Error("failed to send price to kafka",
				zap.String("symbol", feed.Symbol),
				zap.Error(err))
			continue
		}

		metrics.PriceUpdates.WithLabelValues(i.chainName, feed.Symbol, "chainlink").Inc()
		metrics.KafkaMessagesProduced.WithLabelValues(i.cfg.Kafka.Topics.Prices).Inc()

		i.mu.Lock()
		i.lastPrices[feed.Symbol] = price.PriceUSD
		i.mu.Unlock()
	}
}

func (i *Indexer) fetchPrice(ctx context.Context, feed PriceFeed, blockNumber int64) (*models.AssetPrice, error) {
	feedAddr := ethcommon.HexToAddress(feed.Address)

	callMsg := struct {
		To   *ethcommon.Address
		Data []byte
	}{
		To:   &feedAddr,
		Data: latestRoundDataSelector.Bytes()[:4],
	}

	result, err := i.ethClient.CallContract(ctx, callMsg, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("contract call failed: %w", err)
	}

	if len(result) < 160 {
		return nil, fmt.Errorf("invalid response length")
	}

	answer := new(big.Int).SetBytes(result[32:64])

	priceUSD, _ := new(big.Float).Quo(
		new(big.Float).SetInt(answer),
		big.NewFloat(PriceScale),
	).Float64()

	blockNum := blockNumber
	priceID := fmt.Sprintf("%s-%s-%d", i.chainName, strings.ToLower(feed.Symbol), time.Now().UnixNano())

	return &models.AssetPrice{
		PriceID:      priceID,
		Chain:        i.chainName,
		AssetAddress: strings.ToLower(feed.Asset),
		AssetSymbol:  feed.Symbol,
		PriceUSD:     priceUSD,
		Source:       models.PriceSourceChainlink,
		BlockNumber:  &blockNum,
		Timestamp:    time.Now().UTC(),
		IndexedAt:    time.Now().UTC(),
	}, nil
}

func (i *Indexer) GetLastPrice(symbol string) (float64, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	price, ok := i.lastPrices[symbol]
	return price, ok
}

func (i *Indexer) AddPriceFeed(feed PriceFeed) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.priceFeeds = append(i.priceFeeds, feed)
}

func (i *Indexer) CheckStaleness(symbol string, maxAge time.Duration) bool {
	return true
}
