package block

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/TianYu-Yieldera/base-data/internal/checkpoint"
	"github.com/TianYu-Yieldera/base-data/internal/config"
	"github.com/TianYu-Yieldera/base-data/internal/ethereum"
	"github.com/TianYu-Yieldera/base-data/internal/kafka"
	"github.com/TianYu-Yieldera/base-data/internal/metrics"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
	"go.uber.org/zap"
)

const (
	IndexerName = "block-indexer"
)

type Indexer struct {
	cfg        *config.Config
	ethClient  *ethereum.Client
	producer   *kafka.Producer
	checkpoint *checkpoint.Manager
	logger     *zap.Logger
	chainName  string
}

func NewIndexer(
	cfg *config.Config,
	ethClient *ethereum.Client,
	producer *kafka.Producer,
	checkpointMgr *checkpoint.Manager,
	logger *zap.Logger,
) *Indexer {
	return &Indexer{
		cfg:        cfg,
		ethClient:  ethClient,
		producer:   producer,
		checkpoint: checkpointMgr,
		logger:     logger.Named(IndexerName),
		chainName:  "base",
	}
}

func (i *Indexer) Start(ctx context.Context) error {
	i.logger.Info("starting block indexer",
		zap.Int64("chain_id", i.cfg.Chain.ChainID),
		zap.Int64("confirmation_depth", i.cfg.Chain.ConfirmationDepth))

	startBlock, err := i.getStartBlock(ctx)
	if err != nil {
		return fmt.Errorf("failed to get start block: %w", err)
	}

	i.logger.Info("starting from block", zap.Int64("block", startBlock))

	ticker := time.NewTicker(i.cfg.Indexer.BlockIndexer.PollInterval)
	defer ticker.Stop()

	currentBlock := startBlock

	for {
		select {
		case <-ctx.Done():
			i.logger.Info("block indexer stopped")
			return ctx.Err()
		case <-ticker.C:
			confirmedBlock, err := i.ethClient.GetConfirmedBlockNumber(ctx, i.cfg.Chain.ConfirmationDepth)
			if err != nil {
				i.logger.Error("failed to get confirmed block", zap.Error(err))
				continue
			}

			if currentBlock > confirmedBlock {
				continue
			}

			endBlock := currentBlock + int64(i.cfg.Indexer.BlockIndexer.BatchSize) - 1
			if endBlock > confirmedBlock {
				endBlock = confirmedBlock
			}

			if err := i.processBlockRange(ctx, currentBlock, endBlock); err != nil {
				i.logger.Error("failed to process block range",
					zap.Int64("start", currentBlock),
					zap.Int64("end", endBlock),
					zap.Error(err))
				continue
			}

			currentBlock = endBlock + 1

			metrics.BlockLag.WithLabelValues(i.chainName, IndexerName).Set(float64(confirmedBlock - currentBlock + 1))
		}
	}
}

func (i *Indexer) getStartBlock(ctx context.Context) (int64, error) {
	if i.cfg.Indexer.BlockIndexer.StartBlock > 0 {
		return i.cfg.Indexer.BlockIndexer.StartBlock, nil
	}

	lastBlock, err := i.checkpoint.GetLastBlockNumber(ctx, IndexerName, i.chainName, 0)
	if err != nil {
		return 0, err
	}

	if lastBlock > 0 {
		return lastBlock + 1, nil
	}

	confirmedBlock, err := i.ethClient.GetConfirmedBlockNumber(ctx, i.cfg.Chain.ConfirmationDepth)
	if err != nil {
		return 0, err
	}

	return confirmedBlock, nil
}

func (i *Indexer) processBlockRange(ctx context.Context, startBlock, endBlock int64) error {
	start := time.Now()
	defer func() {
		metrics.ProcessingDuration.WithLabelValues(i.chainName, IndexerName, "process_range").Observe(time.Since(start).Seconds())
	}()

	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		block, err := i.ethClient.BlockByNumber(ctx, big.NewInt(blockNum))
		if err != nil {
			return fmt.Errorf("failed to get block %d: %w", blockNum, err)
		}

		blockMsg := i.convertBlock(block)

		if err := i.producer.Send(ctx, i.cfg.Kafka.Topics.Blocks, blockMsg.BlockHash, blockMsg); err != nil {
			return fmt.Errorf("failed to send block %d to kafka: %w", blockNum, err)
		}

		metrics.BlocksProcessed.WithLabelValues(i.chainName, IndexerName).Inc()
		metrics.LastBlockNumber.WithLabelValues(i.chainName, IndexerName).Set(float64(blockNum))
		metrics.KafkaMessagesProduced.WithLabelValues(i.cfg.Kafka.Topics.Blocks).Inc()

		if err := i.checkpoint.UpdateBlockNumber(ctx, IndexerName, i.chainName, blockNum, blockMsg.BlockHash); err != nil {
			i.logger.Warn("failed to update checkpoint", zap.Error(err))
		}
		metrics.CheckpointUpdates.WithLabelValues(i.chainName, IndexerName).Inc()
	}

	i.logger.Debug("processed block range",
		zap.Int64("start", startBlock),
		zap.Int64("end", endBlock),
		zap.Duration("duration", time.Since(start)))

	return nil
}

func (i *Indexer) convertBlock(block interface{}) *models.Block {
	b := block.(interface {
		Number() *big.Int
		Hash() interface{ Hex() string }
		ParentHash() interface{ Hex() string }
		Time() uint64
		Transactions() interface{ Len() int }
		GasUsed() uint64
		GasLimit() uint64
		BaseFee() *big.Int
		Coinbase() interface{ Hex() string }
	})

	var baseFee *string
	if b.BaseFee() != nil {
		fee := b.BaseFee().String()
		baseFee = &fee
	}

	return &models.Block{
		ChainID:       i.cfg.Chain.ChainID,
		BlockNumber:   b.Number().Int64(),
		BlockHash:     b.Hash().Hex(),
		ParentHash:    b.ParentHash().Hex(),
		Timestamp:     time.Unix(int64(b.Time()), 0).UTC(),
		TxCount:       int32(b.Transactions().Len()),
		GasUsed:       int64(b.GasUsed()),
		GasLimit:      int64(b.GasLimit()),
		BaseFeePerGas: baseFee,
		Miner:         b.Coinbase().Hex(),
		IndexedAt:     time.Now().UTC(),
	}
}

func (i *Indexer) Backfill(ctx context.Context, startBlock, endBlock int64) error {
	i.logger.Info("starting backfill",
		zap.Int64("start", startBlock),
		zap.Int64("end", endBlock))

	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		if err := i.processBlockRange(ctx, blockNum, blockNum); err != nil {
			return fmt.Errorf("backfill failed at block %d: %w", blockNum, err)
		}

		if blockNum%1000 == 0 {
			i.logger.Info("backfill progress",
				zap.Int64("current", blockNum),
				zap.Int64("end", endBlock),
				zap.Float64("progress", float64(blockNum-startBlock)/float64(endBlock-startBlock)*100))
		}
	}

	i.logger.Info("backfill completed",
		zap.Int64("start", startBlock),
		zap.Int64("end", endBlock))

	return nil
}
