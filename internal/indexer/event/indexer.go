package event

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/TianYu-Yieldera/base-data/internal/checkpoint"
	"github.com/TianYu-Yieldera/base-data/internal/config"
	"github.com/TianYu-Yieldera/base-data/internal/decoder"
	ethclient "github.com/TianYu-Yieldera/base-data/internal/ethereum"
	"github.com/TianYu-Yieldera/base-data/internal/kafka"
	"github.com/TianYu-Yieldera/base-data/internal/metrics"
	"github.com/TianYu-Yieldera/base-data/pkg/models"
	"go.uber.org/zap"
)

const (
	IndexerName = "event-indexer"
)

type Indexer struct {
	cfg        *config.Config
	ethClient  *ethclient.Client
	producer   *kafka.Producer
	checkpoint *checkpoint.Manager
	decoder    *decoder.Decoder
	logger     *zap.Logger
	chainName  string
}

func NewIndexer(
	cfg *config.Config,
	ethClient *ethclient.Client,
	producer *kafka.Producer,
	checkpointMgr *checkpoint.Manager,
	logger *zap.Logger,
) (*Indexer, error) {
	dec, err := decoder.NewDecoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create decoder: %w", err)
	}

	return &Indexer{
		cfg:        cfg,
		ethClient:  ethClient,
		producer:   producer,
		checkpoint: checkpointMgr,
		decoder:    dec,
		logger:     logger.Named(IndexerName),
		chainName:  "base",
	}, nil
}

func (i *Indexer) Start(ctx context.Context) error {
	i.logger.Info("starting event indexer",
		zap.Int64("chain_id", i.cfg.Chain.ChainID),
		zap.String("aave_pool", i.cfg.Contracts.AavePool))

	startBlock, err := i.getStartBlock(ctx)
	if err != nil {
		return fmt.Errorf("failed to get start block: %w", err)
	}

	i.logger.Info("starting from block", zap.Int64("block", startBlock))

	ticker := time.NewTicker(i.cfg.Indexer.EventIndexer.PollInterval)
	defer ticker.Stop()

	currentBlock := startBlock

	for {
		select {
		case <-ctx.Done():
			i.logger.Info("event indexer stopped")
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

			endBlock := currentBlock + int64(i.cfg.Indexer.EventIndexer.BatchSize) - 1
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
	if i.cfg.Indexer.EventIndexer.StartBlock > 0 {
		return i.cfg.Indexer.EventIndexer.StartBlock, nil
	}

	lastBlock, err := i.checkpoint.GetLastBlockNumber(ctx, IndexerName, i.chainName, 0)
	if err != nil {
		return 0, err
	}

	if lastBlock > 0 {
		if err := i.verifyCheckpointHash(ctx, lastBlock); err != nil {
			i.logger.Warn("checkpoint hash verification failed, will re-index from confirmed block",
				zap.Int64("checkpoint_block", lastBlock),
				zap.Error(err))
			metrics.CheckpointHashMismatch.WithLabelValues(i.chainName, IndexerName).Inc()
			return i.getFallbackStartBlock(ctx)
		}
		return lastBlock + 1, nil
	}

	return i.getFallbackStartBlock(ctx)
}

func (i *Indexer) getFallbackStartBlock(ctx context.Context) (int64, error) {
	confirmedBlock, err := i.ethClient.GetConfirmedBlockNumber(ctx, i.cfg.Chain.ConfirmationDepth)
	if err != nil {
		return 0, err
	}
	return confirmedBlock, nil
}

func (i *Indexer) verifyCheckpointHash(ctx context.Context, blockNumber int64) error {
	savedHash, err := i.checkpoint.GetLastBlockHash(ctx, IndexerName, i.chainName)
	if err != nil {
		return fmt.Errorf("failed to get checkpoint hash: %w", err)
	}

	if savedHash == "" {
		return nil
	}

	blockHeader, err := i.ethClient.HeaderByNumber(ctx, big.NewInt(blockNumber))
	if err != nil {
		return fmt.Errorf("failed to get block header from chain: %w", err)
	}

	chainHash := blockHeader.Hash().Hex()
	if savedHash != chainHash {
		metrics.ChainReorgDetected.WithLabelValues(i.chainName, IndexerName, "startup").Inc()
		return fmt.Errorf("hash mismatch: checkpoint=%s, chain=%s", savedHash, chainHash)
	}

	i.logger.Info("checkpoint hash verified",
		zap.Int64("block", blockNumber),
		zap.String("hash", chainHash))

	return nil
}

func (i *Indexer) processBlockRange(ctx context.Context, startBlock, endBlock int64) error {
	start := time.Now()
	defer func() {
		metrics.ProcessingDuration.WithLabelValues(i.chainName, IndexerName, "process_range").Observe(time.Since(start).Seconds())
	}()

	aavePool := common.HexToAddress(i.cfg.Contracts.AavePool)

	aaveTopics := []common.Hash{
		decoder.AaveSupplyTopic,
		decoder.AaveWithdrawTopic,
		decoder.AaveBorrowTopic,
		decoder.AaveRepayTopic,
		decoder.AaveLiquidationTopic,
		decoder.AaveFlashLoanTopic,
	}

	aaveQuery := ethereum.FilterQuery{
		FromBlock: big.NewInt(startBlock),
		ToBlock:   big.NewInt(endBlock),
		Addresses: []common.Address{aavePool},
		Topics:    [][]common.Hash{aaveTopics},
	}

	aaveLogs, err := i.ethClient.FilterLogs(ctx, aaveQuery)
	if err != nil {
		return fmt.Errorf("failed to filter Aave logs: %w", err)
	}

	for _, log := range aaveLogs {
		if err := i.processAaveEvent(ctx, log); err != nil {
			i.logger.Warn("failed to process Aave event",
				zap.String("tx_hash", log.TxHash.Hex()),
				zap.Error(err))
		}
	}

	transferQuery := ethereum.FilterQuery{
		FromBlock: big.NewInt(startBlock),
		ToBlock:   big.NewInt(endBlock),
		Topics:    [][]common.Hash{{decoder.ERC20TransferTopic}},
	}

	transferLogs, err := i.ethClient.FilterLogs(ctx, transferQuery)
	if err != nil {
		return fmt.Errorf("failed to filter transfer logs: %w", err)
	}

	for _, log := range transferLogs {
		if err := i.processTransferEvent(ctx, log); err != nil {
			i.logger.Debug("failed to process transfer event",
				zap.String("tx_hash", log.TxHash.Hex()),
				zap.Error(err))
		}
	}

	endBlockHeader, err := i.ethClient.HeaderByNumber(ctx, big.NewInt(endBlock))
	if err != nil {
		return fmt.Errorf("failed to get end block header: %w", err)
	}
	endBlockHash := endBlockHeader.Hash().Hex()

	if err := i.checkpoint.UpdateBlockNumber(ctx, IndexerName, i.chainName, endBlock, endBlockHash); err != nil {
		i.logger.Warn("failed to update checkpoint", zap.Error(err))
	}
	metrics.CheckpointUpdates.WithLabelValues(i.chainName, IndexerName).Inc()
	metrics.LastBlockNumber.WithLabelValues(i.chainName, IndexerName).Set(float64(endBlock))

	i.logger.Debug("processed block range",
		zap.Int64("start", startBlock),
		zap.Int64("end", endBlock),
		zap.Int("aave_events", len(aaveLogs)),
		zap.Int("transfers", len(transferLogs)),
		zap.Duration("duration", time.Since(start)))

	return nil
}

func (i *Indexer) processAaveEvent(ctx context.Context, log types.Log) error {
	eventType := i.decoder.GetEventType(log)
	if eventType == "" {
		return nil
	}

	blockHeader, err := i.ethClient.HeaderByNumber(ctx, big.NewInt(int64(log.BlockNumber)))
	if err != nil {
		return fmt.Errorf("failed to get block header: %w", err)
	}

	blockTime := time.Unix(int64(blockHeader.Time), 0).UTC()
	eventID := fmt.Sprintf("%s-%d", log.TxHash.Hex(), log.Index)

	var aaveEvent *models.AaveEvent

	switch eventType {
	case "Supply":
		decoded, err := i.decoder.DecodeAaveSupply(log)
		if err != nil {
			return err
		}
		aaveEvent = &models.AaveEvent{
			EventID:        eventID,
			EventType:      models.AaveEventSupply,
			Protocol:       "aave_v3",
			Chain:          i.chainName,
			UserAddress:    decoded.User.Hex(),
			ReserveAddress: decoded.Reserve.Hex(),
			Amount:         decoded.Amount.String(),
			OnBehalfOf:     strPtr(decoded.OnBehalfOf.Hex()),
			BlockNumber:    int64(log.BlockNumber),
			TxHash:         log.TxHash.Hex(),
			LogIndex:       int32(log.Index),
			Timestamp:      blockTime,
			IndexedAt:      time.Now().UTC(),
		}

	case "Withdraw":
		decoded, err := i.decoder.DecodeAaveWithdraw(log)
		if err != nil {
			return err
		}
		aaveEvent = &models.AaveEvent{
			EventID:        eventID,
			EventType:      models.AaveEventWithdraw,
			Protocol:       "aave_v3",
			Chain:          i.chainName,
			UserAddress:    decoded.User.Hex(),
			ReserveAddress: decoded.Reserve.Hex(),
			Amount:         decoded.Amount.String(),
			BlockNumber:    int64(log.BlockNumber),
			TxHash:         log.TxHash.Hex(),
			LogIndex:       int32(log.Index),
			Timestamp:      blockTime,
			IndexedAt:      time.Now().UTC(),
		}

	case "Borrow":
		decoded, err := i.decoder.DecodeAaveBorrow(log)
		if err != nil {
			return err
		}
		aaveEvent = &models.AaveEvent{
			EventID:        eventID,
			EventType:      models.AaveEventBorrow,
			Protocol:       "aave_v3",
			Chain:          i.chainName,
			UserAddress:    decoded.User.Hex(),
			ReserveAddress: decoded.Reserve.Hex(),
			Amount:         decoded.Amount.String(),
			OnBehalfOf:     strPtr(decoded.OnBehalfOf.Hex()),
			BlockNumber:    int64(log.BlockNumber),
			TxHash:         log.TxHash.Hex(),
			LogIndex:       int32(log.Index),
			Timestamp:      blockTime,
			IndexedAt:      time.Now().UTC(),
		}

	case "Repay":
		decoded, err := i.decoder.DecodeAaveRepay(log)
		if err != nil {
			return err
		}
		aaveEvent = &models.AaveEvent{
			EventID:        eventID,
			EventType:      models.AaveEventRepay,
			Protocol:       "aave_v3",
			Chain:          i.chainName,
			UserAddress:    decoded.User.Hex(),
			ReserveAddress: decoded.Reserve.Hex(),
			Amount:         decoded.Amount.String(),
			BlockNumber:    int64(log.BlockNumber),
			TxHash:         log.TxHash.Hex(),
			LogIndex:       int32(log.Index),
			Timestamp:      blockTime,
			IndexedAt:      time.Now().UTC(),
		}

	case "LiquidationCall":
		decoded, err := i.decoder.DecodeAaveLiquidation(log)
		if err != nil {
			return err
		}
		aaveEvent = &models.AaveEvent{
			EventID:         eventID,
			EventType:       models.AaveEventLiquidation,
			Protocol:        "aave_v3",
			Chain:           i.chainName,
			UserAddress:     decoded.User.Hex(),
			ReserveAddress:  decoded.DebtAsset.Hex(),
			Amount:          decoded.LiquidatedCollateralAmount.String(),
			Liquidator:      strPtr(decoded.Liquidator.Hex()),
			CollateralAsset: strPtr(decoded.CollateralAsset.Hex()),
			DebtToCover:     strPtr(decoded.DebtToCover.String()),
			BlockNumber:     int64(log.BlockNumber),
			TxHash:          log.TxHash.Hex(),
			LogIndex:        int32(log.Index),
			Timestamp:       blockTime,
			IndexedAt:       time.Now().UTC(),
		}

	case "FlashLoan":
		decoded, err := i.decoder.DecodeAaveFlashLoan(log)
		if err != nil {
			return err
		}
		aaveEvent = &models.AaveEvent{
			EventID:        eventID,
			EventType:      models.AaveEventFlashLoan,
			Protocol:       "aave_v3",
			Chain:          i.chainName,
			UserAddress:    decoded.Initiator.Hex(),
			ReserveAddress: decoded.Asset.Hex(),
			Amount:         decoded.Amount.String(),
			BlockNumber:    int64(log.BlockNumber),
			TxHash:         log.TxHash.Hex(),
			LogIndex:       int32(log.Index),
			Timestamp:      blockTime,
			IndexedAt:      time.Now().UTC(),
		}
	}

	if aaveEvent != nil {
		if err := i.producer.Send(ctx, i.cfg.Kafka.Topics.AaveEvents, aaveEvent.EventID, aaveEvent); err != nil {
			return fmt.Errorf("failed to send Aave event to kafka: %w", err)
		}
		metrics.EventsDecoded.WithLabelValues(i.chainName, IndexerName, string(aaveEvent.EventType)).Inc()
		metrics.KafkaMessagesProduced.WithLabelValues(i.cfg.Kafka.Topics.AaveEvents).Inc()
	}

	return nil
}

func (i *Indexer) processTransferEvent(ctx context.Context, log types.Log) error {
	decoded, err := i.decoder.DecodeERC20Transfer(log)
	if err != nil {
		return err
	}

	blockHeader, err := i.ethClient.HeaderByNumber(ctx, big.NewInt(int64(log.BlockNumber)))
	if err != nil {
		return fmt.Errorf("failed to get block header: %w", err)
	}

	blockTime := time.Unix(int64(blockHeader.Time), 0).UTC()
	transferID := fmt.Sprintf("%s-%d", log.TxHash.Hex(), log.Index)

	transfer := &models.ERC20Transfer{
		TransferID:   transferID,
		Chain:        i.chainName,
		TokenAddress: log.Address.Hex(),
		FromAddress:  decoded.From.Hex(),
		ToAddress:    decoded.To.Hex(),
		AmountRaw:    decoded.Amount.String(),
		BlockNumber:  int64(log.BlockNumber),
		TxHash:       log.TxHash.Hex(),
		LogIndex:     int32(log.Index),
		Timestamp:    blockTime,
		IndexedAt:    time.Now().UTC(),
	}

	if err := i.producer.Send(ctx, i.cfg.Kafka.Topics.Transfers, transferID, transfer); err != nil {
		return fmt.Errorf("failed to send transfer to kafka: %w", err)
	}

	metrics.EventsDecoded.WithLabelValues(i.chainName, IndexerName, "Transfer").Inc()
	metrics.KafkaMessagesProduced.WithLabelValues(i.cfg.Kafka.Topics.Transfers).Inc()

	return nil
}

func strPtr(s string) *string {
	return &s
}
