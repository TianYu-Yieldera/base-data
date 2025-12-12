package ethereum

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
)

type Client struct {
	rpcClient *ethclient.Client
	wsClient  *ethclient.Client
	chainID   *big.Int
	logger    *zap.Logger
	mu        sync.RWMutex
}

func NewClient(ctx context.Context, rpcURL, wsURL string, logger *zap.Logger) (*Client, error) {
	rpcClient, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC: %w", err)
	}

	chainID, err := rpcClient.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	client := &Client{
		rpcClient: rpcClient,
		chainID:   chainID,
		logger:    logger,
	}

	if wsURL != "" {
		wsClient, err := ethclient.DialContext(ctx, wsURL)
		if err != nil {
			logger.Warn("failed to connect to WebSocket, will use polling", zap.Error(err))
		} else {
			client.wsClient = wsClient
		}
	}

	return client, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.rpcClient != nil {
		c.rpcClient.Close()
	}
	if c.wsClient != nil {
		c.wsClient.Close()
	}
}

func (c *Client) ChainID() *big.Int {
	return c.chainID
}

func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	return c.rpcClient.BlockNumber(ctx)
}

func (c *Client) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return c.rpcClient.BlockByNumber(ctx, number)
}

func (c *Client) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return c.rpcClient.HeaderByNumber(ctx, number)
}

func (c *Client) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return c.rpcClient.FilterLogs(ctx, query)
}

func (c *Client) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	if c.wsClient == nil {
		return nil, fmt.Errorf("WebSocket client not available")
	}
	return c.wsClient.SubscribeNewHead(ctx, ch)
}

func (c *Client) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return c.rpcClient.CallContract(ctx, call, blockNumber)
}

func (c *Client) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	return c.rpcClient.CodeAt(ctx, account, blockNumber)
}

func (c *Client) GetConfirmedBlockNumber(ctx context.Context, confirmationDepth int64) (int64, error) {
	latestBlock, err := c.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block number: %w", err)
	}

	confirmedBlock := int64(latestBlock) - confirmationDepth
	if confirmedBlock < 0 {
		confirmedBlock = 0
	}

	return confirmedBlock, nil
}

func (c *Client) WaitForBlock(ctx context.Context, targetBlock int64) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			currentBlock, err := c.BlockNumber(ctx)
			if err != nil {
				c.logger.Warn("failed to get block number", zap.Error(err))
				continue
			}
			if int64(currentBlock) >= targetBlock {
				return nil
			}
		}
	}
}

func (c *Client) GetBlockRange(ctx context.Context, startBlock, endBlock int64) ([]*types.Block, error) {
	blocks := make([]*types.Block, 0, endBlock-startBlock+1)

	for i := startBlock; i <= endBlock; i++ {
		block, err := c.BlockByNumber(ctx, big.NewInt(i))
		if err != nil {
			return nil, fmt.Errorf("failed to get block %d: %w", i, err)
		}
		blocks = append(blocks, block)
	}

	return blocks, nil
}

func (c *Client) GetLogsForBlockRange(ctx context.Context, startBlock, endBlock int64, addresses []common.Address, topics [][]common.Hash) ([]types.Log, error) {
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(startBlock),
		ToBlock:   big.NewInt(endBlock),
		Addresses: addresses,
		Topics:    topics,
	}

	return c.FilterLogs(ctx, query)
}
