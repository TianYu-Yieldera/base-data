package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

type Manager struct {
	client  *redis.Client
	logger  *zap.Logger
	prefix  string
	mu      sync.RWMutex
	cache   map[string]*Checkpoint
}

type Checkpoint struct {
	IndexerName     string    `json:"indexer_name"`
	Chain           string    `json:"chain"`
	LastBlockNumber int64     `json:"last_block_number"`
	LastBlockHash   string    `json:"last_block_hash"`
	LastIndexedAt   time.Time `json:"last_indexed_at"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

func NewManager(client *redis.Client, prefix string, logger *zap.Logger) *Manager {
	return &Manager{
		client: client,
		logger: logger,
		prefix: prefix,
		cache:  make(map[string]*Checkpoint),
	}
}

func (m *Manager) key(indexerName, chain string) string {
	return fmt.Sprintf("%s:checkpoint:%s:%s", m.prefix, indexerName, chain)
}

func (m *Manager) Get(ctx context.Context, indexerName, chain string) (*Checkpoint, error) {
	m.mu.RLock()
	cacheKey := m.key(indexerName, chain)
	if cp, ok := m.cache[cacheKey]; ok {
		m.mu.RUnlock()
		return cp, nil
	}
	m.mu.RUnlock()

	data, err := m.client.Get(ctx, cacheKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get checkpoint: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	m.mu.Lock()
	m.cache[cacheKey] = &cp
	m.mu.Unlock()

	return &cp, nil
}

func (m *Manager) Save(ctx context.Context, cp *Checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	key := m.key(cp.IndexerName, cp.Chain)
	if err := m.client.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	m.mu.Lock()
	m.cache[key] = cp
	m.mu.Unlock()

	m.logger.Debug("checkpoint saved",
		zap.String("indexer", cp.IndexerName),
		zap.String("chain", cp.Chain),
		zap.Int64("block", cp.LastBlockNumber))

	return nil
}

func (m *Manager) GetLastBlockNumber(ctx context.Context, indexerName, chain string, defaultBlock int64) (int64, error) {
	cp, err := m.Get(ctx, indexerName, chain)
	if err != nil {
		return 0, err
	}

	if cp == nil {
		return defaultBlock, nil
	}

	return cp.LastBlockNumber, nil
}

func (m *Manager) UpdateBlockNumber(ctx context.Context, indexerName, chain string, blockNumber int64, blockHash string) error {
	cp := &Checkpoint{
		IndexerName:     indexerName,
		Chain:           chain,
		LastBlockNumber: blockNumber,
		LastBlockHash:   blockHash,
		LastIndexedAt:   time.Now().UTC(),
	}

	return m.Save(ctx, cp)
}

func (m *Manager) Delete(ctx context.Context, indexerName, chain string) error {
	key := m.key(indexerName, chain)
	if err := m.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete checkpoint: %w", err)
	}

	m.mu.Lock()
	delete(m.cache, key)
	m.mu.Unlock()

	return nil
}

func (m *Manager) List(ctx context.Context) ([]*Checkpoint, error) {
	pattern := fmt.Sprintf("%s:checkpoint:*", m.prefix)
	keys, err := m.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list checkpoints: %w", err)
	}

	checkpoints := make([]*Checkpoint, 0, len(keys))
	for _, key := range keys {
		data, err := m.client.Get(ctx, key).Bytes()
		if err != nil {
			continue
		}

		var cp Checkpoint
		if err := json.Unmarshal(data, &cp); err != nil {
			continue
		}

		checkpoints = append(checkpoints, &cp)
	}

	return checkpoints, nil
}

func (m *Manager) GetLastBlockHash(ctx context.Context, indexerName, chain string) (string, error) {
	cp, err := m.Get(ctx, indexerName, chain)
	if err != nil {
		return "", err
	}

	if cp == nil {
		return "", nil
	}

	return cp.LastBlockHash, nil
}

func (m *Manager) Invalidate(ctx context.Context, indexerName, chain string) error {
	m.mu.Lock()
	cacheKey := m.key(indexerName, chain)
	delete(m.cache, cacheKey)
	m.mu.Unlock()

	m.logger.Warn("checkpoint invalidated due to chain mismatch",
		zap.String("indexer", indexerName),
		zap.String("chain", chain))

	return nil
}
