# Base Data Hub

A production-grade data infrastructure platform for the Base blockchain ecosystem. Built with a modern streaming architecture, Base Data Hub provides reliable, low-latency blockchain data ingestion, transformation, and serving capabilities for DeFi applications and analytics.

## Overview

Base Data Hub serves as the foundational data layer for blockchain applications, abstracting away the complexity of direct chain interaction and providing clean, normalized data streams. The platform is designed to support multiple downstream consumers including risk monitoring systems, analytics dashboards, and DeFi protocol integrations.

### Key Capabilities

| Capability | Description |
|------------|-------------|
| **Real-time Ingestion** | Sub-second block and event indexing with configurable confirmation depth |
| **Protocol Decoding** | Native support for Aave V3, ERC20, and extensible to other protocols |
| **Stream Processing** | Kafka-based event streaming with exactly-once semantics |
| **OLAP Storage** | ClickHouse for high-performance analytical queries |
| **Reorg Safety** | 6-block confirmation depth with hash verification for data integrity |

## Architecture

```
                                 Base Data Hub Architecture

    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                              Data Sources                                    │
    │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
    │  │  Base Chain  │  │   Chainlink  │  │  Protocol    │  │   External   │    │
    │  │    (RPC)     │  │   Oracles    │  │   Configs    │  │     APIs     │    │
    │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘    │
    └─────────┼─────────────────┼─────────────────┼─────────────────┼────────────┘
              │                 │                 │                 │
              ▼                 ▼                 ▼                 ▼
    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                           Ingestion Layer (Go)                              │
    │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
    │  │    Block     │  │    Event     │  │   Position   │  │    Price     │    │
    │  │   Indexer    │  │   Indexer    │  │   Indexer    │  │   Indexer    │    │
    │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘    │
    │         │                 │                 │                 │            │
    │         │    ┌────────────┴────────────┐    │                 │            │
    │         │    │   Checkpoint Manager    │    │                 │            │
    │         │    │       (Redis)           │    │                 │            │
    │         │    └─────────────────────────┘    │                 │            │
    └─────────┼─────────────────┼─────────────────┼─────────────────┼────────────┘
              │                 │                 │                 │
              ▼                 ▼                 ▼                 ▼
    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                         Message Bus (Kafka)                                 │
    │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
    │  │ base.blocks  │  │ base.aave.*  │  │base.positions│  │market.prices │    │
    │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
    │                        │                                                    │
    │                        ▼                                                    │
    │               ┌──────────────────┐                                         │
    │               │  Schema Registry │                                         │
    │               └──────────────────┘                                         │
    └─────────────────────────────────────────────────────────────────────────────┘
              │                 │                 │                 │
              ▼                 ▼                 ▼                 ▼
    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                      Processing Layer (Flink)                               │
    │  ┌──────────────────────────────────────────────────────────────────────┐  │
    │  │  Aggregation Jobs  │  Enrichment Jobs  │  Alert Detection Jobs       │  │
    │  └──────────────────────────────────────────────────────────────────────┘  │
    └─────────────────────────────────────────────────────────────────────────────┘
              │                 │                 │                 │
              ▼                 ▼                 ▼                 ▼
    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                          Storage Layer                                      │
    │  ┌──────────────────────────────┐  ┌──────────────────────────────┐        │
    │  │         ClickHouse           │  │         PostgreSQL           │        │
    │  │  - Raw event tables          │  │  - Address labels            │        │
    │  │  - Aggregated metrics        │  │  - Alert configurations      │        │
    │  │  - Time-series analytics     │  │  - User preferences          │        │
    │  └──────────────────────────────┘  └──────────────────────────────┘        │
    └─────────────────────────────────────────────────────────────────────────────┘
              │                                   │
              ▼                                   ▼
    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                           Serving Layer                                     │
    │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
    │  │   REST API   │  │  WebSocket   │  │   GraphQL    │  │   gRPC       │    │
    │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
    └─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Ingestion Pipeline

1. **Block Indexer**: Polls confirmed blocks (chain head - 6), extracts block metadata
2. **Event Indexer**: Filters and decodes protocol-specific events (Aave V3, ERC20)
3. **Position Indexer**: Queries on-chain position data with tiered polling intervals
4. **Price Indexer**: Fetches asset prices from Chainlink oracles

### Data Guarantees

| Guarantee | Implementation |
|-----------|----------------|
| **Exactly-once delivery** | Kafka transactional producer + idempotent consumers |
| **Ordering** | Partition by block number, ordered processing |
| **Reorg safety** | 6-block confirmation depth (~12s latency) |
| **Crash recovery** | Redis-backed checkpoints with hash verification |

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Ingestion | Go 1.21+ | High-performance blockchain data extraction |
| Message Bus | Apache Kafka | Durable, scalable event streaming |
| Schema Management | Confluent Schema Registry | Schema evolution and compatibility |
| Stream Processing | Apache Flink | Stateful stream transformations |
| OLAP Storage | ClickHouse | Sub-second analytical queries |
| OLTP Storage | PostgreSQL | Transactional metadata storage |
| Caching | Redis | Checkpoint management and hot data |
| Monitoring | Prometheus + Grafana | Metrics collection and visualization |

## Quick Start

### Prerequisites

- Go 1.21+
- Docker and Docker Compose
- Base RPC endpoint (Alchemy, QuickNode, or self-hosted)

### Local Development

```bash
# Clone repository
git clone https://github.com/TianYu-Yieldera/base-data.git
cd base-data

# Start infrastructure
make docker-up

# Initialize database schemas
make init-db

# Configure RPC endpoint
cp config/local.yaml.example config/local.yaml
# Edit config/local.yaml with your RPC URL

# Run indexer
make run
```

### Docker Deployment

```bash
# Build image
docker build -t base-data-indexer .

# Run with custom config
docker run -v $(pwd)/config:/app/config base-data-indexer
```

## Project Structure

```
base-data/
├── cmd/
│   ├── indexer/              # Indexer service entry point
│   └── api/                  # API service entry point
├── internal/
│   ├── config/               # Configuration management
│   ├── indexer/
│   │   ├── block/            # Block indexer
│   │   ├── event/            # Event indexer (Aave, ERC20)
│   │   ├── position/         # Position snapshot indexer
│   │   └── price/            # Price feed indexer
│   ├── decoder/              # ABI decoding utilities
│   ├── ethereum/             # Ethereum client wrapper
│   ├── kafka/                # Kafka producer
│   ├── checkpoint/           # Crash recovery management
│   └── metrics/              # Prometheus metrics
├── pkg/
│   └── models/               # Shared data models
├── deployments/
│   └── docker/               # Docker Compose configuration
├── terraform/                # Infrastructure as Code
├── schemas/
│   └── avro/                 # Avro schema definitions
├── migrations/
│   ├── clickhouse/           # ClickHouse DDL
│   └── postgres/             # PostgreSQL DDL
├── config/                   # Configuration files
└── docs/                     # Documentation
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BASE_RPC_URL` | Base chain RPC endpoint | - |
| `BASE_WS_URL` | Base chain WebSocket endpoint | - |
| `KAFKA_BROKERS` | Kafka broker addresses | localhost:9092 |
| `CLICKHOUSE_HOST` | ClickHouse host | localhost |
| `REDIS_ADDR` | Redis address | localhost:6379 |
| `CONFIRMATION_DEPTH` | Block confirmation depth | 6 |

### Configuration File

```yaml
chain:
  rpc_url: "${BASE_RPC_URL}"
  ws_url: "${BASE_WS_URL}"
  chain_id: 8453
  confirmation_depth: 6

indexer:
  block_indexer:
    enabled: true
    batch_size: 100
    poll_interval: 2s
  event_indexer:
    enabled: true
    batch_size: 1000
    poll_interval: 2s
  position_indexer:
    enabled: true
    high_risk_poll_interval: 10s
    active_poll_interval: 30s
    inactive_poll_interval: 5m

kafka:
  brokers:
    - "${KAFKA_BROKERS}"
  topics:
    blocks: "base.blocks"
    aave_events: "base.aave.events"
    transfers: "base.erc20.transfers"
    positions: "base.positions.snapshot"
    prices: "market.prices"
```

## Kafka Topics

| Topic | Key | Partitions | Description |
|-------|-----|------------|-------------|
| `base.blocks` | block_hash | 12 | Block headers and metadata |
| `base.aave.events` | event_id | 24 | Aave V3 protocol events |
| `base.erc20.transfers` | transfer_id | 24 | ERC20 transfer events |
| `base.positions.snapshot` | snapshot_id | 12 | Position snapshots |
| `market.prices` | price_id | 6 | Asset price updates |

## Monitoring

### Prometheus Metrics

```
# Indexer metrics
indexer_blocks_processed_total{chain, indexer}
indexer_events_decoded_total{chain, indexer, event_type}
indexer_last_block_number{chain, indexer}
indexer_block_lag{chain, indexer}
indexer_processing_duration_seconds{chain, indexer, operation}

# Reorg detection
indexer_chain_reorg_detected_total{chain, indexer, type}
indexer_checkpoint_hash_mismatch_total{chain, indexer}
indexer_parent_hash_mismatch_total{chain, indexer}

# Kafka metrics
indexer_kafka_messages_produced_total{topic}
indexer_kafka_produce_latency_seconds{topic}
indexer_kafka_produce_errors_total{topic}

# RPC metrics
indexer_rpc_requests_total{chain, method}
indexer_rpc_latency_seconds{chain, method}
indexer_rpc_errors_total{chain, method}
```

### Grafana Dashboards

Pre-configured dashboards available at `http://localhost:3000`:
- Indexer Overview: Block processing rates, lag, errors
- Kafka Metrics: Throughput, latency, consumer lag
- Infrastructure: System resources, database performance

## Supported Protocols

### Aave V3

| Event | Description |
|-------|-------------|
| Supply | Asset deposits into lending pool |
| Withdraw | Asset withdrawals from lending pool |
| Borrow | Loan originations |
| Repay | Loan repayments |
| LiquidationCall | Position liquidations |
| FlashLoan | Flash loan executions |

### ERC20

| Event | Description |
|-------|-------------|
| Transfer | Token transfers between addresses |

## Development

### Build

```bash
# Build all binaries
make build

# Build indexer only
make build-indexer

# Build with race detector
go build -race ./...
```

### Test

```bash
# Run unit tests
make test

# Run with coverage
make test-coverage

# Run linter
make lint
```

### Code Generation

```bash
# Generate mocks and other code
make generate
```

## Roadmap

- [ ] GraphQL API layer
- [ ] WebSocket subscription support
- [ ] Additional protocol decoders (Uniswap, Compound)
- [ ] Cross-chain data aggregation
- [ ] Historical data backfill tooling
- [ ] Kubernetes Helm charts

## Contributing

Contributions are welcome. Please read the contributing guidelines before submitting pull requests.

## License

MIT License - see [LICENSE](LICENSE) for details.
