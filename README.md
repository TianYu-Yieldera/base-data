# Base Data Hub

A full-stack data platform for Base chain, providing real-time blockchain data indexing, stream processing, and analytics capabilities.

## Architecture

```
Base Chain --> Indexers (Go) --> Kafka --> Flink --> ClickHouse --> API
                                   |
                                   v
                            Schema Registry
```

## Features

- Real-time block and event indexing with 6-block confirmation depth
- Aave V3 protocol event tracking (Supply, Borrow, Repay, Withdraw, Liquidation)
- ERC20 transfer monitoring
- Position snapshot and health factor calculation
- Price feed integration via Chainlink oracles
- Prometheus metrics and Grafana dashboards

## Quick Start

### Prerequisites

- Go 1.21+
- Docker and Docker Compose
- Terraform 1.6+

### Local Development

1. Start infrastructure services:

```bash
docker-compose -f deployments/docker/docker-compose.yml up -d
```

2. Initialize database schemas:

```bash
./scripts/init-db.sh
```

3. Run the indexer:

```bash
go run cmd/indexer/main.go --config config/local.yaml
```

## Project Structure

```
.
├── cmd/                    # Application entry points
│   ├── indexer/           # Blockchain indexer service
│   └── api/               # Data API service
├── internal/              # Private application code
│   ├── config/            # Configuration management
│   ├── indexer/           # Indexer implementations
│   ├── kafka/             # Kafka producer/consumer
│   ├── checkpoint/        # Checkpoint management
│   ├── metrics/           # Prometheus metrics
│   ├── ethereum/          # Ethereum client wrapper
│   └── decoder/           # ABI decoder
├── pkg/                   # Public packages
│   ├── models/            # Data models
│   └── utils/             # Utility functions
├── deployments/           # Deployment configurations
│   ├── docker/            # Docker Compose files
│   └── kubernetes/        # Kubernetes manifests
├── terraform/             # Infrastructure as Code
├── schemas/               # Avro schemas
├── migrations/            # Database migrations
└── scripts/               # Utility scripts
```

## Configuration

Configuration is managed via YAML files and environment variables.

```yaml
# config/local.yaml
chain:
  rpc_url: "https://sepolia.base.org"
  ws_url: "wss://sepolia.base.org"
  chain_id: 84532
  confirmation_depth: 6

kafka:
  brokers:
    - "localhost:9092"
  schema_registry: "http://localhost:8081"

clickhouse:
  host: "localhost"
  port: 9000
  database: "base_data"

redis:
  addr: "localhost:6379"
```

## Kafka Topics

| Topic | Description |
|-------|-------------|
| base.blocks | Block headers |
| base.aave.events | Aave V3 protocol events |
| base.erc20.transfers | ERC20 transfer events |
| base.positions.snapshot | Position snapshots |
| market.prices | Asset price feeds |

## Metrics

Prometheus metrics are exposed at `:9090/metrics`:

- `indexer_blocks_processed_total` - Total blocks processed
- `indexer_events_decoded_total` - Total events decoded by type
- `indexer_last_block_number` - Latest indexed block number
- `indexer_kafka_produce_latency_seconds` - Kafka produce latency

## Testing

```bash
# Run unit tests
go test ./...

# Run integration tests
go test -tags=integration ./...

# Run with coverage
go test -cover ./...
```

## License

MIT License
