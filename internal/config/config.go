package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Chain      ChainConfig      `mapstructure:"chain"`
	Kafka      KafkaConfig      `mapstructure:"kafka"`
	ClickHouse ClickHouseConfig `mapstructure:"clickhouse"`
	Postgres   PostgresConfig   `mapstructure:"postgres"`
	Redis      RedisConfig      `mapstructure:"redis"`
	Metrics    MetricsConfig    `mapstructure:"metrics"`
	Indexer    IndexerConfig    `mapstructure:"indexer"`
	Contracts  ContractsConfig  `mapstructure:"contracts"`
}

type ChainConfig struct {
	RPCURL            string        `mapstructure:"rpc_url"`
	WSURL             string        `mapstructure:"ws_url"`
	ChainID           int64         `mapstructure:"chain_id"`
	ConfirmationDepth int64         `mapstructure:"confirmation_depth"`
	BlockTime         time.Duration `mapstructure:"block_time"`
}

type KafkaConfig struct {
	Brokers        []string `mapstructure:"brokers"`
	SchemaRegistry string   `mapstructure:"schema_registry"`
	Topics         Topics   `mapstructure:"topics"`
	Producer       Producer `mapstructure:"producer"`
}

type Topics struct {
	Blocks      string `mapstructure:"blocks"`
	AaveEvents  string `mapstructure:"aave_events"`
	Transfers   string `mapstructure:"transfers"`
	Positions   string `mapstructure:"positions"`
	Prices      string `mapstructure:"prices"`
	RiskAlerts  string `mapstructure:"risk_alerts"`
}

type Producer struct {
	BatchSize       int           `mapstructure:"batch_size"`
	BatchTimeout    time.Duration `mapstructure:"batch_timeout"`
	MaxRetries      int           `mapstructure:"max_retries"`
	RequiredAcks    int           `mapstructure:"required_acks"`
	CompressionType string        `mapstructure:"compression_type"`
}

type ClickHouseConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Database string `mapstructure:"database"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type PostgresConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Database string `mapstructure:"database"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	SSLMode  string `mapstructure:"ssl_mode"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Port    int    `mapstructure:"port"`
	Path    string `mapstructure:"path"`
}

type IndexerConfig struct {
	BlockIndexer    BlockIndexerConfig    `mapstructure:"block"`
	EventIndexer    EventIndexerConfig    `mapstructure:"event"`
	PositionIndexer PositionIndexerConfig `mapstructure:"position"`
	PriceIndexer    PriceIndexerConfig    `mapstructure:"price"`
}

type BlockIndexerConfig struct {
	Enabled      bool          `mapstructure:"enabled"`
	StartBlock   int64         `mapstructure:"start_block"`
	BatchSize    int           `mapstructure:"batch_size"`
	PollInterval time.Duration `mapstructure:"poll_interval"`
}

type EventIndexerConfig struct {
	Enabled      bool          `mapstructure:"enabled"`
	StartBlock   int64         `mapstructure:"start_block"`
	BatchSize    int           `mapstructure:"batch_size"`
	PollInterval time.Duration `mapstructure:"poll_interval"`
}

type PositionIndexerConfig struct {
	Enabled              bool          `mapstructure:"enabled"`
	ActivePollInterval   time.Duration `mapstructure:"active_poll_interval"`
	InactivePollInterval time.Duration `mapstructure:"inactive_poll_interval"`
	HighRiskPollInterval time.Duration `mapstructure:"high_risk_poll_interval"`
	BatchSize            int           `mapstructure:"batch_size"`
}

type PriceIndexerConfig struct {
	Enabled      bool          `mapstructure:"enabled"`
	PollInterval time.Duration `mapstructure:"poll_interval"`
	StalenessMax time.Duration `mapstructure:"staleness_max"`
}

type ContractsConfig struct {
	AavePool           string   `mapstructure:"aave_pool"`
	AaveDataProvider   string   `mapstructure:"aave_data_provider"`
	ChainlinkETHUSD    string   `mapstructure:"chainlink_eth_usd"`
	TrackedTokens      []string `mapstructure:"tracked_tokens"`
	TrackedProtocols   []string `mapstructure:"tracked_protocols"`
}

func Load(configPath string) (*Config, error) {
	v := viper.New()

	v.SetConfigType("yaml")

	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("config")
		v.AddConfigPath(".")
		v.AddConfigPath("./config")
		v.AddConfigPath("/etc/base-data")
	}

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("chain.rpc_url", "https://sepolia.base.org")
	v.SetDefault("chain.ws_url", "wss://sepolia.base.org")
	v.SetDefault("chain.chain_id", 84532)
	v.SetDefault("chain.confirmation_depth", 6)
	v.SetDefault("chain.block_time", "2s")

	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.schema_registry", "http://localhost:8081")
	v.SetDefault("kafka.topics.blocks", "base.blocks")
	v.SetDefault("kafka.topics.aave_events", "base.aave.events")
	v.SetDefault("kafka.topics.transfers", "base.erc20.transfers")
	v.SetDefault("kafka.topics.positions", "base.positions.snapshot")
	v.SetDefault("kafka.topics.prices", "market.prices")
	v.SetDefault("kafka.topics.risk_alerts", "alerts.risk")
	v.SetDefault("kafka.producer.batch_size", 100)
	v.SetDefault("kafka.producer.batch_timeout", "100ms")
	v.SetDefault("kafka.producer.max_retries", 3)
	v.SetDefault("kafka.producer.required_acks", 1)
	v.SetDefault("kafka.producer.compression_type", "snappy")

	v.SetDefault("clickhouse.host", "localhost")
	v.SetDefault("clickhouse.port", 9000)
	v.SetDefault("clickhouse.database", "base_data")
	v.SetDefault("clickhouse.username", "default")
	v.SetDefault("clickhouse.password", "")

	v.SetDefault("postgres.host", "localhost")
	v.SetDefault("postgres.port", 5432)
	v.SetDefault("postgres.database", "base_data")
	v.SetDefault("postgres.username", "postgres")
	v.SetDefault("postgres.password", "postgres")
	v.SetDefault("postgres.ssl_mode", "disable")

	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)

	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.port", 9091)
	v.SetDefault("metrics.path", "/metrics")

	v.SetDefault("indexer.block.enabled", true)
	v.SetDefault("indexer.block.start_block", 0)
	v.SetDefault("indexer.block.batch_size", 100)
	v.SetDefault("indexer.block.poll_interval", "1s")

	v.SetDefault("indexer.event.enabled", true)
	v.SetDefault("indexer.event.start_block", 0)
	v.SetDefault("indexer.event.batch_size", 1000)
	v.SetDefault("indexer.event.poll_interval", "1s")

	v.SetDefault("indexer.position.enabled", true)
	v.SetDefault("indexer.position.active_poll_interval", "30s")
	v.SetDefault("indexer.position.inactive_poll_interval", "5m")
	v.SetDefault("indexer.position.high_risk_poll_interval", "10s")
	v.SetDefault("indexer.position.batch_size", 50)

	v.SetDefault("indexer.price.enabled", true)
	v.SetDefault("indexer.price.poll_interval", "10s")
	v.SetDefault("indexer.price.staleness_max", "1h")

	v.SetDefault("contracts.aave_pool", "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5")
	v.SetDefault("contracts.aave_data_provider", "0x2d8A3C5677189723C4cB8873CfC9C8976FDF38Ac")
	v.SetDefault("contracts.chainlink_eth_usd", "0x4aDC67696bA383F43DD60A9e78F2C97Fbbfc7cb1")
}
