package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	BlocksProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "indexer",
			Name:      "blocks_processed_total",
			Help:      "Total number of blocks processed",
		},
		[]string{"chain", "indexer"},
	)

	EventsDecoded = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "indexer",
			Name:      "events_decoded_total",
			Help:      "Total number of events decoded",
		},
		[]string{"chain", "indexer", "event_type"},
	)

	LastBlockNumber = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "indexer",
			Name:      "last_block_number",
			Help:      "Last processed block number",
		},
		[]string{"chain", "indexer"},
	)

	BlockLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "indexer",
			Name:      "block_lag",
			Help:      "Number of blocks behind chain head",
		},
		[]string{"chain", "indexer"},
	)

	KafkaProduceLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "indexer",
			Name:      "kafka_produce_latency_seconds",
			Help:      "Kafka produce latency in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10),
		},
		[]string{"topic"},
	)

	KafkaMessagesProduced = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "indexer",
			Name:      "kafka_messages_produced_total",
			Help:      "Total number of Kafka messages produced",
		},
		[]string{"topic"},
	)

	KafkaProduceErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "indexer",
			Name:      "kafka_produce_errors_total",
			Help:      "Total number of Kafka produce errors",
		},
		[]string{"topic"},
	)

	RPCRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "indexer",
			Name:      "rpc_requests_total",
			Help:      "Total number of RPC requests",
		},
		[]string{"chain", "method"},
	)

	RPCLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "indexer",
			Name:      "rpc_latency_seconds",
			Help:      "RPC request latency in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 10),
		},
		[]string{"chain", "method"},
	)

	RPCErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "indexer",
			Name:      "rpc_errors_total",
			Help:      "Total number of RPC errors",
		},
		[]string{"chain", "method"},
	)

	CheckpointUpdates = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "indexer",
			Name:      "checkpoint_updates_total",
			Help:      "Total number of checkpoint updates",
		},
		[]string{"chain", "indexer"},
	)

	PositionsPolled = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "indexer",
			Name:      "positions_polled_total",
			Help:      "Total number of positions polled",
		},
		[]string{"chain", "protocol"},
	)

	HighRiskPositions = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "indexer",
			Name:      "high_risk_positions",
			Help:      "Current number of high risk positions",
		},
		[]string{"chain", "protocol", "risk_level"},
	)

	PriceUpdates = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "indexer",
			Name:      "price_updates_total",
			Help:      "Total number of price updates",
		},
		[]string{"chain", "asset", "source"},
	)

	ProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "indexer",
			Name:      "processing_duration_seconds",
			Help:      "Time spent processing blocks/events",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
		},
		[]string{"chain", "indexer", "operation"},
	)
)
