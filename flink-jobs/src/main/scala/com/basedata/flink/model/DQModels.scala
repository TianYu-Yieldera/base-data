package com.basedata.flink.model

/**
 * Generic Kafka message for DQ monitoring
 */
@SerialVersionUID(1L)
case class KafkaMessage(
  topic: String,
  partition: Int,
  offset: Long,
  key: Option[String],
  value: Option[String],
  timestamp: Long,
  headers: Map[String, String] = Map.empty
) extends Serializable

/**
 * Data quality metrics per topic per window
 */
@SerialVersionUID(1L)
case class DQMetrics(
  chain: String,
  topic: String,
  checkTime: Long,
  messageCount: Long,
  messageRatePerSec: Double,
  nullCount: Long,
  schemaErrorCount: Long,
  duplicateCount: Long,
  lateArrivalCount: Long,
  avgLatencyMs: Double,
  p95LatencyMs: Double,
  p99LatencyMs: Double,
  maxLatencyMs: Double,
  isRateAnomaly: Boolean,
  isLatencyAnomaly: Boolean,
  windowStart: Long,
  windowEnd: Long
) extends Serializable

/**
 * Rolling statistics for anomaly detection
 */
@SerialVersionUID(1L)
case class RollingStats(
  meanRate: Double,
  stdRate: Double,
  meanLatency: Double,
  stdLatency: Double,
  sampleCount: Long,
  lastUpdated: Long
) extends Serializable

/**
 * Message statistics collected per window
 */
@SerialVersionUID(1L)
case class MessageStats(
  topic: String,
  messageCount: Long,
  nullFieldCount: Long,
  schemaErrors: Long,
  duplicates: Long,
  lateArrivals: Long,
  latencies: List[Double],
  windowStart: Long,
  windowEnd: Long
) extends Serializable

/**
 * Output record for rt_dq_metrics
 */
@SerialVersionUID(1L)
case class DQMetricsRecord(
  chain: String,
  topic: String,
  checkTime: Long,
  messageCount: Long,
  messageRatePerSec: Double,
  nullCount: Long,
  schemaErrorCount: Long,
  duplicateCount: Long,
  lateArrivalCount: Long,
  avgLatencyMs: Double,
  p95LatencyMs: Double,
  p99LatencyMs: Double,
  maxLatencyMs: Double,
  isRateAnomaly: Int,      // UInt8 in ClickHouse
  isLatencyAnomaly: Int,   // UInt8 in ClickHouse
  windowStart: Long,
  windowEnd: Long
) extends Serializable
