package com.basedata.flink.job

import com.basedata.flink.model._
import com.basedata.flink.sink.ClickHouseTokenFlowSink
import com.basedata.flink.source.KafkaTransferSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.OutputTag
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.UUID

/**
 * Token Flow Aggregator Job
 *
 * Consumes ERC20 transfer events from Kafka,
 * aggregates by token per hour, and outputs to ClickHouse.
 *
 * Features:
 * - 1-hour tumbling windows based on event time
 * - 5-minute allowed lateness
 * - Side output for late events
 */
object TokenFlowAggregatorJob {

  private val logger = LoggerFactory.getLogger(getClass)

  // Side output tag for late events
  val lateEventsTag: OutputTag[TransferEvent] = new OutputTag[TransferEvent]("late-transfers") {}

  // Maximum hash set size for unique address counting
  val MaxHashSetSize: Int = 10000

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    // Configuration
    val kafkaBrokers = params.get("kafka.brokers", "kafka:29092")
    val schemaRegistryUrl = params.get("schema.registry.url", "http://schema-registry:8081")
    val clickhouseHost = params.get("clickhouse.host", "clickhouse")
    val clickhousePort = params.getInt("clickhouse.port", 8123)
    val clickhouseDb = params.get("clickhouse.database", "base_data")
    val parallelism = params.getInt("parallelism", 4)
    val checkpointIntervalMs = params.getLong("checkpoint.interval.ms", 60000)

    val jobId = UUID.randomUUID().toString.take(8)
    logger.info(s"Starting Token Flow Aggregator Job with ID: $jobId")

    // Create execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    // Configure checkpointing
    env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(600000)

    // Watermark strategy: 30 second out-of-orderness
    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[TransferEvent](Duration.ofSeconds(30))
      .withTimestampAssigner((event, _) => event.timestamp)
      .withIdleness(Duration.ofMinutes(2))

    // Create transfer source
    val transferSource = KafkaTransferSource.create(kafkaBrokers, schemaRegistryUrl)
    val transferStream = env
      .fromSource(transferSource, watermarkStrategy, "Transfer Source")
      .uid("transfer-source")
      .name("Kafka Transfer Source")

    // Convert to accumulator for aggregation
    val accumulatorStream = transferStream.map { event =>
      val amount = event.amount.map(BigDecimal(_)).getOrElse(BigDecimal(0))
      TokenFlowAccumulator(
        chain = event.chain,
        tokenAddress = event.tokenAddress,
        hour = (event.timestamp / 3600000) * 3600000,
        transferCount = 1,
        senderHashes = Set(event.fromAddress.hashCode),
        receiverHashes = Set(event.toAddress.hashCode),
        totalVolume = amount,
        maxTransferSize = amount
      )
    }

    // Key by (chain, token_address) and window with reduce
    val aggregatedStream = accumulatorStream
      .keyBy(acc => (acc.chain, acc.tokenAddress))
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .allowedLateness(Time.minutes(5))
      .reduce { (a, b) =>
        val mergedSenders = a.senderHashes ++ b.senderHashes
        val mergedReceivers = a.receiverHashes ++ b.receiverHashes
        TokenFlowAccumulator(
          chain = a.chain,
          tokenAddress = a.tokenAddress,
          hour = Math.max(a.hour, b.hour),
          transferCount = a.transferCount + b.transferCount,
          senderHashes = if (mergedSenders.size > MaxHashSetSize) mergedSenders.take(MaxHashSetSize) else mergedSenders,
          receiverHashes = if (mergedReceivers.size > MaxHashSetSize) mergedReceivers.take(MaxHashSetSize) else mergedReceivers,
          totalVolume = a.totalVolume + b.totalVolume,
          maxTransferSize = a.maxTransferSize.max(b.maxTransferSize)
        )
      }
      .uid("token-flow-aggregator")
      .name("Token Flow Aggregator")

    // Convert to output record
    val outputStream = aggregatedStream.map { acc =>
      val avgSize = if (acc.transferCount > 0) (acc.totalVolume / acc.transferCount).toDouble else 0.0
      TokenFlowRecord(
        chain = acc.chain,
        tokenAddress = acc.tokenAddress,
        hour = acc.hour,
        transferCount = acc.transferCount,
        uniqueSendersCount = acc.senderHashes.size,
        uniqueReceiversCount = acc.receiverHashes.size,
        totalVolume = acc.totalVolume.toString(),
        avgTransferSize = avgSize,
        maxTransferSize = acc.maxTransferSize.toString(),
        updatedAt = System.currentTimeMillis()
      )
    }.uid("to-record")
      .name("Convert to Record")

    // Output to ClickHouse
    outputStream
      .addSink(new ClickHouseTokenFlowSink(clickhouseHost, clickhousePort, clickhouseDb))
      .uid("clickhouse-sink")
      .name("ClickHouse Token Flow Sink")
      .setParallelism(2)

    // Execute
    logger.info("Starting Token Flow Aggregator Job execution...")
    env.execute(s"Token Flow Aggregator [$jobId]")
  }
}

/**
 * Accumulator for token flow aggregation
 * Uses Set[Int] for hash-based unique counting to limit memory
 */
@SerialVersionUID(1L)
case class TokenFlowAccumulator(
  chain: String = "",
  tokenAddress: String = "",
  hour: Long = 0,
  transferCount: Long = 0,
  senderHashes: Set[Int] = Set.empty,
  receiverHashes: Set[Int] = Set.empty,
  totalVolume: BigDecimal = BigDecimal(0),
  maxTransferSize: BigDecimal = BigDecimal(0)
) extends Serializable
