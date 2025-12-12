package com.basedata.flink.job

import com.basedata.flink.model._
import com.basedata.flink.sink.ClickHouseDQSink
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{AggregateFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.UUID
import scala.util.Random

/**
 * Data Quality Monitor Job
 *
 * Monitors all Kafka topics for data quality metrics:
 * - Message rate and volume
 * - Latency (event time to processing time)
 * - Schema errors and null fields
 * - Anomaly detection using Z-score
 */
object DQMonitorJob {

  private val logger = LoggerFactory.getLogger(getClass)

  // Topics to monitor
  val monitoredTopics: java.util.List[String] = java.util.Arrays.asList(
    "base.blocks",
    "base.aave.supply",
    "base.aave.withdraw",
    "base.aave.borrow",
    "base.aave.repay",
    "base.aave.liquidation",
    "base.erc20.transfers",
    "base.positions",
    "market.prices"
  )

  // Maximum reservoir size for percentile calculation
  val MaxReservoirSize: Int = 1000

  // Late arrival threshold in milliseconds
  val LateArrivalThresholdMs: Long = 60000

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    // Configuration
    val kafkaBrokers = params.get("kafka.brokers", "kafka:29092")
    val clickhouseHost = params.get("clickhouse.host", "clickhouse")
    val clickhousePort = params.getInt("clickhouse.port", 8123)
    val clickhouseDb = params.get("clickhouse.database", "base_data")
    val parallelism = params.getInt("parallelism", 2)
    val checkpointIntervalMs = params.getLong("checkpoint.interval.ms", 30000)
    val windowSizeMinutes = params.getInt("window.size.minutes", 1)
    val slideSizeSeconds = params.getInt("slide.size.seconds", 30)

    val jobId = UUID.randomUUID().toString.take(8)
    logger.info(s"Starting DQ Monitor Job with ID: $jobId")

    // Create execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    // Configure checkpointing
    env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)

    // Create multi-topic Kafka source
    val kafkaSource = KafkaSource.builder[KafkaMessage]()
      .setBootstrapServers(kafkaBrokers)
      .setTopics(monitoredTopics)
      .setGroupId("flink-dq-monitor")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(new KafkaMessageDeserializer)
      .build()

    // Watermark strategy based on processing time
    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[KafkaMessage](Duration.ofSeconds(5))
      .withTimestampAssigner((msg, _) => msg.timestamp)

    // Source stream
    val messageStream = env
      .fromSource(kafkaSource, watermarkStrategy, "DQ Monitor Source")
      .uid("dq-source")
      .name("Kafka DQ Monitor Source")

    // Add processing timestamp and calculate latency
    val enrichedStream = messageStream.map { msg =>
      val processingTime = System.currentTimeMillis()
      val latencyMs = processingTime - msg.timestamp
      (msg.topic, msg, latencyMs.toDouble)
    }.uid("enrich-latency")
      .name("Enrich with Latency")

    // Key by topic and window
    val aggregatedStream = enrichedStream
      .keyBy(_._1) // key by topic
      .window(SlidingEventTimeWindows.of(
        Time.minutes(windowSizeMinutes),
        Time.seconds(slideSizeSeconds)
      ))
      .aggregate(new DQAggregator)
      .uid("dq-aggregator")
      .name("DQ Metrics Aggregator")

    // Apply anomaly detection
    val metricsWithAnomaly = aggregatedStream
      .keyBy(_.topic)
      .flatMap(new AnomalyDetector)
      .uid("anomaly-detector")
      .name("Anomaly Detector")

    // Convert to output record
    val outputStream = metricsWithAnomaly.map { metrics =>
      DQMetricsRecord(
        chain = "base",
        topic = metrics.topic,
        checkTime = metrics.checkTime,
        messageCount = metrics.messageCount,
        messageRatePerSec = metrics.messageRatePerSec,
        nullCount = metrics.nullCount,
        schemaErrorCount = metrics.schemaErrorCount,
        duplicateCount = metrics.duplicateCount,
        lateArrivalCount = metrics.lateArrivalCount,
        avgLatencyMs = metrics.avgLatencyMs,
        p95LatencyMs = metrics.p95LatencyMs,
        p99LatencyMs = metrics.p99LatencyMs,
        maxLatencyMs = metrics.maxLatencyMs,
        isRateAnomaly = if (metrics.isRateAnomaly) 1 else 0,
        isLatencyAnomaly = if (metrics.isLatencyAnomaly) 1 else 0,
        windowStart = metrics.windowStart,
        windowEnd = metrics.windowEnd
      )
    }.uid("to-record")
      .name("Convert to Record")

    // Output to ClickHouse
    outputStream
      .addSink(new ClickHouseDQSink(clickhouseHost, clickhousePort, clickhouseDb))
      .uid("clickhouse-sink")
      .name("ClickHouse DQ Metrics Sink")

    // Execute
    logger.info("Starting DQ Monitor Job execution...")
    env.execute(s"Data Quality Monitor [$jobId]")
  }
}

/**
 * Custom deserializer for DQ monitoring
 */
@SerialVersionUID(1L)
class KafkaMessageDeserializer extends org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema[KafkaMessage] with Serializable {

  override def deserialize(
    record: ConsumerRecord[Array[Byte], Array[Byte]],
    out: Collector[KafkaMessage]
  ): Unit = {
    val msg = KafkaMessage(
      topic = record.topic(),
      partition = record.partition(),
      offset = record.offset(),
      key = Option(record.key()).map(new String(_)),
      value = Option(record.value()).map(new String(_)),
      timestamp = record.timestamp(),
      headers = Map.empty
    )
    out.collect(msg)
  }

  override def getProducedType: org.apache.flink.api.common.typeinfo.TypeInformation[KafkaMessage] = {
    org.apache.flink.api.common.typeinfo.TypeInformation.of(classOf[KafkaMessage])
  }
}

/**
 * Aggregate function for DQ metrics with bounded memory using reservoir sampling
 */
class DQAggregator extends AggregateFunction[(String, KafkaMessage, Double), DQAccumulator, DQMetrics] {

  private val maxReservoirSize = DQMonitorJob.MaxReservoirSize
  private val lateThresholdMs = DQMonitorJob.LateArrivalThresholdMs

  override def createAccumulator(): DQAccumulator = DQAccumulator()

  override def add(value: (String, KafkaMessage, Double), acc: DQAccumulator): DQAccumulator = {
    val (topic, msg, latency) = value
    val isNull = msg.value.isEmpty || msg.value.exists(_.trim.isEmpty)
    val isLate = latency > lateThresholdMs

    // Update latency statistics incrementally
    val newLatencySum = acc.latencySum + latency
    val newLatencyCount = acc.latencyCount + 1
    val newLatencyMax = Math.max(acc.latencyMax, latency)
    val newLatencyMin = Math.min(acc.latencyMin, latency)

    // Reservoir sampling for percentile calculation (Algorithm R)
    val newReservoir = if (acc.latencyReservoir.length < maxReservoirSize) {
      acc.latencyReservoir :+ latency
    } else {
      // Randomly replace an element with probability maxReservoirSize / newLatencyCount
      val random = new Random()
      val j = random.nextInt(newLatencyCount.toInt)
      if (j < maxReservoirSize) {
        acc.latencyReservoir.updated(j, latency)
      } else {
        acc.latencyReservoir
      }
    }

    acc.copy(
      topic = topic,
      messageCount = acc.messageCount + 1,
      nullCount = acc.nullCount + (if (isNull) 1 else 0),
      lateArrivalCount = acc.lateArrivalCount + (if (isLate) 1 else 0),
      latencySum = newLatencySum,
      latencyCount = newLatencyCount,
      latencyMax = newLatencyMax,
      latencyMin = newLatencyMin,
      latencyReservoir = newReservoir,
      windowStart = if (acc.windowStart == 0) msg.timestamp else Math.min(acc.windowStart, msg.timestamp),
      windowEnd = Math.max(acc.windowEnd, msg.timestamp)
    )
  }

  override def getResult(acc: DQAccumulator): DQMetrics = {
    val windowDurationSec = Math.max(1, (acc.windowEnd - acc.windowStart) / 1000.0)
    val sortedReservoir = acc.latencyReservoir.sorted

    val avgLatency = if (acc.latencyCount > 0) acc.latencySum / acc.latencyCount else 0.0

    DQMetrics(
      chain = "base",
      topic = acc.topic,
      checkTime = System.currentTimeMillis(),
      messageCount = acc.messageCount,
      messageRatePerSec = acc.messageCount / windowDurationSec,
      nullCount = acc.nullCount,
      schemaErrorCount = acc.schemaErrorCount,
      duplicateCount = acc.duplicateCount,
      lateArrivalCount = acc.lateArrivalCount,
      avgLatencyMs = avgLatency,
      p95LatencyMs = percentile(sortedReservoir, 0.95),
      p99LatencyMs = percentile(sortedReservoir, 0.99),
      maxLatencyMs = acc.latencyMax,
      isRateAnomaly = false, // Set by AnomalyDetector
      isLatencyAnomaly = false, // Set by AnomalyDetector
      windowStart = acc.windowStart,
      windowEnd = acc.windowEnd
    )
  }

  override def merge(a: DQAccumulator, b: DQAccumulator): DQAccumulator = {
    // Merge reservoirs: combine and downsample if needed
    val combinedReservoir = (a.latencyReservoir ++ b.latencyReservoir)
    val mergedReservoir = if (combinedReservoir.length > maxReservoirSize) {
      // Random downsample to maxReservoirSize
      Random.shuffle(combinedReservoir.toList).take(maxReservoirSize).toVector
    } else {
      combinedReservoir
    }

    DQAccumulator(
      topic = if (a.topic.nonEmpty) a.topic else b.topic,
      messageCount = a.messageCount + b.messageCount,
      nullCount = a.nullCount + b.nullCount,
      schemaErrorCount = a.schemaErrorCount + b.schemaErrorCount,
      duplicateCount = a.duplicateCount + b.duplicateCount,
      lateArrivalCount = a.lateArrivalCount + b.lateArrivalCount,
      latencySum = a.latencySum + b.latencySum,
      latencyCount = a.latencyCount + b.latencyCount,
      latencyMax = Math.max(a.latencyMax, b.latencyMax),
      latencyMin = Math.min(a.latencyMin, b.latencyMin),
      latencyReservoir = mergedReservoir,
      windowStart = if (a.windowStart == 0) b.windowStart
                    else if (b.windowStart == 0) a.windowStart
                    else Math.min(a.windowStart, b.windowStart),
      windowEnd = Math.max(a.windowEnd, b.windowEnd)
    )
  }

  private def percentile(sorted: Vector[Double], p: Double): Double = {
    if (sorted.isEmpty) 0
    else {
      val idx = Math.ceil(p * sorted.size).toInt - 1
      sorted(Math.max(0, Math.min(idx, sorted.size - 1)))
    }
  }
}

/**
 * Accumulator for DQ aggregation with bounded memory
 * Uses reservoir sampling for percentile estimation
 */
@SerialVersionUID(1L)
case class DQAccumulator(
  topic: String = "",
  messageCount: Long = 0,
  nullCount: Long = 0,
  schemaErrorCount: Long = 0,
  duplicateCount: Long = 0,
  lateArrivalCount: Long = 0,
  // Incremental latency statistics
  latencySum: Double = 0.0,
  latencyCount: Long = 0,
  latencyMax: Double = Double.MinValue,
  latencyMin: Double = Double.MaxValue,
  // Reservoir sampling for percentiles (bounded to MaxReservoirSize)
  latencyReservoir: Vector[Double] = Vector.empty,
  windowStart: Long = 0,
  windowEnd: Long = 0
) extends Serializable

/**
 * Anomaly detector using Z-score method
 */
class AnomalyDetector extends RichFlatMapFunction[DQMetrics, DQMetrics] {

  private var rollingStats: ValueState[RollingStats] = _
  private val zScoreThreshold = 3.0
  private val alpha = 0.1 // Exponential moving average factor

  override def open(parameters: Configuration): Unit = {
    val statsDesc = new ValueStateDescriptor[RollingStats]("rolling-stats", classOf[RollingStats])
    rollingStats = getRuntimeContext.getState(statsDesc)
  }

  override def flatMap(metrics: DQMetrics, out: Collector[DQMetrics]): Unit = {
    val currentStats = Option(rollingStats.value()).getOrElse(
      RollingStats(
        meanRate = metrics.messageRatePerSec,
        stdRate = 1.0,
        meanLatency = metrics.avgLatencyMs,
        stdLatency = 10.0,
        sampleCount = 0,
        lastUpdated = System.currentTimeMillis()
      )
    )

    // Calculate Z-scores
    val rateZScore = if (currentStats.stdRate > 0) {
      Math.abs(metrics.messageRatePerSec - currentStats.meanRate) / currentStats.stdRate
    } else 0.0

    val latencyZScore = if (currentStats.stdLatency > 0) {
      Math.abs(metrics.avgLatencyMs - currentStats.meanLatency) / currentStats.stdLatency
    } else 0.0

    // Determine anomalies
    val isRateAnomaly = rateZScore > zScoreThreshold && currentStats.sampleCount >= 10
    val isLatencyAnomaly = latencyZScore > zScoreThreshold && currentStats.sampleCount >= 10

    // Update rolling statistics using exponential moving average
    val newMeanRate = currentStats.meanRate * (1 - alpha) + metrics.messageRatePerSec * alpha
    val newMeanLatency = currentStats.meanLatency * (1 - alpha) + metrics.avgLatencyMs * alpha

    // Update standard deviation (simplified)
    val rateDiff = metrics.messageRatePerSec - currentStats.meanRate
    val newStdRate = Math.sqrt(currentStats.stdRate * currentStats.stdRate * (1 - alpha) + rateDiff * rateDiff * alpha)

    val latencyDiff = metrics.avgLatencyMs - currentStats.meanLatency
    val newStdLatency = Math.sqrt(currentStats.stdLatency * currentStats.stdLatency * (1 - alpha) + latencyDiff * latencyDiff * alpha)

    // Save updated stats
    rollingStats.update(RollingStats(
      meanRate = newMeanRate,
      stdRate = Math.max(newStdRate, 0.1), // Prevent zero std
      meanLatency = newMeanLatency,
      stdLatency = Math.max(newStdLatency, 1.0),
      sampleCount = currentStats.sampleCount + 1,
      lastUpdated = System.currentTimeMillis()
    ))

    // Emit metrics with anomaly flags
    out.collect(metrics.copy(
      isRateAnomaly = isRateAnomaly,
      isLatencyAnomaly = isLatencyAnomaly
    ))
  }
}
