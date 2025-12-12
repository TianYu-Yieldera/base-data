package com.basedata.flink.job

import com.basedata.flink.function.RiskProcessFunction
import com.basedata.flink.model._
import com.basedata.flink.sink.{ClickHouseRiskSink, KafkaAlertSink}
import com.basedata.flink.source.{KafkaPositionSource, KafkaPriceSource}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.UUID

/**
 * CDP Risk Processor Job
 *
 * Consumes position snapshots and price updates from Kafka,
 * calculates risk levels, and outputs:
 * - Risk metrics to ClickHouse
 * - Risk alerts to Kafka
 */
object RiskProcessorJob {

  private val logger = LoggerFactory.getLogger(getClass)

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
    logger.info(s"Starting Risk Processor Job with ID: $jobId")

    // Create execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    // Configure checkpointing
    env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(600000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

    // Watermark strategy for position events (30 second out-of-orderness)
    val positionWatermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[PositionEvent](Duration.ofSeconds(30))
      .withTimestampAssigner((event, _) => event.timestamp)
      .withIdleness(Duration.ofMinutes(1))

    // Watermark strategy for price events
    val priceWatermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[PriceEvent](Duration.ofSeconds(10))
      .withTimestampAssigner((event, _) => event.timestamp)
      .withIdleness(Duration.ofMinutes(1))

    // Create position source (keyed stream)
    val positionSource = KafkaPositionSource.create(kafkaBrokers, schemaRegistryUrl)
    val positionStream = env
      .fromSource(positionSource, positionWatermarkStrategy, "Position Source")
      .uid("position-source")
      .name("Kafka Position Source")

    // Create price source (broadcast stream)
    val priceSource = KafkaPriceSource.create(kafkaBrokers, schemaRegistryUrl)
    val priceStream = env
      .fromSource(priceSource, priceWatermarkStrategy, "Price Source")
      .uid("price-source")
      .name("Kafka Price Source")
      .setParallelism(1) // Single parallelism for broadcast source

    // Create process function
    val riskProcessFunction = new RiskProcessFunction(jobId)

    // Broadcast prices and connect with positions
    val broadcastPriceStream = priceStream.broadcast(RiskProcessFunction.priceStateDescriptor)

    // Key positions by user address and connect with broadcast prices
    val resultStream = positionStream
      .keyBy(_.userAddress)
      .connect(broadcastPriceStream)
      .process(riskProcessFunction)
      .uid("risk-processor")
      .name("Risk Processor")

    // Output risk metrics to ClickHouse
    resultStream
      .addSink(new ClickHouseRiskSink(clickhouseHost, clickhousePort, clickhouseDb))
      .uid("clickhouse-sink")
      .name("ClickHouse Risk Metrics Sink")
      .setParallelism(2)

    // Output alerts to Kafka (from side output)
    val alertStream = resultStream
      .getSideOutput(RiskProcessFunction.alertOutputTag)

    alertStream
      .sinkTo(KafkaAlertSink.create(kafkaBrokers, schemaRegistryUrl))
      .uid("kafka-alert-sink")
      .name("Kafka Alert Sink")
      .setParallelism(1)

    // Execute
    logger.info("Starting Risk Processor Job execution...")
    env.execute(s"CDP Risk Processor [$jobId]")
  }
}
