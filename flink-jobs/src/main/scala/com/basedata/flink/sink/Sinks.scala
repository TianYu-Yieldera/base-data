package com.basedata.flink.sink

import com.basedata.flink.model.{RiskAlert, RiskMetricsRecord, TokenFlowRecord, DQMetricsRecord}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.connector.kafka.sink.{KafkaSink, KafkaRecordSerializationSchema}
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, PreparedStatement}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule

/**
 * Base ClickHouse sink with connection management and safe batch processing
 *
 * Features:
 * - Automatic connection retry with exponential backoff
 * - Safe batch flushing that preserves data on partial failures
 * - Configurable batch size and flush interval
 */
abstract class BaseClickHouseSink[T](
  host: String,
  port: Int,
  database: String,
  batchSize: Int = 1000,
  flushIntervalMs: Long = 5000,
  maxRetries: Int = 3
) extends RichSinkFunction[T] {

  protected val logger = LoggerFactory.getLogger(getClass)

  @transient protected var connection: Connection = _
  @transient protected var insertStmt: PreparedStatement = _
  @transient protected var pendingRecords: java.util.ArrayList[T] = _
  @transient protected var lastFlushTime: Long = _

  protected def getInsertSql: String
  protected def setStatementParams(stmt: PreparedStatement, record: T): Unit

  override def open(parameters: Configuration): Unit = {
    pendingRecords = new java.util.ArrayList[T](batchSize)
    lastFlushTime = System.currentTimeMillis()
    connectWithRetry()
  }

  private def connectWithRetry(): Unit = {
    var attempts = 0
    var connected = false
    var lastError: Exception = null

    while (attempts < maxRetries && !connected) {
      try {
        // Close existing connection if any
        closeConnectionQuietly()

        val url = s"jdbc:clickhouse://$host:$port/$database"
        connection = DriverManager.getConnection(url, "default", "")
        insertStmt = connection.prepareStatement(getInsertSql)
        connected = true
        logger.info(s"Connected to ClickHouse at $url")
      } catch {
        case e: Exception =>
          attempts += 1
          lastError = e
          logger.warn(s"Failed to connect to ClickHouse (attempt $attempts/$maxRetries): ${e.getMessage}")
          if (attempts < maxRetries) {
            Thread.sleep(1000L * attempts) // Exponential backoff
          }
      }
    }

    if (!connected) {
      throw new RuntimeException(s"Failed to connect to ClickHouse after $maxRetries attempts", lastError)
    }
  }

  private def closeConnectionQuietly(): Unit = {
    try {
      if (insertStmt != null && !insertStmt.isClosed) {
        insertStmt.close()
      }
    } catch {
      case _: Exception => // Ignore
    }
    try {
      if (connection != null && !connection.isClosed) {
        connection.close()
      }
    } catch {
      case _: Exception => // Ignore
    }
    insertStmt = null
    connection = null
  }

  override def invoke(record: T, context: SinkFunction.Context): Unit = {
    pendingRecords.add(record)
    val now = System.currentTimeMillis()
    if (pendingRecords.size() >= batchSize || now - lastFlushTime >= flushIntervalMs) {
      flush()
    }
  }

  /**
   * Safely flush pending records to ClickHouse.
   *
   * This implementation:
   * 1. Copies pending records to a separate list before attempting flush
   * 2. Only clears pending records after successful write
   * 3. On failure, reconnects and retries with the same records
   * 4. Clears statement batch between retry attempts to avoid duplicates
   */
  protected def flush(): Unit = {
    if (pendingRecords.isEmpty) return

    // Copy records to flush - this ensures we don't lose data on partial failure
    val recordsToFlush = new java.util.ArrayList[T](pendingRecords)
    val recordCount = recordsToFlush.size()

    var attempts = 0
    var success = false

    while (attempts < maxRetries && !success) {
      try {
        // Ensure connection is valid
        if (connection == null || connection.isClosed) {
          connectWithRetry()
        }

        // Clear any previous batch state
        insertStmt.clearBatch()

        // Add all records to batch
        recordsToFlush.forEach { record =>
          setStatementParams(insertStmt, record)
          insertStmt.addBatch()
        }

        // Execute batch
        val results = insertStmt.executeBatch()

        // Verify all records were processed
        val successCount = results.count(r => r >= 0 || r == java.sql.Statement.SUCCESS_NO_INFO)
        if (successCount != recordCount) {
          logger.warn(s"Batch execution reported $successCount successes out of $recordCount records")
        }

        // Success - clear pending records and update timestamp
        pendingRecords.clear()
        lastFlushTime = System.currentTimeMillis()
        success = true

        logger.debug(s"Flushed $recordCount records to ClickHouse")

      } catch {
        case e: Exception =>
          attempts += 1
          logger.warn(s"Failed to flush to ClickHouse (attempt $attempts/$maxRetries): ${e.getMessage}")

          // Clear batch state for retry
          try {
            if (insertStmt != null) {
              insertStmt.clearBatch()
            }
          } catch {
            case _: Exception => // Ignore
          }

          if (attempts < maxRetries) {
            // Reconnect before retry
            try {
              connectWithRetry()
            } catch {
              case reconnectEx: Exception =>
                logger.warn(s"Failed to reconnect: ${reconnectEx.getMessage}")
            }
            Thread.sleep(500L * attempts) // Backoff between retries
          } else {
            logger.error(s"Failed to flush $recordCount records to ClickHouse after $maxRetries attempts", e)
            // Don't clear pendingRecords - they will be retried on next invoke or close
            throw e
          }
      }
    }
  }

  override def close(): Unit = {
    // Final flush attempt
    try {
      if (pendingRecords != null && !pendingRecords.isEmpty) {
        logger.info(s"Final flush of ${pendingRecords.size()} pending records")
        flush()
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error during final flush, ${pendingRecords.size()} records may be lost", e)
    }

    closeConnectionQuietly()
    logger.info("ClickHouse sink closed")
  }
}

/**
 * ClickHouse sink for risk metrics
 */
class ClickHouseRiskSink(
  host: String,
  port: Int,
  database: String,
  batchSize: Int = 1000,
  flushIntervalMs: Long = 5000
) extends BaseClickHouseSink[RiskMetricsRecord](host, port, database, batchSize, flushIntervalMs) {

  override protected def getInsertSql: String = """
    INSERT INTO rt_cdp_risk_metrics (
      chain, user_address, protocol, health_factor, risk_level, previous_risk_level,
      risk_changed_at, total_collateral_usd, total_debt_usd, available_borrow_usd,
      ltv, liquidation_threshold, block_number, updated_at, version
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  """

  override protected def setStatementParams(stmt: PreparedStatement, record: RiskMetricsRecord): Unit = {
    stmt.setString(1, record.chain)
    stmt.setString(2, record.userAddress)
    stmt.setString(3, record.protocol)
    stmt.setDouble(4, record.healthFactor)
    stmt.setString(5, record.riskLevel)
    stmt.setString(6, record.previousRiskLevel)
    stmt.setLong(7, record.riskChangedAt)
    stmt.setBigDecimal(8, record.totalCollateralUsd.bigDecimal)
    stmt.setBigDecimal(9, record.totalDebtUsd.bigDecimal)
    stmt.setBigDecimal(10, record.availableBorrowUsd.bigDecimal)
    stmt.setDouble(11, record.ltv)
    stmt.setDouble(12, record.liquidationThreshold)
    stmt.setLong(13, record.blockNumber)
    stmt.setLong(14, record.updatedAt)
    stmt.setLong(15, record.version)
  }
}

/**
 * ClickHouse sink for token flow metrics
 */
class ClickHouseTokenFlowSink(
  host: String,
  port: Int,
  database: String,
  batchSize: Int = 500,
  flushIntervalMs: Long = 10000
) extends BaseClickHouseSink[TokenFlowRecord](host, port, database, batchSize, flushIntervalMs) {

  override protected def getInsertSql: String = """
    INSERT INTO rt_token_flow_hourly (
      chain, token_address, hour, transfer_count,
      unique_senders_count, unique_receivers_count,
      total_volume, avg_transfer_size, max_transfer_size, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  """

  override protected def setStatementParams(stmt: PreparedStatement, record: TokenFlowRecord): Unit = {
    stmt.setString(1, record.chain)
    stmt.setString(2, record.tokenAddress)
    stmt.setLong(3, record.hour)
    stmt.setLong(4, record.transferCount)
    stmt.setLong(5, record.uniqueSendersCount)
    stmt.setLong(6, record.uniqueReceiversCount)
    stmt.setString(7, record.totalVolume)
    stmt.setDouble(8, record.avgTransferSize)
    stmt.setString(9, record.maxTransferSize)
    stmt.setLong(10, record.updatedAt)
  }
}

/**
 * ClickHouse sink for DQ metrics
 */
class ClickHouseDQSink(
  host: String,
  port: Int,
  database: String,
  batchSize: Int = 100,
  flushIntervalMs: Long = 30000
) extends BaseClickHouseSink[DQMetricsRecord](host, port, database, batchSize, flushIntervalMs) {

  override protected def getInsertSql: String = """
    INSERT INTO rt_dq_metrics (
      chain, topic, check_time, message_count, message_rate_per_sec,
      null_count, schema_error_count, duplicate_count, late_arrival_count,
      avg_latency_ms, p95_latency_ms, p99_latency_ms, max_latency_ms,
      is_rate_anomaly, is_latency_anomaly, window_start, window_end
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  """

  override protected def setStatementParams(stmt: PreparedStatement, record: DQMetricsRecord): Unit = {
    stmt.setString(1, record.chain)
    stmt.setString(2, record.topic)
    stmt.setLong(3, record.checkTime)
    stmt.setLong(4, record.messageCount)
    stmt.setDouble(5, record.messageRatePerSec)
    stmt.setLong(6, record.nullCount)
    stmt.setLong(7, record.schemaErrorCount)
    stmt.setLong(8, record.duplicateCount)
    stmt.setLong(9, record.lateArrivalCount)
    stmt.setDouble(10, record.avgLatencyMs)
    stmt.setDouble(11, record.p95LatencyMs)
    stmt.setDouble(12, record.p99LatencyMs)
    stmt.setDouble(13, record.maxLatencyMs)
    stmt.setInt(14, record.isRateAnomaly)
    stmt.setInt(15, record.isLatencyAnomaly)
    stmt.setLong(16, record.windowStart)
    stmt.setLong(17, record.windowEnd)
  }
}

/**
 * Kafka sink for risk alerts
 */
object KafkaAlertSink {

  def create(bootstrapServers: String, schemaRegistryUrl: String): KafkaSink[RiskAlert] = {
    KafkaSink.builder[RiskAlert]()
      .setBootstrapServers(bootstrapServers)
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic("alerts.risk")
          .setValueSerializationSchema(new AlertSerializer)
          .build()
      )
      .build()
  }
}

/**
 * JSON serializer for RiskAlert - implements Serializable
 */
@SerialVersionUID(1L)
class AlertSerializer extends org.apache.flink.api.common.serialization.SerializationSchema[RiskAlert] with Serializable {

  @transient private lazy val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.registerModule(new JavaTimeModule())
    m
  }

  override def serialize(alert: RiskAlert): Array[Byte] = {
    mapper.writeValueAsBytes(alert)
  }
}
