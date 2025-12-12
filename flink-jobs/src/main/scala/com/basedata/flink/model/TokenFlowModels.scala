package com.basedata.flink.model

import java.time.Instant

/**
 * ERC20 transfer event from Kafka
 */
@SerialVersionUID(1L)
case class TransferEvent(
  transferId: String,
  chain: String,
  tokenAddress: String,
  tokenSymbol: Option[String],
  tokenDecimals: Option[Int],
  fromAddress: String,
  toAddress: String,
  amountRaw: String,
  amount: Option[Double],
  amountUsd: Option[Double],
  blockNumber: Long,
  txHash: String,
  logIndex: Int,
  timestamp: Long,
  eventTime: Instant = Instant.now()
) extends Serializable

/**
 * Hourly token flow aggregation result
 */
@SerialVersionUID(1L)
case class TokenFlowHourly(
  chain: String,
  tokenAddress: String,
  hour: Long,           // hour timestamp in millis
  transferCount: Long,
  uniqueSendersCount: Long,
  uniqueReceiversCount: Long,
  totalVolume: BigDecimal,
  avgTransferSize: Double,
  maxTransferSize: BigDecimal,
  updatedAt: Long
) extends Serializable

/**
 * Output record for rt_token_flow_hourly (simplified for ClickHouse)
 */
@SerialVersionUID(1L)
case class TokenFlowRecord(
  chain: String,
  tokenAddress: String,
  hour: Long,
  transferCount: Long,
  uniqueSendersCount: Long,
  uniqueReceiversCount: Long,
  totalVolume: String,      // Decimal as string
  avgTransferSize: Double,
  maxTransferSize: String,  // Decimal as string
  updatedAt: Long
) extends Serializable

/**
 * Late transfer event (beyond allowed lateness)
 */
@SerialVersionUID(1L)
case class LateTransferEvent(
  originalEvent: TransferEvent,
  windowEnd: Long,
  lateBy: Long  // milliseconds
) extends Serializable
