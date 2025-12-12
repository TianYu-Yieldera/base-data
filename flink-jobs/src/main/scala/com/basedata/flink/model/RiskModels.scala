package com.basedata.flink.model

import java.time.Instant

/**
 * Risk levels based on health factor thresholds
 */
object RiskLevel extends Enumeration {
  type RiskLevel = Value
  val SAFE, WARNING, DANGER, CRITICAL, LIQUIDATABLE = Value

  def fromHealthFactor(hf: Double): RiskLevel = {
    if (hf >= 2.0) SAFE
    else if (hf >= 1.5) WARNING
    else if (hf >= 1.2) DANGER
    else if (hf >= 1.0) CRITICAL
    else LIQUIDATABLE
  }
}

/**
 * Position snapshot from Kafka
 */
@SerialVersionUID(1L)
case class PositionEvent(
  chain: String,
  protocol: String,
  userAddress: String,
  blockNumber: Long,
  totalCollateralEth: BigDecimal,
  totalDebtEth: BigDecimal,
  availableBorrowEth: BigDecimal,
  healthFactor: Double,
  ltv: Double,
  liquidationThreshold: Double,
  totalCollateralUsd: BigDecimal,
  totalDebtUsd: BigDecimal,
  availableBorrowUsd: BigDecimal,
  timestamp: Long,
  eventTime: Instant = Instant.now()
) extends Serializable

/**
 * Price update from Kafka
 */
@SerialVersionUID(1L)
case class PriceEvent(
  chain: String,
  symbol: String,
  assetAddress: String,
  priceUsd: Double,
  source: String,
  timestamp: Long,
  eventTime: Instant = Instant.now()
) extends Serializable

/**
 * Aave event from Kafka (supply, withdraw, borrow, repay, liquidation)
 */
@SerialVersionUID(1L)
case class AaveEvent(
  eventId: String,
  eventType: String,
  chain: String,
  protocol: String,
  userAddress: String,
  reserveAddress: String,
  amount: String,
  amountUsd: Option[Double],
  onBehalfOf: Option[String],
  liquidator: Option[String],
  blockNumber: Long,
  txHash: String,
  logIndex: Int,
  timestamp: Long,
  eventTime: Instant = Instant.now()
) extends Serializable

/**
 * Internal state for risk calculation
 */
@SerialVersionUID(1L)
case class RiskState(
  chain: String,
  protocol: String,
  userAddress: String,
  currentRiskLevel: RiskLevel.Value,
  previousRiskLevel: RiskLevel.Value,
  healthFactor: Double,
  totalCollateralUsd: BigDecimal,
  totalDebtUsd: BigDecimal,
  availableBorrowUsd: BigDecimal,
  ltv: Double,
  liquidationThreshold: Double,
  blockNumber: Long,
  lastUpdated: Instant,
  riskChangedAt: Instant
) extends Serializable

/**
 * Risk alert output
 */
@SerialVersionUID(1L)
case class RiskAlert(
  alertId: String,
  alertType: String, // RISK_INCREASED, RISK_DECREASED, LIQUIDATION_RISK, RAPID_HF_DECLINE
  severity: String,  // INFO, WARNING, HIGH, CRITICAL
  chain: String,
  protocol: String,
  userAddress: String,
  previousRiskLevel: String,
  currentRiskLevel: String,
  previousHealthFactor: Double,
  currentHealthFactor: Double,
  healthFactorChangePct: Double,
  totalCollateralUsd: BigDecimal,
  totalDebtUsd: BigDecimal,
  liquidationThreshold: Double,
  ltv: Double,
  blockNumber: Long,
  eventTime: Instant,
  processingTime: Instant = Instant.now(),
  flinkJobId: String = ""
) extends Serializable

/**
 * Price info stored in broadcast state
 */
@SerialVersionUID(1L)
case class PriceInfo(
  symbol: String,
  priceUsd: Double,
  timestamp: Long,
  source: String
) extends Serializable

/**
 * Output record for rt_cdp_risk_metrics
 */
@SerialVersionUID(1L)
case class RiskMetricsRecord(
  chain: String,
  userAddress: String,
  protocol: String,
  healthFactor: Double,
  riskLevel: String,
  previousRiskLevel: String,
  riskChangedAt: Long,
  totalCollateralUsd: BigDecimal,
  totalDebtUsd: BigDecimal,
  availableBorrowUsd: BigDecimal,
  ltv: Double,
  liquidationThreshold: Double,
  blockNumber: Long,
  updatedAt: Long,
  version: Long
) extends Serializable
