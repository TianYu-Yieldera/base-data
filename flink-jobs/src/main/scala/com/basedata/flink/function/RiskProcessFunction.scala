package com.basedata.flink.function

import com.basedata.flink.model._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor, MapStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.{Collector, OutputTag}
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.UUID

/**
 * Companion object holding static definitions for RiskProcessFunction
 */
object RiskProcessFunction {
  // Side output tag for alerts - defined as static to ensure serialization works correctly
  val alertOutputTag: OutputTag[RiskAlert] = new OutputTag[RiskAlert]("risk-alerts") {}

  // State descriptor for broadcast prices - static for consistency
  val priceStateDescriptor: MapStateDescriptor[String, PriceInfo] = new MapStateDescriptor[String, PriceInfo](
    "price-state",
    classOf[String],
    classOf[PriceInfo]
  )
}

/**
 * Risk calculation process function that joins position events with broadcast price updates.
 *
 * Key: user_address
 * Input 1: PositionEvent (keyed stream)
 * Input 2: PriceEvent (broadcast stream)
 * Output: RiskMetricsRecord to ClickHouse, RiskAlert to Kafka (via side output)
 */
class RiskProcessFunction(
  jobId: String,
  debounceWindowMs: Long = 60000 // 1 minute debounce for alerts
) extends KeyedBroadcastProcessFunction[String, PositionEvent, PriceEvent, RiskMetricsRecord] {

  private val logger = LoggerFactory.getLogger(getClass)

  // Keyed state for risk tracking
  private var riskState: ValueState[RiskState] = _
  private var lastAlertTime: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    // Configure risk state with TTL (7 days for inactive users)
    val riskStateDesc = new ValueStateDescriptor[RiskState]("risk-state", classOf[RiskState])
    val ttlConfig = org.apache.flink.api.common.state.StateTtlConfig
      .newBuilder(Time.days(7))
      .setUpdateType(org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired)
      .cleanupInRocksdbCompactFilter(1000)
      .build()
    riskStateDesc.enableTimeToLive(ttlConfig)
    riskState = getRuntimeContext.getState(riskStateDesc)

    // Last alert time state
    val lastAlertDesc = new ValueStateDescriptor[java.lang.Long]("last-alert-time", classOf[java.lang.Long])
    lastAlertDesc.enableTimeToLive(ttlConfig)
    lastAlertTime = getRuntimeContext.getState(lastAlertDesc)
  }

  /**
   * Process position events (main keyed stream)
   *
   * Processing order for state consistency:
   * 1. Read current state
   * 2. Calculate new state
   * 3. Generate alert if needed (before state update for consistency)
   * 4. Update state
   * 5. Emit metrics
   */
  override def processElement(
    position: PositionEvent,
    ctx: KeyedBroadcastProcessFunction[String, PositionEvent, PriceEvent, RiskMetricsRecord]#ReadOnlyContext,
    out: Collector[RiskMetricsRecord]
  ): Unit = {
    val now = Instant.now()
    val currentState = Option(riskState.value())

    // Calculate new risk level
    val newRiskLevel = RiskLevel.fromHealthFactor(position.healthFactor)

    // Determine if risk level changed
    val (previousRiskLevel, riskChangedAt) = currentState match {
      case Some(state) if state.currentRiskLevel != newRiskLevel =>
        (state.currentRiskLevel, now)
      case Some(state) =>
        (state.previousRiskLevel, state.riskChangedAt)
      case None =>
        (newRiskLevel, now)
    }

    // Build new state
    val newState = RiskState(
      chain = position.chain,
      protocol = position.protocol,
      userAddress = position.userAddress,
      currentRiskLevel = newRiskLevel,
      previousRiskLevel = previousRiskLevel,
      healthFactor = position.healthFactor,
      totalCollateralUsd = position.totalCollateralUsd,
      totalDebtUsd = position.totalDebtUsd,
      availableBorrowUsd = position.availableBorrowUsd,
      ltv = position.ltv,
      liquidationThreshold = position.liquidationThreshold,
      blockNumber = position.blockNumber,
      lastUpdated = now,
      riskChangedAt = riskChangedAt
    )

    // IMPORTANT: Generate alert BEFORE updating state for checkpoint consistency
    // If checkpoint happens after state update but before alert output,
    // we would lose the alert on recovery
    currentState.foreach { prevState =>
      if (shouldGenerateAlert(prevState, newState, now)) {
        val alert = generateAlert(prevState, newState, position.blockNumber, now)
        ctx.output(RiskProcessFunction.alertOutputTag, alert)
        lastAlertTime.update(now.toEpochMilli)
        logger.info(s"Generated alert for ${position.userAddress}: ${alert.alertType}")
      }
    }

    // Update state after alert generation
    riskState.update(newState)

    // Emit metrics record to ClickHouse
    out.collect(RiskMetricsRecord(
      chain = newState.chain,
      userAddress = newState.userAddress,
      protocol = newState.protocol,
      healthFactor = newState.healthFactor,
      riskLevel = newState.currentRiskLevel.toString,
      previousRiskLevel = newState.previousRiskLevel.toString,
      riskChangedAt = newState.riskChangedAt.toEpochMilli,
      totalCollateralUsd = newState.totalCollateralUsd,
      totalDebtUsd = newState.totalDebtUsd,
      availableBorrowUsd = newState.availableBorrowUsd,
      ltv = newState.ltv,
      liquidationThreshold = newState.liquidationThreshold,
      blockNumber = newState.blockNumber,
      updatedAt = now.toEpochMilli,
      version = now.toEpochMilli
    ))
  }

  /**
   * Process broadcast price updates
   */
  override def processBroadcastElement(
    price: PriceEvent,
    ctx: KeyedBroadcastProcessFunction[String, PositionEvent, PriceEvent, RiskMetricsRecord]#Context,
    out: Collector[RiskMetricsRecord]
  ): Unit = {
    val broadcastState = ctx.getBroadcastState(RiskProcessFunction.priceStateDescriptor)
    broadcastState.put(price.symbol, PriceInfo(
      symbol = price.symbol,
      priceUsd = price.priceUsd,
      timestamp = price.timestamp,
      source = price.source
    ))
    logger.debug(s"Updated price for ${price.symbol}: ${price.priceUsd}")
  }

  /**
   * Determine if an alert should be generated based on state transition
   */
  private def shouldGenerateAlert(prev: RiskState, curr: RiskState, now: Instant): Boolean = {
    // Check debounce
    val lastAlert = Option(lastAlertTime.value()).map(_.longValue()).getOrElse(0L)
    val debounceMs = getDebounceMs(curr.currentRiskLevel)
    if (now.toEpochMilli - lastAlert < debounceMs) {
      return false
    }

    // Alert conditions
    val riskIncreased = curr.currentRiskLevel.id > prev.currentRiskLevel.id
    val riskDecreased = curr.currentRiskLevel.id < prev.currentRiskLevel.id &&
                        curr.currentRiskLevel == RiskLevel.SAFE
    val liquidationRisk = curr.currentRiskLevel == RiskLevel.LIQUIDATABLE &&
                          prev.currentRiskLevel != RiskLevel.LIQUIDATABLE

    // Rapid health factor decline (> 20% in state)
    val rapidDecline = prev.healthFactor > 0 &&
                       (prev.healthFactor - curr.healthFactor) / prev.healthFactor > 0.2

    riskIncreased || riskDecreased || liquidationRisk || rapidDecline
  }

  /**
   * Get debounce window based on severity
   */
  private def getDebounceMs(riskLevel: RiskLevel.Value): Long = riskLevel match {
    case RiskLevel.LIQUIDATABLE => 0          // Immediate
    case RiskLevel.CRITICAL => 60000          // 1 minute
    case RiskLevel.DANGER => 120000           // 2 minutes
    case RiskLevel.WARNING => 300000          // 5 minutes
    case RiskLevel.SAFE => 300000             // 5 minutes
  }

  /**
   * Generate alert based on state transition
   */
  private def generateAlert(prev: RiskState, curr: RiskState, blockNumber: Long, now: Instant): RiskAlert = {
    val (alertType, severity) = determineAlertTypeAndSeverity(prev, curr)
    val hfChangePct = if (prev.healthFactor > 0) {
      ((curr.healthFactor - prev.healthFactor) / prev.healthFactor) * 100
    } else 0.0

    RiskAlert(
      alertId = UUID.randomUUID().toString,
      alertType = alertType,
      severity = severity,
      chain = curr.chain,
      protocol = curr.protocol,
      userAddress = curr.userAddress,
      previousRiskLevel = prev.currentRiskLevel.toString,
      currentRiskLevel = curr.currentRiskLevel.toString,
      previousHealthFactor = prev.healthFactor,
      currentHealthFactor = curr.healthFactor,
      healthFactorChangePct = hfChangePct,
      totalCollateralUsd = curr.totalCollateralUsd,
      totalDebtUsd = curr.totalDebtUsd,
      liquidationThreshold = curr.liquidationThreshold,
      ltv = curr.ltv,
      blockNumber = blockNumber,
      eventTime = Instant.ofEpochMilli(curr.lastUpdated.toEpochMilli),
      processingTime = now,
      flinkJobId = jobId
    )
  }

  /**
   * Determine alert type and severity based on state transition
   */
  private def determineAlertTypeAndSeverity(prev: RiskState, curr: RiskState): (String, String) = {
    val riskIncreased = curr.currentRiskLevel.id > prev.currentRiskLevel.id
    val rapidDecline = prev.healthFactor > 0 &&
                       (prev.healthFactor - curr.healthFactor) / prev.healthFactor > 0.2

    if (curr.currentRiskLevel == RiskLevel.LIQUIDATABLE && prev.currentRiskLevel != RiskLevel.LIQUIDATABLE) {
      ("LIQUIDATION_RISK", "CRITICAL")
    } else if (rapidDecline) {
      ("RAPID_HF_DECLINE", "HIGH")
    } else if (riskIncreased) {
      val severity = curr.currentRiskLevel match {
        case RiskLevel.CRITICAL => "HIGH"
        case RiskLevel.DANGER => "WARNING"
        case _ => "INFO"
      }
      ("RISK_INCREASED", severity)
    } else {
      ("RISK_DECREASED", "INFO")
    }
  }
}
