package com.basedata.flink.source

import com.basedata.flink.model.{PositionEvent, PriceEvent, TransferEvent}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema

import java.time.Instant
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Kafka source for Position events
 */
object KafkaPositionSource {

  def create(bootstrapServers: String, schemaRegistryUrl: String): KafkaSource[PositionEvent] = {
    KafkaSource.builder[PositionEvent]()
      .setBootstrapServers(bootstrapServers)
      .setTopics("base.positions")
      .setGroupId("flink-risk-processor-positions")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new PositionDeserializer))
      .setProperty("schema.registry.url", schemaRegistryUrl)
      .build()
  }
}

/**
 * Kafka source for Price events
 */
object KafkaPriceSource {

  def create(bootstrapServers: String, schemaRegistryUrl: String): KafkaSource[PriceEvent] = {
    KafkaSource.builder[PriceEvent]()
      .setBootstrapServers(bootstrapServers)
      .setTopics("market.prices")
      .setGroupId("flink-risk-processor-prices")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new PriceDeserializer))
      .setProperty("schema.registry.url", schemaRegistryUrl)
      .build()
  }
}

/**
 * Kafka source for Transfer events
 */
object KafkaTransferSource {

  def create(bootstrapServers: String, schemaRegistryUrl: String): KafkaSource[TransferEvent] = {
    KafkaSource.builder[TransferEvent]()
      .setBootstrapServers(bootstrapServers)
      .setTopics("base.erc20.transfers")
      .setGroupId("flink-token-flow-transfers")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new TransferDeserializer))
      .setProperty("schema.registry.url", schemaRegistryUrl)
      .build()
  }
}

/**
 * JSON deserializer for Position events
 */
@SerialVersionUID(1L)
class PositionDeserializer extends DeserializationSchema[PositionEvent] with Serializable {

  @transient private lazy val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  override def deserialize(message: Array[Byte]): PositionEvent = {
    val json = mapper.readTree(message)
    PositionEvent(
      chain = json.path("chain").asText("base"),
      protocol = json.path("protocol").asText("aave_v3"),
      userAddress = json.path("user_address").asText(),
      blockNumber = json.path("block_number").asLong(),
      totalCollateralEth = BigDecimal(json.path("total_collateral_eth").asText("0")),
      totalDebtEth = BigDecimal(json.path("total_debt_eth").asText("0")),
      availableBorrowEth = BigDecimal(json.path("available_borrow_eth").asText("0")),
      healthFactor = json.path("health_factor").asDouble(999.0),
      ltv = json.path("ltv").asDouble(0.0),
      liquidationThreshold = json.path("liquidation_threshold").asDouble(0.0),
      totalCollateralUsd = BigDecimal(json.path("total_collateral_usd").asDouble(0.0)),
      totalDebtUsd = BigDecimal(json.path("total_debt_usd").asDouble(0.0)),
      availableBorrowUsd = BigDecimal(json.path("available_borrow_usd").asDouble(0.0)),
      timestamp = json.path("timestamp").asLong(System.currentTimeMillis()),
      eventTime = Instant.now()
    )
  }

  override def isEndOfStream(nextElement: PositionEvent): Boolean = false

  override def getProducedType: TypeInformation[PositionEvent] = TypeInformation.of(classOf[PositionEvent])
}

/**
 * JSON deserializer for Price events
 */
@SerialVersionUID(1L)
class PriceDeserializer extends DeserializationSchema[PriceEvent] with Serializable {

  @transient private lazy val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  override def deserialize(message: Array[Byte]): PriceEvent = {
    val json = mapper.readTree(message)
    PriceEvent(
      chain = json.path("chain").asText("base"),
      symbol = json.path("asset_symbol").asText(),
      assetAddress = json.path("asset_address").asText(),
      priceUsd = json.path("price_usd").asDouble(0.0),
      source = json.path("source").asText("CHAINLINK"),
      timestamp = json.path("timestamp").asLong(System.currentTimeMillis()),
      eventTime = Instant.now()
    )
  }

  override def isEndOfStream(nextElement: PriceEvent): Boolean = false

  override def getProducedType: TypeInformation[PriceEvent] = TypeInformation.of(classOf[PriceEvent])
}

/**
 * JSON deserializer for Transfer events
 */
@SerialVersionUID(1L)
class TransferDeserializer extends DeserializationSchema[TransferEvent] with Serializable {

  @transient private lazy val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  override def deserialize(message: Array[Byte]): TransferEvent = {
    val json = mapper.readTree(message)
    TransferEvent(
      transferId = json.path("transfer_id").asText(java.util.UUID.randomUUID().toString),
      chain = json.path("chain").asText("base"),
      tokenAddress = json.path("token_address").asText(),
      tokenSymbol = Option(json.path("token_symbol").asText(null)),
      tokenDecimals = if (json.has("token_decimals")) Some(json.path("token_decimals").asInt()) else None,
      fromAddress = json.path("from_address").asText(),
      toAddress = json.path("to_address").asText(),
      amountRaw = json.path("amount_raw").asText("0"),
      amount = if (json.has("amount")) Some(json.path("amount").asDouble()) else None,
      amountUsd = if (json.has("amount_usd")) Some(json.path("amount_usd").asDouble()) else None,
      blockNumber = json.path("block_number").asLong(),
      txHash = json.path("tx_hash").asText(),
      logIndex = json.path("log_index").asInt(),
      timestamp = json.path("timestamp").asLong(System.currentTimeMillis()),
      eventTime = Instant.now()
    )
  }

  override def isEndOfStream(nextElement: TransferEvent): Boolean = false

  override def getProducedType: TypeInformation[TransferEvent] = TypeInformation.of(classOf[TransferEvent])
}
