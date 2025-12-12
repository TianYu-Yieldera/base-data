name := "base-data-flink"
version := "0.1.0"
scalaVersion := "2.12.18"

val flinkVersion = "1.17.2"
val clickhouseVersion = "0.5.0"
val kafkaVersion = "3.5.1"

// Add Confluent repository for Schema Registry dependencies
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

// =============================================================================
// 本地开发模式：去掉 "provided"，可以直接 sbt run 运行
// 打包部署模式：加回 "provided"，减小 JAR 体积
// =============================================================================
// 切换方法：
//   本地开发: localDev := true  (当前)
//   打包部署: localDev := false
// =============================================================================
val localDev = true  // <-- 切换这里！

def provided(dev: Boolean): Option[String] = if (dev) None else Some("provided")

// Flink dependencies
libraryDependencies ++= Seq(
  // Flink 核心库 - 本地开发时需要，打包时 Flink 集群已有
  "org.apache.flink" %% "flink-scala" % flinkVersion % provided(localDev).getOrElse("compile"),
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % provided(localDev).getOrElse("compile"),
  "org.apache.flink" % "flink-clients" % flinkVersion % provided(localDev).getOrElse("compile"),
  "org.apache.flink" % "flink-runtime-web" % flinkVersion % provided(localDev).getOrElse("compile"),

  // Flink connectors - 始终打包
  "org.apache.flink" % "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" % "flink-avro-confluent-registry" % flinkVersion,
  "org.apache.flink" % "flink-avro" % flinkVersion,

  // State backend - 本地开发时需要
  "org.apache.flink" % "flink-statebackend-rocksdb" % flinkVersion % provided(localDev).getOrElse("compile"),

  // ClickHouse JDBC
  "com.clickhouse" % "clickhouse-jdbc" % clickhouseVersion,
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1",

  // JSON processing
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.15.2",

  // Logging - 本地开发时需要
  "org.slf4j" % "slf4j-api" % "2.0.9",
  "ch.qos.logback" % "logback-classic" % "1.4.11" % provided(localDev).getOrElse("compile"),

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.apache.flink" % "flink-test-utils" % flinkVersion % Test,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % Test classifier "tests"
)

// Assembly settings
assembly / assemblyJarName := "base-data-flink-assembly.jar"
assembly / mainClass := Some("com.basedata.flink.job.RiskProcessorJob")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case "log4j2.properties" => MergeStrategy.first
  case "log4j.properties" => MergeStrategy.first
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.contains("hadoop") => MergeStrategy.first
  case _ => MergeStrategy.first
}

// Compiler options
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked"
)

// Fork for running locally
run / fork := true
run / javaOptions ++= Seq(
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
)

// Cancel running job with Ctrl+C
run / connectInput := true
