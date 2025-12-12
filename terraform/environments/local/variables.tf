variable "environment" {
  description = "Environment name"
  type        = string
  default     = "local"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "base-data"
}

variable "kafka_image" {
  description = "Kafka Docker image"
  type        = string
  default     = "confluentinc/cp-kafka:7.5.0"
}

variable "clickhouse_image" {
  description = "ClickHouse Docker image"
  type        = string
  default     = "clickhouse/clickhouse-server:23.8"
}

variable "postgres_image" {
  description = "PostgreSQL Docker image"
  type        = string
  default     = "postgres:15"
}

variable "redis_image" {
  description = "Redis Docker image"
  type        = string
  default     = "redis:7.2-alpine"
}
