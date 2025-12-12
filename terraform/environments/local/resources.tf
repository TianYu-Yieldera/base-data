resource "docker_network" "base_data" {
  name = "${var.project_name}-network"
}

resource "docker_volume" "kafka_data" {
  name = "${var.project_name}-kafka-data"
}

resource "docker_volume" "clickhouse_data" {
  name = "${var.project_name}-clickhouse-data"
}

resource "docker_volume" "postgres_data" {
  name = "${var.project_name}-postgres-data"
}

resource "docker_volume" "redis_data" {
  name = "${var.project_name}-redis-data"
}
