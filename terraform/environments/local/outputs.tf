output "network_name" {
  description = "Docker network name"
  value       = docker_network.base_data.name
}
