output "environment_id" {
  value = confluent_environment.main.id
}

output "kafka_cluster_id" {
  value = confluent_kafka_cluster.standard.id
}

output "flink_compute_pool_id" {
  value = confluent_flink_compute_pool.main.id
}

output "tools_artifact_id" {
  value = confluent_flink_artifact.tools.id
}

output "flink_catalog" {
  value       = confluent_environment.main.display_name
  description = "Catalog to select in the Flink workspace"
}

output "flink_database" {
  value       = confluent_kafka_cluster.standard.display_name
  description = "Database to select in the Flink workspace"
}

output "flink_workspace_url" {
  value = "https://confluent.cloud/go/flink"
}

output "dotenv_path" {
  value = local_file.dotenv.filename
}
