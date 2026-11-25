# ---------------------------------------------------------------------------
# The ONLY four values a user must provide (in terraform.tfvars).
# Region, Claude model, resource names and sizing are fixed in main.tf locals.
# ---------------------------------------------------------------------------

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (Cloud resource management)."
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret."
  type        = string
  sensitive   = true
}

variable "aws_access_key_id" {
  description = "AWS IAM user access key ID with Amazon Bedrock invoke permission."
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS IAM user secret access key."
  type        = string
  sensitive   = true
}

variable "deploy_flink_pipeline" {
  description = "true = one-command demo (deploys model, tools, agent, windowing and scheduling statements). false = infra + tables + functions only, so a presenter can build the agent live from sql/ in the Flink workspace."
  type        = bool
  default     = true
}
