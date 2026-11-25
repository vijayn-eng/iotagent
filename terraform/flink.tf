# ---------------------------------------------------------------------------
# All Flink SQL lives in ../sql (single source of truth: the same files are
# pasted live in the Flink workspace during the demo). Leading "--" comment
# lines are stripped before submission.
# ---------------------------------------------------------------------------
locals {
  sql_dir = "${path.module}/../sql"

  tables    = ["work_orders", "machine_telemetry", "material_inventory", "production_schedule"]
  functions = ["reroute_order", "expedite_material", "schedule_maintenance", "notify_supervisor"]

  flink_common = {
    organization_id  = data.confluent_organization.main.id
    environment_id   = confluent_environment.main.id
    compute_pool_id  = confluent_flink_compute_pool.main.id
    principal_id     = confluent_service_account.app_manager.id
    rest_endpoint    = data.confluent_flink_region.main.rest_endpoint
    flink_api_key    = confluent_api_key.flink.id
    flink_api_secret = confluent_api_key.flink.secret
    catalog          = local.catalog
    database         = local.database
  }
}

# Strip SQL comment lines so each statement is submitted clean.
locals {
  strip = "/(?m)^--[^\\n]*\\n/"
}

# ------------------------- Bedrock connection ------------------------------
resource "confluent_flink_connection" "bedrock" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  # Referenced by name in sql/03_model.sql — keep in sync.
  display_name   = "bedrock-prodsched-connection"
  type           = "BEDROCK"
  endpoint       = local.bedrock_endpoint
  aws_access_key = var.aws_access_key_id
  aws_secret_key = var.aws_secret_access_key

  depends_on = [
    confluent_api_key.flink,
    confluent_role_binding.app_manager_env_admin,
  ]
}

# ------------------------- Tools JAR artifact ------------------------------
resource "confluent_flink_artifact" "tools" {
  display_name   = "${local.prefix}-tools-${random_id.suffix.hex}"
  cloud          = local.cloud_provider
  region         = local.aws_region
  content_format = "JAR"
  artifact_file  = "${path.module}/../tools-udf/target/scheduling-tools.jar"
  environment {
    id = confluent_environment.main.id
  }
}

# ------------------------------ Tables -------------------------------------
module "tables" {
  source   = "./modules/flink-statement"
  for_each = toset(local.tables)

  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database

  statement_name = "create-table-${replace(each.key, "_", "-")}-${random_id.suffix.hex}"
  statement      = replace(file("${local.sql_dir}/01_tables/${each.key}.sql"), local.strip, "")
}

# ---------------------- Functions (always deployed) ------------------------
module "functions" {
  source   = "./modules/flink-statement"
  for_each = toset(local.functions)

  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database

  statement_name = "create-fn-${replace(each.key, "_", "-")}-${random_id.suffix.hex}"
  statement = replace(
    templatefile("${local.sql_dir}/02_functions/${each.key}.sql", { artifact_id = confluent_flink_artifact.tools.id }),
    local.strip, ""
  )
  depends_on = [confluent_flink_artifact.tools]
}

# ------------------------------ Model --------------------------------------
module "model" {
  source = "./modules/flink-statement"
  count  = var.deploy_flink_pipeline ? 1 : 0

  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database

  statement_name = "create-model-scheduling-${random_id.suffix.hex}"
  statement      = replace(file("${local.sql_dir}/03_model.sql"), local.strip, "")
  depends_on     = [confluent_flink_connection.bedrock]
}

# ------------------------------ Tools --------------------------------------
module "tools" {
  source   = "./modules/flink-statement"
  for_each = var.deploy_flink_pipeline ? toset(local.functions) : toset([])

  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database

  statement_name = "create-tool-${replace(each.key, "_", "-")}-${random_id.suffix.hex}"
  statement      = replace(file("${local.sql_dir}/04_tools/${each.key}_tool.sql"), local.strip, "")
  depends_on     = [module.functions]
}

# ------------------------------ Agent --------------------------------------
module "agent" {
  source = "./modules/flink-statement"
  count  = var.deploy_flink_pipeline ? 1 : 0

  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database

  statement_name = "create-agent-scheduling-${random_id.suffix.hex}"
  statement      = replace(file("${local.sql_dir}/05_agent.sql"), local.strip, "")
  depends_on     = [module.model, module.tools]
}

# ---------------------- Line snapshots (windowing) -------------------------
module "snapshots" {
  source = "./modules/flink-statement"
  count  = var.deploy_flink_pipeline ? 1 : 0

  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database

  # Fixed idle-timeout: CC's default progressive idleness grows to minutes and
  # would stall the session windows on partitions with no line traffic.
  extra_properties = {
    "sql.tables.scan.idle-timeout" = "5 s"
  }
  statement_name = "build-line-snapshots-${random_id.suffix.hex}"
  statement      = replace(file("${local.sql_dir}/06_line_snapshots.sql"), local.strip, "")
  depends_on     = [module.tables]
}

# ---------------------- Schedule (agent INSERT) ----------------------------
module "schedule" {
  source = "./modules/flink-statement"
  count  = var.deploy_flink_pipeline ? 1 : 0

  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database

  statement_name = "sequence-production-${random_id.suffix.hex}"
  statement      = replace(file("${local.sql_dir}/07_schedule.sql"), local.strip, "")
  depends_on     = [module.agent, module.snapshots, module.tables]
}
