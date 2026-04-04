# ---------------------------------------------------------------------------
# Data sources — workspace + O4S-created datasets
# ---------------------------------------------------------------------------
data "observe_workspace" "default" {
  name = var.workspace_name
}

# O4S auto-creates these datasets when tasks send data to Observe.
# Names match O4S dataset naming convention from the Observe integration.
# If datasets don't exist yet, run `terraform apply` after O4S tasks start.

data "observe_dataset" "query_history" {
  workspace = data.observe_workspace.default.oid
  name      = "Snowflake/QUERY_HISTORY"
}

data "observe_dataset" "warehouse_metering" {
  workspace = data.observe_workspace.default.oid
  name      = "Snowflake/WAREHOUSE_METERING_HISTORY"
}

data "observe_dataset" "metering_history" {
  workspace = data.observe_workspace.default.oid
  name      = "Snowflake/METERING_HISTORY"
}

data "observe_dataset" "login_history" {
  workspace = data.observe_workspace.default.oid
  name      = "Snowflake/LOGIN_HISTORY"
}

data "observe_dataset" "spcs_history" {
  workspace = data.observe_workspace.default.oid
  name      = "Snowflake/SNOWPARK_CONTAINER_SERVICES_HISTORY"
}

data "observe_dataset" "task_history" {
  workspace = data.observe_workspace.default.oid
  name      = "Snowflake/TASK_HISTORY"
}

# Raw observe-agent dataset — Prometheus remote_write + Loki logs land here
data "observe_dataset" "otel_raw" {
  workspace = data.observe_workspace.default.oid
  name      = "prefect-spcs-observe-agent"
}

# OTel datasets — auto-created when observe-agent starts sending traces/metrics
data "observe_dataset" "otel_spans" {
  workspace = data.observe_workspace.default.oid
  name      = "OpenTelemetry/Span"
}

data "observe_dataset" "otel_metrics" {
  workspace = data.observe_workspace.default.oid
  name      = "OpenTelemetry/Metric"
}
