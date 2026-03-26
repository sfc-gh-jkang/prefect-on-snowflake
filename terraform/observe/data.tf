# ---------------------------------------------------------------------------
# Data sources — workspace + O4S-created datasets
# ---------------------------------------------------------------------------
data "observe_workspace" "default" {
  name = var.workspace_name
}

# ---------------------------------------------------------------------------
# OTel datasets — created by observe-agent datastream.
# Raw OTLP data (spans, logs) lands in this dataset.  OPAL pipelines in
# the worker_apm dashboard extract span fields from the FIELDS JSON column.
# ---------------------------------------------------------------------------
data "observe_dataset" "otel_raw" {
  workspace = data.observe_workspace.default.oid
  name      = "prefect-spcs-observe-agent"
}

# ---------------------------------------------------------------------------
# O4S Snowflake datasets — auto-created by the Snowflake app in Observe.
# NOTE: Dataset names use lowercase "snowflake/" prefix, NOT "Snowflake/".
# ---------------------------------------------------------------------------
data "observe_dataset" "query_history" {
  workspace = data.observe_workspace.default.oid
  name      = "snowflake/QUERY_HISTORY"
}

data "observe_dataset" "warehouse_metering" {
  workspace = data.observe_workspace.default.oid
  name      = "snowflake/WAREHOUSE_METERING_HISTORY"
}

data "observe_dataset" "metering_history" {
  workspace = data.observe_workspace.default.oid
  name      = "snowflake/METERING_HISTORY"
}

data "observe_dataset" "login_history" {
  workspace = data.observe_workspace.default.oid
  name      = "snowflake/LOGIN_HISTORY"
}

data "observe_dataset" "spcs_history" {
  workspace = data.observe_workspace.default.oid
  name      = "snowflake/SNOWPARK_CONTAINER_SERVICES_HISTORY"
}

data "observe_dataset" "task_history" {
  workspace = data.observe_workspace.default.oid
  name      = "snowflake/TASK_HISTORY"
}
