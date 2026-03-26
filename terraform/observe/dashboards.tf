# ---------------------------------------------------------------------------
# Prefect-on-Snowflake Observe Dashboards
# ---------------------------------------------------------------------------
#
# IMPORTANT LEARNINGS (from 8+ sessions of debugging):
#
# 1. COLUMN NAMES: Snowflake O4S dataset columns are ALL UPPERCASE
#    (CREDITS_USED, WAREHOUSE_NAME, etc.). OTel/Tracing columns are lowercase
#    (service_name, duration, etc.). OPAL silently returns empty for wrong case.
#
# 2. CHART RENDERING: To render charts (not tables), each stage needs:
#    - layout.viewModel.stageTab = "vis"
#    - layout.managers[].vegaVis (NOT "vis") with type/x/y/color
#    - layout.steps[] with ExpressionBuilder action
#    - layout.label for the card title
#    - Pipeline aliases must match vegaVis.y (e.g. A_CREDITS_USED_sum)
#
# 3. DASHBOARD LAYOUT: Top-level layout must have:
#    - gridLayout.sections referencing correct stageIds
#    - stageListLayout.timeRange with millisFromCurrentTime
#
# 4. TEMPLATEFILE APPROACH: Stages are captured from the working API state
#    into .json.tftpl files with ${dataset_id} placeholders. This avoids
#    hand-coding the complex ExpressionBuilder JSON.
#
# 5. GraphQL API: saveDashboard(dash: DashboardInput!) with fields:
#    id, name, layout (JSON string), stages ([StageQueryInput!])
#    deleteDashboard(id) returns ResultStatus { success }
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 1. SPCS Overview — container services billing, compute pool usage
#    4 panels: Credits/Hour by Pool, Total Credits/Hour, Daily by Pool, Daily Total
# ---------------------------------------------------------------------------
resource "observe_dashboard" "spcs_overview" {
  name      = "Prefect SPCS Overview"
  workspace = data.observe_workspace.default.oid

  layout = templatefile("${path.module}/spcs_layout.json.tftpl", {})

  stages = templatefile("${path.module}/spcs_stages.json.tftpl", {
    spcs_dataset_id = data.observe_dataset.spcs_history.id
  })
}

# ---------------------------------------------------------------------------
# 2. Prefect Worker APM — OTel traces from Prefect worker pools
#    4 panels: Avg Duration, Throughput, Errors by Status, Top Span Names
# ---------------------------------------------------------------------------
resource "observe_dashboard" "worker_apm" {
  name      = "Prefect Worker APM"
  workspace = data.observe_workspace.default.oid

  layout = templatefile("${path.module}/worker_apm_layout.json.tftpl", {})

  stages = templatefile("${path.module}/worker_apm_stages.json.tftpl", {
    otel_raw_dataset_id = data.observe_dataset.otel_raw.id
  })
}

# ---------------------------------------------------------------------------
# 3. Warehouse & Query Performance
#    6 panels: Credits/Hour by WH, Total Credits, Avg Duration, Query Volume,
#              Failed Queries, Data Scanned (GB)
# ---------------------------------------------------------------------------
resource "observe_dashboard" "warehouse_performance" {
  name      = "Warehouse & Query Performance"
  workspace = data.observe_workspace.default.oid

  layout = templatefile("${path.module}/warehouse_layout.json.tftpl", {})

  stages = templatefile("${path.module}/warehouse_stages.json.tftpl", {
    warehouse_metering_dataset_id = data.observe_dataset.warehouse_metering.id
    query_history_dataset_id      = data.observe_dataset.query_history.id
  })
}

# ---------------------------------------------------------------------------
# 4. Cost & Metering Overview
#    6 panels: Credits/Hour by Service, Total Credits, Daily by Service,
#              Compute vs Cloud, Task Runs by State, Failed Tasks
# ---------------------------------------------------------------------------
resource "observe_dashboard" "cost_metering" {
  name      = "Cost & Metering Overview"
  workspace = data.observe_workspace.default.oid

  layout = templatefile("${path.module}/cost_layout.json.tftpl", {})

  stages = templatefile("${path.module}/cost_stages.json.tftpl", {
    metering_history_dataset_id = data.observe_dataset.metering_history.id
    task_history_dataset_id     = data.observe_dataset.task_history.id
  })
}

# ---------------------------------------------------------------------------
# 5. Login & Security Activity
#    6 panels: Success vs Failure, Logins by User, by Client Type,
#              Failed by User, Auth Factor Distribution, Error Codes
# ---------------------------------------------------------------------------
resource "observe_dashboard" "login_security" {
  name      = "Login & Security Activity"
  workspace = data.observe_workspace.default.oid

  layout = templatefile("${path.module}/login_layout.json.tftpl", {})

  stages = templatefile("${path.module}/login_stages.json.tftpl", {
    login_history_dataset_id = data.observe_dataset.login_history.id
  })
}
