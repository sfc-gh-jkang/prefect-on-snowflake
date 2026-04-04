# ---------------------------------------------------------------------------
# Prefect-on-Snowflake Observe Dashboards
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 1. SPCS Overview — container services billing, compute pool usage
# ---------------------------------------------------------------------------
resource "observe_dashboard" "spcs_overview" {
  name      = "Prefect SPCS Overview"
  workspace = data.observe_workspace.default.oid

  stages = jsonencode([
    {
      id = "stage-spcs-credits"
      input = [{
        datasetId   = data.observe_dataset.spcs_history.id
        datasetPath = null
        inputName   = "Snowflake/SNOWPARK_CONTAINER_SERVICES_HISTORY"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        timechart 1h, credits_used:sum(credits_used), group_by(compute_pool_name)
      EOT
    },
    {
      id = "stage-spcs-daily"
      input = [{
        datasetId   = data.observe_dataset.spcs_history.id
        datasetPath = null
        inputName   = "Snowflake/SNOWPARK_CONTAINER_SERVICES_HISTORY"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        timechart 1d, daily_credits:sum(credits_used)
      EOT
    },
  ])
}

# ---------------------------------------------------------------------------
# 2. Prefect Worker APM — OTel traces from all 3 worker pools
# ---------------------------------------------------------------------------
resource "observe_dashboard" "worker_apm" {
  name      = "Prefect Worker APM"
  workspace = data.observe_workspace.default.oid

  stages = jsonencode([
    {
      id = "stage-trace-latency"
      input = [{
        datasetId   = data.observe_dataset.otel_spans.id
        datasetPath = null
        inputName   = "OpenTelemetry/Span"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        filter service.name ~ /prefect-worker/
        timechart 5m, p50:percentile(duration_ms, 50), p95:percentile(duration_ms, 95), p99:percentile(duration_ms, 99), group_by(service.name)
      EOT
    },
    {
      id = "stage-error-rate"
      input = [{
        datasetId   = data.observe_dataset.otel_spans.id
        datasetPath = null
        inputName   = "OpenTelemetry/Span"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        filter service.name ~ /prefect-worker/
        timechart 5m, error_rate:fraction(status_code = "ERROR"), group_by(service.name)
      EOT
    },
    {
      id = "stage-throughput"
      input = [{
        datasetId   = data.observe_dataset.otel_spans.id
        datasetPath = null
        inputName   = "OpenTelemetry/Span"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        filter service.name ~ /prefect-worker/
        filter kind = "SERVER" or kind = "INTERNAL"
        timechart 5m, requests:count(), group_by(service.name)
      EOT
    },
  ])
}

# ---------------------------------------------------------------------------
# 3. Warehouse & Query Performance
# ---------------------------------------------------------------------------
resource "observe_dashboard" "warehouse_performance" {
  name      = "Warehouse & Query Performance"
  workspace = data.observe_workspace.default.oid

  stages = jsonencode([
    {
      id = "stage-wh-credits"
      input = [{
        datasetId   = data.observe_dataset.warehouse_metering.id
        datasetPath = null
        inputName   = "Snowflake/WAREHOUSE_METERING_HISTORY"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        timechart 1h, credits:sum(credits_used), group_by(warehouse_name)
      EOT
    },
    {
      id = "stage-query-duration"
      input = [{
        datasetId   = data.observe_dataset.query_history.id
        datasetPath = null
        inputName   = "Snowflake/QUERY_HISTORY"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        filter execution_status = "SUCCESS"
        timechart 15m, p50_ms:percentile(total_elapsed_time, 50), p95_ms:percentile(total_elapsed_time, 95), group_by(warehouse_name)
      EOT
    },
    {
      id = "stage-query-volume"
      input = [{
        datasetId   = data.observe_dataset.query_history.id
        datasetPath = null
        inputName   = "Snowflake/QUERY_HISTORY"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        timechart 1h, queries:count(), group_by(query_type)
      EOT
    },
    {
      id = "stage-query-errors"
      input = [{
        datasetId   = data.observe_dataset.query_history.id
        datasetPath = null
        inputName   = "Snowflake/QUERY_HISTORY"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        filter execution_status != "SUCCESS"
        timechart 1h, failures:count(), group_by(error_code)
      EOT
    },
  ])
}

# ---------------------------------------------------------------------------
# 4. Cost & Metering Overview
# ---------------------------------------------------------------------------
resource "observe_dashboard" "cost_metering" {
  name      = "Cost & Metering Overview"
  workspace = data.observe_workspace.default.oid

  stages = jsonencode([
    {
      id = "stage-daily-credits"
      input = [{
        datasetId   = data.observe_dataset.metering_history.id
        datasetPath = null
        inputName   = "Snowflake/METERING_HISTORY"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        timechart 1d, credits:sum(credits_used), group_by(service_type)
      EOT
    },
    {
      id = "stage-task-credits"
      input = [{
        datasetId   = data.observe_dataset.task_history.id
        datasetPath = null
        inputName   = "Snowflake/TASK_HISTORY"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        timechart 1d, task_runs:count(), group_by(state)
      EOT
    },
  ])
}

# ---------------------------------------------------------------------------
# 5. Login & Security Activity
# ---------------------------------------------------------------------------
resource "observe_dashboard" "login_security" {
  name      = "Login & Security Activity"
  workspace = data.observe_workspace.default.oid

  stages = jsonencode([
    {
      id = "stage-login-activity"
      input = [{
        datasetId   = data.observe_dataset.login_history.id
        datasetPath = null
        inputName   = "Snowflake/LOGIN_HISTORY"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        timechart 1h, logins:count(), group_by(is_success)
      EOT
    },
    {
      id = "stage-failed-logins"
      input = [{
        datasetId   = data.observe_dataset.login_history.id
        datasetPath = null
        inputName   = "Snowflake/LOGIN_HISTORY"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        filter is_success = "NO"
        timechart 1h, failures:count(), group_by(user_name)
      EOT
    },
    {
      id = "stage-login-clients"
      input = [{
        datasetId   = data.observe_dataset.login_history.id
        datasetPath = null
        inputName   = "Snowflake/LOGIN_HISTORY"
        inputRole   = "Data"
        stageId     = null
      }]
      params   = null
      pipeline = <<-EOT
        timechart 6h, logins:count(), group_by(reported_client_type)
      EOT
    },
  ])
}

# ---------------------------------------------------------------------------
# 6. Infrastructure Health — Prometheus metrics: service up/down, CPU, memory,
#    Redis memory, PostgreSQL connections, Redis commands/sec
# ---------------------------------------------------------------------------
resource "observe_dashboard" "infra_health" {
  name      = "Infrastructure Health"
  workspace = data.observe_workspace.default.oid

  layout = templatefile("${path.module}/infra_layout.json.tftpl", {})

  stages = templatefile("${path.module}/infra_stages.json.tftpl", {
    otel_raw_dataset_id = data.observe_dataset.otel_raw.id
  })
}

# ---------------------------------------------------------------------------
# 7. Prefect Application — Prometheus metrics: flow run states, success rate,
#    active workers, deployment count
# ---------------------------------------------------------------------------
resource "observe_dashboard" "prefect_app" {
  name      = "Prefect Application"
  workspace = data.observe_workspace.default.oid

  layout = templatefile("${path.module}/prefect_app_layout.json.tftpl", {})

  stages = templatefile("${path.module}/prefect_app_stages.json.tftpl", {
    otel_raw_dataset_id = data.observe_dataset.otel_raw.id
  })
}

# ---------------------------------------------------------------------------
# 8. PostgreSQL Detail — Prometheus metrics: tuple operations, dead tuples,
#    cache hit ratio, active connections
# ---------------------------------------------------------------------------
resource "observe_dashboard" "pg_detail" {
  name      = "PostgreSQL Detail"
  workspace = data.observe_workspace.default.oid

  layout = templatefile("${path.module}/pg_detail_layout.json.tftpl", {})

  stages = templatefile("${path.module}/pg_detail_stages.json.tftpl", {
    otel_raw_dataset_id = data.observe_dataset.otel_raw.id
  })
}

# ---------------------------------------------------------------------------
# 9. Redis Detail — Prometheus metrics: memory usage, cache hit rate,
#    network I/O, connected clients
# ---------------------------------------------------------------------------
resource "observe_dashboard" "redis_detail" {
  name      = "Redis Detail"
  workspace = data.observe_workspace.default.oid

  layout = templatefile("${path.module}/redis_detail_layout.json.tftpl", {})

  stages = templatefile("${path.module}/redis_detail_stages.json.tftpl", {
    otel_raw_dataset_id = data.observe_dataset.otel_raw.id
  })
}

# ---------------------------------------------------------------------------
# 10. VM Workers — Prometheus metrics from node-exporter: CPU, memory,
#     disk usage, network I/O
# ---------------------------------------------------------------------------
resource "observe_dashboard" "vm_workers" {
  name      = "VM Workers"
  workspace = data.observe_workspace.default.oid

  layout = templatefile("${path.module}/vm_workers_layout.json.tftpl", {})

  stages = templatefile("${path.module}/vm_workers_stages.json.tftpl", {
    otel_raw_dataset_id = data.observe_dataset.otel_raw.id
  })
}

# ---------------------------------------------------------------------------
# 11. Logs Explorer — Loki logs: log volume over time, logs by source,
#     error log stream
# ---------------------------------------------------------------------------
resource "observe_dashboard" "logs_explorer" {
  name      = "Logs Explorer"
  workspace = data.observe_workspace.default.oid

  layout = templatefile("${path.module}/logs_explorer_layout.json.tftpl", {})

  stages = templatefile("${path.module}/logs_explorer_stages.json.tftpl", {
    otel_raw_dataset_id = data.observe_dataset.otel_raw.id
  })
}
