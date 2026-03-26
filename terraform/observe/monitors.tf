# ---------------------------------------------------------------------------
# Observe Monitors — alerting on critical signals
#
# IMPORTANT: OPAL field names are CASE-SENSITIVE.
# - Snowflake O4S datasets use UPPERCASE fields (CREDITS_USED, WAREHOUSE_NAME)
# - OTel Tracing datasets use lowercase fields (service_name, status_code)
#
# Threshold monitors using align/aggregate require the "metric" interface,
# which in turn requires the dataset to be an "event" dataset first.
# For non-event datasets, use: make_event → make_col metric/value → interface "metric"
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 1. SPCS Credit Spike — alert when daily SPCS credits exceed threshold
# ---------------------------------------------------------------------------
resource "observe_monitor_v2" "spcs_credit_spike" {
  name        = "SPCS Daily Credit Spike"
  description = "Alert when SPCS compute credits exceed ${var.spcs_credit_threshold} in a day"
  workspace   = data.observe_workspace.default.oid
  rule_kind   = "count"

  lookback_time = "24h0m0s"

  inputs = {
    "spcs_from_snowflake/SNOWPARK_CONTAINER_SERVICES_HISTORY" = data.observe_dataset.spcs_history.oid  # pragma: allowlist secret
  }

  stage {
    output_stage = true
    pipeline     = <<-EOT
      filter CREDITS_USED > 0
      make_col link_target:string(COMPUTE_POOL_NAME)
    EOT
  }

  rules {
    level = "warning"
    count {
      compare_values {
        compare_fn = "greater"
        value_int64 = [var.spcs_credit_threshold]
      }
    }
  }
}

# ---------------------------------------------------------------------------
# 2. Long-Running Queries — alert on queries exceeding duration threshold
# ---------------------------------------------------------------------------
resource "observe_monitor_v2" "long_running_queries" {
  name        = "Long-Running Queries"
  description = "Alert when queries exceed ${var.long_query_duration_ms / 60000} minutes"
  workspace   = data.observe_workspace.default.oid
  rule_kind   = "count"

  lookback_time = "15m0s"

  inputs = {
    "queries_from_snowflake/QUERY_HISTORY" = data.observe_dataset.query_history.oid  # pragma: allowlist secret
  }

  stage {
    output_stage = true
    pipeline     = <<-EOT
      filter EXECUTION_STATUS = "SUCCESS"
      filter TOTAL_ELAPSED_TIME > ${var.long_query_duration_ms}
      make_col link_target:string(WAREHOUSE_NAME)
    EOT
  }

  rules {
    level = "warning"
    count {
      compare_values {
        compare_fn = "greater"
        value_int64 = [0]
      }
    }
  }
}

# ---------------------------------------------------------------------------
# 3. Worker Heartbeat Missing — no OTel spans from a worker for 10 min
# ---------------------------------------------------------------------------
resource "observe_monitor_v2" "worker_heartbeat" {
  name        = "Prefect Worker Heartbeat Missing"
  description = "Alert when no OTel spans are received from a Prefect worker for 10 minutes"
  workspace   = data.observe_workspace.default.oid
  rule_kind   = "count"

  lookback_time = "10m0s"

  inputs = {
    "spans_from_Tracing/Span" = data.observe_dataset.otel_spans.oid  # pragma: allowlist secret
  }

  stage {
    output_stage = true
    pipeline     = <<-EOT
      filter service_name ~ /prefect-worker/
    EOT
  }

  rules {
    level = "error"
    count {
      compare_values {
        compare_fn = "less"
        value_int64 = [1]
      }
    }
  }
}

# ---------------------------------------------------------------------------
# 4. Warehouse Idle Cost — alert on high credits with low query count
# ---------------------------------------------------------------------------
resource "observe_monitor_v2" "warehouse_idle_cost" {
  name        = "Warehouse Idle Cost"
  description = "Alert when warehouse has credits but few queries (idle cost)"
  workspace   = data.observe_workspace.default.oid
  rule_kind   = "count"

  lookback_time = "1h0m0s"

  inputs = {
    "metering_from_snowflake/WAREHOUSE_METERING_HISTORY" = data.observe_dataset.warehouse_metering.oid  # pragma: allowlist secret
  }

  stage {
    output_stage = true
    pipeline     = <<-EOT
      filter CREDITS_USED > ${var.warehouse_idle_credit_threshold}
      make_col link_target:string(WAREHOUSE_NAME)
    EOT
  }

  rules {
    level = "warning"
    count {
      compare_values {
        compare_fn = "greater"
        value_int64 = [0]
      }
    }
  }
}
