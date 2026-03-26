# ---------------------------------------------------------------------------
# Observe Monitors — alerting on critical signals
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 1. SPCS Credit Spike — alert when daily SPCS credits exceed threshold
# ---------------------------------------------------------------------------
resource "observe_monitor_v2" "spcs_credit_spike" {
  name        = "SPCS Daily Credit Spike"
  description = "Alert when SPCS compute credits exceed ${var.spcs_credit_threshold} in a day"
  workspace   = data.observe_workspace.default.oid
  rule_kind   = "threshold"

  lookback_time = "1h0m0s"

  inputs = {
    "spcs_from_Snowflake/SNOWPARK_CONTAINER_SERVICES_HISTORY" = data.observe_dataset.spcs_history.oid  # pragma: allowlist secret
  }

  stage {
    output_stage = false
    pipeline     = <<-EOT
      align 1d, credits_sum:sum(credits_used)
      aggregate credits_sum:sum(credits_sum), group_by(compute_pool_name)
    EOT
  }

  rules {
    level = "warning"
    threshold {
      aggregation       = "all_of"
      value_column_name = "credits_sum"
      compare_values {
        compare_fn  = "greater"
        value_float64 = [var.spcs_credit_threshold]
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
  rule_kind   = "threshold"

  lookback_time = "15m0s"

  inputs = {
    "queries_from_Snowflake/QUERY_HISTORY" = data.observe_dataset.query_history.oid  # pragma: allowlist secret
  }

  stage {
    output_stage = false
    pipeline     = <<-EOT
      filter execution_status = "SUCCESS"
      align 15m, max_duration:max(total_elapsed_time)
      aggregate max_duration:max(max_duration), group_by(warehouse_name)
    EOT
  }

  rules {
    level = "warning"
    threshold {
      aggregation       = "all_of"
      value_column_name = "max_duration"
      compare_values {
        compare_fn  = "greater"
        value_float64 = [var.long_query_duration_ms]
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
  rule_kind   = "threshold"

  lookback_time = "10m0s"

  inputs = {
    "spans_from_OpenTelemetry/Span" = data.observe_dataset.otel_spans.oid  # pragma: allowlist secret
  }

  stage {
    output_stage = false
    pipeline     = <<-EOT
      filter service.name ~ /prefect-worker/
      align 5m, span_count:count()
      aggregate span_count:sum(span_count), group_by(service.name)
    EOT
  }

  rules {
    level = "error"
    threshold {
      aggregation       = "all_of"
      value_column_name = "span_count"
      compare_values {
        compare_fn  = "less"
        value_float64 = [1]
      }
    }
  }

  no_data_rules {
    threshold {
      value_column_name = "span_count"
      aggregation       = "all_of"
    }
  }
}

# ---------------------------------------------------------------------------
# 4. Warehouse Idle Cost — alert on high credits with low query count
# ---------------------------------------------------------------------------
resource "observe_monitor_v2" "warehouse_idle_cost" {
  name        = "Warehouse Idle Cost"
  description = "Alert when warehouse credits exceed ${var.warehouse_idle_credit_threshold}/hr"
  workspace   = data.observe_workspace.default.oid
  rule_kind   = "threshold"

  lookback_time = "1h0m0s"

  inputs = {
    "metering_from_Snowflake/WAREHOUSE_METERING_HISTORY" = data.observe_dataset.warehouse_metering.oid  # pragma: allowlist secret
  }

  stage {
    output_stage = false
    pipeline     = <<-EOT
      align 1h, credits_hour:sum(credits_used)
      aggregate credits_hour:sum(credits_hour), group_by(warehouse_name)
    EOT
  }

  rules {
    level = "warning"
    threshold {
      aggregation       = "all_of"
      value_column_name = "credits_hour"
      compare_values {
        compare_fn  = "greater"
        value_float64 = [var.warehouse_idle_credit_threshold]
      }
    }
  }
}
