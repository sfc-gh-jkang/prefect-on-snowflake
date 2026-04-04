output "dashboard_urls" {
  description = "Observe dashboard URLs"
  value = {
    spcs_overview         = "https://${var.observe_customer_id}.observeinc.com/workspace/${data.observe_workspace.default.id}/dashboard/${observe_dashboard.spcs_overview.id}"
    worker_apm            = "https://${var.observe_customer_id}.observeinc.com/workspace/${data.observe_workspace.default.id}/dashboard/${observe_dashboard.worker_apm.id}"
    warehouse_performance = "https://${var.observe_customer_id}.observeinc.com/workspace/${data.observe_workspace.default.id}/dashboard/${observe_dashboard.warehouse_performance.id}"
    cost_metering         = "https://${var.observe_customer_id}.observeinc.com/workspace/${data.observe_workspace.default.id}/dashboard/${observe_dashboard.cost_metering.id}"
    login_security        = "https://${var.observe_customer_id}.observeinc.com/workspace/${data.observe_workspace.default.id}/dashboard/${observe_dashboard.login_security.id}"
    infra_health          = "https://${var.observe_customer_id}.observeinc.com/workspace/${data.observe_workspace.default.id}/dashboard/${observe_dashboard.infra_health.id}"
    prefect_app           = "https://${var.observe_customer_id}.observeinc.com/workspace/${data.observe_workspace.default.id}/dashboard/${observe_dashboard.prefect_app.id}"
    pg_detail             = "https://${var.observe_customer_id}.observeinc.com/workspace/${data.observe_workspace.default.id}/dashboard/${observe_dashboard.pg_detail.id}"
    redis_detail          = "https://${var.observe_customer_id}.observeinc.com/workspace/${data.observe_workspace.default.id}/dashboard/${observe_dashboard.redis_detail.id}"
    vm_workers            = "https://${var.observe_customer_id}.observeinc.com/workspace/${data.observe_workspace.default.id}/dashboard/${observe_dashboard.vm_workers.id}"
    logs_explorer         = "https://${var.observe_customer_id}.observeinc.com/workspace/${data.observe_workspace.default.id}/dashboard/${observe_dashboard.logs_explorer.id}"
  }
}

output "monitor_names" {
  description = "Observe monitor names"
  value = {
    spcs_credit_spike   = observe_monitor_v2.spcs_credit_spike.name
    long_running_queries = observe_monitor_v2.long_running_queries.name
    worker_heartbeat    = observe_monitor_v2.worker_heartbeat.name
    warehouse_idle_cost = observe_monitor_v2.warehouse_idle_cost.name
  }
}
