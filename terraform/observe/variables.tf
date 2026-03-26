variable "observe_customer_id" {
  description = "Observe customer ID (e.g. 175859677949)"
  type        = string
}

variable "observe_api_token" {
  description = "Observe API token for authentication"
  type        = string
  sensitive   = true
}

variable "workspace_name" {
  description = "Observe workspace name"
  type        = string
  default     = "Default"
}

# ---------------------------------------------------------------------------
# Alert thresholds
# ---------------------------------------------------------------------------
variable "spcs_credit_threshold" {
  description = "SPCS daily credit threshold before alerting"
  type        = number
  default     = 50
}

variable "long_query_duration_ms" {
  description = "Query duration threshold in milliseconds (default 30 min)"
  type        = number
  default     = 1800000
}

variable "warehouse_idle_credit_threshold" {
  description = "Warehouse hourly credit threshold for idle alerting"
  type        = number
  default     = 2
}
