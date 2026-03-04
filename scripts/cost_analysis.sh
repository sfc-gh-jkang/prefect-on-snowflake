#!/usr/bin/env bash
# =============================================================================
# cost_analysis.sh — SPCS cost breakdown and optimization recommendations
#
# Queries ACCOUNT_USAGE views to show:
#   1. Credit consumption by compute pool (last 7/30 days)
#   2. Idle compute pool time (nodes active but no queries)
#   3. Service uptime vs suspended time
#   4. Storage costs (stages + block storage)
#   5. Optimization recommendations
#
# Usage:
#   ./scripts/cost_analysis.sh                          # Uses .env defaults
#   ./scripts/cost_analysis.sh --connection aws_spcs    # Explicit connection
#   ./scripts/cost_analysis.sh --days 30                # Custom lookback
#
# Prerequisites:
#   - ACCOUNTADMIN or role with access to SNOWFLAKE.ACCOUNT_USAGE
#   - Snow CLI configured with a valid connection
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env if present
if [[ -f "$PROJECT_DIR/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROJECT_DIR/.env"
    set +a
fi

# Defaults
SNOW_CONN="${SNOWFLAKE_CONNECTION:-aws_spcs}"
DAYS=7

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --connection) SNOW_CONN="$2"; shift 2 ;;
        --days)       DAYS="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--connection NAME] [--days N]"
            echo ""
            echo "  --connection   Snow CLI connection name (default: aws_spcs)"
            echo "  --days         Lookback period in days (default: 7)"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

run_sql() {
    snow sql -q "$1" --connection "$SNOW_CONN" 2>/dev/null
}

echo "============================================="
echo "  SPCS Cost Analysis (last ${DAYS} days)"
echo "============================================="
echo "  Connection: $SNOW_CONN"
echo ""

# ── 1. Compute Pool Credit Consumption ────────────────────────────────────
echo "=== 1. Compute Pool Credit Consumption ==="
run_sql "
SELECT
    resource_attributes['compute_pool_name']::STRING AS compute_pool,
    ROUND(SUM(credits_used), 2) AS total_credits,
    ROUND(SUM(credits_used) / ${DAYS}, 2) AS avg_daily_credits,
    ROUND(SUM(credits_used) * 3.00, 2) AS est_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE service_type = 'SNOWPARK_CONTAINER_SERVICES'
  AND usage_date >= DATEADD(day, -${DAYS}, CURRENT_DATE())
GROUP BY 1
ORDER BY total_credits DESC;
"

# ── 2. Per-Service Credit Breakdown ───────────────────────────────────────
echo ""
echo "=== 2. Per-Service Credit Breakdown ==="
run_sql "
SELECT
    resource_attributes['compute_pool_name']::STRING AS compute_pool,
    resource_attributes['service_name']::STRING AS service_name,
    ROUND(SUM(credits_used), 2) AS total_credits,
    ROUND(SUM(credits_used) * 3.00, 2) AS est_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE service_type = 'SNOWPARK_CONTAINER_SERVICES'
  AND usage_date >= DATEADD(day, -${DAYS}, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY total_credits DESC;
"

# ── 3. Daily Credit Trend ─────────────────────────────────────────────────
echo ""
echo "=== 3. Daily Credit Trend ==="
run_sql "
SELECT
    usage_date,
    resource_attributes['compute_pool_name']::STRING AS compute_pool,
    ROUND(SUM(credits_used), 2) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE service_type = 'SNOWPARK_CONTAINER_SERVICES'
  AND usage_date >= DATEADD(day, -${DAYS}, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
"

# ── 4. Compute Pool Utilization ───────────────────────────────────────────
echo ""
echo "=== 4. Current Compute Pool Status ==="
run_sql "
USE ROLE PREFECT_ROLE;
USE DATABASE PREFECT_DB;
USE SCHEMA PREFECT_SCHEMA;
SHOW COMPUTE POOLS;
"

# ── 5. Stage Storage ─────────────────────────────────────────────────────
echo ""
echo "=== 5. Stage Storage Usage ==="
run_sql "
SELECT
    catalog_name AS database_name,
    stage_name,
    ROUND(average_stage_bytes / (1024*1024), 2) AS avg_mb,
    ROUND(average_stage_bytes / (1024*1024*1024), 4) AS avg_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY
WHERE usage_date = (SELECT MAX(usage_date) FROM SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY)
  AND catalog_name = 'PREFECT_DB'
ORDER BY average_stage_bytes DESC;
"

# ── 6. Warehouse Credits (if any used by flows) ──────────────────────────
echo ""
echo "=== 6. Warehouse Credits (Flow Queries) ==="
run_sql "
SELECT
    warehouse_name,
    ROUND(SUM(credits_used), 2) AS total_credits,
    ROUND(SUM(credits_used) * 3.00, 2) AS est_cost_usd,
    COUNT(*) AS query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(day, -${DAYS}, CURRENT_DATE())
GROUP BY 1
ORDER BY total_credits DESC
LIMIT 10;
"

# ── 7. Optimization Recommendations ──────────────────────────────────────
echo ""
echo "=== 7. Optimization Recommendations ==="
echo ""
echo "  Cost Reduction Strategies:"
echo "  ─────────────────────────────────────────────────────"
echo "  1. SUSPEND compute pools during off-hours:"
echo "     ALTER COMPUTE POOL PREFECT_WORKER_POOL SUSPEND;"
echo "     ALTER COMPUTE POOL PREFECT_MONITOR_POOL SUSPEND;"
echo "     (Suspending stops billing; block storage survives)"
echo ""
echo "  2. Schedule suspend/resume with a cron task:"
echo "     CREATE TASK suspend_pools ... SCHEDULE = 'USING CRON 0 20 * * * America/New_York'"
echo "     AS ALTER COMPUTE POOL PREFECT_WORKER_POOL SUSPEND;"
echo ""
echo "  3. Use MIN_NODES=0 for bursty pools (auto-suspend):"
echo "     ALTER COMPUTE POOL PREFECT_WORKER_POOL SET MIN_NODES=0 AUTO_SUSPEND_SECS=300;"
echo ""
echo "  4. Right-size compute pools:"
echo "     - CPU_X64_S (2 vCPU) is smallest; good for monitoring"
echo "     - CPU_X64_M (4 vCPU) for heavier workloads"
echo "     - Check actual CPU usage in Grafana before upsizing"
echo ""
echo "  5. Reduce monitoring retention:"
echo "     - Prometheus: --storage.tsdb.retention.time (default 15d)"
echo "     - Loki: retention_period in loki-config.yaml (default 168h)"
echo "     - Shorter retention = less storage cost"
echo ""
echo "  6. Consolidate services where possible:"
echo "     - PF_SERVER + PF_SERVICES could share a compute pool"
echo "     - Keep PF_MONITOR on its own pool for isolation"
echo ""
echo "  7. Stage cleanup (old backups):"
echo "     REMOVE @MONITOR_STAGE/backups/prometheus/ PATTERN='.*older_than_7d.*';"
echo ""
echo "============================================="
echo "  Analysis complete. Review sections above"
echo "  for cost allocation and optimization."
echo "============================================="
