#!/usr/bin/env bash
# =============================================================================
# demo.sh — Guided walkthrough of the Prefect SPCS project
#
# Runs through key capabilities with explanations. Safe to run repeatedly.
# Does NOT modify infrastructure — read-only queries and flow triggers only.
#
# Usage:
#   ./scripts/demo.sh                          # Full walkthrough
#   ./scripts/demo.sh --connection aws_spcs    # Explicit connection
#   ./scripts/demo.sh --skip-flows             # Skip flow execution (faster)
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
SKIP_FLOWS=false
DB="PREFECT_DB"
SCHEMA="PREFECT_SCHEMA"

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --connection)   SNOW_CONN="$2"; shift 2 ;;
        --skip-flows)   SKIP_FLOWS=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--connection NAME] [--skip-flows]"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

PREFECT_URL="${PREFECT_API_URL:-}"

# --- Helpers ---
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

section() { echo -e "\n${CYAN}═══════════════════════════════════════════════${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}═══════════════════════════════════════════════${NC}\n"; }
info() { echo -e "${GREEN}▸${NC} $1"; }
warn() { echo -e "${YELLOW}▸${NC} $1"; }
pause() { echo ""; read -r -p "  Press Enter to continue..." ; echo ""; }

run_sql() {
    snow sql -q "$1" --connection "$SNOW_CONN" 2>/dev/null || echo "  (query failed — check connection)"
}

# run_sql_clean: runs multi-statement SQL and only shows the LAST statement's output
# Filters out USE/SHOW noise and garbled SHOW output, keeping only the SELECT result table
run_sql_clean() {
    local sql="$1"
    snow sql -q "${sql}" --connection "$SNOW_CONN" 2>/dev/null \
        | awk '/^SELECT /{found=1; next} found{print}' || echo "  (query failed)"
}

# Build auth header for Prefect API (uses Snowflake PAT for SPCS)
SNOWFLAKE_PAT="${SNOWFLAKE_PAT:-}"
auth_header() {
    if [[ -n "$SNOWFLAKE_PAT" ]]; then
        echo "Authorization: Snowflake Token=\"$SNOWFLAKE_PAT\""
    fi
}

api_get() {
    local url="$1"
    local hdr
    hdr="$(auth_header)"
    if [[ -n "$hdr" ]]; then
        curl -sf "$url" -H "$hdr" -H "Content-Type: application/json" 2>/dev/null
    else
        curl -sf "$url" -H "Content-Type: application/json" 2>/dev/null
    fi
}

api_post() {
    local url="$1" body="$2"
    local hdr
    hdr="$(auth_header)"
    if [[ -n "$hdr" ]]; then
        curl -sf "$url" -H "$hdr" -H "Content-Type: application/json" -d "$body" 2>/dev/null
    else
        curl -sf "$url" -H "Content-Type: application/json" -d "$body" 2>/dev/null
    fi
}

# =============================================================================
echo ""
echo -e "${CYAN}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║    Prefect on Snowpark Container Services — Demo        ║${NC}"
echo -e "${CYAN}║    Production orchestration, hybrid workers, monitoring ║${NC}"
echo -e "${CYAN}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
info "Connection: $SNOW_CONN"
info "Prefect URL: ${PREFECT_URL:-<not set>}"
echo ""

# ── 1. Infrastructure Overview ───────────────────────────────────────────
section "1. SPCS Services"
info "5 services across 5 compute pools — all running on Snowflake."
run_sql_clean "USE DATABASE ${DB}; USE SCHEMA ${SCHEMA}; SHOW SERVICES IN SCHEMA ${DB}.${SCHEMA}; SELECT \"name\", \"status\", \"compute_pool\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE \"status\" = 'RUNNING' ORDER BY \"name\";"
pause

# ── 2. Compute Pools ────────────────────────────────────────────────────
section "2. Compute Pools"
info "Dedicated pools for isolation: core, infra, worker, monitoring, dashboard."
run_sql_clean "SHOW COMPUTE POOLS LIKE 'PREFECT_%'; SELECT \"name\", \"state\", \"instance_family\", \"min_nodes\", \"max_nodes\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ORDER BY \"name\";"
pause

# ── 3. Endpoints ─────────────────────────────────────────────────────────
section "3. Public Endpoints"
info "SPCS services expose HTTPS endpoints with Snowflake auth."
info "PF_SERVER endpoints:"
run_sql_clean "USE DATABASE ${DB}; USE SCHEMA ${SCHEMA}; SHOW ENDPOINTS IN SERVICE ${DB}.${SCHEMA}.PF_SERVER; SELECT \"name\", \"port\", \"ingress_url\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));"
info "PF_MONITOR endpoints:"
run_sql_clean "USE DATABASE ${DB}; USE SCHEMA ${SCHEMA}; SHOW ENDPOINTS IN SERVICE ${DB}.${SCHEMA}.PF_MONITOR; SELECT \"name\", \"port\", \"ingress_url\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));"
pause

# ── 4. Flow Deployments ─────────────────────────────────────────────────
section "4. Flow Deployments"
info "10 flows × 5 pools = 47 deployments across SPCS + GCP + AWS worker pools."
info "Flows use git_clone for code delivery with stage fallback on SPCS."
if [[ -n "$PREFECT_URL" ]]; then
    api_post "${PREFECT_URL}/deployments/filter" '{"limit": 50, "sort": "NAME_ASC"}' | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f'  {len(data)} deployments registered:')
for d in data:
    sched = d.get('schedules', [])
    if sched:
        s = sched[0].get('schedule', {})
        sched_str = s.get('cron') or f'interval={s.get(\"interval\", \"?\")}s'
    else:
        sched_str = 'manual'
    print(f'  {d[\"name\"]:40s} pool={d[\"work_pool_name\"]:18s} schedule={sched_str}')
" 2>/dev/null || warn "Could not query deployments API (check PREFECT_API_URL / SNOWFLAKE_PAT)"
else
    warn "PREFECT_API_URL not set — skipping deployment listing"
fi
pause

# ── 5. Trigger a Flow Run ──────────────────────────────────────────────
section "5. Flow Execution"
if [[ "$SKIP_FLOWS" == "true" ]]; then
    warn "Skipping flow execution (--skip-flows)"
else
    info "Triggering the example-flow to demonstrate worker execution..."
    if [[ -n "$PREFECT_URL" ]]; then
        # Find the deployment ID for example-flow-local
        DEPLOY_ID=$(api_post "${PREFECT_URL}/deployments/filter" \
            '{"deployments": {"name": {"any_": ["example-flow-local"]}}}' \
            | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['id'])" 2>/dev/null || true)

        if [[ -n "$DEPLOY_ID" ]]; then
            info "Creating flow run for deployment: $DEPLOY_ID"
            RUN_ID=$(api_post "${PREFECT_URL}/deployments/${DEPLOY_ID}/create_flow_run" \
                '{"parameters": {"name": "Demo"}}' \
                | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])" 2>/dev/null || true)
            if [[ -n "$RUN_ID" ]]; then
                info "Flow run created: $RUN_ID"
                info "Waiting 15 seconds for completion..."
                sleep 15
                STATE=$(api_get "${PREFECT_URL}/flow_runs/${RUN_ID}" \
                    | python3 -c "import json,sys; print(json.load(sys.stdin)['state']['type'])" 2>/dev/null || echo "UNKNOWN")
                info "Flow run state: $STATE"
            fi
        else
            warn "Could not find example-flow-local deployment"
        fi
    else
        warn "PREFECT_API_URL not set — skipping flow trigger"
    fi
fi
pause

# ── 6. Monitoring Stack ──────────────────────────────────────────────────
section "6. Monitoring Stack"
info "PF_MONITOR: Prometheus + Grafana + Loki (8 containers)"
info "9 Grafana dashboards, 15 alert rules, Slack + email notifications"
info "Grafana has 4 secret mounts: admin password, DB DSN, SMTP, Slack webhook"
run_sql_clean "USE DATABASE ${DB}; USE SCHEMA ${SCHEMA}; SHOW ENDPOINTS IN SERVICE ${DB}.${SCHEMA}.PF_MONITOR; SELECT \"name\", \"port\", \"ingress_url\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));"
pause

# ── 7. Automations & Secret Blocks ──────────────────────────────────────
section "7. Automations & Secret Blocks"
info "Two-layer secret architecture:"
info "  Layer 1: Prefect Secret blocks (read by flows at runtime, no restart)"
info "  Layer 2: .env + Snowflake secrets + GitLab CI (infrastructure)"
if [[ -n "$PREFECT_URL" ]]; then
    info "Prefect Secret blocks:"
    api_post "${PREFECT_URL}/block_documents/filter" '{"block_types": {"slug": {"any_": ["secret"]}}}' | python3 -c "
import json, sys
data = json.load(sys.stdin)
for b in data:
    print(f'  {b[\"name\"]:25s} type={b[\"block_type\"][\"slug\"]}')
" 2>/dev/null || warn "Could not query blocks"
    info "Server-side automations:"
    api_post "${PREFECT_URL}/automations/filter" '{}' | python3 -c "
import json, sys
data = json.load(sys.stdin)
for a in data:
    status = 'ON' if a['enabled'] else 'OFF'
    actions = ', '.join(act['type'] for act in a['actions'])
    print(f'  [{status}] {a[\"name\"]:40s} actions={actions}')
" 2>/dev/null || warn "Could not query automations"
fi
pause

# ── 8. Hybrid Workers ──────────────────────────────────────────────────
section "8. Hybrid Workers"
info "Multi-cloud: SPCS (Snowflake), GCP (us-central1), AWS (us-west-2)"
info "External workers use PAT + auth-proxy sidecar for CSRF-safe connections."
if [[ -n "$PREFECT_URL" ]]; then
    api_post "${PREFECT_URL}/work_pools/filter" '{"limit": 10}' | python3 -c "
import json, sys
data = json.load(sys.stdin)
for p in data:
    status = p.get('status', 'unknown')
    print(f'  {p[\"name\"]:25s} type={p[\"type\"]:15s} status={status}')
" 2>/dev/null || warn "Could not query work pools"
    info "Workers per pool:"
    for pool in spcs-pool gcp-pool gcp-pool-backup aws-pool aws-pool-backup; do
        WORKERS=$(api_post "${PREFECT_URL}/work_pools/${pool}/workers/filter" '{}' 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
online = [w for w in data if w.get('status') == 'ONLINE']
print(f'{len(online)} online')
" 2>/dev/null || echo "?")
        info "  ${pool}: ${WORKERS}"
    done
fi
pause

# ── 9. Cost Analysis ──────────────────────────────────────────────────
section "9. Cost Overview"
info "SPCS credit consumption (last 7 days)..."
run_sql_clean "USE ROLE ACCOUNTADMIN; SELECT service_type, ROUND(SUM(credits_used), 2) AS credits_7d, ROUND(SUM(credits_used) * 3.00, 2) AS est_usd FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY WHERE service_type = 'SNOWPARK_CONTAINER_SERVICES' AND usage_date >= DATEADD(day, -7, CURRENT_DATE()) GROUP BY 1;"
info "Per-pool breakdown..."
run_sql_clean "USE ROLE ACCOUNTADMIN; SELECT compute_pool_name AS pool, ROUND(SUM(credits_used), 2) AS credits_7d FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY WHERE start_time >= DATEADD(day, -7, CURRENT_DATE()) AND compute_pool_name LIKE 'PREFECT_%' GROUP BY 1 ORDER BY 2 DESC;"
pause

# ── 10. Test Coverage ─────────────────────────────────────────────────
section "10. Test Coverage"
info "2344 tests: 2330 unit/integration + 14 E2E against live infrastructure."
info "Pre-commit hooks: ruff, ruff-format, shellcheck, detect-secrets"
info "GitLab CI: lint → test → SAST → secret-detection → build → deploy"
pause

# ── Summary ──────────────────────────────────────────────────────────
section "Summary"
echo "  Production-grade Prefect on SPCS with:"
echo ""
echo "  ✓ 5 SPCS services across 5 dedicated compute pools"
echo "  ✓ 10 flows × 5 pools = 47 deployments with git-based delivery"
echo "  ✓ Hybrid workers: SPCS + GCP + AWS (with backup pools)"
echo "  ✓ Full observability: Prometheus, Grafana (9 dashboards), Loki"
echo "  ✓ Streamlit ops dashboard (7 pages)"
echo "  ✓ 16 alert rules with Slack, webhook, and email notifications"
echo "  ✓ 3 server-side automations: failure alerts, auto-retry, stale detection"
echo "  ✓ Two-layer secret architecture with PAT auto-rotation flow"
echo "  ✓ 2344 tests, GitLab CI/CD, pre-commit hooks"
echo "  ✓ Cost management: analysis script, per-pool tracking"
echo "  ✓ Operations runbook, secrets rotation, version management"
echo ""
info "Demo complete."
