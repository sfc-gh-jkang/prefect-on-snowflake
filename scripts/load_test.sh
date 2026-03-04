#!/usr/bin/env bash
# =============================================================================
# load_test.sh — Validate the full Prefect SPCS + monitoring pipeline
#
# Triggers flow runs, then verifies:
#   1. Flow runs complete via Prefect API
#   2. Prometheus scrapes metrics (prefect_flow_runs_total, etc.)
#   3. Loki has recent logs (SPCS event logs ingested)
#   4. Grafana is healthy and datasources are reachable
#   5. Backup sidecars have created stage files
#
# Usage:
#   ./scripts/load_test.sh                                    # Uses .env
#   PREFECT_API_URL=... PROMETHEUS_URL=... ./scripts/load_test.sh
#
# Prerequisites:
#   - .env file with PREFECT_API_URL, SNOWFLAKE_PAT, SPCS_ENDPOINT
#   - Flows deployed to the Prefect server
#   - PF_MONITOR service running
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

# Endpoints
PREFECT_API="${PREFECT_API_URL:?Set PREFECT_API_URL in .env or environment}"
PAT="${SNOWFLAKE_PAT:?Set SNOWFLAKE_PAT in .env or environment}"
# Derive monitoring URLs from the SPCS endpoint pattern
# PF_MONITOR endpoints use different subdomain prefixes than PF_SERVER
PROMETHEUS_URL="${PROMETHEUS_URL:-}"
GRAFANA_URL="${GRAFANA_URL:-}"
LOKI_URL="${LOKI_URL:-}"

AUTH_HEADER="Authorization: Snowflake Token=\"${PAT}\""

PASSED=0
FAILED=0
SKIPPED=0

pass() { echo "  PASS: $1"; ((PASSED++)); }
fail() { echo "  FAIL: $1"; ((FAILED++)); }
skip() { echo "  SKIP: $1"; ((SKIPPED++)); }

# Helper: authenticated curl
acurl() {
    curl -sf -H "$AUTH_HEADER" -H "Content-Type: application/json" "$@"
}

echo "============================================="
echo "  Prefect SPCS Load Test"
echo "============================================="
echo "  Prefect API:  $PREFECT_API"
echo "  Prometheus:   ${PROMETHEUS_URL:-not set (skip Prometheus checks)}"
echo "  Grafana:      ${GRAFANA_URL:-not set (skip Grafana checks)}"
echo "  Loki:         ${LOKI_URL:-not set (skip Loki checks)}"
echo ""

# ── 1. Prefect API Health ─────────────────────────────────────────────────
echo "=== 1. Prefect API Health ==="
if acurl "${PREFECT_API}/health" > /dev/null 2>&1; then
    pass "Prefect API is healthy"
else
    fail "Prefect API health check failed"
fi

# ── 2. Trigger Flow Runs ──────────────────────────────────────────────────
echo ""
echo "=== 2. Triggering Flow Runs ==="

# List deployments
DEPLOYMENTS=$(acurl "${PREFECT_API}/deployments/filter" -d '{"limit": 20}' 2>/dev/null || echo "[]")
DEPLOY_COUNT=$(echo "$DEPLOYMENTS" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
echo "  Found $DEPLOY_COUNT deployments"

if [[ "$DEPLOY_COUNT" -gt 0 ]]; then
    pass "Deployments exist ($DEPLOY_COUNT)"
else
    fail "No deployments found"
fi

# Find the e2e-pipeline-test deployment and trigger it
E2E_DEPLOY_ID=$(echo "$DEPLOYMENTS" | python3 -c "
import sys, json
deps = json.load(sys.stdin)
for d in deps:
    if 'e2e' in d.get('name', '').lower():
        print(d['id'])
        break
" 2>/dev/null || echo "")

FLOW_RUN_IDS=()
if [[ -n "$E2E_DEPLOY_ID" ]]; then
    echo "  Triggering e2e-pipeline-test (deployment $E2E_DEPLOY_ID)..."
    RUN_RESP=$(acurl "${PREFECT_API}/deployments/${E2E_DEPLOY_ID}/create_flow_run" \
        -d '{"state": {"type": "SCHEDULED"}}' 2>/dev/null || echo "{}")
    RUN_ID=$(echo "$RUN_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
    if [[ -n "$RUN_ID" ]]; then
        FLOW_RUN_IDS+=("$RUN_ID")
        pass "Triggered flow run $RUN_ID"
    else
        fail "Could not trigger e2e flow run"
    fi
else
    skip "No e2e-pipeline-test deployment found"
fi

# Also trigger the example flow if it exists
EXAMPLE_DEPLOY_ID=$(echo "$DEPLOYMENTS" | python3 -c "
import sys, json
deps = json.load(sys.stdin)
for d in deps:
    if 'example' in d.get('name', '').lower():
        print(d['id'])
        break
" 2>/dev/null || echo "")

if [[ -n "$EXAMPLE_DEPLOY_ID" ]]; then
    echo "  Triggering example flow (deployment $EXAMPLE_DEPLOY_ID)..."
    RUN_RESP=$(acurl "${PREFECT_API}/deployments/${EXAMPLE_DEPLOY_ID}/create_flow_run" \
        -d '{"state": {"type": "SCHEDULED"}}' 2>/dev/null || echo "{}")
    RUN_ID=$(echo "$RUN_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
    if [[ -n "$RUN_ID" ]]; then
        FLOW_RUN_IDS+=("$RUN_ID")
        pass "Triggered flow run $RUN_ID"
    else
        fail "Could not trigger example flow run"
    fi
fi

# ── 3. Wait for Flow Runs ─────────────────────────────────────────────────
echo ""
echo "=== 3. Waiting for Flow Runs ==="
MAX_WAIT=300  # 5 minutes
POLL_INTERVAL=15

for RUN_ID in "${FLOW_RUN_IDS[@]}"; do
    echo "  Polling flow run $RUN_ID..."
    ELAPSED=0
    FINAL_STATE="UNKNOWN"
    while [[ $ELAPSED -lt $MAX_WAIT ]]; do
        STATE_RESP=$(acurl "${PREFECT_API}/flow_runs/${RUN_ID}" 2>/dev/null || echo "{}")
        STATE_TYPE=$(echo "$STATE_RESP" | python3 -c "
import sys, json
data = json.load(sys.stdin)
st = data.get('state', {})
print(st.get('type', 'UNKNOWN'))
" 2>/dev/null || echo "UNKNOWN")

        if [[ "$STATE_TYPE" == "COMPLETED" ]]; then
            FINAL_STATE="COMPLETED"
            break
        elif [[ "$STATE_TYPE" == "FAILED" || "$STATE_TYPE" == "CRASHED" || "$STATE_TYPE" == "CANCELLED" ]]; then
            FINAL_STATE="$STATE_TYPE"
            break
        fi
        sleep $POLL_INTERVAL
        ELAPSED=$((ELAPSED + POLL_INTERVAL))
        echo "    ${ELAPSED}s — state: $STATE_TYPE"
    done

    if [[ "$FINAL_STATE" == "COMPLETED" ]]; then
        pass "Flow run $RUN_ID completed"
    elif [[ "$FINAL_STATE" == "UNKNOWN" ]]; then
        fail "Flow run $RUN_ID timed out after ${MAX_WAIT}s"
    else
        fail "Flow run $RUN_ID ended in state: $FINAL_STATE"
    fi
done

# ── 4. Prometheus Metrics ─────────────────────────────────────────────────
echo ""
echo "=== 4. Prometheus Metrics ==="
if [[ -n "$PROMETHEUS_URL" ]]; then
    # Check Prometheus is healthy
    if acurl "${PROMETHEUS_URL}/-/ready" > /dev/null 2>&1; then
        pass "Prometheus is ready"
    else
        fail "Prometheus readiness check failed"
    fi

    # Check for prefect exporter metrics
    PROM_RESULT=$(acurl "${PROMETHEUS_URL}/api/v1/query?query=prefect_flow_runs_total" 2>/dev/null || echo "{}")
    RESULT_COUNT=$(echo "$PROM_RESULT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(len(data.get('data', {}).get('result', [])))
" 2>/dev/null || echo "0")

    if [[ "$RESULT_COUNT" -gt 0 ]]; then
        pass "Prometheus has prefect_flow_runs_total metrics ($RESULT_COUNT series)"
    else
        fail "Prometheus has no prefect_flow_runs_total metrics"
    fi

    # Check recording rules are loaded
    RULES_RESULT=$(acurl "${PROMETHEUS_URL}/api/v1/rules" 2>/dev/null || echo "{}")
    RULE_COUNT=$(echo "$RULES_RESULT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
groups = data.get('data', {}).get('groups', [])
total = sum(len(g.get('rules', [])) for g in groups)
print(total)
" 2>/dev/null || echo "0")

    if [[ "$RULE_COUNT" -gt 0 ]]; then
        pass "Prometheus has $RULE_COUNT alert/recording rules loaded"
    else
        fail "Prometheus has no rules loaded"
    fi
else
    skip "Prometheus URL not set"
fi

# ── 5. Loki Logs ──────────────────────────────────────────────────────────
echo ""
echo "=== 5. Loki Logs ==="
if [[ -n "$LOKI_URL" ]]; then
    if acurl "${LOKI_URL}/ready" > /dev/null 2>&1; then
        pass "Loki is ready"
    else
        fail "Loki readiness check failed"
    fi

    # Query for recent SPCS logs
    NOW_NS=$(($(date +%s) * 1000000000))
    HOUR_AGO_NS=$(( ($(date +%s) - 3600) * 1000000000))
    LOKI_RESULT=$(acurl "${LOKI_URL}/loki/api/v1/query_range?query=%7Bjob%3D%22spcs-logs%22%7D&start=${HOUR_AGO_NS}&end=${NOW_NS}&limit=5" 2>/dev/null || echo "{}")
    STREAM_COUNT=$(echo "$LOKI_RESULT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(len(data.get('data', {}).get('result', [])))
" 2>/dev/null || echo "0")

    if [[ "$STREAM_COUNT" -gt 0 ]]; then
        pass "Loki has SPCS log streams ($STREAM_COUNT streams in last hour)"
    else
        fail "Loki has no SPCS log streams in the last hour"
    fi
else
    skip "Loki URL not set"
fi

# ── 6. Grafana Health ─────────────────────────────────────────────────────
echo ""
echo "=== 6. Grafana Health ==="
if [[ -n "$GRAFANA_URL" ]]; then
    HEALTH=$(acurl "${GRAFANA_URL}/api/health" 2>/dev/null || echo "{}")
    DB_STATUS=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('database',''))" 2>/dev/null || echo "")

    if [[ "$DB_STATUS" == "ok" ]]; then
        pass "Grafana is healthy (database: ok)"
    else
        fail "Grafana health check failed (database: $DB_STATUS)"
    fi

    # Check datasources are working
    DS_RESULT=$(acurl "${GRAFANA_URL}/api/datasources" 2>/dev/null || echo "[]")
    DS_COUNT=$(echo "$DS_RESULT" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")

    if [[ "$DS_COUNT" -ge 2 ]]; then
        pass "Grafana has $DS_COUNT datasources configured"
    else
        fail "Grafana has fewer than 2 datasources ($DS_COUNT)"
    fi
else
    skip "Grafana URL not set"
fi

# ── 7. Backup Stage Files ────────────────────────────────────────────────
echo ""
echo "=== 7. Backup Stage Files ==="
SNOW_CONN="${SNOWFLAKE_CONNECTION:-aws_spcs}"
PROM_BACKUPS=$(snow sql -q "LIST @PREFECT_DB.PREFECT_SCHEMA.MONITOR_STAGE/backups/prometheus/" \
    --connection "$SNOW_CONN" 2>/dev/null | grep -c "tar.gz" || echo "0")
LOKI_BACKUPS=$(snow sql -q "LIST @PREFECT_DB.PREFECT_SCHEMA.MONITOR_STAGE/backups/loki/" \
    --connection "$SNOW_CONN" 2>/dev/null | grep -c "tar.gz" || echo "0")

if [[ "$PROM_BACKUPS" -gt 0 ]]; then
    pass "Prometheus backups on stage ($PROM_BACKUPS files)"
else
    skip "No Prometheus backups yet (first backup after 6h)"
fi

if [[ "$LOKI_BACKUPS" -gt 0 ]]; then
    pass "Loki backups on stage ($LOKI_BACKUPS files)"
else
    skip "No Loki backups yet (first backup after 6h)"
fi

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "============================================="
echo "  RESULTS: $PASSED passed, $FAILED failed, $SKIPPED skipped"
echo "============================================="

if [[ $FAILED -gt 0 ]]; then
    exit 1
fi
