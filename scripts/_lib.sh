#!/usr/bin/env bash
# =============================================================================
# _lib.sh — Shared helper functions for Prefect SPCS scripts
#
# Source this file from other scripts:
#   source "$(dirname "$0")/_lib.sh"
# =============================================================================

# --- Poll SYSTEM$GET_SERVICE_STATUS until READY or timeout ---
# Usage: wait_for_ready SERVICE_NAME [MAX_ATTEMPTS] [CONNECTION]
#   SERVICE_NAME  — SPCS service name (e.g., PF_SERVER)
#   MAX_ATTEMPTS  — polling attempts, default 36 (36 * 5s = 3 min)
#   CONNECTION    — snow CLI connection name (uses $CONN if unset)
wait_for_ready() {
    local service="$1"
    local max_attempts="${2:-36}"  # 36 * 5s = 3 minutes
    local conn="${3:-${CONN:-${CONNECTION:-}}}"
    echo "  Waiting for ${service} to reach READY..."
    for i in $(seq 1 "${max_attempts}"); do
        local status
        status=$(snow sql -q "
            USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;
            SELECT SYSTEM\$GET_SERVICE_STATUS('${service}');
        " --connection "$conn" --format json 2>/dev/null | \
            python3 -c "
import sys, json
data = json.load(sys.stdin)
# Multi-statement: last batch has the result, previous are 'Statement executed successfully'
row = data[-1][0]
val = row[next(iter(row))]
print(json.loads(val)[0].get('status', ''))
" 2>/dev/null || echo "")
        if [[ "${status}" == "READY" ]]; then
            echo "  ${service} is READY (attempt ${i}/${max_attempts})"
            return 0
        fi
        sleep 5
    done
    echo "  WARNING: ${service} did not reach READY within $((max_attempts * 5))s"
    return 1
}
