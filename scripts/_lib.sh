#!/usr/bin/env bash
# =============================================================================
# _lib.sh — Shared helper functions for Prefect SPCS scripts
#
# Source this file from other scripts:
#   source "$(dirname "$0")/_lib.sh"
# =============================================================================

# --- Poll SHOW SERVICES until RUNNING or timeout ---
# Replaces deprecated SYSTEM$GET_SERVICE_STATUS (see BCR-1596).
# SHOW SERVICES returns a "status" column with values like RUNNING, SUSPENDED, etc.
#
# Usage: wait_for_ready SERVICE_NAME [MAX_ATTEMPTS] [CONNECTION]
#   SERVICE_NAME  — SPCS service name (e.g., PF_SERVER)
#   MAX_ATTEMPTS  — polling attempts, default 36 (36 * 5s = 3 min)
#   CONNECTION    — snow CLI connection name (uses $CONN if unset)
wait_for_ready() {
    local service="$1"
    local max_attempts="${2:-36}"  # 36 * 5s = 3 minutes
    local conn="${3:-${CONN:-${CONNECTION:-}}}"
    echo "  Waiting for ${service} to reach RUNNING..."
    for i in $(seq 1 "${max_attempts}"); do
        local status
        status=$(snow sql -q "
            SHOW SERVICES LIKE '${service}' IN SCHEMA PREFECT_DB.PREFECT_SCHEMA;
        " --connection "$conn" --format json 2>/dev/null | \
            python3 -c "
import sys, json
data = json.load(sys.stdin)
# SHOW SERVICES returns a list of rows; pick the first match
if data and len(data) > 0:
    row = data[0] if isinstance(data[0], dict) else data[-1][0] if isinstance(data[-1], list) else {}
    print(row.get('status', ''))
else:
    print('')
" 2>/dev/null || echo "")
        if [[ "${status}" == "RUNNING" ]]; then
            echo "  ${service} is RUNNING (attempt ${i}/${max_attempts})"
            return 0
        fi
        sleep 5
    done
    echo "  WARNING: ${service} did not reach RUNNING within $((max_attempts * 5))s"
    return 1
}
