#!/usr/bin/env bash
# =============================================================================
# teardown.sh — Remove all Prefect SPCS infrastructure
#
# Usage:
#   ./scripts/teardown.sh --connection my_connection          # Drop services + infra (keep DB)
#   ./scripts/teardown.sh --connection my_connection --full    # Drop everything incl. DB + role
#   ./scripts/teardown.sh --connection my_connection --force   # Skip confirmation prompt
#
# Safe to run multiple times — all DROP statements use IF EXISTS.
# =============================================================================
set -uo pipefail

CONN=""
FULL=false
FORCE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --connection) CONN="${2:-}"; shift 2 ;;
        --full)       FULL=true; shift ;;
        --force)      FORCE=true; shift ;;
        -h|--help)
            echo "Usage: teardown.sh --connection <name> [--full] [--force]"
            echo ""
            echo "Options:"
            echo "  --connection NAME   Snow CLI connection name (required)"
            echo "  --full              Also drop database PREFECT_DB and role PREFECT_ROLE"
            echo "  --force             Skip confirmation prompt"
            exit 0
            ;;
        *)
            if [[ -z "$CONN" ]]; then CONN="$1"; fi
            shift
            ;;
    esac
done

if [[ -z "$CONN" ]]; then
    echo "Error: --connection is required"
    echo "Usage: teardown.sh --connection <name> [--full] [--force]"
    exit 1
fi

DB="PREFECT_DB"
SCHEMA="PREFECT_SCHEMA"

echo "============================================="
echo "  Prefect SPCS Teardown"
echo "  Connection: $CONN"
echo "  Mode:       $(if $FULL; then echo 'FULL (drop database + role)'; else echo 'Standard (keep database)'; fi)"
echo "============================================="
echo ""

if ! $FORCE; then
    echo "WARNING: This will destroy all Prefect SPCS services and infrastructure."
    if $FULL; then
        echo "         --full mode will also DROP DATABASE $DB and ROLE PREFECT_ROLE."
    fi
    read -p "Continue? (y/N) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
fi

# Helper: run SQL as PREFECT_ROLE with DB/SCHEMA context
_sql_prefect() {
    snow sql -q "USE ROLE PREFECT_ROLE; USE DATABASE $DB; USE SCHEMA $SCHEMA; $1" \
        --connection "$CONN" 2>&1 | tail -3 || true
}

# Helper: run SQL as ACCOUNTADMIN (account-level objects)
_sql_admin() {
    snow sql -q "USE ROLE ACCOUNTADMIN; $1" \
        --connection "$CONN" 2>&1 | tail -3 || true
}

# ── 1. Drop all services (must happen before compute pool drop) ──────────────
echo "=== [1/10] Dropping services ==="
for SVC in PF_WORKER PF_SERVICES PF_SERVER PF_MONITOR PF_POSTGRES PF_REDIS; do
    echo "  Dropping $SVC..."
    _sql_prefect "DROP SERVICE IF EXISTS ${DB}.${SCHEMA}.$SVC FORCE"
done

# Drop any stale job services (DONE/FAILED state — may be owned by either role)
echo "  Dropping stale job services..."
for JOB in PF_DEPLOY_JOB PF_DEPLOY_ALL PF_CREATE_POOLS PF_MIGRATE PF_TRIGGER_HC PF_TRIGGER_JOB; do
    _sql_prefect "DROP SERVICE IF EXISTS ${DB}.${SCHEMA}.$JOB FORCE"
    _sql_admin "DROP SERVICE IF EXISTS ${DB}.${SCHEMA}.$JOB FORCE"
done

# ── 2. Drop dashboard ────────────────────────────────────────────────────────
echo ""
echo "=== [2/10] Dropping dashboard ==="
echo "  Dropping Streamlit app..."
_sql_prefect "DROP STREAMLIT IF EXISTS ${DB}.${SCHEMA}.PREFECT_DASHBOARD_APP"
echo "  Dropping dashboard UDF..."
_sql_prefect "DROP FUNCTION IF EXISTS ${DB}.${SCHEMA}.GET_PREFECT_PAT()"

# ── 3. Suspend compute pools ────────────────────────────────────────────────
echo ""
echo "=== [3/10] Suspending compute pools ==="
for POOL in PREFECT_WORKER_POOL PREFECT_CORE_POOL PREFECT_INFRA_POOL PREFECT_DASHBOARD_POOL PREFECT_MONITOR_POOL; do
    echo "  Suspending $POOL..."
    _sql_admin "ALTER COMPUTE POOL IF EXISTS $POOL SUSPEND"
done

echo "  Waiting 15s for pools to drain..."
sleep 15

# ── 4. Drop compute pools ───────────────────────────────────────────────────
echo ""
echo "=== [4/10] Dropping compute pools ==="
for POOL in PREFECT_WORKER_POOL PREFECT_CORE_POOL PREFECT_INFRA_POOL PREFECT_DASHBOARD_POOL PREFECT_MONITOR_POOL; do
    echo "  Stopping all services on $POOL..."
    _sql_admin "ALTER COMPUTE POOL IF EXISTS $POOL STOP ALL"
done

sleep 5

for POOL in PREFECT_WORKER_POOL PREFECT_CORE_POOL PREFECT_INFRA_POOL PREFECT_DASHBOARD_POOL PREFECT_MONITOR_POOL; do
    echo "  Dropping $POOL..."
    _sql_admin "DROP COMPUTE POOL IF EXISTS $POOL"
done

# ── 5. Drop stages ───────────────────────────────────────────────────────────
echo ""
echo "=== [5/10] Dropping stages ==="
for STAGE in PREFECT_SPECS PREFECT_FLOWS PREFECT_DASHBOARD MONITOR_STAGE; do
    echo "  Dropping $STAGE..."
    _sql_prefect "DROP STAGE IF EXISTS ${DB}.${SCHEMA}.$STAGE"
done

# ── 6. Drop secrets ──────────────────────────────────────────────────────────
echo ""
echo "=== [6/10] Dropping secrets ==="
for SECRET in PREFECT_DB_PASSWORD GIT_ACCESS_TOKEN PREFECT_SVC_PAT \
              POSTGRES_EXPORTER_DSN GRAFANA_ADMIN_PASSWORD GRAFANA_DB_DSN \
              GRAFANA_SMTP_PASSWORD SLACK_WEBHOOK_URL; do
    echo "  Dropping $SECRET..."
    _sql_prefect "DROP SECRET IF EXISTS ${DB}.${SCHEMA}.$SECRET"
done

# ── 7. Drop external access integrations ─────────────────────────────────────
echo ""
echo "=== [7/10] Dropping external access integrations ==="
for EAI in PREFECT_WORKER_EAI PREFECT_DASHBOARD_EAI PREFECT_MONITOR_EAI PREFECT_PG_EAI; do
    echo "  Dropping $EAI..."
    _sql_admin "DROP INTEGRATION IF EXISTS $EAI"
done

# ── 8. Drop network rules ───────────────────────────────────────────────────
echo ""
echo "=== [8/10] Dropping network rules ==="
for RULE in PREFECT_WORKER_EGRESS_RULE DASHBOARD_EGRESS_RULE MONITOR_EGRESS_RULE PREFECT_PG_EGRESS_RULE; do
    echo "  Dropping $RULE..."
    _sql_prefect "DROP NETWORK RULE IF EXISTS ${DB}.${SCHEMA}.$RULE"
done

# ── 9. Drop image repository ────────────────────────────────────────────────
echo ""
echo "=== [9/10] Dropping image repository ==="
echo "  Dropping PREFECT_REPOSITORY..."
_sql_prefect "DROP IMAGE REPOSITORY IF EXISTS ${DB}.${SCHEMA}.PREFECT_REPOSITORY"

# ── 10. (Optional) Drop database + role ──────────────────────────────────────
echo ""
if $FULL; then
    echo "=== [10/10] Dropping database and role ==="

    # Unset network policies on Prefect service user (blocks DB drop otherwise)
    echo "  Unsetting network policies on PREFECT_SVC..."
    _sql_admin "ALTER USER IF EXISTS PREFECT_SVC UNSET NETWORK_POLICY"

    # Unset network policy from Managed Postgres instance (if exists)
    echo "  Unsetting network policy on Managed Postgres instance..."
    _sql_admin "ALTER POSTGRES INSTANCE IF EXISTS PREFECT_PG UNSET NETWORK_POLICY"

    # Drop Prefect-related network policies
    echo "  Dropping Prefect network policies..."
    _sql_admin "DROP NETWORK POLICY IF EXISTS PREFECT_SVC_POLICY"
    _sql_admin "DROP NETWORK POLICY IF EXISTS PREFECT_PG_POLICY"

    # Drop any remaining network rules in the DB (owned by ACCOUNTADMIN)
    echo "  Dropping leftover network rules..."
    _sql_admin "DROP NETWORK RULE IF EXISTS ${DB}.${SCHEMA}.PREFECT_SVC_ALL_RULE"
    _sql_admin "DROP NETWORK RULE IF EXISTS ${DB}.${SCHEMA}.PREFECT_PG_INGRESS_RULE"

    echo "  Dropping database $DB..."
    _sql_admin "DROP DATABASE IF EXISTS $DB"
    echo "  Dropping role PREFECT_ROLE..."
    _sql_admin "DROP ROLE IF EXISTS PREFECT_ROLE"
else
    echo "=== [10/10] Skipped (database + role kept) ==="
    echo "  Database $DB was NOT dropped. Use --full to remove everything."
fi

# ── Verify ───────────────────────────────────────────────────────────────────
echo ""
echo "=== Verification ==="
echo "  Checking for remaining compute pools..."
_sql_admin "SHOW COMPUTE POOLS LIKE 'PREFECT_%'" 2>/dev/null || echo "  (none)"

if ! $FULL; then
    echo "  Checking for remaining services..."
    _sql_prefect "SHOW SERVICES LIKE 'PF_%' IN SCHEMA ${DB}.${SCHEMA}" 2>/dev/null || echo "  (none)"
else
    echo "  Database dropped — no service check needed."
fi

echo ""
echo "=== Teardown complete for connection: $CONN ==="
