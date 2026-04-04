#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Run SQL setup scripts in order to deploy Prefect on SPCS
#
# Usage:
#   ./scripts/deploy.sh --connection my_connection           # First-time deploy
#   ./scripts/deploy.sh --connection my_connection --update  # Update existing services
#
# The --update flag uses ALTER SERVICE (07b_update_services.sql) instead of
# CREATE SERVICE, which preserves the public endpoint URL. Use --update for
# all changes after the initial deployment: image updates, spec changes,
# env var changes, resource adjustments, etc.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SQL_DIR="$PROJECT_DIR/sql"

# --- Load .env (if present) for deploy-time substitution ---
ENV_FILE="$PROJECT_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

# --- Parse arguments ---
CONN=""
UPDATE_MODE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --connection) CONN="${2:-}"; shift 2 ;;
        --update)     UPDATE_MODE=true; shift ;;
        -h|--help)
            echo "Usage: deploy.sh --connection <name> [--update]"
            echo ""
            echo "Options:"
            echo "  --connection NAME   Snow CLI connection name (required)"
            echo "  --update            Update existing services via ALTER SERVICE"
            echo "                      (preserves public endpoint URL)"
            echo "  -h, --help          Show this help"
            exit 0
            ;;
        *)  # Bare positional arg for backwards compatibility
            if [[ -z "$CONN" ]]; then
                CONN="$1"
            fi
            shift
            ;;
    esac
done

if [[ -z "$CONN" ]]; then
    echo "Error: --connection is required"
    echo "Usage: deploy.sh --connection <name> [--update]"
    exit 1
fi

SNOW_CMD="snow sql --connection $CONN"

run_sql() {
    local file="$1"
    local desc="$2"
    echo "  [$desc] $(basename "$file")..."
    snow sql -f "$file" --connection "$CONN" 2>&1 | tail -3
    echo ""
}

# --- Shared helpers (wait_for_ready, etc.) ---
# shellcheck source=scripts/_lib.sh
source "${SCRIPT_DIR}/_lib.sh"

# --- Check if services already exist ---
services_exist() {
    local result
    result=$(${SNOW_CMD} --query "USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA; SHOW SERVICES LIKE 'PF_%' IN SCHEMA PREFECT_DB.PREFECT_SCHEMA;" --format json 2>/dev/null || echo "[]")
    local count
    count=$(echo "${result}" | python3 -c "import sys,json; data=json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "0")
    [[ "${count}" -gt 0 ]]
}

# --- Upload specs with .env substitution ---
upload_specs() {
    local tmp_dir
    tmp_dir="$(mktemp -d)"
    cp "$PROJECT_DIR"/specs/*.yaml "$tmp_dir/"

    local git_url="${GIT_REPO_URL:-CHANGE_ME_GIT_REPO_URL}"
    local git_branch="${GIT_BRANCH:-main}"
    for spec_file in pf_worker.yaml pf_deploy_job.yaml; do
      if [[ -f "$tmp_dir/$spec_file" ]]; then
        sed -i.bak \
          -e "s|GIT_REPO_URL: \"CHANGE_ME_GIT_REPO_URL\"|GIT_REPO_URL: \"${git_url}\"|" \
          -e "s|GIT_BRANCH: \"main\"|GIT_BRANCH: \"${git_branch}\"|" \
          "$tmp_dir/$spec_file"
      fi
    done

    # Substitute Observe OTLP placeholders in pf_worker.yaml.
    # Direct-to-Observe export requires a datastream token (ds1xxxxx:yyyyyy)
    # and the collection URL. See README "APM Trace Collection" for details.
    local obs_ds_token="${OBSERVE_DATASTREAM_TOKEN:-}"
    local obs_collect_url="${OBSERVE_COLLECTION_URL:-}"
    if [[ -n "$obs_ds_token" && -n "$obs_collect_url" && -f "$tmp_dir/pf_worker.yaml" ]]; then
        sed -i.bak \
          -e "s|__OBSERVE_DATASTREAM_TOKEN__|${obs_ds_token}|" \
          -e "s|__OBSERVE_COLLECTION_URL__|${obs_collect_url}|" \
          "$tmp_dir/pf_worker.yaml"
    fi
    rm -f "$tmp_dir"/*.bak

    snow stage copy "$tmp_dir/" @PREFECT_DB.PREFECT_SCHEMA.PREFECT_SPECS/ \
        --overwrite --connection "$CONN"
    rm -rf "$tmp_dir"
}

echo "============================================="
echo "  Prefect SPCS Deployment"
echo "  Connection: ${CONN}"
echo "  Mode:       $(if ${UPDATE_MODE}; then echo 'UPDATE (ALTER SERVICE)'; else echo 'CREATE (first-time)'; fi)"
echo "============================================="
echo ""

if ${UPDATE_MODE}; then
    # --update mode: verify services exist before attempting ALTER
    if ! services_exist; then
        echo "ERROR: No existing Prefect services found."
        echo "  Run a first-time deploy first (without --update):"
        echo "    ./scripts/deploy.sh --connection ${CONN}"
        exit 1
    fi

    # Upload specs to stage (with .env substitution)
    echo "=== Uploading specs to @PREFECT_SPECS ==="
    upload_specs
    echo ""

    # ALTER SERVICE for all long-running services
    echo "=== Updating services (ALTER SERVICE) ==="
    run_sql "$SQL_DIR/07b_update_services.sql"  "1/2"

    # Poll each service until READY (rolling restart after ALTER)
    echo "=== Waiting for services to reach READY ==="
    POLL_FAILED=false
    for svc in PF_REDIS PF_SERVER PF_SERVICES PF_WORKER; do
        wait_for_ready "${svc}" || POLL_FAILED=true
    done

    if ${POLL_FAILED}; then
        echo ""
        echo "WARNING: Some services did not reach READY. Check status manually:"
        echo "  snow sql -q \"SHOW SERVICES LIKE '<service>' IN SCHEMA PREFECT_DB.PREFECT_SCHEMA\" --connection ${CONN}"
    fi

    echo ""
    run_sql "$SQL_DIR/08_validate.sql"          "2/2"

    echo "=== Update complete ==="
    echo ""
    echo "Services updated in-place. Public endpoint URL preserved."
    echo "Run 'uv run pytest' to verify consistency."

else
    # First-time mode: warn if services already exist
    if services_exist; then
        echo "WARNING: Prefect services already exist."
        echo "  Running CREATE again is safe (IF NOT EXISTS), but to update"
        echo "  services without changing the ingress URL, use --update:"
        echo ""
        echo "    ./scripts/deploy.sh --connection ${CONN} --update"
        echo ""
        read -rp "  Continue with first-time deploy anyway? [y/N]: " CONFIRM
        if [[ "${CONFIRM}" != "y" && "${CONFIRM}" != "Y" ]]; then
            echo "Aborted. Use --update to update existing services."
            exit 0
        fi
        echo ""
    fi

    # Upload specs to stage (with .env substitution)
    echo "=== Uploading specs to @PREFECT_SPECS ==="
    upload_specs
    echo ""

    echo "=== Running SQL setup scripts ==="
    run_sql "$SQL_DIR/01_setup_database.sql"     "1/8"
    run_sql "$SQL_DIR/02_setup_stages.sql"       "2/8"

    # Secrets: check if the filled-in file exists
    if [[ -f "$SQL_DIR/03_setup_secrets.sql" ]]; then
        run_sql "$SQL_DIR/03_setup_secrets.sql"  "3/8"
    else
        echo "  [3/8] SKIPPED: sql/03_setup_secrets.sql not found"
        echo "         Copy 03_setup_secrets.sql.template → 03_setup_secrets.sql and fill in passwords."
        echo ""
    fi

    run_sql "$SQL_DIR/04_setup_networking.sql"   "4/8"
    run_sql "$SQL_DIR/05_setup_compute_pools.sql" "5/8"
    run_sql "$SQL_DIR/06_setup_image_repo.sql"   "6/8"
    run_sql "$SQL_DIR/07_create_services.sql"    "7/8"
    run_sql "$SQL_DIR/08_validate.sql"           "8/8"

    echo "=== Deployment complete ==="
fi

# --- Dashboard deployment (both first-time and update modes) ---
echo ""
echo "=== Deploying observability dashboard ==="
DASHBOARD_DIR="$PROJECT_DIR/dashboard"
if [[ -d "$DASHBOARD_DIR" && -f "$DASHBOARD_DIR/snowflake.yml" ]]; then
    # Create the GET_PREFECT_PAT UDF (Container Runtime uses SQL UDF for secrets)
    ${SNOW_CMD} --query "
        USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;
        CREATE OR REPLACE FUNCTION GET_PREFECT_PAT()
          RETURNS STRING
          LANGUAGE PYTHON
          RUNTIME_VERSION = '3.11'
          HANDLER = 'get_pat'
          EXTERNAL_ACCESS_INTEGRATIONS = (PREFECT_DASHBOARD_EAI)
          SECRETS = ('prefect_svc_pat' = PREFECT_DB.PREFECT_SCHEMA.PREFECT_SVC_PAT)
          AS \$\$
import _snowflake
def get_pat():
    return _snowflake.get_generic_secret_string('prefect_svc_pat')
\$\$;
    " 2>&1 | tail -1

    # Deploy Streamlit app via snow CLI (handles versioned stage automatically)
    (cd "$DASHBOARD_DIR" && snow streamlit deploy --replace --connection "$CONN")
    echo "  Dashboard deployed."
else
    echo "  SKIPPED: dashboard/ directory or snowflake.yml not found"
fi

# --- Monitoring stack deployment ---
echo ""
echo "=== Deploying monitoring stack (Prometheus + Grafana + Loki) ==="
MONITOR_DIR="$PROJECT_DIR/monitoring"
if [[ -d "$MONITOR_DIR" && -f "$MONITOR_DIR/specs/pf_monitor.yaml" ]]; then
    bash "$MONITOR_DIR/deploy_monitoring.sh" --connection "$CONN"
else
    echo "  SKIPPED: monitoring/ directory or pf_monitor.yaml not found"
fi
