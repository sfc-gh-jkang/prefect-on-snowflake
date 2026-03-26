#!/usr/bin/env bash
# =============================================================================
# setup-o4s.sh — End-to-end Observe for Snowflake (O4S) setup
#
# Automates:
#   Phase 1: Snowflake-side O4S configuration (secrets, grants, connector)
#   Phase 2: Validation (task health, dataset discovery)
#   Phase 3: Terraform dashboards & monitors deployment
#
# Prerequisites:
#   1. "Observe for Snowflake" installed from Snowflake Marketplace
#   2. Observe-side Snowflake app installed (Applications → Snowflake → Install)
#   3. App ingest token from Observe (Snowflake app → Connections tab)
#   4. Observe API token (Settings → Account → API Keys)
#   5. snow CLI configured with a connection that has ACCOUNTADMIN
#   6. terraform >= 1.3.0 installed
#
# Usage:
#   ./setup-o4s.sh                         # Interactive — prompts for all values
#   ./setup-o4s.sh --validate-only         # Skip setup, just validate
#   ./setup-o4s.sh --terraform-only        # Skip SQL, deploy Terraform only
#
# Token types (do NOT confuse):
#   App ingest token: ds1xxxxx:yyyyyy  → for O4S native app (Snowflake secret)
#   API token:        VcG3ZZ...        → for Terraform + GraphQL API
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Colors & helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*"; }
fatal() { err "$@"; exit 1; }

divider() { echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TF_DIR="$PROJECT_DIR/terraform/observe"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
VALIDATE_ONLY=false
TERRAFORM_ONLY=false

for arg in "$@"; do
  case "$arg" in
    --validate-only)  VALIDATE_ONLY=true ;;
    --terraform-only) TERRAFORM_ONLY=true ;;
    --help|-h)
      echo "Usage: $0 [--validate-only] [--terraform-only] [--help]"
      echo ""
      echo "  --validate-only   Skip setup, only validate O4S tasks and Observe datasets"
      echo "  --terraform-only  Skip Snowflake SQL, deploy Terraform dashboards/monitors only"
      echo "  --help            Show this help"
      exit 0
      ;;
    *) fatal "Unknown argument: $arg" ;;
  esac
done

# ---------------------------------------------------------------------------
# Prompt for required values
# ---------------------------------------------------------------------------
prompt_value() {
  local varname="$1" prompt="$2" default="${3:-}"
  local value
  if [[ -n "$default" ]]; then
    read -rp "$(echo -e "${YELLOW}$prompt${NC} [$default]: ")" value
    value="${value:-$default}"
  else
    read -rp "$(echo -e "${YELLOW}$prompt${NC}: ")" value
  fi
  [[ -z "$value" ]] && fatal "$varname is required"
  eval "$varname='$value'"
}

prompt_secret() {
  local varname="$1" prompt="$2"
  local value
  read -srp "$(echo -e "${YELLOW}$prompt${NC}: ")" value
  echo ""
  [[ -z "$value" ]] && fatal "$varname is required"
  eval "$varname='$value'"
}

divider
echo -e "${GREEN}Observe for Snowflake (O4S) — Setup Script${NC}"
divider

# Check tools
command -v snow >/dev/null 2>&1 || fatal "snow CLI not found. Install: pip install snowflake-cli"
command -v terraform >/dev/null 2>&1 || fatal "terraform not found. Install: brew install terraform"
command -v curl >/dev/null 2>&1 || fatal "curl not found"
command -v python3 >/dev/null 2>&1 || fatal "python3 not found"

ok "All required tools found (snow, terraform, curl, python3)"

# ---------------------------------------------------------------------------
# Collect configuration
# ---------------------------------------------------------------------------
divider
echo -e "${BLUE}Configuration${NC}"
echo ""

prompt_value SNOW_CONN      "Snowflake connection name (from snow config)" "aws_spcs"
prompt_value OBSERVE_CUSTOMER_ID "Observe customer ID (numeric, e.g. 175859677949)" ""
prompt_value WAREHOUSE_NAME "Snowflake warehouse for O4S tasks" "PREFECT_WH"

if [[ "$VALIDATE_ONLY" == "false" && "$TERRAFORM_ONLY" == "false" ]]; then
  echo ""
  warn "Token types — make sure you use the RIGHT one:"
  echo "  App ingest token (ds1xxxxx:yyyyyy) → Snowflake secret (O4S native app)"
  echo "  API token (alphanumeric string)    → Terraform + GraphQL API"
  echo ""
  prompt_secret OBSERVE_INGEST_TOKEN "App ingest token (from Observe Snowflake app → Connections)"
  # Validate token format
  if [[ ! "$OBSERVE_INGEST_TOKEN" =~ ^ds[0-9A-Za-z]+:.+ ]]; then
    warn "Token doesn't match expected format 'ds1xxxxx:yyyyyy'"
    warn "Make sure this is an APP INGEST token, not an API token or datastream token"
    read -rp "Continue anyway? (y/N): " confirm
    [[ "$confirm" =~ ^[Yy] ]] || fatal "Aborted — get the correct token from Observe Snowflake app → Connections tab"
  fi
fi

prompt_secret OBSERVE_API_TOKEN "Observe API token (from Settings → Account → API Keys)"

OBSERVE_ENDPOINT="${OBSERVE_CUSTOMER_ID}.collect.observeinc.com"
info "Observe endpoint: $OBSERVE_ENDPOINT"

# =====================================================================
# PHASE 1: Snowflake-side SQL setup
# =====================================================================
if [[ "$VALIDATE_ONLY" == "false" && "$TERRAFORM_ONLY" == "false" ]]; then
  divider
  echo -e "${GREEN}Phase 1: Snowflake SQL Setup${NC}"
  divider

  run_sql() {
    local desc="$1" sql="$2"
    info "$desc"
    if snow sql -q "$sql" --connection "$SNOW_CONN" >/dev/null 2>&1; then
      ok "$desc"
    else
      # Retry with output for debugging
      snow sql -q "$sql" --connection "$SNOW_CONN" 2>&1 || true
      warn "$desc — may have partially succeeded (check output above)"
    fi
  }

  # Step 1: Secrets database
  run_sql "Creating secrets database" \
    "USE ROLE ACCOUNTADMIN; CREATE DATABASE IF NOT EXISTS SEND_TO_OBSERVE; CREATE SCHEMA IF NOT EXISTS SEND_TO_OBSERVE.O4S;"

  run_sql "Creating/updating observe token secret" \
    "USE ROLE ACCOUNTADMIN; CREATE OR REPLACE SECRET SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN TYPE = GENERIC_STRING SECRET_STRING = '${OBSERVE_INGEST_TOKEN}';"

  run_sql "Creating/updating observe endpoint secret" \
    "USE ROLE ACCOUNTADMIN; CREATE OR REPLACE SECRET SEND_TO_OBSERVE.O4S.OBSERVE_ENDPOINT TYPE = GENERIC_STRING SECRET_STRING = '${OBSERVE_ENDPOINT}';"

  # Step 2: Network rule + EAI
  run_sql "Creating network rule" \
    "USE ROLE ACCOUNTADMIN; CREATE NETWORK RULE IF NOT EXISTS SEND_TO_OBSERVE.O4S.OBSERVE_INGEST_NETWORK_RULE MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('${OBSERVE_ENDPOINT}:443');"

  run_sql "Creating external access integration" \
    "USE ROLE ACCOUNTADMIN; CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION OBSERVE_INGEST_ACCESS_INTEGRATION ALLOWED_NETWORK_RULES = (SEND_TO_OBSERVE.O4S.OBSERVE_INGEST_NETWORK_RULE) ALLOWED_AUTHENTICATION_SECRETS = (SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN, SEND_TO_OBSERVE.O4S.OBSERVE_ENDPOINT) ENABLED = TRUE;"

  # Step 3: Telemetry sharing
  run_sql "Enabling telemetry event sharing" \
    "USE ROLE ACCOUNTADMIN; ALTER APPLICATION OBSERVE_FOR_SNOWFLAKE SET AUTHORIZE_TELEMETRY_EVENT_SHARING = TRUE;"

  run_sql "Setting shared telemetry events" \
    "USE ROLE ACCOUNTADMIN; ALTER APPLICATION OBSERVE_FOR_SNOWFLAKE SET SHARED TELEMETRY EVENTS ('SNOWFLAKE\$ALL');"

  # Step 4: ACCOUNT_USAGE + task grants
  run_sql "Granting ACCOUNT_USAGE to O4S app" \
    "USE ROLE ACCOUNTADMIN; GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION OBSERVE_FOR_SNOWFLAKE;"

  run_sql "Granting EXECUTE TASK to O4S app" \
    "USE ROLE ACCOUNTADMIN; GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION OBSERVE_FOR_SNOWFLAKE;"

  run_sql "Granting EXECUTE MANAGED TASK to O4S app" \
    "USE ROLE ACCOUNTADMIN; GRANT EXECUTE MANAGED TASK ON ACCOUNT TO APPLICATION OBSERVE_FOR_SNOWFLAKE;"

  # Step 5: Secrets + EAI grants to app
  run_sql "Granting secrets database access" \
    "USE ROLE ACCOUNTADMIN; GRANT USAGE ON DATABASE SEND_TO_OBSERVE TO APPLICATION OBSERVE_FOR_SNOWFLAKE; GRANT USAGE ON SCHEMA SEND_TO_OBSERVE.O4S TO APPLICATION OBSERVE_FOR_SNOWFLAKE;"

  run_sql "Granting secret READ to O4S app" \
    "USE ROLE ACCOUNTADMIN; GRANT READ ON SECRET SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN TO APPLICATION OBSERVE_FOR_SNOWFLAKE; GRANT READ ON SECRET SEND_TO_OBSERVE.O4S.OBSERVE_ENDPOINT TO APPLICATION OBSERVE_FOR_SNOWFLAKE;"

  run_sql "Granting EAI to O4S app" \
    "USE ROLE ACCOUNTADMIN; GRANT USAGE ON INTEGRATION OBSERVE_INGEST_ACCESS_INTEGRATION TO APPLICATION OBSERVE_FOR_SNOWFLAKE;"

  # Step 6: Event table access
  run_sql "Granting EVENTS_ADMIN to O4S app" \
    "USE ROLE ACCOUNTADMIN; GRANT APPLICATION ROLE SNOWFLAKE.EVENTS_ADMIN TO APPLICATION OBSERVE_FOR_SNOWFLAKE;"

  # Step 7: Warehouse
  run_sql "Creating/ensuring warehouse" \
    "USE ROLE ACCOUNTADMIN; CREATE WAREHOUSE IF NOT EXISTS ${WAREHOUSE_NAME} WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;"

  run_sql "Registering warehouse with O4S" \
    "USE ROLE ACCOUNTADMIN; CALL OBSERVE_FOR_SNOWFLAKE.CONFIG.REGISTER_SINGLE_REFERENCE('warehouse', 'ADD', SYSTEM\$REFERENCE('WAREHOUSE', '${WAREHOUSE_NAME}', 'PERSISTENT', 'USAGE'));"

  # Step 8: Provision connector
  run_sql "Provisioning O4S connector" \
    "USE ROLE ACCOUNTADMIN; CALL OBSERVE_FOR_SNOWFLAKE.PUBLIC.PROVISION_CONNECTOR(PARSE_JSON('{\"observe_token\": \"SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN\", \"observe_endpoint\": \"SEND_TO_OBSERVE.O4S.OBSERVE_ENDPOINT\", \"external_access_integration\": \"OBSERVE_INGEST_ACCESS_INTEGRATION\"}'));"

  # Step 8b: EAI for SPCS role (optional)
  run_sql "Granting EAI to PREFECT_ROLE (for observe-agent sidecar)" \
    "USE ROLE ACCOUNTADMIN; GRANT USAGE ON INTEGRATION OBSERVE_INGEST_ACCESS_INTEGRATION TO ROLE PREFECT_ROLE;" || true

  divider
  echo -e "${GREEN}Phase 1 Complete!${NC}"
  echo ""
  warn "MANUAL STEP REQUIRED (cannot be automated):"
  echo "  1. Open Snowsight → Data Products → Apps → OBSERVE_FOR_SNOWFLAKE"
  echo "  2. History Views → click '+ Add View' and add:"
  echo "     QUERY_HISTORY, WAREHOUSE_METERING_HISTORY, METERING_HISTORY,"
  echo "     LOGIN_HISTORY, SNOWPARK_CONTAINER_SERVICES_HISTORY, TASK_HISTORY"
  echo "  3. Event Tables → click '+ Add' → snowflake.telemetry.events"
  echo "  4. Click 'Save Changes' then 'Start All'"
  echo "  5. Stagger schedules 1-2 min apart for best performance"
  echo ""
  read -rp "$(echo -e "${YELLOW}Press ENTER after completing the manual step (or Ctrl+C to exit)...${NC}")"
fi

# =====================================================================
# PHASE 2: Validation
# =====================================================================
divider
echo -e "${GREEN}Phase 2: Validation${NC}"
divider

# Check O4S tasks in Snowflake
info "Checking O4S task status in ACCOUNT_USAGE..."
TASK_OUTPUT=$(snow sql -q "
  SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME
  FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
  WHERE DATABASE_NAME = 'OBSERVE_FOR_SNOWFLAKE'
    AND SCHEDULED_TIME > DATEADD('hour', -2, CURRENT_TIMESTAMP())
  ORDER BY SCHEDULED_TIME DESC
  LIMIT 20;
" --connection "$SNOW_CONN" 2>&1) || true

if echo "$TASK_OUTPUT" | grep -qi "SUCCEEDED"; then
  ok "O4S tasks are running and succeeding"
  echo "$TASK_OUTPUT" | head -25
else
  warn "No SUCCEEDED tasks found in last 2 hours"
  warn "Tasks may need time to start, or the manual step (adding views) hasn't been done"
  echo "$TASK_OUTPUT" | head -15
fi

# Check Observe datasets via GraphQL API
echo ""
info "Checking Observe datasets via GraphQL API..."
DATASET_RESPONSE=$(curl -sf \
  -H "Authorization: Bearer ${OBSERVE_CUSTOMER_ID} ${OBSERVE_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ datasetSearch { dataset { id name } } }"}' \
  "https://${OBSERVE_CUSTOMER_ID}.observeinc.com/v1/meta" 2>&1) || true

if echo "$DATASET_RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
datasets = data.get('data', {}).get('datasetSearch', [])
sf = [d['dataset']['name'] for d in datasets if d['dataset']['name'].startswith('snowflake/')]
tr = [d['dataset']['name'] for d in datasets if d['dataset']['name'].startswith('Tracing/')]
print(f'Snowflake datasets: {len(sf)}')
print(f'Tracing datasets:   {len(tr)}')
if sf:
    print('Sample Snowflake:', ', '.join(sf[:5]))
if tr:
    print('Sample Tracing:  ', ', '.join(tr[:5]))
sys.exit(0 if len(sf) > 0 else 1)
" 2>/dev/null; then
  ok "Snowflake datasets found in Observe"
else
  warn "No snowflake/ datasets found yet — data may still be propagating (5-15 min after first task run)"
  warn "If this persists, check that you used an APP INGEST token (ds1xxxxx:yyyyyy), not an API token"
fi

# =====================================================================
# PHASE 3: Terraform dashboards & monitors
# =====================================================================
if [[ "$VALIDATE_ONLY" == "false" ]]; then
  divider
  echo -e "${GREEN}Phase 3: Terraform Dashboards & Monitors${NC}"
  divider

  if [[ ! -d "$TF_DIR" ]]; then
    fatal "Terraform directory not found: $TF_DIR"
  fi

  cd "$TF_DIR"

  # Write tfvars (overwrite)
  info "Writing terraform.tfvars..."
  cat > terraform.tfvars <<EOF
observe_customer_id = "${OBSERVE_CUSTOMER_ID}"
observe_api_token   = "${OBSERVE_API_TOKEN}"
EOF
  ok "terraform.tfvars written"

  # Init
  info "Running terraform init..."
  if terraform init -no-color 2>&1 | tail -3; then
    ok "Terraform initialized"
  else
    fatal "Terraform init failed"
  fi

  # Plan
  info "Running terraform plan..."
  PLAN_OUTPUT=$(terraform plan -no-color 2>&1)
  PLAN_EXIT=$?

  if [[ $PLAN_EXIT -ne 0 ]]; then
    echo "$PLAN_OUTPUT"
    echo ""
    err "Terraform plan failed"

    # Check for common errors
    if echo "$PLAN_OUTPUT" | grep -q "is not accessible"; then
      echo ""
      warn "COMMON FIX: Dataset not found. Possible causes:"
      warn "  1. O4S datasets haven't appeared yet (wait 5-15 min after tasks succeed)"
      warn "  2. Dataset name case mismatch — use lowercase 'snowflake/' prefix"
      warn "  3. OTel datasets use 'Tracing/Span', NOT 'OpenTelemetry/Span'"
      echo ""
      warn "Run with --validate-only to check which datasets exist"
    fi
    exit 1
  fi

  # Show plan summary
  ADDS=$(echo "$PLAN_OUTPUT" | grep -oP '\d+ to add' | head -1 || echo "0 to add")
  CHANGES=$(echo "$PLAN_OUTPUT" | grep -oP '\d+ to change' | head -1 || echo "0 to change")
  DESTROYS=$(echo "$PLAN_OUTPUT" | grep -oP '\d+ to destroy' | head -1 || echo "0 to destroy")
  info "Plan: $ADDS, $CHANGES, $DESTROYS"

  if echo "$PLAN_OUTPUT" | grep -q "No changes"; then
    ok "Infrastructure is up to date — no changes needed"
  else
    # Apply
    echo ""
    read -rp "$(echo -e "${YELLOW}Apply these changes? (y/N): ${NC}")" apply_confirm
    if [[ "$apply_confirm" =~ ^[Yy] ]]; then
      info "Running terraform apply..."
      if terraform apply -auto-approve -no-color 2>&1; then
        ok "Terraform apply succeeded!"
        echo ""
        terraform output -no-color 2>/dev/null || true
      else
        err "Terraform apply failed"
        echo ""
        warn "Common failures:"
        warn "  - 'field does not exist': OPAL field names are CASE-SENSITIVE"
        warn "    Snowflake: UPPERCASE (CREDITS_USED, WAREHOUSE_NAME)"
        warn "    OTel: lowercase (service_name, duration, status_code)"
        warn "  - 'interface metric requires event': Use count monitors, not threshold"
        warn "    O4S datasets are not event datasets — rule_kind must be 'count'"
        warn "  - Dashboard cards empty / blank canvas:"
        warn "    Layout must use gridLayout.sections[].items[] format (NOT widgets[])"
        warn "    Each item needs card={cardType:'stage',id,stageId} + layout={h,w,x,y,i}"
        warn "    See README 'Dataset Naming & Terraform' section for full format"
        exit 1
      fi
    else
      info "Skipped apply"
    fi
  fi
fi

# =====================================================================
# Summary
# =====================================================================
divider
echo -e "${GREEN}Setup Complete!${NC}"
divider
echo ""
echo "Observe dashboards: https://${OBSERVE_CUSTOMER_ID}.observeinc.com"
echo ""
echo "What to check:"
echo "  1. Dashboards → search 'Prefect' — should see 5 dashboards"
echo "  2. Monitors   → should see 4 monitors (SPCS credits, long queries, heartbeat, idle cost)"
echo "  3. Datasets   → search 'snowflake/' — should see QUERY_HISTORY, LOGIN_HISTORY, etc."
echo ""
echo "Troubleshooting:"
echo "  - No datasets?       → Check token type (must be APP INGEST, not API token)"
echo "  - Tasks failing?     → Check ACCOUNT_USAGE.TASK_HISTORY WHERE DATABASE_NAME='OBSERVE_FOR_SNOWFLAKE'"
echo "  - Dashboards empty?  → Layout must use sections[].items[] format, not widgets[]"
echo "                         See README 'Dataset Naming & Terraform' for correct format"
echo "  - Update token:      → ALTER SECRET SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN SET SECRET_STRING='new_token'"  # pragma: allowlist secret
echo "  - Re-validate:       → $0 --validate-only"
echo ""
