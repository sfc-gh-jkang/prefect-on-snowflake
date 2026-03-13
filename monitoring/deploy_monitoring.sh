#!/usr/bin/env bash
# monitoring/deploy_monitoring.sh — Deploy the Prefect SPCS monitoring stack.
#
# Creates: compute pool, stage, network rule, EAI, service.
# Uploads: Prometheus, Grafana, and Loki configs to the stage.
# Idempotent — safe to re-run.
#
# Usage:
#   ./monitoring/deploy_monitoring.sh [--connection <name>]
#
# Requires: snow CLI, PREFECT_ROLE or ACCOUNTADMIN.

set -euo pipefail

if [[ -f .env ]]; then
    set -a && source .env && set +a
fi

CONNECTION="${1:-aws_spcs}"
if [[ "$1" == "--connection" ]]; then
    CONNECTION="${2:-aws_spcs}"
fi

DB="PREFECT_DB"
SCHEMA="PREFECT_SCHEMA"
ROLE="PREFECT_ROLE"
POOL="PREFECT_MONITOR_POOL"
STAGE="MONITOR_STAGE"
SERVICE="PF_MONITOR"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Prefect SPCS Monitoring — Deploy ==="
echo "Connection: $CONNECTION"
echo ""

# --- Compute Pool (CPU_X64_S — 2 vCPU for 6 containers) ----------------------
echo "[1/7] Creating compute pool $POOL ..."
snow sql -q "
  CREATE COMPUTE POOL IF NOT EXISTS $POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_S
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 3600
    COMMENT = 'Monitoring stack: Prometheus + Grafana + Loki';
" --connection "$CONNECTION" || true

echo "[1/7] Granting USAGE on $POOL to $ROLE ..."
snow sql -q "GRANT USAGE ON COMPUTE POOL $POOL TO ROLE $ROLE;" --connection "$CONNECTION" || true
snow sql -q "GRANT MONITOR ON COMPUTE POOL $POOL TO ROLE $ROLE;" --connection "$CONNECTION" || true

# --- Network Rule + EAI (for Slack/webhook alerting + Postgres access) --------
echo "[2/7] Creating network rule for monitoring egress ..."
PG_HOST="${PREFECT_PG_HOST:-}"
if [[ -z "$PG_HOST" ]]; then
    echo "  PREFECT_PG_HOST env var not set."
    read -r -p "  Enter Postgres host (e.g., xxx.postgres.snowflake.app): " PG_HOST
fi

PG_RULE_ENTRY=""
if [[ -n "$PG_HOST" ]]; then
    PG_RULE_ENTRY=", '${PG_HOST}:5432'"
fi

SLACK_RULE_ENTRIES=""
SLACK_ENABLED="${SLACK_ENABLED:-false}"
if [[ "$SLACK_ENABLED" == "true" ]]; then
    SLACK_RULE_ENTRIES="'hooks.slack.com:443', 'api.slack.com:443', "
fi

snow sql -q "
  CREATE OR REPLACE NETWORK RULE ${DB}.${SCHEMA}.MONITOR_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
      ${SLACK_RULE_ENTRIES}'discord.com:443',
      'smtp.gmail.com:587'${PG_RULE_ENTRY}
    )
    COMMENT = 'Grafana alerting egress — Slack (opt-in), Discord, email (STARTTLS 587) + Postgres';
" --connection "$CONNECTION" || true

echo "[3/7] Creating External Access Integration for monitoring ..."
SLACK_SECRET_ENTRY=""
if [[ "$SLACK_ENABLED" == "true" ]]; then
    SLACK_SECRET_ENTRY=", ${DB}.${SCHEMA}.SLACK_WEBHOOK_URL"
fi
snow sql -q "
  CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PREFECT_MONITOR_EAI
    ALLOWED_NETWORK_RULES = (${DB}.${SCHEMA}.MONITOR_EGRESS_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (${DB}.${SCHEMA}.POSTGRES_EXPORTER_DSN, ${DB}.${SCHEMA}.GRAFANA_DB_DSN, ${DB}.${SCHEMA}.GRAFANA_SMTP_PASSWORD${SLACK_SECRET_ENTRY})
    ENABLED = TRUE
    COMMENT = 'Monitoring stack egress for alert delivery, Postgres access, and SMTP email';
" --connection "$CONNECTION" || true
snow sql -q "GRANT USAGE ON INTEGRATION PREFECT_MONITOR_EAI TO ROLE $ROLE;" --connection "$CONNECTION" || true

# --- Stage for configs --------------------------------------------------------
echo "[4/7] Creating stage $STAGE ..."
snow sql -q "
  CREATE STAGE IF NOT EXISTS ${DB}.${SCHEMA}.${STAGE}
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Monitoring config files: Prometheus, Grafana, Loki';
" --connection "$CONNECTION" || true

# --- Upload config files to stage ---------------------------------------------
echo "[5/7] Uploading monitoring configs to stage ..."

# Prometheus
snow stage copy "$PROJECT_DIR/monitoring/prometheus/prometheus.yml" \
  "@${DB}.${SCHEMA}.${STAGE}/prometheus/" \
  --connection "$CONNECTION" --overwrite

snow stage copy "$PROJECT_DIR/monitoring/prometheus/rules/alerts.yml" \
  "@${DB}.${SCHEMA}.${STAGE}/prometheus/rules/" \
  --connection "$CONNECTION" --overwrite

snow stage copy "$PROJECT_DIR/monitoring/prometheus/rules/recording.yml" \
  "@${DB}.${SCHEMA}.${STAGE}/prometheus/rules/" \
  --connection "$CONNECTION" --overwrite

# Grafana provisioning — datasources + dashboards
# IMPORTANT: Only upload ds.yaml (SPCS). ds-local.yaml is for docker-compose only.
# Both files set isDefault=true on Prometheus, causing Grafana crash if both uploaded.
snow stage copy "$PROJECT_DIR/monitoring/grafana/provisioning/datasources/ds.yaml" \
  "@${DB}.${SCHEMA}.${STAGE}/grafana/provisioning/datasources/" \
  --connection "$CONNECTION" --overwrite

snow sql -q "REMOVE @${DB}.${SCHEMA}.${STAGE}/grafana/provisioning/datasources/ds-local.yaml;" \
  --connection "$CONNECTION" 2>/dev/null || true

snow stage copy "$PROJECT_DIR/monitoring/grafana/provisioning/dashboards/dash.yaml" \
  "@${DB}.${SCHEMA}.${STAGE}/grafana/provisioning/dashboards/" \
  --connection "$CONNECTION" --overwrite

# Grafana provisioning — alerting (contact points, policies, rules)
for f in "$PROJECT_DIR"/monitoring/grafana/provisioning/alerting/*.yaml; do
  [ -f "$f" ] && snow stage copy "$f" \
    "@${DB}.${SCHEMA}.${STAGE}/grafana/provisioning/alerting/" \
    --connection "$CONNECTION" --overwrite
done

# Grafana dashboards
for f in "$PROJECT_DIR"/monitoring/grafana/dashboards/*.json; do
  [ -f "$f" ] && snow stage copy "$f" \
    "@${DB}.${SCHEMA}.${STAGE}/grafana/dashboards/" \
    --connection "$CONNECTION" --overwrite
done

# Loki
snow stage copy "$PROJECT_DIR/monitoring/loki/loki-config.yaml" \
  "@${DB}.${SCHEMA}.${STAGE}/loki/" \
  --connection "$CONNECTION" --overwrite

# Loki alerting rules
snow stage copy "$PROJECT_DIR/monitoring/loki/rules/fake/alerts.yaml" \
  "@${DB}.${SCHEMA}.${STAGE}/loki/rules/fake/" \
  --connection "$CONNECTION" --overwrite

# SPCS spec file — substitute SMTP config from env, then upload.
# The repo spec uses CHANGE_ME@example.com placeholders; we replace them
# with real values from .env so secrets never leak into version control.
SMTP_USER="${GRAFANA_SMTP_USER:-CHANGE_ME@example.com}"
SMTP_RECIPIENTS="${GRAFANA_SMTP_RECIPIENTS:-${GRAFANA_SMTP_USER:-CHANGE_ME@example.com}}"
SPEC_SRC="$PROJECT_DIR/monitoring/specs/pf_monitor.yaml"
SPEC_TMP="$(mktemp)"
sed \
  -e "s|GF_SMTP_USER: \"CHANGE_ME@example.com\"|GF_SMTP_USER: \"${SMTP_USER}\"|" \
  -e "s|GF_SMTP_FROM_ADDRESS: \"CHANGE_ME@example.com\"|GF_SMTP_FROM_ADDRESS: \"${SMTP_USER}\"|" \
  -e "s|GF_SMTP_ALERT_RECIPIENTS: \"CHANGE_ME@example.com\"|GF_SMTP_ALERT_RECIPIENTS: \"${SMTP_RECIPIENTS}\"|" \
  "$SPEC_SRC" > "$SPEC_TMP"
snow stage copy "$SPEC_TMP" \
  "@${DB}.${SCHEMA}.${STAGE}/specs/pf_monitor.yaml" \
  --connection "$CONNECTION" --overwrite
rm -f "$SPEC_TMP"

echo "  Uploaded all configs + spec."

# --- Helper: create a GENERIC_STRING secret + GRANT READ --------------------
_create_secret() {
    local secret_name="$1" secret_value="$2" comment="$3"
    local escaped="${secret_value//\'/\'\'}"
    snow sql -q "
      CREATE SECRET IF NOT EXISTS ${DB}.${SCHEMA}.${secret_name}
        TYPE = GENERIC_STRING
        SECRET_STRING = '${escaped}'
        COMMENT = '${comment}';
    " --connection "$CONNECTION" || true
    snow sql -q "GRANT READ ON SECRET ${DB}.${SCHEMA}.${secret_name} TO ROLE $ROLE;" --connection "$CONNECTION" || true
}

# --- Grafana admin secret -----------------------------------------------------
echo "[5b/7] Creating Grafana admin secret ..."
GRAFANA_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-}"
if [[ -z "$GRAFANA_PASSWORD" ]]; then
    echo "  GRAFANA_ADMIN_PASSWORD env var not set."
    read -r -s -p "  Enter Grafana admin password: " GRAFANA_PASSWORD
    echo ""
fi
if [[ -z "$GRAFANA_PASSWORD" ]]; then
    echo "  ERROR: Grafana admin password cannot be empty."
    exit 1
fi
_create_secret GRAFANA_ADMIN_PASSWORD "$GRAFANA_PASSWORD" "Grafana admin password for PF_MONITOR"

# --- Grafana Postgres DSN secret -----------------------------------------------
echo "[5c/7] Creating Grafana database DSN secret ..."
GRAFANA_DB_DSN="${GRAFANA_DB_DSN:-}"
if [[ -z "$GRAFANA_DB_DSN" ]]; then
    echo "  GRAFANA_DB_DSN env var not set."
    read -r -s -p "  Enter Grafana Postgres DSN (postgres://user:pass@host:5432/db?sslmode=require&search_path=grafana): " GRAFANA_DB_DSN  # pragma: allowlist secret
    echo ""
fi
if [[ -z "$GRAFANA_DB_DSN" ]]; then
    echo "  WARNING: Grafana DB DSN not set — Grafana will use SQLite instead."
else
    _create_secret GRAFANA_DB_DSN "$GRAFANA_DB_DSN" "Grafana database DSN for Postgres persistence"
fi

# --- Grafana SMTP password secret ------------------------------------------------
echo "[5d/7] Creating Grafana SMTP password secret ..."
SMTP_PASSWORD="${GRAFANA_SMTP_PASSWORD:-}"
if [[ -z "$SMTP_PASSWORD" ]]; then
    # Try macOS Keychain (matches launchctl email setup pattern)
    SMTP_ACCOUNT="${GRAFANA_SMTP_USER:-}"
    if [[ -n "$SMTP_ACCOUNT" ]]; then
        SMTP_PASSWORD=$(security find-generic-password -s "gmail-smtp" -a "$SMTP_ACCOUNT" -w 2>/dev/null || true)
    fi
fi
if [[ -z "$SMTP_PASSWORD" ]]; then
    echo "  GRAFANA_SMTP_PASSWORD env var not set and not found in Keychain."
    read -r -s -p "  Enter Gmail App Password (16-char): " SMTP_PASSWORD
    echo ""
fi
if [[ -z "$SMTP_PASSWORD" ]]; then
    echo "  WARNING: SMTP password not set — Grafana email alerts will not work."
else
    _create_secret GRAFANA_SMTP_PASSWORD "$SMTP_PASSWORD" "Gmail App Password for Grafana SMTP alerting"

    echo "  Validating SMTP login..."
    if python3 -c "
import smtplib, sys
try:
    s = smtplib.SMTP('smtp.gmail.com', 587, timeout=10)
    s.starttls()
    s.login('${SMTP_USER}', '${SMTP_PASSWORD// /}')
    s.quit()
except Exception as e:
    print(f'  SMTP validation FAILED: {e}', file=sys.stderr)
    sys.exit(1)
" 2>&1; then
        echo "  SMTP login validated successfully."
    else
        echo ""
        echo "  ╔══════════════════════════════════════════════════════════════════╗"
        echo "  ║  WARNING: SMTP login failed — email alerts will NOT work.       ║"
        echo "  ║                                                                  ║"
        echo "  ║  Common cause: Google password was changed, which revokes ALL    ║"
        echo "  ║  app passwords. Generate a new one at:                           ║"
        echo "  ║    https://myaccount.google.com/apppasswords                     ║"
        echo "  ║                                                                  ║"
        echo "  ║  Then run:                                                       ║"
        echo "  ║    ./scripts/rotate_secrets.sh --all-clouds --smtp               ║"
        echo "  ╚══════════════════════════════════════════════════════════════════╝"
        echo ""
    fi
fi

# --- Slack webhook URL secret (opt-in — requires SLACK_ENABLED=true) ----------
if [[ "$SLACK_ENABLED" == "true" ]]; then
    echo "[5e/7] Creating Slack webhook URL secret ..."
    SLACK_URL="${SLACK_WEBHOOK_URL:-}"
    if [[ -z "$SLACK_URL" ]]; then
        echo "  SLACK_WEBHOOK_URL env var not set."
        read -r -s -p "  Enter Slack webhook URL (or press Enter to skip): " SLACK_URL
        echo ""
    fi
    if [[ -z "$SLACK_URL" ]]; then
        echo "  WARNING: Slack webhook URL not set — Grafana Slack alerts will not work."
    else
        _create_secret SLACK_WEBHOOK_URL "$SLACK_URL" "Slack incoming webhook URL for Grafana alerting"
    fi
else
    echo "[5e/7] Skipping Slack webhook secret (SLACK_ENABLED=false)."
fi

# --- Create service -----------------------------------------------------------
echo "[6/7] Creating service $SERVICE ..."

# Check if service exists
EXISTS=$(snow sql -q "SHOW SERVICES LIKE '$SERVICE' IN SCHEMA ${DB}.${SCHEMA};" --connection "$CONNECTION" 2>/dev/null | grep -c "$SERVICE" || true)

if [ "$EXISTS" -gt 0 ]; then
    echo "  Service $SERVICE already exists. Use ALTER to update spec."
    echo "  To force recreate: snow sql -q 'DROP SERVICE ${DB}.${SCHEMA}.${SERVICE}' --connection $CONNECTION"
else
    snow sql -q "
      CREATE SERVICE ${DB}.${SCHEMA}.${SERVICE}
        IN COMPUTE POOL ${POOL}
        FROM @${DB}.${SCHEMA}.${STAGE}/specs
        SPECIFICATION_FILE = 'pf_monitor.yaml'
        EXTERNAL_ACCESS_INTEGRATIONS = (PREFECT_MONITOR_EAI)
        MIN_INSTANCES = 1
        MAX_INSTANCES = 1
        COMMENT = 'Monitoring stack: Prometheus + Grafana + Loki';
    " --connection "$CONNECTION"
    echo "  Service $SERVICE created."
fi

# --- Show status --------------------------------------------------------------
echo ""
echo "[7/7] Service status:"
snow sql -q "SELECT SYSTEM\$GET_SERVICE_STATUS('${DB}.${SCHEMA}.${SERVICE}');" --connection "$CONNECTION" 2>/dev/null || true

echo ""
echo "=== Monitoring deployment complete ==="
echo ""
echo "Endpoints (will be available once containers are READY):"
echo "  Grafana:    https://<endpoint>/grafana/"
echo "  Prometheus: https://<endpoint>/ (remote_write receiver)"
echo "  Loki:       https://<endpoint>/ (log push endpoint)"
echo ""
echo "To get the actual URLs:"
echo "  snow sql -q \"SHOW ENDPOINTS IN SERVICE ${DB}.${SCHEMA}.${SERVICE}\" --connection $CONNECTION"
echo ""

# --- Restart VM monitoring agents (if VMs are reachable) ----------------------
echo "=== Restarting VM monitoring agents ==="
echo ""
echo "VM workers use docker-compose.monitoring.yml overlay for monitoring sidecars."
echo "If you have VMs running, restart their monitoring stack to pick up any config changes."
echo ""

GCP_VM="${GCP_VM_NAME:?Set GCP_VM_NAME in .env}"
GCP_ZONE="${GCP_ZONE:?Set GCP_ZONE in .env}"

if command -v gcloud &>/dev/null; then
    echo "Attempting to restart GCP VM monitoring agents ($GCP_VM)..."
    if gcloud compute ssh "$GCP_VM" --zone="$GCP_ZONE" \
        --command="cd /opt/prefect-gcp && \
            set -a && source .env && set +a && \
            docker compose -f docker-compose.gcp.yaml -f docker-compose.monitoring.yml up -d \
                node-exporter cadvisor auth-proxy-monitor prometheus-agent promtail" 2>&1; then
        echo "  GCP VM monitoring agents restarted."
    else
        echo "  Could not reach GCP VM — restart manually if needed."
    fi
else
    echo "  gcloud CLI not found — skip GCP VM monitoring restart."
fi

echo ""
echo "To manually restart VM monitoring agents:"
echo "  gcloud compute ssh $GCP_VM --zone=$GCP_ZONE --command='cd /opt/prefect-gcp && set -a && source .env && set +a && docker compose -f docker-compose.gcp.yaml -f docker-compose.monitoring.yml up -d node-exporter cadvisor auth-proxy-monitor prometheus-agent promtail'"
