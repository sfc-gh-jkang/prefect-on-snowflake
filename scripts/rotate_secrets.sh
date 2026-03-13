#!/usr/bin/env bash
# =============================================================================
# rotate_secrets.sh — Rotate Snowflake secrets and check PAT expiry
#
# Usage:
#   ./scripts/rotate_secrets.sh --connection aws_spcs           # interactive rotate
#   ./scripts/rotate_secrets.sh --connection aws_spcs --check   # read-only status
#   ./scripts/rotate_secrets.sh --connection aws_spcs --dry-run # show without executing
#   ./scripts/rotate_secrets.sh --all-clouds --check            # check all clouds
#   ./scripts/rotate_secrets.sh --all-clouds --smtp             # rotate SMTP on all clouds
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_DIR/.env"

CONN=""
CHECK_ONLY=false
DRY_RUN=false
ALL_CLOUDS=false
SMTP_ONLY=false

CLOUD_CONNECTIONS=()

ALL_SECRETS=(
    PREFECT_DB_PASSWORD
    GIT_ACCESS_TOKEN
    POSTGRES_EXPORTER_DSN
    GRAFANA_DB_DSN
    GRAFANA_ADMIN_PASSWORD
    GRAFANA_SMTP_PASSWORD
    GRAFANA_SMTP_USER
    PREFECT_SVC_PAT
    SLACK_WEBHOOK_URL
)

usage() {
    cat <<'EOF'
Usage: rotate_secrets.sh [OPTIONS]

Rotate Snowflake secret objects and check PAT expiry.

Options:
  --connection NAME    Snow CLI connection name (required unless --all-clouds)
  --all-clouds         Run against all clouds configured in .env (reads *_spcs connections)
  --check              Read-only: report ALL secret status, SMTP login, and PAT expiry
  --smtp               Non-interactive: rotate GRAFANA_SMTP_PASSWORD only
  --dry-run            Show what would be executed without making changes
  -h, --help           Show this help

Secrets managed (9 total):
  PREFECT_DB_PASSWORD     — Postgres connection URL (PF_SERVER, PF_SERVICES, PF_WORKER)
  GIT_ACCESS_TOKEN        — Git clone token (PF_WORKER, GCP worker)
  POSTGRES_EXPORTER_DSN   — Postgres DSN for monitoring exporter
  GRAFANA_DB_DSN          — Grafana Postgres persistence DSN
  GRAFANA_ADMIN_PASSWORD  — Grafana admin UI password
  GRAFANA_SMTP_PASSWORD   — Gmail App Password for email alerts
  GRAFANA_SMTP_USER       — Gmail sender address
  PREFECT_SVC_PAT         — Snowflake PAT for auth-proxy sidecar
  SLACK_WEBHOOK_URL       — Slack incoming webhook for Grafana alerts
  SNOWFLAKE_PAT           — Programmatic access token (auto-rotation updates ALL consumers)

IMPORTANT: Changing your Google password revokes ALL Gmail App Passwords.
  Regenerate at: https://myaccount.google.com/apppasswords
  Then run:  ./scripts/rotate_secrets.sh --all-clouds --smtp
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --connection)  CONN="$2"; shift 2 ;;
        --all-clouds)  ALL_CLOUDS=true; shift ;;
        --check)       CHECK_ONLY=true; shift ;;
        --smtp)        SMTP_ONLY=true; shift ;;
        --dry-run)     DRY_RUN=true; shift ;;
        -h|--help)     usage ;;
        *)             echo "Unknown option: $1"; usage ;;
    esac
done

# --- Resolve cloud connections ------------------------------------------------
_resolve_clouds() {
    if [[ -f "$ENV_FILE" ]]; then
        # shellcheck source=/dev/null
        source "$ENV_FILE" 2>/dev/null || true
    fi
    local clouds=()
    for cloud in aws azure gcp; do
        local cloud_upper
        cloud_upper=$(echo "$cloud" | tr '[:lower:]' '[:upper:]')
        local endpoint_var="SPCS_ENDPOINT_${cloud_upper}"
        if [[ -n "${!endpoint_var:-}" ]]; then
            clouds+=("${cloud}_spcs")
        fi
    done
    if [[ ${#clouds[@]} -eq 0 ]]; then
        echo "Error: No cloud endpoints found in .env (SPCS_ENDPOINT_AWS, SPCS_ENDPOINT_AZURE, SPCS_ENDPOINT_GCP)"
        exit 1
    fi
    CLOUD_CONNECTIONS=("${clouds[@]}")
}

if $ALL_CLOUDS; then
    _resolve_clouds
elif [[ -z "$CONN" ]]; then
    echo "Error: --connection is required (or use --all-clouds)"
    exit 1
else
    CLOUD_CONNECTIONS=("$CONN")
fi

PREAMBLE="USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;"

# --- Helper: run SQL against a specific connection ---
_sql() {
    local conn="$1"
    shift
    snow sql -q "${PREAMBLE} $1" --connection "$conn" 2>/dev/null
}

# --- Helper: decode JWT exp claim ---
_check_pat_expiry() {
    if [[ ! -f "$ENV_FILE" ]]; then
        echo "  .env not found — skipping PAT check"
        return
    fi
    local pat
    pat=$(grep "^SNOWFLAKE_PAT=" "$ENV_FILE" 2>/dev/null | cut -d= -f2-)
    if [[ -z "$pat" ]]; then
        echo "  SNOWFLAKE_PAT not set in .env — skipping"
        return
    fi

    local payload
    payload=$(echo "$pat" | cut -d. -f2)
    local pad=$(( 4 - ${#payload} % 4 ))
    [[ $pad -lt 4 ]] && payload="${payload}$(printf '%*s' "$pad" '' | tr ' ' '=')"
    payload=$(echo "$payload" | tr '_-' '/+')

    local exp
    exp=$(echo "$payload" | base64 -d 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('exp',''))" 2>/dev/null || echo "")

    if [[ -z "$exp" ]]; then
        echo "  Could not decode PAT expiry"
        return
    fi

    local now
    now=$(date +%s)
    local days_left=$(( (exp - now) / 86400 ))
    local exp_date
    exp_date=$(date -r "$exp" "+%Y-%m-%d" 2>/dev/null || date -d "@$exp" "+%Y-%m-%d" 2>/dev/null || echo "unknown")

    if [[ $days_left -lt 0 ]]; then
        echo "  SNOWFLAKE_PAT: ❌ EXPIRED on $exp_date ($((days_left * -1)) days ago)"
    elif [[ $days_left -lt 30 ]]; then
        echo "  SNOWFLAKE_PAT: ⚠️  EXPIRING SOON — $exp_date ($days_left days remaining)"
    else
        echo "  SNOWFLAKE_PAT: ✅ OK — expires $exp_date ($days_left days remaining)"
    fi
}

# --- Helper: check if secret exists ---
_check_secret() {
    local conn="$1" name="$2"
    local result
    result=$(_sql "$conn" "DESCRIBE SECRET $name;" 2>&1 || echo "NOT_FOUND")
    if echo "$result" | grep -q "NOT_FOUND\|does not exist"; then
        echo "  $name: ❌ NOT FOUND"
    else
        echo "  $name: ✅ EXISTS"
    fi
}

# --- Helper: test SMTP login ---
_test_smtp() {
    local user="$1" password="$2"
    python3 -c "
import smtplib, sys
try:
    s = smtplib.SMTP('smtp.gmail.com', 587, timeout=10)
    s.starttls()
    s.login('$user', '$password')
    s.quit()
    print('  SMTP login: ✅ SUCCESS')
except Exception as e:
    print(f'  SMTP login: ❌ FAILED — {e}')
    sys.exit(1)
" 2>/dev/null
}

# --- Helper: update .env key ---
_update_env() {
    local key="$1" value="$2"
    if [[ ! -f "$ENV_FILE" ]]; then return; fi
    if grep -q "^${key}=" "$ENV_FILE"; then
        if [[ "$(uname)" == "Darwin" ]]; then
            sed -i '' "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
        else
            sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
        fi
        echo "  Updated .env ${key}"
    else
        echo "${key}=${value}" >> "$ENV_FILE"
        echo "  Added ${key} to .env"
    fi
}

# --- Helper: update macOS Keychain ---
_update_keychain() {
    local service="$1" account="$2" password="$3"
    if [[ "$(uname)" != "Darwin" ]]; then return; fi
    security delete-generic-password -s "$service" -a "$account" 2>/dev/null || true
    security add-generic-password -s "$service" -a "$account" -w "$password" 2>/dev/null || true
    echo "  Updated macOS Keychain (service=$service)"
}

# --- Helper: suspend/resume a service ---
_restart_service() {
    local conn="$1" service="$2"
    echo "  Restarting $service on $conn..."
    _sql "$conn" "ALTER SERVICE $service SUSPEND;" >/dev/null 2>&1
    sleep 5
    _sql "$conn" "ALTER SERVICE $service RESUME;" >/dev/null 2>&1
    echo "  $service restarted."
}

# =============================================================================
# --check mode: read-only status report for ALL secrets + SMTP test
# =============================================================================
if $CHECK_ONLY; then
    for conn in "${CLOUD_CONNECTIONS[@]}"; do
        echo ""
        echo "=== Secret Status [$conn] ==="
        for secret in "${ALL_SECRETS[@]}"; do
            _check_secret "$conn" "$secret"
        done
    done

    echo ""
    echo "=== PAT Expiry ==="
    _check_pat_expiry

    echo ""
    echo "=== SMTP Login Test ==="
    if [[ -f "$ENV_FILE" ]]; then
        # shellcheck source=/dev/null
        source "$ENV_FILE" 2>/dev/null || true
    fi
    smtp_user="${GRAFANA_SMTP_USER:-}"
    smtp_pass="${GRAFANA_SMTP_PASSWORD:-}"
    if [[ -z "$smtp_pass" ]] && [[ -n "$smtp_user" ]] && [[ "$(uname)" == "Darwin" ]]; then
        smtp_pass=$(security find-generic-password -s "gmail-smtp" -a "$smtp_user" -w 2>/dev/null || true)
    fi
    if [[ -n "$smtp_user" ]] && [[ -n "$smtp_pass" ]]; then
        _test_smtp "$smtp_user" "$smtp_pass" || true
    else
        echo "  SMTP: ⚠️  GRAFANA_SMTP_USER or GRAFANA_SMTP_PASSWORD not available — skipping"
    fi

    exit 0
fi

# =============================================================================
# --smtp mode: non-interactive SMTP password rotation
# =============================================================================
if $SMTP_ONLY; then
    echo "=== Rotate GRAFANA_SMTP_PASSWORD ==="
    echo ""

    if [[ -f "$ENV_FILE" ]]; then
        # shellcheck source=/dev/null
        source "$ENV_FILE" 2>/dev/null || true
    fi
    smtp_user="${GRAFANA_SMTP_USER:-}"

    smtp_pass=""
    if [[ -n "${GRAFANA_SMTP_PASSWORD:-}" ]]; then
        smtp_pass="$GRAFANA_SMTP_PASSWORD"
        echo "  Reading password from .env"
    elif [[ -n "$smtp_user" ]] && [[ "$(uname)" == "Darwin" ]]; then
        smtp_pass=$(security find-generic-password -s "gmail-smtp" -a "$smtp_user" -w 2>/dev/null || true)
        [[ -n "$smtp_pass" ]] && echo "  Reading password from macOS Keychain"
    fi

    if [[ -z "$smtp_pass" ]]; then
        echo "  No password found in .env or Keychain."
        echo "  Generate one at: https://myaccount.google.com/apppasswords"
        read -r -s -p "  Enter Gmail App Password (16-char, no spaces): " smtp_pass
        echo ""
    fi

    if [[ -z "$smtp_pass" ]]; then
        echo "  Empty value — aborting."
        exit 1
    fi

    smtp_pass_nospaces="${smtp_pass// /}"

    echo ""
    echo "  Testing SMTP login..."
    if ! _test_smtp "$smtp_user" "$smtp_pass_nospaces"; then
        echo ""
        echo "  ❌ SMTP login failed. The password is invalid or revoked."
        echo "  Generate a new one at: https://myaccount.google.com/apppasswords"
        exit 1
    fi

    if $DRY_RUN; then
        echo ""
        echo "[dry-run] Would update GRAFANA_SMTP_PASSWORD on: ${CLOUD_CONNECTIONS[*]}"
        echo "[dry-run] Would update .env and macOS Keychain"
        echo "[dry-run] Would restart PF_MONITOR on each cloud"
        exit 0
    fi

    for conn in "${CLOUD_CONNECTIONS[@]}"; do
        echo ""
        echo "  Updating GRAFANA_SMTP_PASSWORD on $conn..."
        _sql "$conn" "ALTER SECRET GRAFANA_SMTP_PASSWORD SET SECRET_STRING = '${smtp_pass_nospaces}';"
        _restart_service "$conn" "PF_MONITOR"
    done

    _update_env "GRAFANA_SMTP_PASSWORD" "$smtp_pass_nospaces"
    _update_keychain "gmail-smtp" "$smtp_user" "$smtp_pass_nospaces"

    echo ""
    echo "=== SMTP password rotated on ${#CLOUD_CONNECTIONS[@]} cloud(s) ==="
    echo "  Clouds: ${CLOUD_CONNECTIONS[*]}"
    echo "  .env updated, Keychain updated, PF_MONITOR restarted."
    exit 0
fi

# =============================================================================
# Interactive rotation
# =============================================================================
echo "=== Rotate Secrets ==="
echo "Connection(s): ${CLOUD_CONNECTIONS[*]}"
echo ""
echo "Which secret to rotate?"
echo "  1) PREFECT_DB_PASSWORD       (Postgres connection URL)"
echo "  2) GIT_ACCESS_TOKEN          (Git clone token)"
echo "  3) Check PAT expiry only"
echo "  4) SNOWFLAKE_PAT             (Full auto-rotation — all consumers)"
echo "  5) GRAFANA_SMTP_PASSWORD     (Gmail App Password for email alerts)"
echo "  6) GRAFANA_ADMIN_PASSWORD    (Grafana admin UI password)"
echo "  7) SLACK_WEBHOOK_URL         (Slack incoming webhook)"
echo ""
read -r -p "Choice [1-7]: " choice

case "$choice" in
    1)
        echo ""
        echo "--- Rotate PREFECT_DB_PASSWORD ---"
        echo "Enter the new full Postgres connection URL:"
        echo "  Format: postgresql+asyncpg://user:password@host:5432/prefect"
        read -r -s -p "> " new_value
        echo ""

        if [[ -z "$new_value" ]]; then
            echo "Empty value — aborting."
            exit 1
        fi

        escaped_value="${new_value//\'/\'\'}"
        SQL="ALTER SECRET PREFECT_DB_PASSWORD SET SECRET_STRING = '$escaped_value';"

        if $DRY_RUN; then
            echo "[dry-run] Would execute: ALTER SECRET PREFECT_DB_PASSWORD SET SECRET_STRING = '***';"
            echo "[dry-run] Would restart: PF_SERVICES, PF_WORKER, PF_MIGRATE"
            echo "[dry-run] WARNING: PF_SERVER restart skipped (may regenerate public URL)"
        else
            for conn in "${CLOUD_CONNECTIONS[@]}"; do
                echo "Updating PREFECT_DB_PASSWORD on $conn..."
                _sql "$conn" "$SQL"
                echo "  Done."

                echo "Restarting services that use this secret..."
                _sql "$conn" "ALTER SERVICE PF_SERVICES SUSPEND;" && sleep 2
                _sql "$conn" "ALTER SERVICE PF_SERVICES RESUME;"
                echo "  PF_SERVICES restarted."

                _sql "$conn" "ALTER SERVICE PF_WORKER SUSPEND;" && sleep 2
                _sql "$conn" "ALTER SERVICE PF_WORKER RESUME;"
                echo "  PF_WORKER restarted."
            done

            echo ""
            echo "  WARNING: PF_SERVER was NOT restarted to preserve the public endpoint URL."
            echo "  If PF_SERVER needs the new password, manually run:"
            echo "    snow sql -q \"USE ROLE PREFECT_ROLE; ... ALTER SERVICE PF_SERVER SUSPEND;\" --connection <conn>"
            echo "    snow sql -q \"USE ROLE PREFECT_ROLE; ... ALTER SERVICE PF_SERVER RESUME;\" --connection <conn>"
        fi
        ;;

    2)
        echo ""
        echo "--- Rotate GIT_ACCESS_TOKEN ---"
        read -r -s -p "Enter new git access token: " new_value
        echo ""

        if [[ -z "$new_value" ]]; then
            echo "Empty value — aborting."
            exit 1
        fi

        escaped_value="${new_value//\'/\'\'}"
        SQL="ALTER SECRET GIT_ACCESS_TOKEN SET SECRET_STRING = '$escaped_value';"

        if $DRY_RUN; then
            echo "[dry-run] Would execute: ALTER SECRET GIT_ACCESS_TOKEN SET SECRET_STRING = '***';"
            echo "[dry-run] Would restart: PF_WORKER"
            echo "[dry-run] Would update: .env GIT_ACCESS_TOKEN"
        else
            for conn in "${CLOUD_CONNECTIONS[@]}"; do
                echo "Updating GIT_ACCESS_TOKEN on $conn..."
                _sql "$conn" "$SQL"
                echo "  Done."

                echo "Restarting PF_WORKER on $conn..."
                _sql "$conn" "ALTER SERVICE PF_WORKER SUSPEND;" && sleep 2
                _sql "$conn" "ALTER SERVICE PF_WORKER RESUME;"
                echo "  PF_WORKER restarted."
            done

            _update_env "GIT_ACCESS_TOKEN" "$new_value"

            echo ""
            echo "  If the GCP worker is running, restart it to pick up the new token:"
            echo "    cd workers/gcp && docker compose --env-file ../../.env -f docker-compose.gcp.yaml up -d"
        fi
        ;;

    3)
        _check_pat_expiry
        ;;

    4)
        echo ""
        echo "--- Rotate SNOWFLAKE_PAT (Full Auto-Rotation) ---"
        echo ""
        echo "This will update ALL PAT consumers:"
        echo "  - Prefect Secret block 'snowflake-pat'"
        echo "  - .env file (SNOWFLAKE_PAT=...)"
        echo "  - Snowflake secret PREFECT_SVC_PAT"
        echo "  - GitLab CI variable SNOWFLAKE_PAT"
        echo "  - Restart: GCP, AWS, local, monitoring auth-proxies"
        echo ""
        read -r -s -p "Enter new PAT: " new_value
        echo ""

        if [[ -z "$new_value" ]]; then
            echo "Empty value — aborting."
            exit 1
        fi

        if $DRY_RUN; then
            echo "[dry-run] Would run PAT rotation flow with new PAT"
            echo "[dry-run] Consumers: Prefect block, .env, Snowflake secret, GitLab CI, 4 auth-proxies"
        else
            echo "Running PAT rotation flow..."
            cd "$PROJECT_DIR" && NEW_SNOWFLAKE_PAT="$new_value" uv run python flows/pat_rotation_flow.py
        fi
        ;;

    5)
        echo ""
        echo "--- Rotate GRAFANA_SMTP_PASSWORD (Gmail App Password) ---"
        echo ""
        echo "IMPORTANT: Changing your Google password revokes ALL app passwords."
        echo "  Generate a new one at: https://myaccount.google.com/apppasswords"
        echo ""
        read -r -s -p "Enter new Gmail App Password (16-char): " new_value
        echo ""

        if [[ -z "$new_value" ]]; then
            echo "Empty value — aborting."
            exit 1
        fi

        new_value_nospaces="${new_value// /}"

        if [[ -f "$ENV_FILE" ]]; then
        # shellcheck source=/dev/null
        source "$ENV_FILE" 2>/dev/null || true
        fi
        smtp_user="${GRAFANA_SMTP_USER:-}"

        echo "  Testing SMTP login..."
        if ! _test_smtp "$smtp_user" "$new_value_nospaces"; then
            echo "  ❌ SMTP login failed. Password invalid or revoked."
            exit 1
        fi

        if $DRY_RUN; then
            echo "[dry-run] Would update GRAFANA_SMTP_PASSWORD on: ${CLOUD_CONNECTIONS[*]}"
            echo "[dry-run] Would restart PF_MONITOR on each cloud"
            echo "[dry-run] Would update .env and macOS Keychain"
        else
            for conn in "${CLOUD_CONNECTIONS[@]}"; do
                echo "  Updating GRAFANA_SMTP_PASSWORD on $conn..."
                _sql "$conn" "ALTER SECRET GRAFANA_SMTP_PASSWORD SET SECRET_STRING = '${new_value_nospaces}';"
                _restart_service "$conn" "PF_MONITOR"
            done

            _update_env "GRAFANA_SMTP_PASSWORD" "$new_value_nospaces"
            _update_keychain "gmail-smtp" "$smtp_user" "$new_value_nospaces"

            echo ""
            echo "  ✅ SMTP password rotated on ${#CLOUD_CONNECTIONS[@]} cloud(s)."
        fi
        ;;

    6)
        echo ""
        echo "--- Rotate GRAFANA_ADMIN_PASSWORD ---"
        read -r -s -p "Enter new Grafana admin password: " new_value
        echo ""

        if [[ -z "$new_value" ]]; then
            echo "Empty value — aborting."
            exit 1
        fi

        escaped_value="${new_value//\'/\'\'}"
        SQL="ALTER SECRET GRAFANA_ADMIN_PASSWORD SET SECRET_STRING = '$escaped_value';"

        if $DRY_RUN; then
            echo "[dry-run] Would update GRAFANA_ADMIN_PASSWORD on: ${CLOUD_CONNECTIONS[*]}"
            echo "[dry-run] Would restart PF_MONITOR on each cloud"
        else
            for conn in "${CLOUD_CONNECTIONS[@]}"; do
                echo "  Updating GRAFANA_ADMIN_PASSWORD on $conn..."
                _sql "$conn" "$SQL"
                _restart_service "$conn" "PF_MONITOR"
            done
            echo "  ✅ Grafana admin password rotated."
        fi
        ;;

    7)
        echo ""
        echo "--- Rotate SLACK_WEBHOOK_URL ---"
        read -r -s -p "Enter new Slack webhook URL: " new_value
        echo ""

        if [[ -z "$new_value" ]]; then
            echo "Empty value — aborting."
            exit 1
        fi

        escaped_value="${new_value//\'/\'\'}"
        SQL="ALTER SECRET SLACK_WEBHOOK_URL SET SECRET_STRING = '$escaped_value';"

        if $DRY_RUN; then
            echo "[dry-run] Would update SLACK_WEBHOOK_URL on: ${CLOUD_CONNECTIONS[*]}"
            echo "[dry-run] Would restart PF_MONITOR on each cloud"
        else
            for conn in "${CLOUD_CONNECTIONS[@]}"; do
                echo "  Updating SLACK_WEBHOOK_URL on $conn..."
                _sql "$conn" "$SQL"
                _restart_service "$conn" "PF_MONITOR"
            done

            _update_env "SLACK_WEBHOOK_URL" "$new_value"
            echo "  ✅ Slack webhook URL rotated."
        fi
        ;;

    *)
        echo "Invalid choice."
        exit 1
        ;;
esac

echo ""
echo "=== Done ==="
