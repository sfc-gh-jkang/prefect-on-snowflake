#!/usr/bin/env bash
# =============================================================================
# rotate_secrets.sh — Rotate Snowflake secrets and check PAT expiry
#
# Usage:
#   ./scripts/rotate_secrets.sh --connection aws_spcs           # interactive rotate
#   ./scripts/rotate_secrets.sh --connection aws_spcs --check   # read-only status
#   ./scripts/rotate_secrets.sh --connection aws_spcs --dry-run # show without executing
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_DIR/.env"

CONN=""
CHECK_ONLY=false
DRY_RUN=false

usage() {
    cat <<'EOF'
Usage: rotate_secrets.sh [OPTIONS]

Rotate Snowflake secret objects and check PAT expiry.

Options:
  --connection NAME    Snow CLI connection name (required)
  --check              Read-only: report secret status and PAT expiry
  --dry-run            Show what would be executed without making changes
  -h, --help           Show this help

Secrets managed:
  PREFECT_DB_PASSWORD  — Postgres connection URL (used by PF_SERVER, PF_SERVICES, PF_WORKER)
  GIT_ACCESS_TOKEN     — Git clone token (used by PF_WORKER, GCP worker)
  SNOWFLAKE_PAT        — Programmatic access token (auto-rotation updates ALL consumers)
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --connection) CONN="$2"; shift 2 ;;
        --check)      CHECK_ONLY=true; shift ;;
        --dry-run)    DRY_RUN=true; shift ;;
        -h|--help)    usage ;;
        *)            echo "Unknown option: $1"; usage ;;
    esac
done

if [[ -z "$CONN" ]]; then
    echo "Error: --connection is required"
    exit 1
fi

PREAMBLE="USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;"

# --- Helper: run SQL ---
_sql() {
    snow sql -q "${PREAMBLE} $1" --connection "$CONN" 2>/dev/null
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

    # Decode JWT payload (second segment, base64url)
    local payload
    payload=$(echo "$pat" | cut -d. -f2)
    # Fix base64url padding
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
        echo "  SNOWFLAKE_PAT: EXPIRED on $exp_date ($((days_left * -1)) days ago)"
    elif [[ $days_left -lt 30 ]]; then
        echo "  SNOWFLAKE_PAT: EXPIRING SOON — $exp_date ($days_left days remaining)"
    else
        echo "  SNOWFLAKE_PAT: OK — expires $exp_date ($days_left days remaining)"
    fi
}

# --- Helper: check if secret exists ---
_check_secret() {
    local name="$1"
    local result
    result=$(_sql "DESCRIBE SECRET $name;" 2>&1 || echo "NOT_FOUND")
    if echo "$result" | grep -q "NOT_FOUND\|does not exist"; then
        echo "  $name: NOT FOUND"
    else
        echo "  $name: EXISTS"
    fi
}

# =============================================================================
# --check mode: read-only status report
# =============================================================================
if $CHECK_ONLY; then
    echo "=== Secret Status ==="
    _check_secret "PREFECT_DB_PASSWORD"
    _check_secret "GIT_ACCESS_TOKEN"
    echo ""
    echo "=== PAT Expiry ==="
    _check_pat_expiry
    exit 0
fi

# =============================================================================
# Interactive rotation
# =============================================================================
echo "=== Rotate Secrets ==="
echo "Connection: $CONN"
echo ""
echo "Which secret to rotate?"
echo "  1) PREFECT_DB_PASSWORD  (Postgres connection URL)"
echo "  2) GIT_ACCESS_TOKEN     (Git clone token)"
echo "  3) Check PAT expiry only"
echo "  4) SNOWFLAKE_PAT        (Full auto-rotation — all consumers)"
echo ""
read -r -p "Choice [1/2/3/4]: " choice

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

        # Escape single quotes for safe SQL interpolation (' → '')
        escaped_value="${new_value//\'/\'\'}"
        SQL="ALTER SECRET PREFECT_DB_PASSWORD SET SECRET_STRING = '$escaped_value';"

        if $DRY_RUN; then
            echo "[dry-run] Would execute: ALTER SECRET PREFECT_DB_PASSWORD SET SECRET_STRING = '***';"
            echo "[dry-run] Would restart: PF_SERVICES, PF_WORKER, PF_MIGRATE"
            echo "[dry-run] WARNING: PF_SERVER restart skipped (may regenerate public URL)"
        else
            echo "Updating PREFECT_DB_PASSWORD..."
            _sql "$SQL"
            echo "  Done."

            echo "Restarting services that use this secret..."
            _sql "ALTER SERVICE PF_SERVICES SUSPEND;" && sleep 2
            _sql "ALTER SERVICE PF_SERVICES RESUME;"
            echo "  PF_SERVICES restarted."

            _sql "ALTER SERVICE PF_WORKER SUSPEND;" && sleep 2
            _sql "ALTER SERVICE PF_WORKER RESUME;"
            echo "  PF_WORKER restarted."

            echo ""
            echo "  WARNING: PF_SERVER was NOT restarted to preserve the public endpoint URL."
            echo "  If PF_SERVER needs the new password, manually run:"
            echo "    snow sql -q \"USE ROLE PREFECT_ROLE; ... ALTER SERVICE PF_SERVER SUSPEND;\" --connection $CONN"
            echo "    snow sql -q \"USE ROLE PREFECT_ROLE; ... ALTER SERVICE PF_SERVER RESUME;\" --connection $CONN"
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

        # Escape single quotes for safe SQL interpolation (' → '')
        escaped_value="${new_value//\'/\'\'}"
        SQL="ALTER SECRET GIT_ACCESS_TOKEN SET SECRET_STRING = '$escaped_value';"

        if $DRY_RUN; then
            echo "[dry-run] Would execute: ALTER SECRET GIT_ACCESS_TOKEN SET SECRET_STRING = '***';"
            echo "[dry-run] Would restart: PF_WORKER"
            echo "[dry-run] Would update: .env GIT_ACCESS_TOKEN"
        else
            echo "Updating GIT_ACCESS_TOKEN in Snowflake..."
            _sql "$SQL"
            echo "  Done."

            echo "Restarting PF_WORKER..."
            _sql "ALTER SERVICE PF_WORKER SUSPEND;" && sleep 2
            _sql "ALTER SERVICE PF_WORKER RESUME;"
            echo "  PF_WORKER restarted."

            # Update .env if it exists
            if [[ -f "$ENV_FILE" ]]; then
                if grep -q "^GIT_ACCESS_TOKEN=" "$ENV_FILE"; then
                    if [[ "$(uname)" == "Darwin" ]]; then
                        sed -i '' "s|^GIT_ACCESS_TOKEN=.*|GIT_ACCESS_TOKEN=$new_value|" "$ENV_FILE"
                    else
                        sed -i "s|^GIT_ACCESS_TOKEN=.*|GIT_ACCESS_TOKEN=$new_value|" "$ENV_FILE"
                    fi
                    echo "  Updated .env GIT_ACCESS_TOKEN"
                fi
            fi

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

    *)
        echo "Invalid choice."
        exit 1
        ;;
esac

echo ""
echo "=== Done ==="
