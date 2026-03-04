#!/usr/bin/env bash
# =============================================================================
# update_versions.sh — Update container image versions across all config files
#
# Usage:
#   ./scripts/update_versions.sh --prefect 3.1-python3.12
#   ./scripts/update_versions.sh --redis 7.4 --postgres 16
#   ./scripts/update_versions.sh --python 3.13-slim --dry-run
#   ./scripts/update_versions.sh --prefect 3.1-python3.12 --apply --connection my_conn
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# --- Defaults (empty = no change) ---
NEW_PREFECT=""
NEW_REDIS=""
NEW_POSTGRES=""
NEW_PYTHON=""
DRY_RUN=false
APPLY=false
CONNECTION=""

# --- Usage ---
usage() {
    cat <<'EOF'
Usage: update_versions.sh [OPTIONS]

Update container image version tags across all project files.
Optionally build, push, and deploy the changes to SPCS and GCP.

Options:
  --prefect VERSION    Prefect image tag (e.g., "3.1-python3.12")
  --redis VERSION      Redis image tag (e.g., "7.4")
  --postgres VERSION   Postgres image tag, local dev only (e.g., "16")
  --python VERSION     Python slim base for SPCS worker (e.g., "3.13-slim")
  --apply              Build, push, and deploy changes to SPCS + GCP
  --connection NAME    Snow CLI connection name (required with --apply)
  --dry-run            Show what would change without modifying files
  -h, --help           Show this help

At least one version flag is required.

Examples:
  # See what a Prefect upgrade would change
  ./scripts/update_versions.sh --prefect 3.1-python3.12 --dry-run

  # Update files only (manual build/deploy later)
  ./scripts/update_versions.sh --prefect 3.1-python3.12

  # Update files AND build+push+deploy to SPCS + rebuild GCP worker
  ./scripts/update_versions.sh --prefect 3.1-python3.12 --apply --connection aws_spcs

  # Upgrade everything at once
  ./scripts/update_versions.sh --prefect 3.1-python3.12 --redis 7.4 --python 3.13-slim \
    --apply --connection aws_spcs
EOF
    exit 0
}

# --- Parse args ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --prefect)    NEW_PREFECT="$2"; shift 2 ;;
        --redis)      NEW_REDIS="$2"; shift 2 ;;
        --postgres)   NEW_POSTGRES="$2"; shift 2 ;;
        --python)     NEW_PYTHON="$2"; shift 2 ;;
        --apply)      APPLY=true; shift ;;
        --connection) CONNECTION="$2"; shift 2 ;;
        --dry-run)    DRY_RUN=true; shift ;;
        -h|--help)    usage ;;
        *)            echo "Unknown option: $1"; usage ;;
    esac
done

if [[ -z "$NEW_PREFECT" && -z "$NEW_REDIS" && -z "$NEW_POSTGRES" && -z "$NEW_PYTHON" ]]; then
    echo "Error: at least one version flag is required."
    echo ""
    usage
fi

if $APPLY && [[ -z "$CONNECTION" ]]; then
    echo "Error: --apply requires --connection <name>"
    exit 1
fi

if $APPLY && $DRY_RUN; then
    echo "Error: --apply and --dry-run are mutually exclusive"
    exit 1
fi

# --- Detect current versions from canonical files ---
CUR_PREFECT=$(sed -n 's|.*prefect:\([^ "]*\).*|\1|p' "$PROJECT_DIR/specs/pf_server.yaml" | head -1)
CUR_REDIS=$(sed -n 's|^FROM redis:\(.*\)|\1|p' "$PROJECT_DIR/images/redis/Dockerfile")
CUR_POSTGRES=$(sed -n 's|^FROM postgres:\(.*\)|\1|p' "$PROJECT_DIR/images/postgres/Dockerfile")
CUR_PYTHON=$(sed -n 's|^FROM python:\(.*\)|\1|p' "$PROJECT_DIR/images/prefect/Dockerfile")

echo "=== Current Versions ==="
echo "  Prefect:  $CUR_PREFECT"
echo "  Redis:    $CUR_REDIS"
echo "  Postgres: $CUR_POSTGRES"
echo "  Python:   $CUR_PYTHON"
echo ""

# --- Portable sed in-place (macOS vs Linux) ---
_sed_i() {
    if [[ "$(uname)" == "Darwin" ]]; then
        sed -i '' "$@"
    else
        sed -i "$@"
    fi
}

# --- Replace helper: sed across file(s) ---
# Usage: _replace OLD NEW FILE [FILE ...]
_replace() {
    local old="$1" new="$2"
    shift 2
    for f in "$@"; do
        if [[ ! -f "$f" ]]; then
            echo "  WARN: $f not found, skipping"
            continue
        fi
        if $DRY_RUN; then
            if grep -q "$old" "$f"; then
                echo "  [dry-run] ${f#"$PROJECT_DIR"/}: $old → $new"
            fi
        else
            if grep -q "$old" "$f"; then
                _sed_i "s|${old}|${new}|g" "$f"
                echo "  Updated: ${f#"$PROJECT_DIR"/}"
            fi
        fi
    done
}

# --- Shared helpers (wait_for_ready, etc.) ---
# shellcheck source=scripts/_lib.sh
source "${SCRIPT_DIR}/_lib.sh"

# Track what changed for --apply phase
PREFECT_CHANGED=false
REDIS_CHANGED=false
PYTHON_CHANGED=false

# --- Prefect version ---
if [[ -n "$NEW_PREFECT" ]]; then
    if [[ "$NEW_PREFECT" == "$CUR_PREFECT" ]]; then
        echo "Prefect: already at $CUR_PREFECT, skipping"
    else
        echo "=== Prefect: $CUR_PREFECT → $NEW_PREFECT ==="
        _replace "prefect:${CUR_PREFECT}" "prefect:${NEW_PREFECT}" \
            "$PROJECT_DIR/specs/pf_server.yaml" \
            "$PROJECT_DIR/specs/pf_services.yaml" \
            "$PROJECT_DIR/specs/pf_migrate.yaml" \
            "$PROJECT_DIR/scripts/build_and_push.sh"
        _replace "prefecthq/prefect:${CUR_PREFECT}" "prefecthq/prefect:${NEW_PREFECT}" \
            "$PROJECT_DIR/docker-compose.yaml"
        # Update all external worker Dockerfiles
        for wf in "$PROJECT_DIR"/workers/*/Dockerfile.worker; do
            [ -f "$wf" ] || continue
            _replace "prefecthq/prefect:${CUR_PREFECT}" "prefecthq/prefect:${NEW_PREFECT}" "$wf"
        done
        PREFECT_CHANGED=true
        echo ""
        echo "  WARNING: ALTER SERVICE PF_SERVER can regenerate the public endpoint URL."
        echo ""
    fi
fi

# --- Redis version ---
if [[ -n "$NEW_REDIS" ]]; then
    if [[ "$NEW_REDIS" == "$CUR_REDIS" ]]; then
        echo "Redis: already at $CUR_REDIS, skipping"
    else
        echo "=== Redis: $CUR_REDIS → $NEW_REDIS ==="
        _replace "redis:${CUR_REDIS}" "redis:${NEW_REDIS}" \
            "$PROJECT_DIR/images/redis/Dockerfile" \
            "$PROJECT_DIR/specs/pf_redis.yaml" \
            "$PROJECT_DIR/docker-compose.yaml" \
            "$PROJECT_DIR/scripts/build_and_push.sh"
        REDIS_CHANGED=true
    fi
fi

# --- Postgres version ---
if [[ -n "$NEW_POSTGRES" ]]; then
    if [[ "$NEW_POSTGRES" == "$CUR_POSTGRES" ]]; then
        echo "Postgres: already at $CUR_POSTGRES, skipping"
    else
        echo "=== Postgres: $CUR_POSTGRES → $NEW_POSTGRES ==="
        _replace "postgres:${CUR_POSTGRES}" "postgres:${NEW_POSTGRES}" \
            "$PROJECT_DIR/images/postgres/Dockerfile" \
            "$PROJECT_DIR/docker-compose.yaml"
    fi
fi

# --- Python version ---
if [[ -n "$NEW_PYTHON" ]]; then
    if [[ "$NEW_PYTHON" == "$CUR_PYTHON" ]]; then
        echo "Python: already at $CUR_PYTHON, skipping"
    else
        echo "=== Python: $CUR_PYTHON → $NEW_PYTHON ==="
        _replace "python:${CUR_PYTHON}" "python:${NEW_PYTHON}" \
            "$PROJECT_DIR/images/prefect/Dockerfile"
        PYTHON_CHANGED=true
    fi
fi

# =====================================================================
# --apply: Build, push, and deploy
# =====================================================================
if $APPLY; then
    echo ""
    echo "========================================="
    echo "  APPLY: Build, push, and deploy"
    echo "========================================="

    # --- Get SPCS registry URL ---
    echo ""
    echo "--- Getting SPCS registry URL ---"
    REGISTRY=$(snow sql -q "SHOW IMAGE REPOSITORIES LIKE 'PREFECT_REPOSITORY' IN SCHEMA PREFECT_DB.PREFECT_SCHEMA" \
        --connection "$CONNECTION" --format json 2>/dev/null | \
        python3 -c "import sys,json; print(json.load(sys.stdin)[0]['repository_url'])" 2>/dev/null || echo "")

    if [[ -z "$REGISTRY" ]]; then
        echo "ERROR: Could not get registry URL. Check your --connection."
        exit 1
    fi
    echo "  Registry: $REGISTRY"

    # --- Login to registry ---
    echo ""
    echo "--- Logging into SPCS registry ---"
    snow spcs image-registry login --connection "$CONNECTION"

    # Read the NEW versions from the files we just updated
    LIVE_PREFECT=$(sed -n 's|.*prefect:\([^ "]*\).*|\1|p' "$PROJECT_DIR/specs/pf_server.yaml" | head -1)
    LIVE_REDIS=$(sed -n 's|^FROM redis:\(.*\)|\1|p' "$PROJECT_DIR/images/redis/Dockerfile")

    # --- Build and push Prefect stock image (if changed) ---
    if $PREFECT_CHANGED; then
        echo ""
        echo "--- Building Prefect stock image: prefecthq/prefect:${LIVE_PREFECT} ---"
        docker pull "prefecthq/prefect:${LIVE_PREFECT}"
        docker tag "prefecthq/prefect:${LIVE_PREFECT}" "$REGISTRY/prefect:${LIVE_PREFECT}"
        docker push "$REGISTRY/prefect:${LIVE_PREFECT}"
        echo "  Pushed: $REGISTRY/prefect:${LIVE_PREFECT}"
    fi

    # --- Build and push Redis (if changed) ---
    if $REDIS_CHANGED; then
        echo ""
        echo "--- Building Redis image: redis:${LIVE_REDIS} ---"
        docker build --platform linux/amd64 \
            -t "$REGISTRY/redis:${LIVE_REDIS}" \
            "$PROJECT_DIR/images/redis/"
        docker push "$REGISTRY/redis:${LIVE_REDIS}"
        echo "  Pushed: $REGISTRY/redis:${LIVE_REDIS}"
    fi

    # --- Build and push SPCS worker (if prefect or python changed) ---
    if $PREFECT_CHANGED || $PYTHON_CHANGED; then
        echo ""
        echo "--- Building SPCS worker image ---"
        docker build --platform linux/amd64 \
            -t "$REGISTRY/prefect-worker:latest" \
            "$PROJECT_DIR/images/prefect/"
        docker push "$REGISTRY/prefect-worker:latest"
        echo "  Pushed: $REGISTRY/prefect-worker:latest"
    fi

    # --- Upload updated specs to stage ---
    echo ""
    echo "--- Uploading specs to @PREFECT_SPECS ---"
    SPECS_TO_UPLOAD=""
    if $PREFECT_CHANGED; then
        SPECS_TO_UPLOAD="pf_server.yaml pf_services.yaml pf_migrate.yaml pf_worker.yaml"
    fi
    if $REDIS_CHANGED; then
        SPECS_TO_UPLOAD="$SPECS_TO_UPLOAD pf_redis.yaml"
    fi
    if $PYTHON_CHANGED && ! $PREFECT_CHANGED; then
        # Worker image changed but prefect didn't — still need to restart worker
        SPECS_TO_UPLOAD="$SPECS_TO_UPLOAD pf_worker.yaml"
    fi

    for spec in $SPECS_TO_UPLOAD; do
        snow sql -q "
            USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;
            PUT file://${PROJECT_DIR}/specs/${spec} @PREFECT_SPECS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
        " --connection "$CONNECTION" >/dev/null 2>&1
        echo "  Uploaded: specs/${spec}"
    done

    # --- Run database migration (if Prefect version changed) ---
    if $PREFECT_CHANGED; then
        echo ""
        echo "--- Running database migration ---"
        echo "  Suspending PF_SERVER and PF_SERVICES for migration..."
        snow sql -q "
            USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;
            ALTER SERVICE PF_SERVER SUSPEND;
            ALTER SERVICE PF_SERVICES SUSPEND;
        " --connection "$CONNECTION" >/dev/null 2>&1

        echo "  Running migration job..."
        snow sql -q "
            USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;
            EXECUTE JOB SERVICE
              IN COMPUTE POOL PREFECT_CORE_POOL
              FROM SPECIFICATION_FILE = 'pf_migrate.yaml'
              USING ('@PREFECT_SPECS');
        " --connection "$CONNECTION" 2>&1 | tail -5
        echo "  Migration complete."
    fi

    # --- ALTER SERVICE for each changed component ---
    # (Same pattern as sql/07b_update_services.sql, but selective based on what changed)
    echo ""
    echo "--- Updating SPCS services ---"

    if $REDIS_CHANGED; then
        echo "  ALTER SERVICE PF_REDIS..."
        snow sql -q "
            USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;
            ALTER SERVICE PF_REDIS FROM SPECIFICATION_FILE = 'pf_redis.yaml' USING ('@PREFECT_SPECS');
        " --connection "$CONNECTION" >/dev/null 2>&1
        wait_for_ready "PF_REDIS" || true
    fi

    if $PREFECT_CHANGED || $PYTHON_CHANGED; then
        echo "  ALTER SERVICE PF_WORKER..."
        snow sql -q "
            USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;
            ALTER SERVICE PF_WORKER FROM SPECIFICATION_FILE = 'pf_worker.yaml' USING ('@PREFECT_SPECS');
        " --connection "$CONNECTION" >/dev/null 2>&1
        wait_for_ready "PF_WORKER" || true
    fi

    if $PREFECT_CHANGED; then
        echo "  ALTER SERVICE PF_SERVICES..."
        snow sql -q "
            USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;
            ALTER SERVICE PF_SERVICES FROM SPECIFICATION_FILE = 'pf_services.yaml' USING ('@PREFECT_SPECS');
        " --connection "$CONNECTION" >/dev/null 2>&1
        wait_for_ready "PF_SERVICES" || true

        # Record endpoint BEFORE altering PF_SERVER
        echo ""
        echo "  --- PF_SERVER (public endpoint — checking URL stability) ---"
        ENDPOINT_BEFORE=$(snow sql -q "SHOW ENDPOINTS IN SERVICE PREFECT_DB.PREFECT_SCHEMA.PF_SERVER" \
            --connection "$CONNECTION" --format json 2>/dev/null | \
            python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('ingress_url',''))" 2>/dev/null || echo "unknown")
        echo "  Endpoint BEFORE: $ENDPOINT_BEFORE"

        echo "  ALTER SERVICE PF_SERVER..."
        snow sql -q "
            USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;
            ALTER SERVICE PF_SERVER FROM SPECIFICATION_FILE = 'pf_server.yaml' USING ('@PREFECT_SPECS');
        " --connection "$CONNECTION" >/dev/null 2>&1

        wait_for_ready "PF_SERVER" || true

        ENDPOINT_AFTER=$(snow sql -q "SHOW ENDPOINTS IN SERVICE PREFECT_DB.PREFECT_SCHEMA.PF_SERVER" \
            --connection "$CONNECTION" --format json 2>/dev/null | \
            python3 -c "import sys,json; print(json.load(sys.stdin)[0].get('ingress_url',''))" 2>/dev/null || echo "unknown")
        echo "  Endpoint AFTER:  $ENDPOINT_AFTER"

        if [[ "$ENDPOINT_BEFORE" != "$ENDPOINT_AFTER" ]]; then
            echo ""
            echo "  *** ENDPOINT URL CHANGED! ***"
            echo "  Old: $ENDPOINT_BEFORE"
            echo "  New: $ENDPOINT_AFTER"
            echo "  Update SPCS_ENDPOINT in .env and restart GCP worker."
        else
            echo "  Endpoint unchanged."
        fi
    fi

    # --- Rebuild external workers (if prefect changed) ---
    if $PREFECT_CHANGED; then
        echo ""
        echo "--- Rebuilding external workers ---"
        found_worker=false
        for compose_file in "$PROJECT_DIR"/workers/*/docker-compose.*.yaml; do
            [ -f "$compose_file" ] || continue
            found_worker=true
            worker_dir=$(dirname "$compose_file")
            worker_name=$(basename "$worker_dir")
            echo "  Building $worker_name worker..."
            docker compose -f "$compose_file" --env-file "$PROJECT_DIR/.env" build --no-cache 2>&1
            docker compose -f "$compose_file" --env-file "$PROJECT_DIR/.env" up -d 2>&1
            echo "  Done: $worker_name worker rebuilt and restarted."
        done
        if ! $found_worker; then
            echo "  No external worker compose files found in workers/*/"
        fi
    fi

    echo ""
    echo "========================================="
    echo "  APPLY COMPLETE"
    echo "========================================="
    echo ""
    echo "Run tests to verify: uv run pytest"

# =====================================================================
# No --apply: just print next steps
# =====================================================================
else
    echo ""
    if $DRY_RUN; then
        echo "=== Dry run complete — no files were modified ==="
        echo "Remove --dry-run to apply changes."
    else
        if $PREFECT_CHANGED || $REDIS_CHANGED || $PYTHON_CHANGED || [[ -n "$NEW_POSTGRES" ]]; then
            echo "=== Files updated ==="
            echo ""
            echo "Next steps:"
            echo "  1. Run tests to verify consistency:"
            echo "       uv run pytest"
            echo "  2. Build, push, and deploy automatically:"
            echo "       ./scripts/update_versions.sh <same flags> --apply --connection <conn>"
            echo ""
            echo "  Or do it manually:"
            echo "    ./scripts/build_and_push.sh --connection <conn>"
            if $PREFECT_CHANGED; then
                echo "    # Run database migration"
                echo "    # ALTER SERVICE PF_SERVICES, PF_WORKER"
                echo "    # ALTER SERVICE PF_SERVER (caution: may change endpoint URL)"
            fi
            if $REDIS_CHANGED; then
                echo "    # ALTER SERVICE PF_REDIS"
            fi
            echo "    # Rebuild external workers: docker compose -f workers/<name>/docker-compose.<name>.yaml build && up -d"
        else
            echo "No changes needed — all versions already current."
        fi
    fi
fi
