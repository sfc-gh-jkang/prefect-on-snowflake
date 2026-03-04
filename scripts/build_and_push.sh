#!/usr/bin/env bash
# =============================================================================
# build_and_push.sh — Build Docker images and push to SPCS registry
#
# Builds all SPCS images: core services + monitoring stack.
# SPCS requires linux/amd64 — all custom builds use buildx.
#
# Usage:
#   ./scripts/build_and_push.sh --connection my_connection
#   ./scripts/build_and_push.sh --connection my_connection --monitoring-only
#   ./scripts/build_and_push.sh --connection my_connection --core-only
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# --- Parse arguments ---
CONN_VALUE="my_connection"
BUILD_CORE=true
BUILD_MONITORING=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --connection) CONN_VALUE="${2:-my_connection}"; shift 2 ;;
        --monitoring-only) BUILD_CORE=false; shift ;;
        --core-only) BUILD_MONITORING=false; shift ;;
        -h|--help)
            echo "Usage: $0 --connection <name> [--monitoring-only|--core-only]"
            exit 0
            ;;
        *)
            if [[ "$CONN_VALUE" == "my_connection" ]]; then CONN_VALUE="$1"; fi
            shift
            ;;
    esac
done

echo "=== Getting SPCS registry URL ==="
REGISTRY=$(snow sql -q "SHOW IMAGE REPOSITORIES LIKE 'PREFECT_REPOSITORY' IN SCHEMA PREFECT_DB.PREFECT_SCHEMA" \
    --connection "$CONN_VALUE" --format json 2>/dev/null | \
    python3 -c "import sys,json; print(json.load(sys.stdin)[0]['repository_url'])" 2>/dev/null || echo "")

if [[ -z "$REGISTRY" ]]; then
    echo "ERROR: Could not get registry URL. Check your connection."
    echo "Usage: $0 --connection <snow_connection_name>"
    exit 1
fi

echo "Registry: $REGISTRY"

# Resolve git SHA for image tagging (short hash, e.g. "c8a115d")
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
echo "Git SHA:  $GIT_SHA"

echo ""
echo "=== Logging into SPCS registry ==="
snow spcs image-registry login --connection "$CONN_VALUE"

PUSH_LIST=()

# ---------------------------------------------------------------------------
# Core service images
# ---------------------------------------------------------------------------
if $BUILD_CORE; then
    echo ""
    echo "=== Building core images ==="

    # Redis
    echo "  Building redis:7..."
    docker buildx build --platform linux/amd64 -t "$REGISTRY/redis:7" --load "$PROJECT_DIR/images/redis"
    PUSH_LIST+=("redis:7")

    # Postgres — Managed by Snowflake Postgres on AWS/Azure.
    # GCP uses containerized postgres:16 — pull + retag here.
    echo "  Pulling + tagging postgres:16 (GCP only)..."
    docker pull --platform linux/amd64 postgres:16
    docker tag postgres:16 "$REGISTRY/postgres:16"
    PUSH_LIST+=("postgres:16")

    # Redis exporter sidecar (runs inside pf_redis spec)
    echo "  Pulling + tagging redis_exporter:v1.67.0..."
    docker pull --platform linux/amd64 oliver006/redis_exporter:v1.67.0
    docker tag oliver006/redis_exporter:v1.67.0 "$REGISTRY/redis_exporter:v1.67.0"
    PUSH_LIST+=("redis_exporter:v1.67.0")

    # Prefect worker (custom image with flow deps)
    echo "  Building prefect-worker:latest + prefect-worker:$GIT_SHA..."
    docker buildx build --platform linux/amd64 \
        -t "$REGISTRY/prefect-worker:latest" \
        -t "$REGISTRY/prefect-worker:$GIT_SHA" \
        --load "$PROJECT_DIR/images/prefect"
    PUSH_LIST+=("prefect-worker:latest" "prefect-worker:$GIT_SHA")

    # Prefect server/services (stock image, re-tagged)
    echo "  Pulling + tagging prefect:3-python3.12..."
    docker pull --platform linux/amd64 prefecthq/prefect:3-python3.12
    docker tag prefecthq/prefect:3-python3.12 "$REGISTRY/prefect:3-python3.12"
    PUSH_LIST+=("prefect:3-python3.12")
fi

# ---------------------------------------------------------------------------
# Monitoring stack images
# ---------------------------------------------------------------------------
if $BUILD_MONITORING; then
    echo ""
    echo "=== Building monitoring images ==="

    # --- Stock images: pull + retag ---
    echo "  Pulling + tagging prometheus:v3.4.1..."
    docker pull --platform linux/amd64 prom/prometheus:v3.4.1
    docker tag prom/prometheus:v3.4.1 "$REGISTRY/prometheus:v3.4.1"
    PUSH_LIST+=("prometheus:v3.4.1")

    echo "  Pulling + tagging grafana:12.0.1..."
    docker pull --platform linux/amd64 grafana/grafana:12.0.1
    docker tag grafana/grafana:12.0.1 "$REGISTRY/grafana:12.0.1"
    PUSH_LIST+=("grafana:12.0.1")

    echo "  Pulling + tagging loki:3.5.0..."
    docker pull --platform linux/amd64 grafana/loki:3.5.0
    docker tag grafana/loki:3.5.0 "$REGISTRY/loki:3.5.0"
    PUSH_LIST+=("loki:3.5.0")

    echo "  Pulling + tagging postgres_exporter:v0.16.0..."
    docker pull --platform linux/amd64 quay.io/prometheuscommunity/postgres-exporter:v0.16.0
    docker tag quay.io/prometheuscommunity/postgres-exporter:v0.16.0 "$REGISTRY/postgres_exporter:v0.16.0"
    PUSH_LIST+=("postgres_exporter:v0.16.0")

    # --- Custom-built images ---
    echo "  Building prefect-exporter:v3-status..."
    docker buildx build --platform linux/amd64 \
        -t "$REGISTRY/prefect-exporter:v3-status" \
        -t "$REGISTRY/prefect-exporter:v2-fix" \
        --load "$PROJECT_DIR/images/prefect-exporter"
    PUSH_LIST+=("prefect-exporter:v3-status" "prefect-exporter:v2-fix")

    echo "  Building event-log-poller:v5-fix..."
    docker buildx build --platform linux/amd64 \
        -t "$REGISTRY/event-log-poller:v5-fix" \
        --load "$PROJECT_DIR/images/spcs-log-poller"
    PUSH_LIST+=("event-log-poller:v5-fix")

    # Backup sidecars — build context is images/ (they COPY stage-backup/backup_lib.py)
    echo "  Building prom-backup:v1..."
    docker buildx build --platform linux/amd64 \
        -f "$PROJECT_DIR/images/prom-backup/Dockerfile" \
        -t "$REGISTRY/prom-backup:v1" \
        --load "$PROJECT_DIR/images"
    PUSH_LIST+=("prom-backup:v1")

    echo "  Building loki-backup:v1..."
    docker buildx build --platform linux/amd64 \
        -f "$PROJECT_DIR/images/loki-backup/Dockerfile" \
        -t "$REGISTRY/loki-backup:v1" \
        --load "$PROJECT_DIR/images"
    PUSH_LIST+=("loki-backup:v1")
fi

# ---------------------------------------------------------------------------
# Push all images
# ---------------------------------------------------------------------------
echo ""
echo "=== Pushing ${#PUSH_LIST[@]} images ==="
for IMG in "${PUSH_LIST[@]}"; do
    echo "  Pushing $IMG..."
    docker push "$REGISTRY/$IMG"
done

echo ""
echo "=== Done ==="
echo "Images pushed to: $REGISTRY"
