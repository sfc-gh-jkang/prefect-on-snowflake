#!/usr/bin/env bash
# =============================================================================
# setup_gcp_worker.sh — Provision a GCP VM and start a Prefect worker
#                        that connects to the SPCS-hosted server.
#
# Architecture:
#   nginx auth-proxy (injects Snowflake PAT) → SPCS public endpoint
#   prefect worker → http://auth-proxy:4200/api
#
# Prerequisites:
#   - gcloud CLI authenticated
#   - SNOWFLAKE_PAT set to a Snowflake programmatic access token
#   - SPCS_ENDPOINT set to the SPCS server hostname (no https://)
#
# Optional (enables monitoring sidecars):
#   - SPCS_MONITOR_ENDPOINT set to PF_MONITOR Prometheus endpoint hostname
#   - SPCS_MONITOR_LOKI_ENDPOINT set to PF_MONITOR Loki endpoint hostname
#
# Usage:
#   export SNOWFLAKE_PAT="ver:1:..."
#   export SPCS_ENDPOINT="xxxxx-orgname-acctname.snowflakecomputing.app"
#   export SPCS_MONITOR_ENDPOINT="xxxxx-orgname-acctname.snowflakecomputing.app"
#   export SPCS_MONITOR_LOKI_ENDPOINT="xxxxx-orgname-acctname.snowflakecomputing.app"
#   ./workers/gcp/setup_gcp_worker.sh
# =============================================================================
set -euo pipefail

PROJECT="${GCP_PROJECT:?Set GCP_PROJECT}"
ZONE="${GCP_ZONE:-us-central1-a}"
VM_NAME="${GCP_VM_NAME:-prefect-worker-gcp}"
PAT="${SNOWFLAKE_PAT:?Set SNOWFLAKE_PAT}"
ENDPOINT="${SPCS_ENDPOINT:?Set SPCS_ENDPOINT}"
GIT_TOKEN="${GIT_ACCESS_TOKEN:?Set GIT_ACCESS_TOKEN}"
GIT_URL="${GIT_REPO_URL:-https://github.com/your-org/your-repo.git}"
GIT_REF="${GIT_BRANCH:-main}"

# Monitoring endpoints (optional — monitoring sidecars start only when set)
MONITOR_PROM="${SPCS_MONITOR_ENDPOINT:-}"
MONITOR_LOKI="${SPCS_MONITOR_LOKI_ENDPOINT:-}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=== Creating GCP VM for hybrid Prefect worker ==="
echo "Project:       $PROJECT"
echo "Zone:          $ZONE"
echo "VM:            $VM_NAME"
echo "SPCS Endpoint: $ENDPOINT"
echo ""

# Create VM with Docker pre-installed (Container-Optimized OS)
gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-small \
    --image-family=cos-stable \
    --image-project=cos-cloud \
    --tags=prefect-worker \
    --metadata=startup-script='#!/bin/bash
# Wait for Docker
while ! docker info &>/dev/null; do sleep 2; done
mkdir -p /opt/prefect-gcp
'

echo "Waiting for VM to be ready..."
sleep 30

# Copy compose files to the VM
gcloud compute scp \
    "$SCRIPT_DIR/docker-compose.gcp.yaml" \
    "$SCRIPT_DIR/Dockerfile.worker" \
    "$SCRIPT_DIR/nginx.conf" \
    "$VM_NAME:/opt/prefect-gcp/" \
    --project="$PROJECT" \
    --zone="$ZONE"

# Copy monitoring configs (compose overlay + agent configs)
gcloud compute ssh "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --command="mkdir -p /opt/prefect-gcp/vm-agents"

gcloud compute scp \
    "$REPO_ROOT/monitoring/docker-compose.monitoring.yml" \
    "$VM_NAME:/opt/prefect-gcp/" \
    --project="$PROJECT" \
    --zone="$ZONE"

gcloud compute scp \
    "$REPO_ROOT/monitoring/vm-agents/prometheus-agent.yml" \
    "$REPO_ROOT/monitoring/vm-agents/promtail-config.yaml" \
    "$REPO_ROOT/monitoring/vm-agents/nginx-monitor.conf" \
    "$VM_NAME:/opt/prefect-gcp/vm-agents/" \
    --project="$PROJECT" \
    --zone="$ZONE"

# Build the compose command — include monitoring overlay if endpoints are set
COMPOSE_CMD="docker compose -f docker-compose.gcp.yaml"
COMPOSE_ENV="SNOWFLAKE_PAT='$PAT' SPCS_ENDPOINT='$ENDPOINT' GIT_ACCESS_TOKEN='$GIT_TOKEN' GIT_REPO_URL='$GIT_URL' GIT_BRANCH='$GIT_REF'"

if [ -n "$MONITOR_PROM" ] && [ -n "$MONITOR_LOKI" ]; then
    echo "Monitoring enabled — starting with monitoring sidecars"
    COMPOSE_CMD="$COMPOSE_CMD -f docker-compose.monitoring.yml"
    COMPOSE_ENV="$COMPOSE_ENV SPCS_MONITOR_ENDPOINT='$MONITOR_PROM' SPCS_MONITOR_LOKI_ENDPOINT='$MONITOR_LOKI' WORKER_LOCATION='gcp-${ZONE}' WORKER_POOL='gcp-pool' DNS_RESOLVER='8.8.8.8'"
else
    echo "Monitoring disabled — set SPCS_MONITOR_ENDPOINT and SPCS_MONITOR_LOKI_ENDPOINT to enable"
fi

# Start the services via Docker Compose
gcloud compute ssh "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --command="cd /opt/prefect-gcp && \
        export $COMPOSE_ENV && \
        docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v /opt/prefect-gcp:/workdir -w /workdir \
            docker/compose:latest $COMPOSE_CMD up -d"

echo ""
echo "=== GCP worker VM created ==="
echo "The worker will appear in the Prefect UI under work pool 'gcp-pool'."
echo ""
echo "To check logs:"
echo "  gcloud compute ssh $VM_NAME --zone=$ZONE --command='cd /opt/prefect-gcp && docker compose -f docker-compose.gcp.yaml logs -f'"
echo ""
echo "To delete:"
echo "  gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
