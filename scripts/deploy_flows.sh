#!/usr/bin/env bash
# =============================================================================
# deploy_flows.sh — Register flow deployments with the Prefect server
#
# Reads prefect.yaml and registers all deployment metadata so workers know
# which flows to run.  Requires PREFECT_API_URL pointing to the server.
#
# Usage:
#   # Deploy to SPCS server (get endpoint from: SHOW ENDPOINTS IN SERVICE PF_SERVER)
#   PREFECT_API_URL=https://<endpoint>/api ./scripts/deploy_flows.sh
#
#   # Validate only (offline, no server connection needed)
#   ./scripts/deploy_flows.sh --validate
#
# Environment variables for git-based pull steps:
#   GIT_REPO_URL       — HTTPS URL to your git repo
#   GIT_BRANCH         — Branch to clone (default: main)
#   GIT_ACCESS_TOKEN   — Access token for private repos
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# --validate: offline check via deploy.py (no server needed)
if [[ "${1:-}" == "--validate" ]]; then
    echo "=== Validating flow registry (offline) ==="
    cd "$PROJECT_DIR"
    python flows/deploy.py --validate
    exit $?
fi

if [[ -z "${PREFECT_API_URL:-}" ]]; then
    echo "ERROR: PREFECT_API_URL is not set."
    echo "  SPCS:   export PREFECT_API_URL=https://<endpoint>/api"
    exit 1
fi

echo "=== Deploying flows to Prefect server ==="
echo "API URL: $PREFECT_API_URL"
echo ""

cd "$PROJECT_DIR"
prefect deploy --all

echo ""
echo "=== Done ==="
echo "Check the Prefect UI at: ${PREFECT_API_URL%/api}"
