#!/usr/bin/env bash
# dashboard/deploy.sh — Deploy the Streamlit observability dashboard to SiS.
#
# Creates: PAT UDF, Streamlit app via snow CLI.
# Idempotent — safe to re-run.
#
# Usage:
#   ./dashboard/deploy.sh [--connection <name>]
#
# Requires: snow CLI, PREFECT_ROLE.

set -euo pipefail

CONNECTION="${1:-aws_spcs}"
if [[ "$1" == "--connection" ]]; then
    CONNECTION="${2:-aws_spcs}"
fi

DB="PREFECT_DB"
SCHEMA="PREFECT_SCHEMA"
ROLE="PREFECT_ROLE"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Prefect SPCS Dashboard — Deploy ==="
echo "Connection: $CONNECTION"
echo ""

# --- [1/3] Create the GET_PREFECT_PAT UDF -----------------------------------
echo "[1/3] Creating GET_PREFECT_PAT UDF ..."
snow sql -q "
  USE ROLE ${ROLE}; USE DATABASE ${DB}; USE SCHEMA ${DB}.${SCHEMA};
  CREATE OR REPLACE FUNCTION GET_PREFECT_PAT()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    HANDLER = 'get_pat'
    EXTERNAL_ACCESS_INTEGRATIONS = (PREFECT_DASHBOARD_EAI)
    SECRETS = ('prefect_svc_pat' = ${DB}.${SCHEMA}.PREFECT_SVC_PAT)
    AS \$\$
import _snowflake
def get_pat():
    return _snowflake.get_generic_secret_string('prefect_svc_pat')
\$\$;
" --connection "$CONNECTION" || true

# --- [2/3] Deploy Streamlit app via snow CLI ---------------------------------
echo "[2/3] Deploying Streamlit app ..."
(cd "$SCRIPT_DIR" && snow streamlit deploy --replace --connection "$CONNECTION")
echo "  Dashboard deployed."

# --- [3/3] Show status -------------------------------------------------------
echo ""
echo "[3/3] Dashboard status:"
snow sql -q "SHOW STREAMLITS LIKE 'PREFECT_DASHBOARD_APP' IN SCHEMA ${DB}.${SCHEMA};" --connection "$CONNECTION" 2>/dev/null || true

echo ""
echo "=== Dashboard deployment complete ==="
echo ""
echo "Access the dashboard at:"
echo "  https://app.snowflake.com — navigate to Streamlit apps > PREFECT_DASHBOARD_APP"
