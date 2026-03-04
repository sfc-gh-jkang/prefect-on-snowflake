#!/usr/bin/env bash
# monitoring/teardown_monitoring.sh — Tear down the Prefect SPCS monitoring stack.
#
# Drops: service, EAI, network rule, stage, compute pool.
# Does NOT drop block storage snapshots (retained by Snowflake default policy).
#
# Usage:
#   ./monitoring/teardown_monitoring.sh [--connection <name>]

set -euo pipefail

CONNECTION="${1:-aws_spcs}"
if [[ "$1" == "--connection" ]]; then
    CONNECTION="${2:-aws_spcs}"
fi

DB="PREFECT_DB"
SCHEMA="PREFECT_SCHEMA"

echo "=== Prefect SPCS Monitoring — Teardown ==="
echo "Connection: $CONNECTION"
echo ""

echo "[1/5] Dropping service PF_MONITOR ..."
snow sql -q "DROP SERVICE IF EXISTS ${DB}.${SCHEMA}.PF_MONITOR;" --connection "$CONNECTION" || true

echo "[2/5] Dropping EAI ..."
snow sql -q "DROP INTEGRATION IF EXISTS PREFECT_MONITOR_EAI;" --connection "$CONNECTION" || true

echo "[3/5] Dropping network rule ..."
snow sql -q "DROP NETWORK RULE IF EXISTS ${DB}.${SCHEMA}.MONITOR_EGRESS_RULE;" --connection "$CONNECTION" || true

echo "[4/5] Dropping stage ..."
snow sql -q "DROP STAGE IF EXISTS ${DB}.${SCHEMA}.MONITOR_STAGE;" --connection "$CONNECTION" || true

echo "[5/5] Dropping compute pool ..."
snow sql -q "DROP COMPUTE POOL IF EXISTS PREFECT_MONITOR_POOL;" --connection "$CONNECTION" || true

echo ""
echo "=== Monitoring teardown complete ==="
