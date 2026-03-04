#!/usr/bin/env bash
# =============================================================================
# sync_flows.sh — Upload flow code to @PREFECT_FLOWS Snowflake stage
#
# Recursively uploads all .py files from flows/ to the internal stage,
# preserving directory structure. Uses PUT via snow sql to avoid the
# wrong-database context issue with `snow stage copy`.
#
# Stage volume v2 (GA) means the SPCS worker reads latest data from cloud
# storage on every access — no worker restart needed after upload.
#
# Usage:
#   ./scripts/sync_flows.sh                           # uses default connection
#   ./scripts/sync_flows.sh --connection my_connection
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
FLOWS_DIR="$PROJECT_DIR/flows"

CONN="${1:-my_connection}"
[[ "$CONN" == "--connection" ]] && CONN="${2:-my_connection}"

STAGE="@PREFECT_FLOWS"
PREAMBLE="USE ROLE PREFECT_ROLE; USE DATABASE PREFECT_DB; USE SCHEMA PREFECT_SCHEMA;"

echo "=== Syncing flows to $STAGE ==="
echo "Source:     $FLOWS_DIR"
echo "Stage:      PREFECT_DB.PREFECT_SCHEMA.PREFECT_FLOWS"
echo "Connection: $CONN"
echo ""

# Clean stale files before uploading (removes __pycache__, .pyc, duplicate paths)
echo "Cleaning stale files from stage..."
snow sql -q "${PREAMBLE} REMOVE ${STAGE}/ PATTERN='.*\\.pyc';" \
    --connection "$CONN" > /dev/null 2>&1 || true
snow sql -q "${PREAMBLE} REMOVE ${STAGE}/ PATTERN='.*__pycache__.*';" \
    --connection "$CONN" > /dev/null 2>&1 || true
# Remove the duplicate nested prefect_flows/ path that accumulated from old uploads
snow sql -q "${PREAMBLE} REMOVE ${STAGE}/prefect_flows/;" \
    --connection "$CONN" > /dev/null 2>&1 || true
echo "  Cleanup complete"
echo ""

COUNT=0

# Find all .py files under flows/, compute stage subpath, and PUT each one
while IFS= read -r -d '' file; do
    # Get relative path from flows dir (e.g., "analytics/reports/quarterly_flow.py")
    relpath="${file#"$FLOWS_DIR/"}"
    # Get the subdirectory part (e.g., "analytics/reports" or "" for root files)
    subdir="$(dirname "$relpath")"

    if [[ "$subdir" == "." ]]; then
        stage_path="$STAGE"
    else
        stage_path="$STAGE/$subdir"
    fi

    snow sql -q "${PREAMBLE} PUT file://${file} ${stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;" \
        --connection "$CONN" > /dev/null

    echo "  PUT $relpath -> $stage_path"
    COUNT=$((COUNT + 1))
done < <(find "$FLOWS_DIR" -name "*.py" -type f \
    -not -path "*/__pycache__/*" \
    -not -path "*/tests/*" \
    -print0 | sort -z)

echo ""
echo "=== Uploaded $COUNT files ==="
echo ""

# Upload pools.yaml alongside flows (deploy.py needs it when running from stage mount)
POOLS_FILE="$PROJECT_DIR/pools.yaml"
if [[ -f "$POOLS_FILE" ]]; then
    snow sql -q "${PREAMBLE} PUT file://${POOLS_FILE} ${STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;" \
        --connection "$CONN" > /dev/null
    echo "  PUT pools.yaml -> $STAGE"
    COUNT=$((COUNT + 1))
fi

echo ""
echo "=== Total: $COUNT files uploaded ==="
echo ""

# List stage contents to verify
snow sql -q "${PREAMBLE} LIST $STAGE;" --connection "$CONN" 2>/dev/null \
    | head -30 || true

echo ""
echo "Done. SPCS worker will see changes immediately (stage volume v2)."
