#!/usr/bin/env bash
# PITR Backup-Restore Verification Script
#
# This script performs end-to-end verification of DynamoDB PITR backup-restore:
# 1. Creates a table with GSI/LSI and populates it with test data
# 2. Triggers a FULL export to S3
# 3. Performs UPDATE and DELETE operations
# 4. Triggers an INCREMENTAL export to S3
# 5. Creates a target table and restores data using ddb-pitr
# 6. Validates that source and target tables match exactly
#
# Prerequisites:
# - AWS CLI configured with appropriate permissions
# - PITR_TEST_BUCKET environment variable set
# - jq installed for JSON processing
# - ddb-pitr and ddb-datagen binaries built in ./bin/

set -euo pipefail

# Disable AWS CLI pager
export AWS_PAGER=""

# Configuration
TIMESTAMP=$(date +%s)
SOURCE_TABLE="pitr-verify-source-${TIMESTAMP}"
TARGET_TABLE="pitr-verify-target-${TIMESTAMP}"
S3_BUCKET="${PITR_TEST_BUCKET:?Error: PITR_TEST_BUCKET environment variable must be set}"
REGION="${AWS_REGION:-us-east-1}"
S3_PREFIX="verify/${SOURCE_TABLE}"

# Test parameters
ITEM_COUNT="${ITEM_COUNT:-100}"
UPDATE_COUNT="${UPDATE_COUNT:-30}"
DELETE_COUNT="${DELETE_COUNT:-20}"
SEED="${SEED:-42}"

# Script directory for finding binaries
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="${PROJECT_DIR}/bin"

# Verify binaries exist
for bin in ddb-datagen ddb-pitr; do
    if [[ ! -x "${BIN_DIR}/${bin}" ]]; then
        echo "Error: ${BIN_DIR}/${bin} not found. Run 'go build -o bin/${bin} ./cmd/${bin}'"
        exit 1
    fi
done

# Verify jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required but not installed"
    exit 1
fi

echo "=== PITR Verification Script ==="
echo "Source table: ${SOURCE_TABLE}"
echo "Target table: ${TARGET_TABLE}"
echo "S3 bucket: ${S3_BUCKET}"
echo "S3 prefix: ${S3_PREFIX}"
echo "Region: ${REGION}"
echo "Items: ${ITEM_COUNT}, Updates: ${UPDATE_COUNT}, Deletes: ${DELETE_COUNT}"
echo ""

# Cleanup function - runs on exit
cleanup() {
    local exit_code=$?
    echo ""
    echo "=== Cleanup ==="

    echo "Deleting source table ${SOURCE_TABLE}..."
    aws dynamodb delete-table --table-name "${SOURCE_TABLE}" --region "${REGION}" > /dev/null 2>&1 || true

    echo "Deleting target table ${TARGET_TABLE}..."
    aws dynamodb delete-table --table-name "${TARGET_TABLE}" --region "${REGION}" > /dev/null 2>&1 || true

    echo "Deleting S3 exports..."
    aws s3 rm "s3://${S3_BUCKET}/${S3_PREFIX}" --recursive --region "${REGION}" > /dev/null 2>&1 || true

    if [[ $exit_code -eq 0 ]]; then
        echo "Cleanup complete. Verification PASSED."
    else
        echo "Cleanup complete. Verification FAILED with exit code ${exit_code}."
    fi

    exit $exit_code
}
trap cleanup EXIT

# Get AWS account ID for table ARN
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "${REGION}")
TABLE_ARN="arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${SOURCE_TABLE}"

# Phase 1: Create and populate source table
echo "=== Phase 1: Create source table and populate with data ==="
echo "Creating table with GSI/LSI and populating ${ITEM_COUNT} items..."

"${BIN_DIR}/ddb-datagen" \
    -items "${ITEM_COUNT}" \
    -gsi \
    -lsi \
    -mode put \
    -seed "${SEED}" 2>&1 | tee /tmp/datagen_output.txt

# Extract the actual table name created by datagen
SOURCE_TABLE=$(grep "^Table:" /tmp/datagen_output.txt | awk '{print $2}')
if [[ -z "${SOURCE_TABLE}" ]]; then
    echo "Error: Could not determine table name from datagen output"
    exit 1
fi

TABLE_ARN="arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${SOURCE_TABLE}"
TARGET_TABLE="${SOURCE_TABLE}-target"

echo "Source table: ${SOURCE_TABLE}"
echo "Target table will be: ${TARGET_TABLE}"

# Phase 2: Wait for PITR warm-up
echo ""
echo "=== Phase 2: Waiting for PITR warm-up (5 minutes) ==="
echo "PITR needs time after enabling before exports can be triggered..."
for i in {1..30}; do
    printf "\rWaiting... %d/300 seconds" $((i * 10))
    sleep 10
done
echo ""

# Phase 3: Trigger FULL export
echo ""
echo "=== Phase 3: Trigger FULL export ==="
FULL_EXPORT_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "Export time: ${FULL_EXPORT_TIME}"

FULL_EXPORT_ARN=$(aws dynamodb export-table-to-point-in-time \
    --table-arn "${TABLE_ARN}" \
    --s3-bucket "${S3_BUCKET}" \
    --s3-prefix "${S3_PREFIX}/full" \
    --export-format DYNAMODB_JSON \
    --region "${REGION}" \
    --query 'ExportDescription.ExportArn' \
    --output text)

echo "Full export ARN: ${FULL_EXPORT_ARN}"
echo "Waiting for FULL export to complete..."

while true; do
    STATUS=$(aws dynamodb describe-export \
        --export-arn "${FULL_EXPORT_ARN}" \
        --region "${REGION}" \
        --query 'ExportDescription.ExportStatus' \
        --output text)

    echo "Export status: ${STATUS}"

    if [[ "${STATUS}" == "COMPLETED" ]]; then
        break
    elif [[ "${STATUS}" == "FAILED" ]]; then
        echo "Error: FULL export failed"
        aws dynamodb describe-export --export-arn "${FULL_EXPORT_ARN}" --region "${REGION}"
        exit 1
    fi

    sleep 30
done

echo "FULL export completed"

# Phase 4: Perform UPDATE and DELETE operations
echo ""
echo "=== Phase 4: Perform lifecycle operations (UPDATE/DELETE) ==="
"${BIN_DIR}/ddb-datagen" \
    -table "${SOURCE_TABLE}" \
    -items "${ITEM_COUNT}" \
    -mode lifecycle \
    -update-count "${UPDATE_COUNT}" \
    -delete-count "${DELETE_COUNT}" \
    -seed "${SEED}"

# Phase 5: Trigger INCREMENTAL export
echo ""
echo "=== Phase 5: Trigger INCREMENTAL export ==="

# DynamoDB requires at least 15 minutes between export from and to times
# Calculate how long since FULL export and wait if needed
FULL_EXPORT_EPOCH=$(date -d "${FULL_EXPORT_TIME}" +%s 2>/dev/null || date -j -f "%Y-%m-%dT%H:%M:%SZ" "${FULL_EXPORT_TIME}" +%s)
CURRENT_EPOCH=$(date +%s)
ELAPSED=$((CURRENT_EPOCH - FULL_EXPORT_EPOCH))
MIN_WAIT=$((15 * 60))  # 15 minutes in seconds

if [[ ${ELAPSED} -lt ${MIN_WAIT} ]]; then
    WAIT_MORE=$((MIN_WAIT - ELAPSED + 60))  # Add 60s buffer
    echo "DynamoDB requires 15+ minutes between export times."
    echo "Elapsed: ${ELAPSED}s, waiting additional ${WAIT_MORE}s..."
    for ((i=0; i<WAIT_MORE; i+=10)); do
        printf "\rWaiting... %d/%d seconds" $i ${WAIT_MORE}
        sleep 10
    done
    echo ""
fi

INC_EXPORT_TO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "Incremental export from ${FULL_EXPORT_TIME} to ${INC_EXPORT_TO}"

INC_EXPORT_ARN=$(aws dynamodb export-table-to-point-in-time \
    --table-arn "${TABLE_ARN}" \
    --s3-bucket "${S3_BUCKET}" \
    --s3-prefix "${S3_PREFIX}/incremental" \
    --export-format DYNAMODB_JSON \
    --export-type INCREMENTAL_EXPORT \
    --incremental-export-specification "ExportFromTime=${FULL_EXPORT_TIME},ExportToTime=${INC_EXPORT_TO},ExportViewType=NEW_AND_OLD_IMAGES" \
    --region "${REGION}" \
    --query 'ExportDescription.ExportArn' \
    --output text)

echo "Incremental export ARN: ${INC_EXPORT_ARN}"
echo "Waiting for INCREMENTAL export to complete..."

while true; do
    STATUS=$(aws dynamodb describe-export \
        --export-arn "${INC_EXPORT_ARN}" \
        --region "${REGION}" \
        --query 'ExportDescription.ExportStatus' \
        --output text)

    echo "Export status: ${STATUS}"

    if [[ "${STATUS}" == "COMPLETED" ]]; then
        break
    elif [[ "${STATUS}" == "FAILED" ]]; then
        echo "Error: INCREMENTAL export failed"
        aws dynamodb describe-export --export-arn "${INC_EXPORT_ARN}" --region "${REGION}"
        exit 1
    fi

    sleep 30
done

echo "INCREMENTAL export completed"

# Phase 6: Create target table with same schema
echo ""
echo "=== Phase 6: Create target table ==="

# Get source table description and extract schema
TABLE_DESC=$(aws dynamodb describe-table \
    --table-name "${SOURCE_TABLE}" \
    --region "${REGION}" \
    --output json \
    --query 'Table.{AttributeDefinitions:AttributeDefinitions,KeySchema:KeySchema,LocalSecondaryIndexes:LocalSecondaryIndexes,GlobalSecondaryIndexes:GlobalSecondaryIndexes}')

# Create target table with same schema
echo "Creating target table ${TARGET_TABLE}..."
CREATE_INPUT=$(echo "${TABLE_DESC}" | jq --arg name "${TARGET_TABLE}" '
    . + {TableName: $name, BillingMode: "PAY_PER_REQUEST"}
    | if .GlobalSecondaryIndexes then
        .GlobalSecondaryIndexes |= map(del(.ProvisionedThroughput, .IndexStatus, .IndexSizeBytes, .ItemCount, .IndexArn, .Backfilling, .WarmThroughput, .OnDemandThroughput))
      else . end
    | if .LocalSecondaryIndexes then
        .LocalSecondaryIndexes |= map(del(.IndexSizeBytes, .ItemCount, .IndexArn))
      else . end
    | del(..|nulls)
')

aws dynamodb create-table \
    --cli-input-json "${CREATE_INPUT}" \
    --region "${REGION}" > /dev/null

echo "Waiting for target table to become active..."
aws dynamodb wait table-exists --table-name "${TARGET_TABLE}" --region "${REGION}"
echo "Target table created"

# Phase 7: Restore FULL export
echo ""
echo "=== Phase 7: Restore FULL export ==="

# Find the manifest path
FULL_MANIFEST_DIR=$(aws s3 ls "s3://${S3_BUCKET}/${S3_PREFIX}/full/AWSDynamoDB/" --region "${REGION}" | grep -oE '[0-9]+-[a-f0-9]+/' | head -1)
FULL_MANIFEST_URI="s3://${S3_BUCKET}/${S3_PREFIX}/full/AWSDynamoDB/${FULL_MANIFEST_DIR}manifest-summary.json"

echo "Restoring from: ${FULL_MANIFEST_URI}"
"${BIN_DIR}/ddb-pitr" \
    -table "${TARGET_TABLE}" \
    -export "${FULL_MANIFEST_URI}" \
    -type FULL \
    -region "${REGION}"

# Phase 8: Apply INCREMENTAL export
echo ""
echo "=== Phase 8: Apply INCREMENTAL export ==="

# Find the incremental manifest path
INC_MANIFEST_DIR=$(aws s3 ls "s3://${S3_BUCKET}/${S3_PREFIX}/incremental/AWSDynamoDB/" --region "${REGION}" | grep -oE '[0-9]+-[a-f0-9]+/' | head -1)
INC_MANIFEST_URI="s3://${S3_BUCKET}/${S3_PREFIX}/incremental/AWSDynamoDB/${INC_MANIFEST_DIR}manifest-summary.json"

echo "Applying incremental from: ${INC_MANIFEST_URI}"
"${BIN_DIR}/ddb-pitr" \
    -table "${TARGET_TABLE}" \
    -export "${INC_MANIFEST_URI}" \
    -type INCREMENTAL \
    -view NEW_AND_OLD \
    -region "${REGION}"

# Phase 9: Verify data completeness
echo ""
echo "=== Phase 9: Verify data completeness ==="

# Count items in both tables
SOURCE_COUNT=$(aws dynamodb scan \
    --table-name "${SOURCE_TABLE}" \
    --select COUNT \
    --region "${REGION}" \
    --query 'Count' \
    --output text)

TARGET_COUNT=$(aws dynamodb scan \
    --table-name "${TARGET_TABLE}" \
    --select COUNT \
    --region "${REGION}" \
    --query 'Count' \
    --output text)

EXPECTED_COUNT=$((ITEM_COUNT - DELETE_COUNT))

echo "Expected items: ${EXPECTED_COUNT}"
echo "Source table items: ${SOURCE_COUNT}"
echo "Target table items: ${TARGET_COUNT}"

if [[ "${SOURCE_COUNT}" != "${EXPECTED_COUNT}" ]]; then
    echo "ERROR: Source table count mismatch! Expected ${EXPECTED_COUNT}, got ${SOURCE_COUNT}"
    exit 1
fi

if [[ "${TARGET_COUNT}" != "${EXPECTED_COUNT}" ]]; then
    echo "ERROR: Target table count mismatch! Expected ${EXPECTED_COUNT}, got ${TARGET_COUNT}"
    exit 1
fi

if [[ "${SOURCE_COUNT}" != "${TARGET_COUNT}" ]]; then
    echo "ERROR: Table counts don't match! Source=${SOURCE_COUNT}, Target=${TARGET_COUNT}"
    exit 1
fi

echo "Item counts match: ${SOURCE_COUNT}"

# Full item comparison
echo ""
echo "Performing full item comparison..."

aws dynamodb scan \
    --table-name "${SOURCE_TABLE}" \
    --region "${REGION}" \
    --output json | jq -S '.Items | sort_by(.PK.S, .SK.S)' > /tmp/source_items.json

aws dynamodb scan \
    --table-name "${TARGET_TABLE}" \
    --region "${REGION}" \
    --output json | jq -S '.Items | sort_by(.PK.S, .SK.S)' > /tmp/target_items.json

if diff -q /tmp/source_items.json /tmp/target_items.json > /dev/null; then
    echo ""
    echo "=== VERIFICATION PASSED ==="
    echo "All ${SOURCE_COUNT} items match between source and target tables"
    echo ""
    echo "Summary:"
    echo "  - Created ${ITEM_COUNT} items"
    echo "  - Updated ${UPDATE_COUNT} items"
    echo "  - Deleted ${DELETE_COUNT} items"
    echo "  - Final count: ${EXPECTED_COUNT} items"
    echo "  - FULL export and restore: SUCCESS"
    echo "  - INCREMENTAL export and restore: SUCCESS"
    echo "  - Data integrity: VERIFIED"
else
    echo ""
    echo "=== VERIFICATION FAILED ==="
    echo "Items differ between source and target tables:"
    diff /tmp/source_items.json /tmp/target_items.json | head -100
    exit 1
fi
