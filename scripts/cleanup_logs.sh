#!/usr/bin/env bash
set -euo pipefail

# ClickHouse cleanup for demo/test data.
# Env vars:
#   CLICKHOUSE_URL (default: http://localhost:8123)
#   CLICKHOUSE_DB (default: log_beaver)
#   CLICKHOUSE_TABLE (default: raw_user_logs)
#   CLICKHOUSE_USER (default: log_beaver)
#   CLICKHOUSE_PASSWORD (default: log_beaver)

CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-log_beaver}"
CLICKHOUSE_TABLE="${CLICKHOUSE_TABLE:-raw_user_logs}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-log_beaver}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-log_beaver}"

echo "Truncating ${CLICKHOUSE_DB}.${CLICKHOUSE_TABLE} on ${CLICKHOUSE_URL}..."
curl -s -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
  --data-binary "TRUNCATE TABLE ${CLICKHOUSE_DB}.${CLICKHOUSE_TABLE}" \
  "${CLICKHOUSE_URL}/?max_execution_time=60" \
  || { echo "ClickHouse truncate failed"; exit 1; }

echo "Done."
