#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_PATH="${CONFIG_PATH:-kafka/connectors/clickhouse-sink.json}"

if [ ! -f "$CONFIG_PATH" ]; then
  echo "Connector config not found at $CONFIG_PATH" >&2
  exit 1
fi

curl -s -o /dev/null -w "%{http_code}\n" -X POST \
  -H "Content-Type: application/json" \
  --data @"$CONFIG_PATH" \
  "$CONNECT_URL/connectors" | grep -E "^(200|201)$" >/dev/null || {
  echo "Connector registration may have failed. Check Connect logs." >&2
  exit 1
}

echo "Connector registered from $CONFIG_PATH to $CONNECT_URL"
