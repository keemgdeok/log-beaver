#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9093}"

if ! command -v kafka-topics >/dev/null 2>&1; then
  echo "kafka-topics command not found. Run inside Kafka container or install Kafka CLIs." >&2
  exit 1
fi

create_topic() {
  local topic="$1"
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$topic" --replication-factor 1 --partitions 3
}

create_topic "user_logs"
create_topic "user_logs_dlq"

echo "Topics created (if missing) on $BOOTSTRAP"
