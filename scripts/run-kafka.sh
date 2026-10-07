#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/kafka-common.sh"
setup_paths

render_config


CLUSTER_ID="$(cat "$KAFKA_DIR/cluster.id" 2>/dev/null || kafka-storage.sh random-uuid)"
echo "$CLUSTER_ID" > "$KAFKA_DIR/cluster.id"
kafka-storage.sh format -t "$CLUSTER_ID" -c "$KAFKA_CONFIG" --ignore-formatted

PID_FILE="$KAFKA_DIR/kafka.pid"

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "Kafka already running (PID $(cat "$PID_FILE"))."
    exit 0
fi

LOG_FILE="$KAFKA_DIR/kafka.log"
echo "Starting Kafka..."
# export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:/path/to/your/custom-log4j2.yaml" # this line is in case i change my mind about a custom log options file
kafka-server-start.sh "$KAFKA_CONFIG" > "$LOG_FILE" 2>&1 &

PID=$!
echo "$PID" > "$PID_FILE"

# Wait a moment and verify it's actually alive
sleep 2
if ! kill -0 "$PID" 2>/dev/null; then
    echo "ERROR: Kafka died during startup. Last 20 lines of $LOG_FILE:" >&2
    tail -n 20 "$LOG_FILE" >&2
    rm -f "$PID_FILE"
    exit 1
fi

echo "Kafka started (PID $PID). Logs: $LOG_FILE"

# kafka-topics.sh --create \
#   --topic orders \
#   --partitions 1 \
#   --replication-factor 1 \
#   --bootstrap-server localhost:9092 \
#   --if-not-exists

tail -f "$LOG_FILE"