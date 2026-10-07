#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/kafka-common.sh"
setup_paths

PID_FILE="$KAFKA_DIR/kafka.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "No PID file at $PID_FILE. Nothing to stop."
    exit 0
fi

PID="$(cat "$PID_FILE")"

if ! kill -0 "$PID" 2>/dev/null; then
    echo "Process $PID not running. Cleaning up PID file."
    rm -f "$PID_FILE"
    exit 0
fi

echo "Stopping Kafka (PID $PID)..."
kill -TERM "$PID"

# Wait up to 30s for graceful shutdown
for i in $(seq 1 30); do
    if ! kill -0 "$PID" 2>/dev/null; then
        rm -f "$PID_FILE"
        echo "Kafka stopped."
        exit 0
    fi
    sleep 1
done

echo "Kafka did not stop in 30s. Sending SIGKILL."
kill -KILL "$PID" 2>/dev/null || true
rm -f "$PID_FILE"
echo "Kafka killed."