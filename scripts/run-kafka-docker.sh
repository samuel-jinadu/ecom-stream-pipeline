#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/kafka-common.sh"
setup_paths

render_config

CLUSTER_ID="$(cat "$KAFKA_DIR/cluster.id" 2>/dev/null || kafka-storage.sh random-uuid)"
echo "$CLUSTER_ID" > "$KAFKA_DIR/cluster.id"
kafka-storage.sh format -t "$CLUSTER_ID" -c "$KAFKA_CONFIG" --ignore-formatted

exec kafka-server-start.sh "$KAFKA_CONFIG"