#!/bin/bash
# Shared helpers for Kafka scripts. Source this, don't execute it.

find_project_root() {
    local dir="$1"
    while [ "$dir" != "/" ]; do
        if [ -f "$dir/pyproject.toml" ]; then
            echo "$dir"
            return 0
        fi
        dir="$(dirname "$dir")"
    done
    echo "ERROR: could not find project root (no pyproject.toml)" >&2
    return 1
}

# Sets: PROJECT_ROOT, SCRIPT_DIR, KAFKA_DIR, KAFKA_CONFIG, KAFKA_DATA_DIR
setup_paths() {
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_ROOT="$(find_project_root "$SCRIPT_DIR")"
    KAFKA_DIR="$PROJECT_ROOT/kafka"
    KAFKA_CONFIG="$KAFKA_DIR/server.properties"
    KAFKA_TEMPLATE="$KAFKA_DIR/server.properties.template"
    KAFKA_DATA_DIR="$KAFKA_DIR/data"
    export PROJECT_ROOT SCRIPT_DIR KAFKA_DIR KAFKA_CONFIG KAFKA_TEMPLATE KAFKA_DATA_DIR

}

render_config() {
    mkdir -p "$KAFKA_DATA_DIR"
    sed "s|__KAFKA_DATA_DIR__|$KAFKA_DATA_DIR|g" \
        "$KAFKA_TEMPLATE" > "$KAFKA_CONFIG"
}