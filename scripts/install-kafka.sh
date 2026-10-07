#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/kafka-common.sh"
setup_paths

: "${KAFKA_VERSION:=4.3.1}"
: "${SCALA_VERSION:=2.13}"
: "${ARCHIVE_NAME:=kafka_${SCALA_VERSION}-${KAFKA_VERSION}}"
TARBALL="${ARCHIVE_NAME}.tgz"
DOWNLOAD_URL="https://archive.apache.org/kafka/${KAFKA_VERSION}/${TARBALL}"
INSTALL_DIR="$KAFKA_DIR/${ARCHIVE_NAME}"

mkdir -p "$KAFKA_DIR"
cd "$KAFKA_DIR"

# ---- Download (skip if already there) ----
if [ ! -d "$INSTALL_DIR" ]; then
    if [ ! -f "$TARBALL" ]; then
        echo "Downloading $TARBALL ..."
        curl -fL --retry 3 -O "$DOWNLOAD_URL"
    fi
    echo "Extracting ..."
    tar -xzf "$TARBALL"
    rm -f "$TARBALL"
else
    echo "Kafka already extracted at $INSTALL_DIR"
fi

# ---- Make it available to the current script ----
export KAFKA_HOME="$INSTALL_DIR"
export PATH="$KAFKA_HOME/bin:$PATH"

echo "KAFKA_HOME=$KAFKA_HOME"
echo "Kafka version: $("$KAFKA_HOME/bin/kafka-topics.sh" --version 2>/dev/null || echo 'unknown')"

