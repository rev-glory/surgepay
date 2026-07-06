#!/usr/bin/env bash

set -euo pipefail

BROKER="redpanda:9092"
MAX_ATTEMPTS=30
ATTEMPT=1

echo "Waiting for Redpanda to become available at $BROKER..."

until rpk cluster info --brokers "$BROKER" >/dev/null 2>&1; do
  if [ "$ATTEMPT" -ge "$MAX_ATTEMPTS" ]; then
    echo "Redpanda did not become available in time. Exiting."
    exit 1
  fi
  echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: Redpanda is not ready yet. Retrying in 2 seconds..."
  sleep 2
  ATTEMPT=$((ATTEMPT + 1))
done

echo "Redpanda is ready! Executing topic creation..."
/opt/redpanda/create-topics.sh
