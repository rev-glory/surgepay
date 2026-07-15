#!/usr/bin/env bash

set -euo pipefail

# List of topics to be created
TOPICS=(
  # Payment Topics
  "payments.initiated"
  "payments.completed"
  "payments.failed"
  "payments.flagged"
  "payments.dlq"

  # Ledger Topics
  "ledger.commands"
  "ledger.events"

  # Risk Topics
  "risk.commands"
  "risk.events"

  # Balance Topics
  "balance.commands"
  "balance.events"

  # Notification Topics
  "notification.commands"
  "notification.events"

  # Order Topics
  "order.commands"
  "order.events"

  # Retry Scheduler Topics
  "retry.commands"
  "retry.events"
)

echo "Starting topic creation on Redpanda..."

for topic in "${TOPICS[@]}"; do
  echo "Creating topic: $topic"
  rpk topic create "$topic" --brokers redpanda:9092
done

echo "All topics successfully created."
