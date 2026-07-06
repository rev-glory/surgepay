# Redpanda Event Broker & Kafka Topics

SurgePay uses **Redpanda** as its core asynchronous event broker, serving as the Kafka-compatible message backbone for the platform's Saga Orchestrator and service communication.

## Why Redpanda?

1. **Kafka API Compatibility**: Redpanda implements the Kafka protocol directly. SurgePay microservices connect using standard Kafka libraries (such as `@nestjs/microservices` or `kafkajs`) without code modifications.
2. **No ZooKeeper**: Redpanda uses Raft consensus natively. It doesn't require a separate ZooKeeper or KRaft metadata coordination cluster, reducing setup complexity.
3. **Fast Startup & Low Resource Usage**: Written in C++, Redpanda boots instantly (typically < 1s) and uses significantly less memory than Apache Kafka / JVM, making it ideal for local microservice development.

## Topic Initialization

To automate developer onboarding, topics are created automatically during docker compose startup via a bootstrap sidecar. The sidecar waits until the broker is ready and executes `rpk topic create` for the following topics:

### Payments Lifecycle
- `payments.initiated`: Triggered when a client request passes gateway and fraud checks.
- `payments.completed`: Published by the simulator when external settlement succeeds.
- `payments.failed`: Published if payment is declined.
- `payments.flagged`: Published by asynchronous deep fraud checks.
- `payments.dlq`: Dead Letter Queue for retries exhaustion.

### Downstream Sagas & Commands
- `ledger.commands` / `ledger.events`: Commands to write/reverse ledger entries and their status.
- `risk.commands` / `risk.events`: Payout eligibility evaluations.
- `balance.commands` / `balance.events`: Balance reservation and compensation actions.
- `notification.commands` / `notification.events`: Merchant status updates.

## Service Integration

Every microservice connects to Redpanda inside Docker via the DNS alias `redpanda:9092`.
