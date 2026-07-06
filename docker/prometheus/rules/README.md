# Future Alert Rules

This directory will store Prometheus alerting rule configurations (*.yml).

The future rules will cover the following scenarios:

1. **Service Availability**: Detect if any microservice container is down or unreachable.
2. **High Latency**: Trigger alerts when response times exceed defined SLAs (e.g., synchronous pre-check exceeding 50ms).
3. **Retry Spikes**: Alert on a sudden rise in transaction retries, indicating downstream gateway or internal processing bottlenecks.
4. **DLQ Growth**: Flag when the depth of the Kafka Dead Letter Queue (DLQ) increases, indicating permanently failed messages.
5. **Saga Failures**: Identify when sagas are aborted, terminated, or trigger compensatory flows.
