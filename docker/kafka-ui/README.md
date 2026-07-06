# Kafka UI (Topic Inspection & Debugging)

SurgePay integrates **Provectus Kafka UI** as a local development dashboard to monitor asynchronous event streams.

## Features & Usage

Developers can open Kafka UI at [http://localhost:8080](http://localhost:8080) to perform the following debugging tasks:

1. **Inspect Topics**: Verify that all bootstrapped topics exist and are healthy.
2. **Inspect Message Streams**: View JSON event envelopes moving through topics in real-time. This helps debug causation IDs, correlation IDs, and transaction payloads.
3. **Inspect Consumer Offsets**: Check consumer group states (e.g., Ledger Service, Balance Service) to trace which events have been successfully consumed or if there is consumer lag.
4. **Inspect Partitions**: Inspect broker partition counts and replication settings.

No manual configuration is required; Kafka UI connects to Redpanda automatically on startup.
