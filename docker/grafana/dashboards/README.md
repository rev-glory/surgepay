# Future Grafana Dashboards

This directory is designated for provisioning JSON files for Grafana dashboards.

In future commits, we will add dashboards visualising both application performance and financial state metrics:

1. **Payment Success Rate**: Real-time ratio of completed to failed/rejected payments.
2. **Saga Progress**: Breakdown of Saga executions by state (e.g., Ledger Pending, Balance Reserved) and active sagas duration tracking.
3. **Retry Scheduler**: Performance metrics of the retry queue, processing rate, and backoff queue depth.
4. **Kafka Throughput**: Message ingestion rate and consumer offsets/lag per topic.
5. **DLQ Messages**: Visualisation of permanently failed events in the Dead Letter Queue awaiting operator review.
6. **Merchant Activity**: API request rate, error rate, and volume of transactions split by active merchants.
7. **API Latency**: End-to-end trace latencies and microservice-to-microservice REST/gRPC response times.
8. **Outbox Lag**: Measure of delay between database transaction commit and Kafka event publication.
9. **Risk Decisions**: Audit rate of pre-checks, deep risk analysis scores, and block ratios.
