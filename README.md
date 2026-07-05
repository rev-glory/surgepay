# surgepay
Production-inspired event-driven payment orchestration platform built with Spring Boot, Kafka-compatible messaging, PostgreSQL, Redis, Docker, and OpenTelemetry to demonstrate reliable distributed payment processing.

---

## Architecture Diagrams

> **Source of truth:** The Mermaid source for each diagram lives in [`diagrams/code/`](diagrams/code/). The PNGs in [`diagrams/images/`](diagrams/images/) are generated from those `.mmd` files. If the two ever diverge, the Mermaid source takes precedence.

---

### System Context

SurgePay is a distributed payment orchestration platform that simulates the coordination problem that exists around a payment gateway in real-world fintech systems — keeping order state, ledger state, merchant balances, and merchant notifications consistent across independent services that can each fail, restart, or receive duplicate messages at any time. This diagram shows the platform's external boundaries: the clients and merchants that interact with it, and the supporting infrastructure it depends on.

![System Context](diagrams/images/01-system-context.png)

---

### Container Diagram

Each service in SurgePay owns a clearly defined business capability together with its own logical database; no service reads another service's database directly. Services communicate either synchronously through internal APIs when immediate data is required or asynchronously through Kafka events when eventual consistency is acceptable, allowing services to evolve, deploy, and scale independently while preserving clear ownership boundaries.

![Container Diagram](diagrams/images/02-container-diagram.png)

---

### Payment Request Sequence

The synchronous request path is the only part of the system in which the client waits for a response — it is intentionally kept short, deterministic, and independent of downstream processing. Expensive operations such as payment processing, ledger updates, balance reservation, and merchant notification are deliberately excluded from this path and are handled asynchronously after the client has already received a `202 Accepted` response.

![Payment Request Sequence](diagrams/images/03-payment-request-sequence.png)

---

### Payment Processing Saga

The Saga Pattern coordinates business operations that span multiple independently deployed services; instead of using distributed transactions or Two-Phase Commit (2PC), every participating service performs its own local database transaction and communicates progress through Kafka events. The saga begins only after the Payment Processor publishes a `PaymentCompleted` event and then executes a deterministic sequence — `RecordLedgerEntry` → `CheckPayoutEligibility` → `ReserveBalance` → `NotifyMerchant` — with the Order Service alone determining the workflow and issuing compensating commands when any step fails permanently.

![Payment Processing Saga](diagrams/images/04-payment-processing-saga.png)

---

### Order Saga State Machine

The Order Service owns the saga lifecycle, which answers a different question from the payment lifecycle: "Have all downstream systems reached a financially consistent state?" A typical saga progression is `LEDGER_PENDING` → `LEDGER_RECORDED` → `ELIGIBILITY_PENDING` → `BALANCE_PENDING` → `BALANCE_RESERVED` → `NOTIFICATION_PENDING` → `NOTIFIED` → `CLOSED`, and the orchestrator persists its execution state after every successful transition so that orchestration can safely resume after crashes, deployments, or service restarts.

![Order Saga State Machine](diagrams/images/05-order-saga-state-machine.png)

---

### Payment State Machine

The Payment Service owns the payment lifecycle, whose responsibility is to determine whether the customer's payment method was successfully processed. A typical payment progression is `INITIATED` → `PROCESSOR_SUBMITTED` → `COMPLETED` (or `FAILED`); once the Payment Processor publishes a `PaymentCompleted` event, the payment itself is considered successful and the client-facing workflow is finished, while the saga lifecycle continues asynchronously in the Order Service.

![Payment State Machine](diagrams/images/06-payment-state-machine.png)

---

### Payment Service Components

The Payment Service owns the payment lifecycle and payment records; its responsibilities include validating payment requests, performing the synchronous fraud pre-check, creating the payment record, and publishing the initial payment event through the Transactional Outbox Pattern. The service deliberately does not communicate directly with downstream services such as the Ledger Service, Balance Service, or Notification Service — those responsibilities belong to the saga orchestrated by the Order Service.

![Payment Service Components](diagrams/images/07-payment-service-components.png)

---

### Deployment Diagram

The initial version of SurgePay is designed for local development, demonstration, and architectural validation; all application services execute as independent containers within a single Docker Compose network, with supporting infrastructure — Redpanda, Redis, PostgreSQL, Prometheus, and Grafana — also running as containers on the same network. Although several services share the same PostgreSQL instance, each service owns its own logical schema and accesses only its own database objects, preserving the ownership boundaries defined in the design document while simplifying local deployment.

![Deployment Diagram](diagrams/images/08-deployment-diagram.png)

---

### Component Diagram

SurgePay's 17 application services are grouped into platform services (API Gateway, Idempotency, Merchant), core business services (Order, Payment, Fraud, Risk, Payment Processor), financial services (Ledger, Balance, Notification, Webhook), reliability infrastructure (Retry Scheduler, Dead Letter Queue), and operational services (Reconciliation, Audit, Analytics). This diagram shows the internal components of each service and the synchronous and asynchronous dependencies between them.

![Component Diagram](diagrams/images/09-component-diagram.png)

---

### Class Diagram

SurgePay enforces strict data ownership boundaries at the domain model level: each service's aggregate roots, entities, value objects, and repository interfaces are wholly contained within that service and are never shared across service boundaries. This diagram shows the key domain classes across services — including `Payment`, `Order`, `SagaInstance`, `LedgerEntry`, `MerchantBalance`, and their relationships — illustrating how the design document's ownership model is reflected in code structure.

![Class Diagram](diagrams/images/10-class-diagram.png)
