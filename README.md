# SurgePay — Distributed Payment Orchestration Platform

SurgePay is a production-inspired, event-driven distributed payment orchestration platform designed to demonstrate modern, high-reliability fintech architectures. It is built as a portfolio-grade learning resource to model how complex distributed workflows, transactional guarantees, and eventual consistency are coordinated in real-world payment networks.

> [!IMPORTANT]
> **SurgePay is a simulation.** It does not integrate with a real payment gateway. Instead, it focuses on solving the distributed consistency and coordination challenges that surround payment gateways—keeping order state, ledger entries, merchant balances, and notification states aligned across independent services that can fail, timeout, restart, or receive duplicate messages at any time.

---

## 1. Project Overview

SurgePay demonstrates how a highly resilient payment orchestration platform handles distributed state transitions. Rather than focusing on simple database updates, the platform targets the core architectural challenges of payment processing:
- **Consistency Without Distributed Transactions**: Guarantees eventual consistency and transaction integrity without relying on expensive and brittle 2PC/XA protocols.
- **Idempotent Executions**: Prevents double-spend, double-ledgering, or duplicate notifications at both the HTTP request and Kafka messaging layers.
- **Explicit Failure States**: Implements bounded recovery paths, structured retries, and dead-letter queueing to ensure that no message is ever silently lost.
- **Microservice Boundaries**: Enforces database per service, strictly preventing any database sharing or cross-schema querying.

---

## 2. Architecture Overview

At a high level, the platform consists of 16 independent, service-oriented modules coordinated through a mix of synchronous APIs and asynchronous event-driven flows:
- **API Gateway**: Single entry point handling TLS termination, rate-limiting, and request routing.
- **Idempotency Service**: Redis-backed cache to intercept duplicate HTTP requests at the gateway level.
- **Payment Service**: Owns the payment record and initiates processing transactions.
- **Merchant Service**: Manages merchant profiles, API credentials, fees, and configurations.
- **Order Service**: Manages the order aggregate and hosts the Saga Orchestrator for asynchronous workflows.
- **Ledger Service**: Immutable, double-entry financial record book.
- **Balance Service**: Projects and reserves merchant balances derived from ledger history.
- **Notification Service**: Coordinates external communication triggers.
- **Webhook Service**: Delivers secure, signature-validated payment events to merchants.
- **Fraud Service**: Performs synchronous pre-checks and deep asynchronous fraud scoring.
- **Risk Engine**: Manages dynamic transaction limits and reserve ratios.
- **Payment Processor Simulator**: Simulates third-party merchant bank and card network integrations.
- **Retry Scheduler**: Coordinates exponential backoff and jittered event retries.
- **Audit Service**: Records immutable operational actions for compliance.
- **Analytics Service**: Aggregates real-time transaction performance metrics.
- **Reconciliation Service**: Discovers financial anomalies between internal records and payment networks.

### Core Architecture Patterns
- **Transactional Outbox**: Guarantees atomic database updates and event publishing.
- **Saga Orchestrator**: Manages multi-step payment workflows and compensation logic.
- **Inbox Pattern (Deduplication)**: Enforces event-level idempotency inside consumers.
- **Redis-Backed Lock**: Manages gateway request-level idempotency.
- **OpenTelemetry & Prometheus/Grafana**: End-to-end trace propagation and critical business metrics tracking.

---

## 3. Tech Stack

- **Core & Runtime**: TypeScript, Node.js (>=20.0.0)
- **Application Framework**: NestJS (v10)
- **Database & ORM**: PostgreSQL, Prisma ORM
- **In-Memory Caching & Lock Manager**: Redis
- **Message Broker & Event Stream**: Redpanda (Kafka-compatible)
- **Workspace Tooling**: TurboRepo, pnpm (>=9.0.0)
- **Infrastructure**: Docker Compose
- **Observability**: OpenTelemetry, Prometheus, Grafana

---

## 4. Repository Layout

```
surgepay/
├── apps/                        # Core Application Services
│   ├── gateway/                 # API Gateway Service
│   ├── idempotency-service/     # Request Deduplication Service
│   ├── merchant-service/        # Merchant Configurations
│   ├── payment-service/         # Payment Aggregate & Outbox
│   ├── order-service/           # Order Aggregate & Saga Orchestrator
│   ├── ledger-service/          # Immutable Double-Entry Ledger
│   ├── balance-service/         # Merchant Balances & Reserves
│   ├── fraud-service/           # Sync Pre-check & Async Scoring
│   ├── risk-engine/             # Risk Analysis Engine
│   ├── payment-processor/       # Gateway Simulator
│   ├── webhook-service/         # Outbound Webhook Delivery
│   ├── notification-service/    # Email/SMS Merchant Notifications
│   ├── retry-scheduler/         # Outbox & Failed Event Retrier
│   ├── audit-service/           # Compliance Logging
│   ├── analytics-service/       # Operational Analytics
│   └── reconciliation-service/  # Discrepancy Auditing
├── packages/                    # Shared Monorepo Packages
│   ├── common/                  # Shared utilities, health checks, and standard logger
│   ├── config/                  # Shared environment and config schemes
│   ├── contracts/               # Shared HTTP/RPC DTOs and API specifications
│   ├── database/                # Shared Prisma client, schemas, migrations, and seeds
│   └── events/                  # Shared Kafka Event envelope and schemas
├── docker/                      # Docker files and container compose stack
├── infrastructure/              # Deployment orchestration (Helm, Terraform, etc.)
├── docs/                        # Project design documentation and specs
├── scripts/                     # Utility scripts for local dev, seeds, and migrations
├── diagrams/                    # System architecture Mermaid designs & PNGs
└── .github/                     # Issue, PR templates and CI/CD GitHub Action workflows
```

---

## 5. Getting Started

### Local Setup
Ensure you have **Node.js (>=20.0.0)**, **pnpm (>=9.0.0)**, and **Docker** installed.

#### 1. Clone the Repository
```bash
git clone <repository_url>
cd surgepay
```

#### 2. Install Dependencies
```bash
pnpm install
```

#### 3. Environment Configuration
Copy the development environment template to create your local environment file:
```bash
cp .env.example .env.development
```
Verify the default values inside `.env.development` are correct for your local setup.

#### 4. Start Infrastructure
Launch the local Docker containers:
```bash
cd docker
docker compose up -d
cd ..
```
Wait for all containers to report healthy. You can verify this by running `docker ps`.

#### 5. Verify Workspace Setup
You can verify the TypeScript compilation of all shared packages:
```bash
pnpm build
```

---

## 6. Docker Services

The SurgePay platform depends on the following dockerized services to support high-reliability orchestrations and monitoring:

* **PostgreSQL** (Port `5432`): Provides isolated databases and schemas for each microservice, enforcing strict data ownership boundaries.
* **Redis** (Port `6379`): Backs the Idempotency Service to intercept duplicate HTTP requests at the gateway level.
* **Redpanda** (Port `9092`/`29092`): Kafka-compatible message broker that handles all asynchronous microservice event streams.
* **Kafka UI** (Port `8080`): Web-based user interface to monitor Redpanda clusters, topics, consumers, and message envelopes.
* **Prometheus** (Port `9090`): Time-series database that pulls metrics emitted from the platform microservices.
* **Grafana** (Port `3000`): Visualization dashboard displaying real-time metrics, SLOs, and alert indicators.
* **OpenTelemetry Collector** (Port `4317`/`4318`): Core routing pipeline to ingest distributed traces and logs from active services and forward them to downstream storage backends.

---

## 7. Development Commands

Common workspace tasks can be run directly using Turborepo and pnpm scripts at the root level:

```bash
# Clean build artifacts, dist files, and node_modules recursively
pnpm clean

# Install workspace dependencies
pnpm install

# Build all shared packages and verify TypeScript compilation
pnpm build

# Run formatting checks on TypeScript files
pnpm format

# Run linter checks (ESLint flat configuration)
pnpm lint

# Run automated tests across all packages
pnpm test

# Start dockerized infrastructure in the background
cd docker && docker compose up -d

# Stop running dockerized infrastructure
cd docker && docker compose down
```

---

## 8. Development Workflow

- **Feature-Driven**: All work is mapped to Git feature branches. Direct commits to `main` are disabled.
- **Code Reviews**: Every pull request requires review and approval from at least one engineer.
- **Conventional Commits**: Commit messages must follow the Angular Conventional Commits style and will be linted prior to commit checks.

---

## 9. Branch Strategy

We follow a trunk-based development strategy centered directly around the `main` branch.

```
Create feature branch (feature/<name>)
        ↓
Make local commits (Conventional Commits style)
        ↓
Push feature branch to remote repository
        ↓
Open Pull Request to `main`
        ↓
Review approved & CI checks pass
        ↓
Merge into `main` (Squash & Merge)
        ↓
Delete feature branch
```

- **No Long-Lived `develop` Branch**: Code goes from `feature/*` directly into `main` after validation.

---

## 10. Conventional Commit Examples

We use standard Conventional Commits scopes to identify affected modules.

- `feat(payment): implement payment creation`
- `fix(order): prevent duplicate saga execution`
- `refactor(common): simplify logger`
- `docs(readme): update architecture`
- `test(payment): add integration tests`
- `chore(repo): update workspace tooling`

---

## 11. Troubleshooting

If you run into issues during workspace setup, use the following guidelines:

* **Docker Not Running**:
  - *Symptom*: Docker compose commands hang or error with `cannot connect to Docker daemon`.
  - *Solution*: Start the Docker Desktop client or systemd service (`sudo systemctl start docker`).
* **Port Conflicts (e.g. 5432 or 6379)**:
  - *Symptom*: Container fails to start because a port is already in use.
  - *Solution*: Identify the conflicting process (e.g. a local PostgreSQL or Redis server running natively) and stop it, or update host ports in `.env.development` and `docker-compose.yml`.
* **Missing Environment Variables**:
  - *Symptom*: Setup scripts throw configuration schema validation errors.
  - *Solution*: Ensure you copied `.env.example` to `.env.development` and that the file is present in the workspace root.
* **Failed Dependency Installation**:
  - *Symptom*: `pnpm install` fails due to lockfile or resolution conflicts.
  - *Solution*: Run `pnpm clean` to wipe build caches and try running `pnpm install` again.
* **pnpm Workspace Issues**:
  - *Symptom*: Packages cannot resolve imports from neighbor workspace libraries (e.g. `@surgepay/common`).
  - *Solution*: Run `pnpm build` at the root level to compile shared packages and generate declaration files (`dist/` folder outputs), which allows TypeScript path aliases to resolve correctly.
* **Container Startup Failures**:
  - *Symptom*: Individual containers (like Redpanda or Grafana) restart in loops.
  - *Solution*: Run `docker compose logs <container-name>` to view specific container logs. Ensure you have allocated sufficient resource limits (e.g., memory) to Docker Desktop.

---

## 12. First Contribution Guide

Welcome to the SurgePay development team! Follow this checklist to complete your first contribution:

1. **Clone the repository** to your local machine:
   `git clone <repository_url>`
2. **Install workspace dependencies**:
   `pnpm install`
3. **Start local infrastructure**:
   `cd docker && docker compose up -d && cd ..`
4. **Verify package compilation**:
   `pnpm build`
5. **Create a feature branch** from `main`:
   `git checkout -b feature/your-feature-name`
6. **Implement your changes** inside the appropriate directories (abiding by design doc and AGENT.md guidelines).
7. **Run local quality checks**:
   `pnpm lint && pnpm build && pnpm test`
8. **Format your code**:
   `pnpm format`
9. **Commit changes** following Conventional Commit conventions:
   `git commit -m "feat(service): describe your changes"`
10. **Push your branch** to the remote repository and **open a Pull Request** to `main` for review.

---

## 13. Future Roadmap

Our implementation plan proceeds incrementally in the following phases:
1. **Infrastructure**: Setup Docker Compose for databases, Redis, and Redpanda brokers.
2. **Shared Packages**: Core typescript configuration and events contracts.
3. **Database Layer**: Prisma schemas and Inbox/Outbox table definitions.
4. **Configuration & Logging**: Global environment bindings and OTel integration.
5. **Services Integration**: Sequentially deploy API Gateway, Merchant, Idempotency, Risk, Payment, Order, Ledger, Balance, Webhook, and Notification services.
6. **Saga Integration**: Connect Saga Orchestrator to all event listeners and handle compensatory paths.
7. **CI/CD & Observability**: Complete OTEL monitoring metrics and deployment pipeline testing.

---

## 14. Architecture Diagrams

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
