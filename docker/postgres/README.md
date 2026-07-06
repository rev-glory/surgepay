# PostgreSQL Shared Database Instance

SurgePay uses a single PostgreSQL database instance for local development to minimize resource usage, but enforces strict **logical database ownership boundaries** at the schema level.

## Logical Database Boundaries

Each service in SurgePay owns its database objects completely. Services do NOT share tables, read another service's tables directly, or run cross-service JOINs. Instead, cross-service interactions occur asynchronously via Redpanda (Kafka events) or synchronously via HTTP API calls.

For local development, logical boundaries are mapped to distinct database schemas within the shared PostgreSQL instance:

- **Merchant Service**: `merchant` schema
- **Payment Service**: `payment` schema
- **Order Service**: `order` schema
- **Ledger Service**: `ledger` schema
- **Balance Service**: `balance` schema
- **Notification Service**: `notification` schema
- **Webhook Service**: `webhook` schema
- **Audit Service**: `audit` schema
- **Analytics Service**: `analytics` schema

## Initialization Scripts

Any SQL scripts placed in the `docker/postgres/init/` directory will be executed in alphabetical order when the container is started for the first time.
In subsequent commits, schema generation and migrations will be managed programmatically by Prisma ORM within each service.
