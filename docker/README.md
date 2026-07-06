# SurgePay Shared Infrastructure Stack

This directory contains the Docker Compose stack providing all infrastructure dependencies required for local development.

## Infrastructure Overview & Services

The local stack includes the following containers:

- **PostgreSQL (`postgres`)**: Port `5432`. Shared database instance. Logical boundaries are enforced using service-specific schemas (`merchant`, `payment`, `order`, `ledger`, `balance`, `notification`, `webhook`, `audit`, `analytics`).
- **Redis (`redis`)**: Port `6379`. High-performance key-value store used for request-level idempotency caching, distributed locks, and TTL status tracking. Secured with password-based authentication.
- **Redpanda (`redpanda`)**: Port `9092` (Kafka API), `9644` (Admin API). Lightweight, ZooKeeper-free event broker compatible with Apache Kafka APIs.
- **Kafka UI (`kafka-ui`)**: Port `8080`. Interactive web interface to inspect Kafka topics, offsets, partitions, consumer groups, and messages.

## Managing the Stack

### Startup

To start the infrastructure stack, run:

```bash
pnpm docker:up
```

*Or manually:*

```bash
docker compose --env-file .env.docker up -d
```

On startup:
1. `postgres`, `redis`, and `redpanda` launch and perform their health checks.
2. A short-lived sidecar container (`redpanda-bootstrap`) waits for the Redpanda API to become healthy and then automatically creates the 13 required Kafka topics.
3. `kafka-ui` starts and connects to `redpanda:9092`.

### Checking Health Status

To verify that all services have successfully transitioned to a `healthy` state, run:

```bash
docker compose --env-file .env.docker ps
```

### Viewing Logs

To stream logs from all containers:

```bash
docker compose --env-file .env.docker logs -f
```

To view logs for a specific service (e.g. `redpanda`):

```bash
docker compose --env-file .env.docker logs -f redpanda
```

### Shutdown

To stop and remove containers (preserving data volume state):

```bash
docker compose --env-file .env.docker down
```

### Resetting Data (Wiping Volumes)

To stop the containers and wipe all persistent database records, cache data, and event logs:

```bash
docker compose --env-file .env.docker down -v
```
This command removes the named Docker volumes (`postgres-data`, `redis-data`, `redpanda-data`).
