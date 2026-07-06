# Redis Caching & State Management

SurgePay uses Redis as a high-performance, low-latency key-value store to manage fast-moving operational state.

## Key Responsibilities

1. **Idempotency Service Caching**:
   - Stores client-side idempotency keys paired with their execution status (`in-flight` or `completed`) and response payloads.
   - Ensures that duplicate HTTP requests with the same idempotency key receive the exact same response without re-executing business logic.
2. **Distributed Locks**:
   - Short-lived distributed locks are used to prevent race conditions (e.g., concurrent duplicate requests hitting the gateway simultaneously).
3. **TTL Storage**:
   - Idempotency records are configured with a Time-To-Live (typically 24 hours) to limit storage growth.

## Configuration & Security

- For local development, Redis is protected using a password specified in `.env.docker` via the `REDIS_PASSWORD` variable.
- Persistence is enabled using Append Only File (AOF) storage, mapped to the `redis-data` Docker volume.
