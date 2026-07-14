import { z } from 'zod';

export const outboxSchema = z.object({
  OUTBOX_POLLING_INTERVAL: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(500),
  ),
  OUTBOX_BATCH_SIZE: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(100),
  ),
  OUTBOX_RETRY_LIMIT: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(3),
  ),
  OUTBOX_PUBLISH_TIMEOUT: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(5000),
  ),
  OUTBOX_RETENTION_DAYS: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(7),
  ),
  OUTBOX_STALE_TIMEOUT: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(300000),
  ),
  OUTBOX_MAX_IN_FLIGHT_MESSAGES: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(1000),
  ),
  OUTBOX_FLUSH_INTERVAL: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(100),
  ),
});
