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
  OUTBOX_PUBLISH_TIMEOUT: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(5000),
  ),
  OUTBOX_RETRY_LIMIT: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().nonnegative().default(3),
  ),
});
