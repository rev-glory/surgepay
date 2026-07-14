import { z } from 'zod';

export const sagaSchema = z.object({
  SAGA_SCAN_INTERVAL_MS: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(30000),
  ),
  SAGA_STEP_TIMEOUT_MS: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(60000),
  ),
  SAGA_RETRY_BASE_DELAY_MS: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(5000),
  ),
  SAGA_RETRY_MAX_DELAY_MS: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(30000),
  ),
  SAGA_MAX_RETRY_ATTEMPTS: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(3),
  ),
  SAGA_BATCH_SIZE: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(50),
  ),
  SAGA_HANDOFF_TIMEOUT_MS: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(300000),
  ),
});
