import { registerAs } from '@nestjs/config';

import type { SagaConfig } from '../types';

export default registerAs('saga', (): SagaConfig => ({
  scanIntervalMs: process.env.SAGA_SCAN_INTERVAL_MS
    ? parseInt(process.env.SAGA_SCAN_INTERVAL_MS, 10)
    : 30000,
  stepTimeoutMs: process.env.SAGA_STEP_TIMEOUT_MS
    ? parseInt(process.env.SAGA_STEP_TIMEOUT_MS, 10)
    : 60000,
  retryBaseDelayMs: process.env.SAGA_RETRY_BASE_DELAY_MS
    ? parseInt(process.env.SAGA_RETRY_BASE_DELAY_MS, 10)
    : 5000,
  retryMaxDelayMs: process.env.SAGA_RETRY_MAX_DELAY_MS
    ? parseInt(process.env.SAGA_RETRY_MAX_DELAY_MS, 10)
    : 30000,
  maxRetryAttempts: process.env.SAGA_MAX_RETRY_ATTEMPTS
    ? parseInt(process.env.SAGA_MAX_RETRY_ATTEMPTS, 10)
    : 3,
  batchSize: process.env.SAGA_BATCH_SIZE
    ? parseInt(process.env.SAGA_BATCH_SIZE, 10)
    : 50,
  handoffTimeoutMs: process.env.SAGA_HANDOFF_TIMEOUT_MS
    ? parseInt(process.env.SAGA_HANDOFF_TIMEOUT_MS, 10)
    : 300000,
}));
