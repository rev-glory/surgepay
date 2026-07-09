import { registerAs } from '@nestjs/config';

import type { OutboxConfig } from '../types';

export default registerAs('outbox', (): OutboxConfig => ({
  pollingInterval: process.env.OUTBOX_POLLING_INTERVAL
    ? parseInt(process.env.OUTBOX_POLLING_INTERVAL, 10)
    : 500,
  batchSize: process.env.OUTBOX_BATCH_SIZE
    ? parseInt(process.env.OUTBOX_BATCH_SIZE, 10)
    : 100,
  publishTimeout: process.env.OUTBOX_PUBLISH_TIMEOUT
    ? parseInt(process.env.OUTBOX_PUBLISH_TIMEOUT, 10)
    : 5000,
  retryLimit: process.env.OUTBOX_RETRY_LIMIT
    ? parseInt(process.env.OUTBOX_RETRY_LIMIT, 10)
    : 3,
}));
